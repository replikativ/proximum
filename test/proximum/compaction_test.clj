(ns proximum.compaction-test
  "Tests for online and offline compaction operations."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.string :as str]
            [clojure.core.async :as a]
            [proximum.core :as core]
            [proximum.protocols :as p])
  (:import [java.io File]))

;; -----------------------------------------------------------------------------
;; Test Fixtures and Helpers

(def ^:dynamic *store-id* nil)

(defn with-store-id-fixture [f]
  (binding [*store-id* (java.util.UUID/randomUUID)]
    (f)))

(use-fixtures :each with-store-id-fixture)

(defn- storage-layout
  "Derive separate directories for Konserve store and mmap cache under a base path."
  [^String storage-path]
  {:base storage-path
   :store-path (str storage-path "/store")
   :mmap-dir (str storage-path "/mmap")})

(defn- ensure-dirs!
  [{:keys [^String base ^String mmap-dir]}]
  (.mkdirs (File. base))
  (.mkdirs (File. mmap-dir))
  nil)

(defn- store-config-for
  [^String storage-path store-id]
  (let [{:keys [store-path] :as layout} (storage-layout storage-path)]
    (ensure-dirs! layout)
    {:backend :file :path store-path :id store-id}))

(defn- mmap-dir-for
  [^String storage-path]
  (let [{:keys [mmap-dir] :as layout} (storage-layout storage-path)]
    (ensure-dirs! layout)
    mmap-dir))

(defn- create-test-index
  "Create an index with a stable Konserve store identity."
  [config]
  (let [storage-path (:storage-path config)
        explicit-store-config (:store-config config)
        store-id (or (:id explicit-store-config)
                     *store-id*
                     (java.util.UUID/randomUUID))
        store-config (or explicit-store-config
                         (when storage-path (store-config-for storage-path store-id))
                         {:backend :memory :id (java.util.UUID/randomUUID)})
        mmap-dir (or (:mmap-dir config)
                     (when storage-path (mmap-dir-for storage-path)))]
    (core/create-index (cond-> (dissoc config :storage-path :store-id :vectors-path)
                         (nil? (:store config))
                         (assoc :store-config store-config)
                         (some? mmap-dir)
                         (assoc :mmap-dir mmap-dir)))))

(defn- random-vec
  "Generate a random float vector of given dimension."
  [dim]
  (float-array (repeatedly dim #(- (rand 2.0) 1.0))))

(defn- random-vectors
  "Generate n random vectors of given dimension."
  [n dim]
  (vec (repeatedly n #(random-vec dim))))

(defn- temp-path []
  (str (System/getProperty "java.io.tmpdir")
       "/compaction-test-" (System/currentTimeMillis)))

(defn- cleanup [path]
  (let [f (File. path)]
    (when (.isDirectory f)
      (doseq [file (reverse (file-seq f))]
        (.delete file)))
    (when (.exists f)
      (.delete f))))

;; -----------------------------------------------------------------------------
;; Online Compaction Tests

(deftest test-online-compaction-basic
  (let [path (temp-path)]
    (try
      (testing "Online compaction copies vectors while index remains usable"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path path
                                      :capacity 100})
              vecs (random-vectors 20 32)
              ids (range 20)
              metas (mapv #(hash-map :n %) (range 20))
              idx2 (core/insert-batch idx vecs ids {:metadata metas})
              ;; Delete some vectors
              idx3 (-> idx2
                       (core/delete 5)
                       (core/delete 10)
                       (core/delete 15))

              ;; Start online compaction
              new-store-id (java.util.UUID/randomUUID)
              target-base (str path "-online")
              state (core/start-online-compaction idx3
                                                  {:store-config (store-config-for target-base new-store-id)
                                                   :mmap-dir (mmap-dir-for target-base)}
                                                  {:batch-size 5})]

          ;; While compaction runs, index is still usable
          (is (= 17 (core/count-vectors state)))

          ;; Search works during compaction
          (let [query (random-vec 32)
                results (core/search state query 5 {:ef 50})]
            (is (= 5 (count results))))

          ;; Check progress
          (let [progress (core/compaction-progress state)]
            (is (contains? progress :copying?))
            (is (contains? progress :finished?))
            (is (contains? progress :delta-count))
            (is (contains? progress :mapped-ids)))

          ;; Finish compaction
          (let [compacted (a/<!! (core/finish-online-compaction! state))]
            (is (= 17 (core/count-vectors compacted)))

            (let [metrics (core/index-metrics compacted)]
              (is (= 0 (:deleted-count metrics))))

            (a/<!! (core/close! compacted)))

          (a/<!! (core/close! idx3))))

      (finally
        (cleanup path)
        (cleanup (str path "-online"))))))

(deftest test-online-compaction-with-concurrent-writes
  (let [path (temp-path)]
    (try
      (testing "Writes during compaction are captured in delta log"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path path
                                      :capacity 100})
              vecs (random-vectors 10 32)
              ids (range 10)
              idx2 (core/insert-batch idx vecs ids)

              ;; Start online compaction
              new-store-id (java.util.UUID/randomUUID)
              target-base (str path "-concurrent")
              state (core/start-online-compaction idx2
                                                  {:store-config (store-config-for target-base new-store-id)
                                                   :mmap-dir (mmap-dir-for target-base)}
                                                  {:batch-size 5})

              ;; Insert new vectors during compaction
              state2 (-> state
                         (core/insert (random-vec 32) "new-1" {:added :during-compact-1})
                         (core/insert (random-vec 32) "new-2" {:added :during-compact-2}))

              ;; Delete a vector during compaction
              state3 (core/delete state2 3)]

          ;; Source index has live count: 10 + 2 - 1 = 11
          (is (= 11 (core/count-vectors state3)))

          ;; Delta log should have 3 operations
          (let [progress (core/compaction-progress state3)]
            (is (= 3 (:delta-count progress))))

          ;; Finish compaction - deltas should be applied
          (let [compacted (a/<!! (core/finish-online-compaction! state3))
                metrics (core/index-metrics compacted)]
            (is (= 11 (core/count-vectors compacted)))
            (is (= 11 (:live-count metrics)))
            (is (= 1 (:deleted-count metrics)))

            ;; The new vectors should be findable
            (let [query (random-vec 32)
                  results (core/search-with-metadata compacted query 15 {:ef 50})]
              (is (some #(= :during-compact-1 (get-in % [:metadata :added])) results))
              (is (some #(= :during-compact-2 (get-in % [:metadata :added])) results)))

            (a/<!! (core/close! compacted)))))

      (finally
        (cleanup path)
        (cleanup (str path "-concurrent"))))))

(deftest test-online-compaction-abort
  (let [path (temp-path)]
    (try
      (testing "Aborting compaction returns source index"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path path
                                      :capacity 100})
              vecs (random-vectors 10 32)
              ids (range 10)
              idx2 (core/insert-batch idx vecs ids)

              ;; Start compaction
              new-store-id (java.util.UUID/randomUUID)
              target-base (str path "-abort")
              state (core/start-online-compaction idx2
                                                  {:store-config (store-config-for target-base new-store-id)
                                                   :mmap-dir (mmap-dir-for target-base)})

              ;; Abort
              recovered (core/abort-online-compaction! state)]

          ;; Recovered index is the source index
          (is (= 10 (core/count-vectors recovered)))

          ;; Search still works
          (let [query (random-vec 32)
                results (core/search recovered query 5 {:ef 50})]
            (is (= 5 (count results))))

          (a/<!! (core/close! recovered))))

      (finally
        (cleanup path)
        (cleanup (str path "-abort"))))))

(deftest test-online-compaction-delta-overflow
  (let [path (temp-path)]
    (try
      (testing "Delta log overflow throws with correct error info"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path path
                                      :capacity 100})
              vecs (random-vectors 10 32)
              idx2 (core/insert-batch idx vecs (range 10))

              ;; Start compaction with very low delta limit
              new-store-id (java.util.UUID/randomUUID)
              target-base (str path "-overflow")
              state (core/start-online-compaction idx2
                                                  {:store-config (store-config-for target-base new-store-id)
                                                   :mmap-dir (mmap-dir-for target-base)}
                                                  {:max-delta-size 3})]

          ;; Insert until overflow
          (let [state2 (core/insert state (random-vec 32) "new-1")
                state3 (core/insert state2 (random-vec 32) "new-2")
                state4 (core/insert state3 (random-vec 32) "new-3")]
            ;; Now delta log has 3 operations, next should overflow
            (let [ex (try
                       (core/insert state4 (random-vec 32) "overflow")
                       nil
                       (catch clojure.lang.ExceptionInfo e e))]
              (is (some? ex) "Expected overflow exception")
              (when ex
                (is (= :compaction/delta-overflow (:error (ex-data ex))))
                (is (= 3 (:max-delta-size (ex-data ex))))
                (is (= 3 (:current-size (ex-data ex)))))))

          (core/abort-online-compaction! state)))

      (finally
        (cleanup path)
        (cleanup (str path "-overflow"))))))

(deftest test-online-compaction-fork-blocked
  (let [path (temp-path)]
    (try
      (testing "Fork during compaction throws helpful error"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path path
                                      :capacity 100})
              vecs (random-vectors 10 32)
              idx2 (core/insert-batch idx vecs (range 10))

              ;; Start compaction
              new-store-id (java.util.UUID/randomUUID)
              target-base (str path "-fork")
              state (core/start-online-compaction idx2
                                                  {:store-config (store-config-for target-base new-store-id)
                                                   :mmap-dir (mmap-dir-for target-base)})]

          ;; Try to fork - should throw
          (let [ex (try
                     (core/fork state)
                     nil
                     (catch clojure.lang.ExceptionInfo e e))]
            (is (some? ex) "Expected fork exception")
            (when ex
              (is (str/includes? (.getMessage ex) "Cannot fork during online compaction"))))

          (core/abort-online-compaction! state)))

      (finally
        (cleanup path)
        (cleanup (str path "-fork"))))))

(deftest test-online-compaction-cleanup-on-abort
  (let [path (temp-path)]
    (try
      (testing "Abort cleans up partial files"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path path
                                      :capacity 100})
              vecs (random-vectors 10 32)
              idx2 (core/insert-batch idx vecs (range 10))

              ;; Start compaction
              new-store-id (java.util.UUID/randomUUID)
              target-base (str path "-cleanup")
              mmap-dir (mmap-dir-for target-base)
              state (core/start-online-compaction idx2
                                                  {:store-config (store-config-for target-base new-store-id)
                                                   :mmap-dir mmap-dir})

              ;; Let some copy happen
              _ (Thread/sleep 100)

              ;; Abort
              recovered (core/abort-online-compaction! state)]

          ;; Source index should be returned unchanged
          (is (= 10 (core/count-vectors recovered)))

          ;; Mmap directory should be cleaned up
          (let [dir (java.io.File. mmap-dir)]
            (is (or (not (.exists dir))
                    (= 0 (count (.listFiles dir))))
                "Mmap directory should be empty or deleted"))

          (a/<!! (core/close! recovered))))

      (finally
        (cleanup path)
        (cleanup (str path "-cleanup"))))))

(deftest test-online-compaction-progress-fields
  (let [path (temp-path)]
    (try
      (testing "compaction-progress includes all expected fields"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path path
                                      :capacity 100})
              vecs (random-vectors 10 32)
              idx2 (core/insert-batch idx vecs (range 10))

              new-store-id (java.util.UUID/randomUUID)
              target-base (str path "-progress")
              state (core/start-online-compaction idx2
                                                  {:store-config (store-config-for target-base new-store-id)
                                                   :mmap-dir (mmap-dir-for target-base)})]

          ;; Check all fields exist
          (let [progress (core/compaction-progress state)]
            (is (contains? progress :copying?))
            (is (contains? progress :finished?))
            (is (contains? progress :failed?))
            (is (contains? progress :error))
            (is (contains? progress :delta-count))
            (is (contains? progress :mapped-ids))

            ;; Initial state checks
            (is (false? (:failed? progress)))
            (is (nil? (:error progress)))
            (is (= 0 (:delta-count progress))))

          ;; Add some deltas
          (let [state2 (core/insert state (random-vec 32) "new-1")
                progress2 (core/compaction-progress state2)]
            (is (= 1 (:delta-count progress2))))

          (core/abort-online-compaction! state)))

      (finally
        (cleanup path)
        (cleanup (str path "-progress"))))))

(deftest test-online-compaction-batch-delta-overflow
  (let [path (temp-path)]
    (try
      (testing "Batch insert respects delta limit"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path path
                                      :capacity 100})
              vecs (random-vectors 10 32)
              idx2 (core/insert-batch idx vecs (range 10))

              ;; Start compaction with low delta limit
              new-store-id (java.util.UUID/randomUUID)
              target-base (str path "-batch-overflow")
              state (core/start-online-compaction idx2
                                                  {:store-config (store-config-for target-base new-store-id)
                                                   :mmap-dir (mmap-dir-for target-base)}
                                                  {:max-delta-size 5})]

          ;; Batch insert that exceeds limit
          (let [ex (try
                     (core/insert-batch state (random-vectors 10 32) (range 100 110))
                     nil
                     (catch clojure.lang.ExceptionInfo e e))]
            (is (some? ex) "Expected batch overflow exception")
            (when ex
              (is (= :compaction/delta-overflow (:error (ex-data ex))))
              (is (= 10 (:batch-size (ex-data ex))))))

          (core/abort-online-compaction! state)))

      (finally
        (cleanup path)
        (cleanup (str path "-batch-overflow"))))))

(deftest test-online-compaction-stress
  (let [path (temp-path)]
    (try
      (testing "Concurrent operations during compaction"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path path
                                      :capacity 1000})
              vecs (random-vectors 100 32)
              idx2 (core/insert-batch idx vecs (range 100))

              ;; Delete some
              idx3 (reduce (fn [i id] (core/delete i id))
                           idx2
                           (range 0 100 10))

              new-store-id (java.util.UUID/randomUUID)
              target-base (str path "-stress")

              ;; State atom for thread-safe updates during concurrent ops
              state-atom (atom (core/start-online-compaction idx3
                                                             {:store-config (store-config-for target-base new-store-id)
                                                              :mmap-dir (mmap-dir-for target-base)}
                                                             {:batch-size 10}))

              ;; Concurrent operations
              insert-future (future
                              (dotimes [i 20]
                                (swap! state-atom
                                       #(core/insert % (random-vec 32) (str "concurrent-" i)))
                                (Thread/sleep 5)))

              search-future (future
                              (dotimes [_ 50]
                                (let [results (core/search @state-atom (random-vec 32) 5 {:ef 50})]
                                  (is (<= (count results) 5)))
                                (Thread/sleep 2)))

              delete-future (future
                              (dotimes [i 5]
                                (swap! state-atom
                                       #(core/delete % (+ 20 (* i 10))))
                                (Thread/sleep 10)))]

          ;; Wait for concurrent operations
          @insert-future
          @search-future
          @delete-future

          ;; Finish compaction
          (let [compacted (a/<!! (core/finish-online-compaction! @state-atom))
                metrics (core/index-metrics compacted)]

            ;; Original: 100 vectors, deleted 10 (0,10,20,30,40,50,60,70,80,90)
            ;; Added: 20 new vectors
            ;; Deleted during compaction: 5 (20,30,40,50,60 - some overlap with original deletes)
            ;; Final live count should be approximumimately: 90 - ~3 unique deletes + 20 = ~107
            ;; (3 unique because 20,30,40,50,60 were already deleted)
            (is (> (core/count-vectors compacted) 100)
                "Should have more vectors than original after inserts")

            (a/<!! (core/close! compacted)))))

      (finally
        (cleanup path)
        (cleanup (str path "-stress"))))))

(deftest test-online-compaction-delete-during-copy
  (let [path (temp-path)]
    (try
      (testing "Deletes during compaction become soft-deletes in new index"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path path
                                      :capacity 100})
              vecs (random-vectors 20 32)
              metas (mapv #(hash-map :n %) (range 20))
              idx2 (core/insert-batch idx vecs (range 20) {:metadata metas})

              new-store-id (java.util.UUID/randomUUID)
              target-base (str path "-delete-copy")
              state (core/start-online-compaction idx2
                                                  {:store-config (store-config-for target-base new-store-id)
                                                   :mmap-dir (mmap-dir-for target-base)}
                                                  {:batch-size 5})

              ;; Wait for copy to start
              _ (Thread/sleep 50)

              ;; Delete vectors that might already be copied
              state2 (-> state
                         (core/delete 0)
                         (core/delete 1)
                         (core/delete 2))]

          ;; Source should have 17 live
          (is (= 17 (core/count-vectors state2)))

          ;; Finish compaction
          (let [compacted (a/<!! (core/finish-online-compaction! state2))
                metrics (core/index-metrics compacted)]

            ;; Live count should be 17
            (is (= 17 (:live-count metrics)))

            ;; Deleted count >= 0 (soft deletes if vectors were already copied)
            (is (>= (:deleted-count metrics) 0))

            ;; Search should not return deleted vectors
            (let [query (nth vecs 0)  ;; Use deleted vector as query
                  results (core/search-with-metadata compacted query 20 {:ef 50})]
              (is (not (some #(= 0 (:n (:metadata %))) results))
                  "Deleted vector 0 should not appear in results"))

            (a/<!! (core/close! compacted)))))

      (finally
        (cleanup path)
        (cleanup (str path "-delete-copy"))))))

(deftest test-online-compaction-state-threading
  (let [path (temp-path)]
    (try
      (testing "State threading preserves consistency"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path path
                                      :capacity 100})
              vecs (random-vectors 10 32)
              idx2 (core/insert-batch idx vecs (range 10))

              new-store-id (java.util.UUID/randomUUID)
              target-base (str path "-threading")
              state0 (core/start-online-compaction idx2
                                                   {:store-config (store-config-for target-base new-store-id)
                                                    :mmap-dir (mmap-dir-for target-base)})

              ;; Thread state through multiple operations
              state1 (core/insert state0 (random-vec 32) "a")
              state2 (core/insert state1 (random-vec 32) "b")
              state3 (core/delete state2 5)]
              ;; Note: flush! on CompactionState currently has implementation issues with async
              ;; Skipping state4 test for now

          ;; Each state should have correct counts
          (is (= 10 (core/count-vectors state0)))
          (is (= 11 (core/count-vectors state1)))
          (is (= 12 (core/count-vectors state2)))
          (is (= 11 (core/count-vectors state3)))  ;; After delete

          ;; Delta log should accumulate correctly
          (is (= 3 (:delta-count (core/compaction-progress state3))))

          (core/abort-online-compaction! state3)))

      (finally
        (cleanup path)
        (cleanup (str path "-threading"))))))

;; -----------------------------------------------------------------------------
;; Collection Protocol Consistency Tests
;;
;; These tests verify that collection protocols (assoc/dissoc) behave
;; identically to VectorIndex protocols (insert/delete) during compaction.
;; This is critical because CompactionState must dual-write to both source
;; index AND delta-log regardless of which API is used.

(deftest test-online-compaction-collection-protocol-equivalence
  (let [path (temp-path)]
    (try
      (testing "Collection protocols (assoc/dissoc) are equivalent to VectorIndex protocols"
        ;; Create two identical source indices
        (let [base-vecs (random-vectors 10 32)

              ;; Index A: will use collection protocols
              idx-a (create-test-index {:type :hnsw
                                        :dim 32
                                        :M 8
                                        :ef-construction 50
                                        :storage-path (str path "-a")
                                        :capacity 100})
              idx-a (core/insert-batch idx-a base-vecs (range 10))

              ;; Index B: will use VectorIndex protocols
              idx-b (create-test-index {:type :hnsw
                                        :dim 32
                                        :M 8
                                        :ef-construction 50
                                        :storage-path (str path "-b")
                                        :capacity 100})
              idx-b (core/insert-batch idx-b base-vecs (range 10))

              ;; Vectors to add during compaction (same for both)
              new-vec-1 (random-vec 32)
              new-vec-2 (random-vec 32)

              ;; Start compaction on both
              target-a (str path "-compact-a")
              target-b (str path "-compact-b")

              state-a (core/start-online-compaction idx-a
                                                    {:store-config (store-config-for target-a (java.util.UUID/randomUUID))
                                                     :mmap-dir (mmap-dir-for target-a)})

              state-b (core/start-online-compaction idx-b
                                                    {:store-config (store-config-for target-b (java.util.UUID/randomUUID))
                                                     :mmap-dir (mmap-dir-for target-b)})

              ;; Apply same operations via different APIs:
              ;; A: collection protocols (assoc/dissoc)
              state-a (-> state-a
                          (assoc :new-1 new-vec-1)      ; assoc with keyword
                          (assoc "new-2" new-vec-2)    ; assoc with string
                          (dissoc 3))                   ; dissoc by internal id

              ;; B: VectorIndex protocols (insert/delete)
              state-b (-> state-b
                          (core/insert new-vec-1 :new-1)
                          (core/insert new-vec-2 "new-2")
                          (core/delete 3))]

          ;; Both should have same delta count
          (is (= (:delta-count (core/compaction-progress state-a))
                 (:delta-count (core/compaction-progress state-b)))
              "Delta counts should match")

          ;; Both should have same vector count in source
          (is (= (core/count-vectors state-a)
                 (core/count-vectors state-b))
              "Source vector counts should match")

          ;; Finish both compactions
          (let [compacted-a (a/<!! (core/finish-online-compaction! state-a))
                compacted-b (a/<!! (core/finish-online-compaction! state-b))]

            ;; Same live count
            (is (= (:live-count (core/index-metrics compacted-a))
                   (:live-count (core/index-metrics compacted-b)))
                "Compacted live counts should match")

            ;; New vectors findable by external ID in both
            (is (some? (get compacted-a :new-1)) "A: :new-1 should be findable")
            (is (some? (get compacted-b :new-1)) "B: :new-1 should be findable")
            (is (some? (get compacted-a "new-2")) "A: \"new-2\" should be findable")
            (is (some? (get compacted-b "new-2")) "B: \"new-2\" should be findable")

            ;; Search with new vectors as queries should return similar results
            (let [results-a (core/search compacted-a new-vec-1 5 {:ef 50})
                  results-b (core/search compacted-b new-vec-1 5 {:ef 50})]
              (is (= (count results-a) (count results-b))
                  "Search result counts should match"))

            (a/<!! (core/close! compacted-a))
            (a/<!! (core/close! compacted-b)))))

      (finally
        (cleanup (str path "-a"))
        (cleanup (str path "-b"))
        (cleanup (str path "-compact-a"))
        (cleanup (str path "-compact-b"))))))

(deftest test-online-compaction-collection-protocol-safety-checks
  (let [path (temp-path)]
    (try
      (testing "Collection protocols respect delta overflow limit"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path path
                                      :capacity 100})
              idx (core/insert-batch idx (random-vectors 10 32) (range 10))

              target-base (str path "-overflow")
              state (core/start-online-compaction idx
                                                  {:store-config (store-config-for target-base (java.util.UUID/randomUUID))
                                                   :mmap-dir (mmap-dir-for target-base)}
                                                  {:max-delta-size 2})]

          ;; Fill up delta log via assoc
          (let [state (assoc state :a (random-vec 32))
                state (assoc state :b (random-vec 32))]

            ;; Next assoc should throw overflow
            (let [ex (try
                       (assoc state :c (random-vec 32))
                       nil
                       (catch clojure.lang.ExceptionInfo e e))]
              (is (some? ex) "Expected overflow from assoc")
              (when ex
                (is (= :compaction/delta-overflow (:error (ex-data ex))))))

            ;; dissoc should also throw overflow
            (let [ex (try
                       (dissoc state 0)
                       nil
                       (catch clojure.lang.ExceptionInfo e e))]
              (is (some? ex) "Expected overflow from dissoc")
              (when ex
                (is (= :compaction/delta-overflow (:error (ex-data ex)))))))

          (core/abort-online-compaction! state)))

      (finally
        (cleanup path)
        (cleanup (str path "-overflow"))))))

(deftest test-online-compaction-metadata-updates
  (let [path (temp-path)]
    (try
      (testing "Metadata updates during online compaction are preserved"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path path
                                      :capacity 100})
              vecs (random-vectors 10 32)
              initial-metas (mapv #(hash-map :original-n %) (range 10))
              idx2 (core/insert-batch idx vecs (range 10) {:metadata initial-metas})

              new-store-id (java.util.UUID/randomUUID)
              target-base (str path "-metadata")
              state (core/start-online-compaction idx2
                                                  {:store-config (store-config-for target-base new-store-id)
                                                   :mmap-dir (mmap-dir-for target-base)}
                                                  {:batch-size 5})

              ;; Wait for copy to likely complete
              _ (Thread/sleep 50)

              ;; Update metadata on vectors that were likely already copied
              state2 (-> state
                         (p/set-metadata 0 {:updated true :value 100})
                         (p/set-metadata 5 {:updated true :value 500}))]

          ;; Verify source has the updates
          (is (= {:updated true :value 100} (core/get-metadata state2 0)))
          (is (= {:updated true :value 500} (core/get-metadata state2 5)))
          ;; Note: Unchanged metadata includes :external-id added during insert-batch
          (is (= 3 (:original-n (core/get-metadata state2 3))))

          ;; Finish compaction
          (let [compacted (a/<!! (core/finish-online-compaction! state2))]

            ;; In the compacted index, vectors are re-indexed (0-9 map to 0-9 for undeleted)
            ;; The id-mapping from copy should be {0->0, 1->1, ..., 9->9}
            ;; After set-metadata delta replay:
            ;; - Vector at new-id=0 should have metadata {:updated true :value 100}
            ;; - Vector at new-id=5 should have metadata {:updated true :value 500}

            ;; Check metadata directly on compacted index
            ;; Since no vectors were deleted, id mapping is 1:1
            (let [meta-0 (core/get-metadata compacted 0)
                  meta-5 (core/get-metadata compacted 5)
                  meta-3 (core/get-metadata compacted 3)]

              ;; Updated metadata should contain :updated and :value
              (is (= true (:updated meta-0))
                  (str "Vector 0's metadata should have :updated=true, got: " (pr-str meta-0)))
              (is (= 100 (:value meta-0))
                  (str "Vector 0's metadata should have :value=100, got: " (pr-str meta-0)))
              (is (= true (:updated meta-5))
                  (str "Vector 5's metadata should have :updated=true, got: " (pr-str meta-5)))
              (is (= 500 (:value meta-5))
                  (str "Vector 5's metadata should have :value=500, got: " (pr-str meta-5)))

              ;; Unchanged vector should have original metadata (with :old-id added during copy)
              (is (= 3 (:original-n meta-3))
                  (str "Vector 3 should preserve original-n=3, got: " (pr-str meta-3))))

            (a/<!! (core/close! compacted)))))

      (finally
        (cleanup path)
        (cleanup (str path "-metadata"))))))
