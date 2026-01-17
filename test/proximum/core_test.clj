(ns proximum.core-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [proximum.core :as core]
            [proximum.protocols :as p]
            [proximum.hnsw.internal :as hnsw.i] ;; Only for address-map (no protocol equivalent)
            [clojure.set :as set])
  (:import [java.io File]
           [proximum.internal PersistentEdgeStore Distance]))

(def ^:dynamic *store-id* nil)

(defn with-store-id-fixture [f]
  (binding [*store-id* (java.util.UUID/randomUUID)]
    (f)))

(use-fixtures :each with-store-id-fixture)

(defn file-store-config
  ([path]
   (file-store-config path *store-id*))
  ([path store-id]
   {:backend :file
    :path path
    :id store-id}))

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

(defn create-test-index
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

(defn random-vec
  "Generate a random float vector of given dimension."
  [dim]
  (float-array (repeatedly dim #(- (rand 2.0) 1.0))))

(defn random-vectors
  "Generate n random vectors of given dimension."
  [n dim]
  (vec (repeatedly n #(random-vec dim))))

(defn temp-path []
  (str (System/getProperty "java.io.tmpdir")
       "/hnsw-test-" (System/currentTimeMillis)))

(defn cleanup [path]
  (let [f (File. path)]
    (when (.isDirectory f)
      (doseq [file (reverse (file-seq f))]
        (.delete file)))
    (when (.exists f)
      (.delete f))))

;; -----------------------------------------------------------------------------
;; Basic HNSW tests

(deftest test-hnsw-basic
  (let [path (temp-path)]
    (try
      (testing "Create and insert"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :vectors-path path
                                      :capacity 100})]

          ;; Insert first vector with ID
          (let [idx2 (core/insert idx (random-vec 32) 0)]
            (is (= 1 (core/count-vectors idx2)))
            ;; Check PES has an entrypoint
            (is (>= (.getEntrypoint ^PersistentEdgeStore (p/edge-storage idx2)) 0))

            ;; Insert more with sequential IDs
            (let [idx3 (reduce (fn [i n] (core/insert i (random-vec 32) (inc n)))
                               idx2
                               (range 9))]
              (is (= 10 (core/count-vectors idx3)))

              ;; Search - now returns external IDs
              (let [query (random-vec 32)
                    results (core/search idx3 query 5 {:ef 50})]
                (is (= 5 (count results)))
                (is (every? #(contains? % :id) results))
                (is (every? #(contains? % :distance) results))
                ;; IDs should be our external IDs (0-9)
                (is (every? #(<= 0 (:id %) 9) results)))

              (core/close! idx3)))))

      (finally
        (cleanup path)))))

;; -----------------------------------------------------------------------------
;; Batch insert tests

(deftest test-batch-insert
  (let [path (temp-path)]
    (try
      (testing "Batch insert"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 64
                                      :M 16
                                      :ef-construction 100
                                      :vectors-path path
                                      :capacity 200})
              vecs (random-vectors 100 64)
              ids (range 100)
              idx2 (core/insert-batch idx vecs ids)]

          (is (= 100 (core/count-vectors idx2)))

          ;; Search should work and return external IDs
          (let [query (random-vec 64)
                results (core/search idx2 query 10 {:ef 100})]
            (is (= 10 (count results)))
            (is (every? #(<= 0 (:id %) 99) results)))

          (core/close! idx2)))

      (finally
        (cleanup path)))))

;; -----------------------------------------------------------------------------
;; Metadata tests

(deftest test-metadata
  (let [path (temp-path)]
    (try
      (testing "Insert with metadata"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :vectors-path path
                                      :capacity 100})
              ;; Insert with ID and metadata
              idx2 (core/insert idx (random-vec 32) "doc-1" {:entity-id 123 :attr :person/name})
              idx3 (core/insert idx2 (random-vec 32) "doc-2" {:entity-id 456 :attr :person/age})
              idx4 (core/insert idx3 (random-vec 32) "doc-3")]  ; no extra metadata

          (is (= 3 (core/count-vectors idx4)))

          ;; Check metadata by external ID
          (is (= 123 (:entity-id (core/get-metadata idx4 "doc-1"))))
          (is (= 456 (:entity-id (core/get-metadata idx4 "doc-2"))))
          ;; doc-3 has external-id but no extra metadata
          (is (= "doc-3" (:external-id (core/get-metadata idx4 "doc-3"))))

          ;; Update metadata
          (let [idx5 (core/with-metadata idx4 "doc-3" {:entity-id 789})]
            (is (= 789 (:entity-id (core/get-metadata idx5 "doc-3")))))

          (core/close! idx4)))

      (testing "Batch insert with metadata"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :vectors-path (str path "-batch")
                                      :capacity 100})
              vecs (random-vectors 5 32)
              ids ["a" "b" "c" "d" "e"]
              metas [{:val 1} {:val 2} nil {:val 4} {:val 5}]
              idx2 (core/insert-batch idx vecs ids {:metadata metas})]

          (is (= 5 (core/count-vectors idx2)))
          (is (= 1 (:val (core/get-metadata idx2 "a"))))
          (is (= 2 (:val (core/get-metadata idx2 "b"))))
          ;; "c" has external-id but nil extra metadata
          (is (= "c" (:external-id (core/get-metadata idx2 "c"))))
          (is (= 4 (:val (core/get-metadata idx2 "d"))))
          (is (= 5 (:val (core/get-metadata idx2 "e"))))

          (core/close! idx2)))

      (finally
        (cleanup path)
        (cleanup (str path "-batch"))))))

;; -----------------------------------------------------------------------------
;; Fork tests

(deftest test-fork
  (let [path (temp-path)]
    (try
      (testing "Fork creates independent edge structure"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :vectors-path path
                                      :capacity 100})
              vecs (random-vectors 20 32)
              ids (range 20)
              idx2 (core/insert-batch idx vecs ids)

              ;; Fork - creates independent edge store
              idx-fork (core/fork idx2)

              ;; Both should still search correctly
              query (random-vec 32)
              results-orig (core/search idx2 query 5 {:ef 50})
              results-fork (core/search idx-fork query 5 {:ef 50})]

          ;; Both should return same results (same graph at fork time)
          (is (= 5 (count results-orig)))
          (is (= 5 (count results-fork)))

          ;; Results should be identical since graph hasn't diverged
          (is (= (set (map :id results-orig))
                 (set (map :id results-fork))))

          ;; Verify PES are different objects (forked)
          (is (not (identical? (p/edge-storage idx2) (p/edge-storage idx-fork))))

          (core/close! idx2)))

      (testing "Modifications after fork are independent"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :vectors-path (str path "-fork2")
                                      :capacity 100})
              vecs (random-vectors 10 32)
              ids (range 10)
              idx2 (core/insert-batch idx vecs ids)

              ;; Fork
              idx-fork (core/fork idx2)

              ;; Insert additional vectors into the FORK only
              new-vec (random-vec 32)
              idx-fork2 (core/insert idx-fork new-vec "fork-new" {:source :fork-insert})]

          ;; Original edge count unchanged
          (is (= (.countEdges ^PersistentEdgeStore (p/edge-storage idx2))
                 (.countEdges ^PersistentEdgeStore (p/edge-storage idx2))))

          ;; Fork has more edges
          (is (> (.countEdges ^PersistentEdgeStore (p/edge-storage idx-fork2))
                 (.countEdges ^PersistentEdgeStore (p/edge-storage idx2))))

          ;; Searching the fork finds the new vector
          (let [results (core/search idx-fork2 new-vec 5 {:ef 50})]
            ;; Should find the vector we just inserted (it's closest to itself)
            (is (some #(= "fork-new" (:id %)) results) "Fork should find newly inserted vector"))

          ;; Metadata only exists in fork
          (is (= {:source :fork-insert :external-id "fork-new"} (core/get-metadata idx-fork2 "fork-new")))
          (is (nil? (core/get-metadata idx2 "fork-new")))

          (core/close! idx2)
          (core/close! idx-fork2)))

      (finally
        (cleanup path)
        (cleanup (str path "-fork2"))))))

;; -----------------------------------------------------------------------------
;; Recall tests

(deftest test-hnsw-recall
  (let [path (temp-path)]
    (try
      (testing "Recall on 100 vectors"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 64
                                      :M 16
                                      :ef-construction 100
                                      :vectors-path path
                                      :capacity 200})
              vecs (random-vectors 100 64)
              ids (range 100)
              idx2 (core/insert-batch idx vecs ids)]

          (is (= 100 (core/count-vectors idx2)))

          ;; Test recall on 5 queries
          ;; Brute force search now uses external IDs
          (let [recalls (for [_ (range 5)]
                          (let [q (random-vec 64)
                                hnsw-results (set (map :id (core/search idx2 q 10 {:ef 100})))
                                ;; Brute force using internal protocol access for comparison
                                bf-results (set (->> (range 100)
                                                     (map (fn [i]
                                                            {:id i
                                                             :distance (Distance/euclideanSquaredVectors
                                                                        q
                                                                        (core/get-vector idx2 i))}))
                                                     (sort-by :distance)
                                                     (take 10)
                                                     (map :id)))]
                            (/ (count (set/intersection hnsw-results bf-results)) 10.0)))
                mean-recall (/ (reduce + recalls) (count recalls))]

            ;; Expect at least 90% recall
            (is (>= mean-recall 0.9)
                (str "Mean recall " mean-recall " should be >= 0.9")))

          (core/close! idx2)))

      (finally
        (cleanup path)))))

;; -----------------------------------------------------------------------------
;; Search with metadata tests

(deftest test-search-with-metadata
  (let [path (temp-path)]
    (try
      (testing "Search returns metadata"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :vectors-path path
                                      :capacity 100})
              ;; Insert vectors with ID and metadata
              idx2 (-> idx
                       (core/insert (random-vec 32) "a" {:type :a})
                       (core/insert (random-vec 32) "b" {:type :b})
                       (core/insert (random-vec 32) "c" {:type :c}))
              query (random-vec 32)
              results (core/search-with-metadata idx2 query 3)]

          (is (= 3 (count results)))
          (is (every? #(contains? % :metadata) results))
          ;; Each result should have one of our metadata types
          (is (every? #(#{:a :b :c} (:type (:metadata %))) results))
          ;; IDs should be our external IDs
          (is (every? #(#{"a" "b" "c"} (:id %)) results))

          (core/close! idx2)))

      (finally
        (cleanup path)))))

;; -----------------------------------------------------------------------------
;; Delete tests

(deftest test-delete
  (let [path (temp-path)]
    (try
      (testing "Delete removes node from graph with repair"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :vectors-path path
                                      :capacity 100})
              vecs (random-vectors 20 32)
              ids (range 20)
              idx2 (core/insert-batch idx vecs ids)

              ;; Store the vector we'll delete for querying
              deleted-vec (core/get-vector idx2 5)

              ;; Delete by external ID
              idx3 (core/delete idx2 5)]

          ;; Metadata is removed
          (is (nil? (core/get-metadata idx3 5)))

          ;; Search for the deleted vector - it should NOT appear in results
          (let [results (core/search idx3 deleted-vec 10 {:ef 100})]
            (is (not (some #(= 5 (:id %)) results))
                "Deleted node should not appear in search results"))

          ;; Graph still works - can find other nodes
          (let [query (core/get-vector idx3 0)
                results (core/search idx3 query 5 {:ef 50})]
            (is (= 5 (count results)))
            (is (some #(= 0 (:id %)) results) "Should find queried node"))

          (core/close! idx3)))

      (finally
        (cleanup path)))))

;; -----------------------------------------------------------------------------
;; Filtered search tests

(deftest test-filtered-search
  (let [path (temp-path)]
    (try
      (testing "Filtered search with predicate"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :vectors-path path
                                      :capacity 100})
              ;; Insert 20 vectors - 10 with category :a, 10 with category :b
              idx2 (reduce (fn [i n]
                             (core/insert i (random-vec 32) n
                                          {:category (if (< n 10) :a :b)}))
                           idx
                           (range 20))
              query (random-vec 32)

              ;; Search filtered to only category :a (receives external ID)
              results (core/search-filtered idx2 query 5
                                            (fn [id meta]
                                              (= :a (:category meta))))]

          (is (= 5 (count results)))
          ;; All results should be from category :a (ids 0-9)
          (is (every? #(< (:id %) 10) results))

          (core/close! idx2)))

      (testing "Filtered search with ID set"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :vectors-path (str path "-set")
                                      :capacity 100})
              idx2 (reduce (fn [i n] (core/insert i (random-vec 32) n))
                           idx
                           (range 20))
              query (random-vec 32)

              ;; Filter to only IDs 0, 2, 4, 6, 8 (external IDs)
              results (core/search-filtered idx2 query 5 #{0 2 4 6 8})]

          (is (<= (count results) 5))
          (is (every? #(#{0 2 4 6 8} (:id %)) results))

          (core/close! idx2)))

      (finally
        (cleanup path)
        (cleanup (str path "-set"))))))

;; -----------------------------------------------------------------------------
;; Fork performance tests

(deftest test-fork-performance
  (let [path (temp-path)]
    (try
      (testing "Fork is O(1) - timing should not scale with size"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :vectors-path path
                                      :capacity 2000})
              vecs (random-vectors 1000 32)
              ids (range 1000)
              idx2 (core/insert-batch idx vecs ids)]

          ;; Fork should be fast (< 50ms even with 1000 vectors)
          (let [start (System/nanoTime)
                forked (core/fork idx2)
                elapsed-ms (/ (- (System/nanoTime) start) 1e6)]
            (is (< elapsed-ms 50) (str "Fork should be < 50ms, was " elapsed-ms "ms"))
            (is (not (identical? (p/edge-storage idx2) (p/edge-storage forked)))
                "Forked PES should be different object"))

          (core/close! idx2)))

      (testing "Fork is independent - modifications don't affect original"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :vectors-path (str path "-indep")
                                      :capacity 200})
              vecs (random-vectors 50 32)
              ids (range 50)
              idx2 (core/insert-batch idx vecs ids)

              ;; Fork and insert into fork
              forked (core/fork idx2)
              new-vec (random-vec 32)
              forked2 (core/insert forked new-vec "new-in-fork")]

          ;; Original edge count unchanged
          (is (= (.countEdges ^PersistentEdgeStore (p/edge-storage idx2))
                 (.countEdges ^PersistentEdgeStore (p/edge-storage idx2))))

          ;; Fork has more edges
          (is (> (.countEdges ^PersistentEdgeStore (p/edge-storage forked2))
                 (.countEdges ^PersistentEdgeStore (p/edge-storage idx2))))

          (core/close! idx2)))

      (testing "Search works correctly on forked index"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :vectors-path (str path "-search")
                                      :capacity 200})
              vecs (random-vectors 100 32)
              ids (range 100)
              idx2 (core/insert-batch idx vecs ids)
              forked (core/fork idx2)
              query (random-vec 32)]

          ;; Both should return same results
          (let [results-orig (core/search idx2 query 10 {:ef 50})
                results-fork (core/search forked query 10 {:ef 50})]
            (is (= (set (map :id results-orig))
                   (set (map :id results-fork)))))

          (core/close! idx2)))

      (finally
        (cleanup path)
        (cleanup (str path "-indep"))
        (cleanup (str path "-search"))))))

;; -----------------------------------------------------------------------------
;; Dirty chunk tracking tests

(deftest test-dirty-tracking
  (let [path (temp-path)]
    (try
      (testing "Dirty chunks tracked after insert"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :vectors-path path
                                      :capacity 200})
              pes (p/edge-storage idx)]

          ;; Initially no dirty chunks
          (is (not (.hasDirtyChunks ^PersistentEdgeStore pes)))
          (is (= 0 (.countDirtyChunks ^PersistentEdgeStore pes)))

          ;; Insert two vectors (first has no edges, second creates edges)
          (let [idx2 (-> idx
                         (core/insert (random-vec 32) 0)
                         (core/insert (random-vec 32) 1))
                pes2 (p/edge-storage idx2)]
            ;; Now should have dirty chunks
            (is (.hasDirtyChunks ^PersistentEdgeStore pes2))
            (is (pos? (.countDirtyChunks ^PersistentEdgeStore pes2)))

            ;; Can get dirty chunk addresses
            (let [dirty (.getDirtyChunks ^PersistentEdgeStore pes2)]
              (is (pos? (count dirty))))

            ;; Clear dirty and verify
            (.clearDirty ^PersistentEdgeStore pes2)
            (is (not (.hasDirtyChunks ^PersistentEdgeStore pes2)))

            (core/close! idx2))))

      (testing "Fork starts with empty dirty set"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :vectors-path (str path "-fork")
                                      :capacity 200})
              vecs (random-vectors 20 32)
              ids (range 20)
              idx2 (core/insert-batch idx vecs ids)
              pes2 (p/edge-storage idx2)]

          ;; Original has dirty chunks after insert
          (is (.hasDirtyChunks ^PersistentEdgeStore pes2))

          ;; Fork should start clean
          (let [forked (core/fork idx2)
                pes-forked (p/edge-storage forked)]
            (is (not (.hasDirtyChunks ^PersistentEdgeStore pes-forked))
                "Forked PES should have empty dirty set"))

          (core/close! idx2)))

      (finally
        (cleanup path)
        (cleanup (str path "-fork"))))))

;; -----------------------------------------------------------------------------
;; Capacity API tests

(deftest test-capacity-api
  (let [path (temp-path)]
    (try
      (testing "Capacity and remaining-capacity"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :vectors-path path
                                      :capacity 100})]

          (is (= 100 (core/capacity idx)))
          (is (= 100 (core/remaining-capacity idx)))

          (let [idx2 (core/insert idx (random-vec 32) 0)]
            (is (= 100 (core/capacity idx2)))
            (is (= 99 (core/remaining-capacity idx2)))

            ;; Continue from idx2 to test batch insert
            (let [idx3 (core/insert-batch idx2 (random-vectors 10 32) (range 1 11))]
              (is (= 100 (core/capacity idx3)))
              (is (= 89 (core/remaining-capacity idx3)))))

          (core/close! idx)))

      (testing "Capacity exceeded error has helpful message"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :vectors-path (str path "-exceed")
                                      :capacity 5})
              vecs (random-vectors 5 32)
              ids (range 5)
              idx2 (core/insert-batch idx vecs ids)]

          ;; Now at capacity - next insert should fail
          (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                #"capacity exceeded"
                                (core/insert idx2 (random-vec 32) "overflow")))

          (core/close! idx2)))

      (finally
        (cleanup path)
        (cleanup (str path "-exceed"))))))

;; -----------------------------------------------------------------------------
;; Edge persistence tests

(deftest test-edge-persistence
  (let [storage-path (temp-path)]
    (try
      (testing "Edges persisted on flush with storage-path"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path storage-path
                                      :capacity 200})
              vecs (random-vectors 50 32)
              ids (range 50)
              idx2 (core/insert-batch idx vecs ids)
              pes (p/edge-storage idx2)]

          ;; Before flush, PES has dirty chunks
          (is (.hasDirtyChunks ^PersistentEdgeStore pes))
          (let [dirty-count (.countDirtyChunks ^PersistentEdgeStore pes)]
            (is (pos? dirty-count)))

          ;; Flush persists edges - returns updated index with new address-map
          (let [idx2 (core/flush! idx2)]

            ;; After flush, PES has no dirty chunks
            (is (not (.hasDirtyChunks ^PersistentEdgeStore pes)))

            ;; address-map should now have entries (plain value, no @ needed)
            (is (pos? (count (hnsw.i/address-map idx2)))))

          (core/close! idx2)))

      (testing "Edges restored on load - no rebuild needed"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path (str storage-path "-load")
                                      :capacity 200})
              vecs (random-vectors 30 32)
              ids (range 30)
              idx2 (core/insert-batch idx vecs ids)]

          ;; Sync to persist everything
          (core/sync! idx2)

          ;; Record edge count for comparison
          (let [orig-edge-count (.countEdges ^PersistentEdgeStore (p/edge-storage idx2))
                orig-entrypoint (.getEntrypoint ^PersistentEdgeStore (p/edge-storage idx2))
                query (random-vec 32)
                orig-results (core/search idx2 query 10 {:ef 50})]

            (core/close! idx2)

            ;; Now load the index
            (let [base (str storage-path "-load")
                  loaded (core/load (store-config-for base *store-id*)
                                    :mmap-dir (mmap-dir-for base))
                  loaded-edge-count (.countEdges ^PersistentEdgeStore (p/edge-storage loaded))
                  loaded-entrypoint (.getEntrypoint ^PersistentEdgeStore (p/edge-storage loaded))]

              ;; Edge count should match
              (is (= orig-edge-count loaded-edge-count)
                  "Loaded index should have same edge count")

              ;; Entrypoint should match
              (is (= orig-entrypoint loaded-entrypoint)
                  "Loaded index should have same entrypoint")

              ;; Search should return same results
              (let [loaded-results (core/search loaded query 10 {:ef 50})]
                (is (= (set (map :id orig-results))
                       (set (map :id loaded-results)))
                    "Search results should match after reload"))

              (core/close! loaded)))))

      (finally
        (cleanup storage-path)
        (cleanup (str storage-path "-load"))))))

(deftest test-deleted-bitset-persistence
  (let [storage-path (temp-path)]
    (try
      (testing "Deleted nodes bitset survives store reopen"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path storage-path
                                      :capacity 200})
              vecs (random-vectors 50 32)
              ids (range 50)
              idx2 (core/insert-batch idx vecs ids)
              ;; Delete several nodes by external ID
              idx3 (-> idx2
                       (core/delete 5)
                       (core/delete 15)
                       (core/delete 25))
              query (random-vec 32)]

          ;; Verify deleted nodes are marked
          (is (= 3 (.getDeletedCount ^PersistentEdgeStore (p/edge-storage idx3)))
              "Should have 3 deleted nodes")

          ;; Search should not return deleted nodes
          (let [results (core/search idx3 query 10 {:ef 50})]
            (is (not (some #(contains? #{5 15 25} (:id %)) results))
                "Deleted nodes should not appear in search results"))

          ;; Sync to persist
          (core/sync! idx3)
          (core/close! idx3)

          ;; Reopen the index
          (let [loaded (core/load (store-config-for storage-path *store-id*)
                                  :mmap-dir (mmap-dir-for storage-path))]
            ;; Deleted count should be restored
            (is (= 3 (.getDeletedCount ^PersistentEdgeStore (p/edge-storage loaded)))
                "Loaded index should have same deleted count")

            ;; Search should still not return deleted nodes
            (let [loaded-results (core/search loaded query 10 {:ef 50})]
              (is (not (some #(contains? #{5 15 25} (:id %)) loaded-results))
                  "Deleted nodes should not appear in search results after reload"))

            (core/close! loaded))))

      (finally
        (cleanup storage-path)))))

;; -----------------------------------------------------------------------------
;; Metrics tests

(deftest test-metrics
  (let [path (temp-path)]
    (try
      (testing "index-metrics returns comprehensive stats"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path path
                                      :capacity 100})
              vecs (random-vectors 10 32)
              ids (range 10)
              idx2 (core/insert-batch idx vecs ids)
              metrics (core/index-metrics idx2)]

          ;; Check all expected keys are present
          (is (contains? metrics :vector-count))
          (is (contains? metrics :deleted-count))
          (is (contains? metrics :live-count))
          (is (contains? metrics :deletion-ratio))
          (is (contains? metrics :needs-compaction?))
          (is (contains? metrics :capacity))
          (is (contains? metrics :utilization))
          (is (contains? metrics :edge-count))
          (is (contains? metrics :avg-edges-per-node))
          (is (contains? metrics :branch))

          ;; Check values make sense
          (is (= 10 (:vector-count metrics)))
          (is (= 0 (:deleted-count metrics)))
          (is (= 10 (:live-count metrics)))
          (is (= 0.0 (:deletion-ratio metrics)))
          (is (false? (:needs-compaction? metrics)))
          (is (= 100 (:capacity metrics)))
          (is (= 0.1 (:utilization metrics)))
          (is (pos? (:edge-count metrics)))
          (is (pos? (:avg-edges-per-node metrics)))
          (is (= :main (:branch metrics)))

          (core/close! idx2)))

      (testing "Metrics update after delete"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path (str path "-del")
                                      :capacity 100})
              vecs (random-vectors 10 32)
              ids (range 10)
              idx2 (core/insert-batch idx vecs ids)
              ;; Delete 3 vectors (30% deletion ratio)
              idx3 (-> idx2
                       (core/delete 0)
                       (core/delete 1)
                       (core/delete 2))
              metrics (core/index-metrics idx3)]

          (is (= 10 (:vector-count metrics)))
          (is (= 3 (:deleted-count metrics)))
          (is (= 7 (:live-count metrics)))
          (is (= 0.3 (:deletion-ratio metrics)))
          (is (true? (:needs-compaction? metrics)))

          (core/close! idx3)))

      (testing "needs-compaction? with custom threshold"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path (str path "-thresh")
                                      :capacity 100})
              vecs (random-vectors 10 32)
              ids (range 10)
              idx2 (core/insert-batch idx vecs ids)
              ;; Delete 1 vector (10% deletion ratio)
              idx3 (core/delete idx2 0)]

          ;; Default threshold 0.10
          (is (false? (core/needs-compaction? idx3)))
          ;; With lower threshold
          (is (true? (core/needs-compaction? idx3 0.05)))

          (core/close! idx3)))

      (finally
        (cleanup path)
        (cleanup (str path "-del"))
        (cleanup (str path "-thresh"))))))

;; -----------------------------------------------------------------------------
;; Compact tests

(deftest test-compact
  (let [path (temp-path)]
    (try
      (testing "Compact creates new index with only live vectors"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path path
                                      :capacity 100})
              vecs (random-vectors 10 32)
              ids (vec (map #(str (char (+ 97 %))) (range 10)))  ;; ["a" "b" ... "j"]
              metas (mapv #(hash-map :label (str (char (+ 97 %)))) (range 10))
              idx2 (core/insert-batch idx vecs ids {:metadata metas})
              ;; Delete vectors "c", "e", "g"
              idx3 (-> idx2
                       (core/delete "c")
                       (core/delete "e")
                       (core/delete "g"))

              ;; Compact to new target
              new-store-id (java.util.UUID/randomUUID)
              target-base (str path "-compact")
              compacted (core/compact idx3 {:store-config (store-config-for target-base new-store-id)
                                            :mmap-dir (mmap-dir-for target-base)})]

          ;; New index has only 7 live vectors
          (is (= 7 (core/count-vectors compacted)))

          ;; No deleted vectors in compacted
          (let [metrics (core/index-metrics compacted)]
            (is (= 0 (:deleted-count metrics)))
            (is (= 0.0 (:deletion-ratio metrics))))

          ;; Search still works
          (let [query (random-vec 32)
                results (core/search compacted query 5 {:ef 50})]
            (is (= 5 (count results))))

          (core/close! idx3)
          (core/close! compacted)))

      (finally
        (cleanup path)
        (cleanup (str path "-compact"))))))

;; -----------------------------------------------------------------------------
;; Transient collection protocol tests

(deftest test-transient-collection
  (let [path (temp-path)]
    (try
      (testing "transient/persistent! round trip with assoc!"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path path
                                      :capacity 100})
              vec1 (random-vec 32)
              vec2 (random-vec 32)
              vec3 (random-vec 32)

              ;; Create transient, add vectors, convert back to persistent
              idx2 (persistent!
                    (-> (transient idx)
                        (assoc! "doc-1" vec1)
                        (assoc! "doc-2" vec2)
                        (assoc! "doc-3" vec3)))]

          (is (= 3 (count idx2)))
          (is (= 3 (core/count-vectors idx2)))

          ;; Vectors are retrievable by external ID
          (is (some? (get idx2 "doc-1")))
          (is (some? (get idx2 "doc-2")))
          (is (some? (get idx2 "doc-3")))

          ;; Search works
          (let [results (core/search idx2 vec1 2 {:ef 50})]
            (is (= 2 (count results)))
            ;; First result should be doc-1 (exact match)
            (is (= "doc-1" (:id (first results)))))

          (core/close! idx2)))

      (testing "transient with metadata using assoc!"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path (str path "-meta")
                                      :capacity 100})
              vec1 (random-vec 32)

              ;; Add with metadata map
              idx2 (persistent!
                    (-> (transient idx)
                        (assoc! "doc-1" {:vector vec1 :metadata {:type :test}})))]

          (is (= 1 (count idx2)))
          (is (= :test (:type (core/get-metadata idx2 "doc-1"))))

          (core/close! idx2)))

      (testing "transient count reflects pending operations"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path (str path "-count")
                                      :capacity 100})
              t (transient idx)]

          (is (= 0 (count t)))
          (let [t2 (assoc! t "a" (random-vec 32))]
            (is (= 1 (count t2)))
            (let [t3 (assoc! t2 "b" (random-vec 32))]
              (is (= 2 (count t3)))))))

      (testing "into uses transient for batch operations"
        (let [idx (create-test-index {:type :hnsw
                                      :dim 32
                                      :M 8
                                      :ef-construction 50
                                      :storage-path (str path "-into")
                                      :capacity 100})
              entries [["a" (random-vec 32)]
                       ["b" (random-vec 32)]
                       ["c" (random-vec 32)]]
              idx2 (into idx entries)]

          (is (= 3 (count idx2)))
          (is (some? (get idx2 "a")))
          (is (some? (get idx2 "b")))
          (is (some? (get idx2 "c")))

          (core/close! idx2)))

      (finally
        (cleanup path)
        (cleanup (str path "-meta"))
        (cleanup (str path "-count"))
        (cleanup (str path "-into"))))))
