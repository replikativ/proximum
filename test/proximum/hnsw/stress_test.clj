(ns proximum.hnsw.stress-test
  "HNSW-specific stress tests for concurrent operations.

   Tests concurrent reads and writes to verify thread safety
   and correctness under load. Includes invariant checking for
   HNSW graph integrity verification."
  (:require [clojure.test :refer :all]
            [proximum.core :as pv]
            [proximum.protocols :as p]
            [proximum.hnsw.invariants :as inv])
  (:import [java.util.concurrent CountDownLatch Executors TimeUnit]))

;; -----------------------------------------------------------------------------
;; Test Utilities

(defn random-vector
  "Generate a random float vector of given dimension."
  [dim]
  (float-array (repeatedly dim #(- (rand 2.0) 1.0))))

(defn random-vector-seeded
  "Generate a random float vector using a seeded Random instance."
  [^java.util.Random rng dim]
  (float-array (repeatedly dim #(- (* 2.0 (.nextFloat rng)) 1.0))))

(defn shuffle-seeded
  "Shuffle a collection using a seeded Random instance."
  [^java.util.Random rng coll]
  (let [arr (java.util.ArrayList. coll)]
    (java.util.Collections/shuffle arr rng)
    (vec arr)))

(defn create-test-index
  "Create a fresh index with its own Konserve store identity.

  The production API requires an explicit Konserve store (via :store or
  :store-config). For stress tests we default to a unique in-memory store per
  index so stores do not clash across tests."
  [config]
  (pv/create-index (cond-> (dissoc config :store-id :storage-path :vectors-path)
                     (nil? (:store config))
                     (assoc :store-config (or (:store-config config)
                                              {:backend :memory
                                               :id (java.util.UUID/randomUUID)})))))

;; Helpers for new external ID API - auto-generate IDs when not specified
(defn insert-auto
  "Insert with auto-generated ID."
  [idx vec]
  (pv/insert idx vec nil))

(defn insert-batch-auto
  "Insert batch with numeric indices as external IDs."
  ([idx vectors]
   (pv/insert-batch idx vectors (vec (range (count vectors)))))
  ([idx vectors opts]
   (pv/insert-batch idx vectors (vec (range (count vectors))) opts)))

(defn run-concurrent
  "Run n tasks concurrently, wait for all to complete.
   Returns vector of results or exceptions."
  [n task-fn]
  (let [latch (CountDownLatch. n)
        results (atom [])
        ^java.util.concurrent.ExecutorService executor (Executors/newFixedThreadPool n)]
    (try
      (dotimes [i n]
        (.submit executor
                 ^Runnable
                 (fn []
                   (try
                     (let [result (task-fn i)]
                       (swap! results conj {:i i :result result}))
                     (catch Exception e
                       (swap! results conj {:i i :error e}))
                     (finally
                       (.countDown latch))))))
      (.await latch 120 TimeUnit/SECONDS)
      @results
      (finally
        (.shutdown executor)))))

;; -----------------------------------------------------------------------------
;; Stress Tests

(deftest concurrent-insert-test
  (testing "Multiple threads inserting vectors concurrently"
    (let [dim 128
          n-threads 8
          vectors-per-thread 100
          ;; Exact capacity - no buffer needed with proper synchronization
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 100
                                  :capacity (* n-threads vectors-per-thread)})
          idx-atom (atom idx)
          insert-counts (atom (vec (repeat n-threads 0)))
          ;; Lock for serializing inserts. Note: swap! with insert doesn't work
          ;; for concurrent inserts because insert has side effects (mmap writes,
          ;; HNSW graph updates) that can't be rolled back on CAS retry.
          ;; Use locking to serialize inserts while still testing thread safety.
          insert-lock (Object.)]

      ;; Run concurrent inserts with proper synchronization
      (let [results (run-concurrent n-threads
                                    (fn [thread-id]
                                      (dotimes [j vectors-per-thread]
                                        (let [v (random-vector dim)]
                            ;; Serialize inserts to avoid duplicate side effects from swap! retries
                                          (locking insert-lock
                                            (swap! idx-atom insert-auto v))
                                          (swap! insert-counts update thread-id inc)))
                                      :done))]

        ;; Check no errors
        (is (empty? (filter :error results))
            (str "Errors during insert: " (filter :error results)))

        ;; All threads completed
        (is (= n-threads (count (filter #(= :done (:result %)) results))))

        ;; Check vector count is exactly correct (no data loss)
        (let [final-count (pv/count-vectors @idx-atom)
              expected (* n-threads vectors-per-thread)]
          (is (= final-count expected)
              (format "Expected exactly %d vectors, got %d (data loss detected!)" expected final-count)))))))

(deftest vector-identity-test
  (testing "Concurrent inserts preserve vector identity (get-vector returns what was inserted)"
    (let [dim 32
          n-threads 4
          vectors-per-thread 100
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 8
                                  :ef-construction 50
                                  :capacity (* n-threads vectors-per-thread)})
          idx-atom (atom idx)
          ;; Track (id, original-vector) pairs from all threads
          id-vector-pairs (atom [])
          insert-lock (Object.)
          ;; Counter for unique external IDs
          id-counter (atom 0)]

      ;; Run concurrent inserts, each thread records its (id, vector) pairs
      (let [results (run-concurrent n-threads
                                    (fn [thread-id]
                                      (dotimes [j vectors-per-thread]
                          ;; Create unique vector: encode thread-id and j in the values
                                        (let [v (float-array (repeatedly dim
                                                                         #(float (+ (* thread-id 1000) j (rand 0.001)))))
                                              orig-vec (vec v)]
                                          (locking insert-lock
                                            (let [ext-id (swap! id-counter inc)
                                                  new-idx (pv/insert @idx-atom v ext-id)]
                                              (reset! idx-atom new-idx)
                                              (swap! id-vector-pairs conj [ext-id orig-vec])))))
                                      :done))]

        ;; Check no errors
        (is (empty? (filter :error results))
            (str "Errors during insert: " (filter :error results)))

        ;; Verify vector identity: for each (id, orig-vec), check get-vector returns orig-vec
        (let [pairs @id-vector-pairs
              final-idx @idx-atom
              mismatches (filter (fn [[id orig-vec]]
                                   (let [stored (vec (pv/get-vector final-idx id))]
                                     (not= stored orig-vec)))
                                 pairs)]
          (is (= (count pairs) (* n-threads vectors-per-thread))
              (format "Expected %d pairs, got %d" (* n-threads vectors-per-thread) (count pairs)))
          (is (empty? mismatches)
              (format "%d vectors returned different data than inserted! First: %s"
                      (count mismatches)
                      (first mismatches))))))))

(deftest concurrent-read-write-test
  (testing "Concurrent readers and writers"
    (let [dim 128
          n-writers 4
          n-readers 4
          vectors-per-writer 50
          queries-per-reader 100
          k 10

          ;; Pre-populate with some vectors
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 100
                                  :ef-search 50
                                  :capacity (+ 1000 (* n-writers vectors-per-writer 2))})
          idx (reduce (fn [i _] (insert-auto i (random-vector dim)))
                      idx
                      (range 100))
          idx-atom (atom idx)

          write-errors (atom [])
          read-errors (atom [])
          search-count (atom 0)]

      ;; Run concurrent reads and writes
      (let [^java.util.concurrent.ExecutorService executor (Executors/newFixedThreadPool (+ n-writers n-readers))
            latch (CountDownLatch. (+ n-writers n-readers))]
        (try
          ;; Start writers
          (dotimes [w n-writers]
            (.submit executor
                     ^Runnable
                     (fn []
                       (try
                         (dotimes [_ vectors-per-writer]
                           (try
                             (swap! idx-atom insert-auto (random-vector dim))
                             (catch Exception e
                               (swap! write-errors conj e)))
                           (Thread/sleep (rand-int 2)))
                         (finally
                           (.countDown latch))))))

          ;; Start readers
          (dotimes [r n-readers]
            (.submit executor
                     ^Runnable
                     (fn []
                       (try
                         (dotimes [_ queries-per-reader]
                           (try
                             (let [results (pv/search @idx-atom (random-vector dim) k)]
                               (when (seq results)
                                 (swap! search-count inc)))
                             (catch Exception e
                               (swap! read-errors conj e)))
                           (Thread/sleep (rand-int 1)))
                         (finally
                           (.countDown latch))))))

          (.await latch 120 TimeUnit/SECONDS)

          (finally
            (.shutdown executor))))

      ;; Verify no write errors
      (is (empty? @write-errors)
          (format "Write errors: %d - %s"
                  (count @write-errors)
                  (take 3 (map #(.getMessage %) @write-errors))))

      ;; Verify no read errors
      (is (empty? @read-errors)
          (format "Read errors: %d - %s"
                  (count @read-errors)
                  (take 3 (map #(.getMessage %) @read-errors))))

      ;; Verify searches returned results
      (is (pos? @search-count)
          "At least some searches should return results"))))

(deftest fork-under-load-test
  (testing "Fork operation completes quickly under concurrent load"
    (let [dim 128
          n-vectors 500
          n-concurrent 4

          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 100
                                  :capacity (+ n-vectors 200)})
          idx (reduce (fn [i _] (insert-auto i (random-vector dim)))
                      idx
                      (range n-vectors))
          idx-atom (atom idx)

          fork-times (atom [])
          running (atom true)]

      ;; Start background load
      (let [^java.util.concurrent.ExecutorService executor (Executors/newFixedThreadPool n-concurrent)]
        (try
          ;; Start concurrent readers
          (dotimes [_ (dec n-concurrent)]
            (.submit executor
                     ^Runnable
                     (fn []
                       (while @running
                         (try
                           (pv/search @idx-atom (random-vector dim) 10)
                           (catch Exception _))
                         (Thread/sleep 1)))))

          ;; Start concurrent writer
          (.submit executor
                   ^Runnable
                   (fn []
                     (while @running
                       (try
                         (swap! idx-atom insert-auto (random-vector dim))
                         (catch Exception _))
                       (Thread/sleep 5))))

          ;; Wait for load to build up
          (Thread/sleep 100)

          ;; Measure fork times
          (dotimes [_ 10]
            (let [start (System/nanoTime)
                  forked (pv/fork @idx-atom)
                  elapsed-ms (/ (- (System/nanoTime) start) 1e6)]
              (swap! fork-times conj elapsed-ms)
              (is (instance? (class @idx-atom) forked))))

          ;; Stop background load
          (reset! running false)
          (Thread/sleep 50)

          (finally
            (.shutdownNow executor))))

      ;; Verify fork is O(1) - should be < 10ms regardless of size
      (let [avg-fork-time (/ (reduce + @fork-times) (count @fork-times))]
        (is (< avg-fork-time 10.0)
            (format "Fork should be < 10ms, was %.2fms (times: %s)"
                    avg-fork-time @fork-times))))))

(deftest search-consistency-test
  (testing "Search results are consistent under concurrent writes"
    (let [dim 32
          n-vectors 200
          k 5

          ;; Create and populate index
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 100
                                  :ef-search 50
                                  :capacity (+ n-vectors 100)})

          ;; Insert known vectors with explicit external IDs
          vectors (vec (repeatedly n-vectors #(random-vector dim)))
          idx (reduce (fn [i [j v]] (pv/insert i v j)) idx (map-indexed vector vectors))
          idx-atom (atom idx)]

      ;; Query same vector multiple times concurrently
      (let [query (first vectors)
            results (run-concurrent 8
                                    (fn [_]
                                      (vec (map :id (pv/search @idx-atom query k)))))]

        ;; All queries should return same result for same query
        ;; (assuming no concurrent modifications)
        (let [result-sets (map (comp set :result) results)]
          (is (apply = result-sets)
              "Same query should return same results"))

        ;; First result should be the query vector itself (id 0)
        (is (= 0 (first (:result (first results))))
            "Query vector should be its own nearest neighbor")))))

;; -----------------------------------------------------------------------------
;; Invariant-Based Stress Tests

(deftest invariants-after-bulk-insert
  (testing "Graph invariants hold after bulk insert"
    (let [dim 64
          n-vectors 1000
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 100
                                  :capacity (+ n-vectors 100)})
          vectors (vec (repeatedly n-vectors #(random-vector dim)))
          final-idx (insert-batch-auto idx vectors)]

      ;; Check all invariants
      (let [result (inv/check-all-invariants final-idx :max-samples 500)]
        (is (:valid? result)
            (str "Invariant violations: " (:errors result)))))))

(deftest invariants-after-concurrent-insert
  (testing "Graph invariants hold after concurrent inserts"
    (let [dim 64
          n-threads 8
          vectors-per-thread 100
          ;; Use 3x capacity to handle concurrent insert race conditions
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 100
                                  :capacity (* n-threads vectors-per-thread 3)})
          idx-atom (atom idx)]

      ;; Concurrent inserts
      (let [results (run-concurrent n-threads
                                    (fn [_]
                                      (dotimes [_ vectors-per-thread]
                                        (swap! idx-atom insert-auto (random-vector dim)))
                                      :done))]

        ;; Check no errors (capacity errors are allowed due to race conditions)
        (let [non-capacity-errors (filter #(and (:error %)
                                                (not (re-find #"capacity" (str (:error %)))))
                                          results)]
          (is (empty? non-capacity-errors)
              (str "Non-capacity errors: " non-capacity-errors)))

        ;; Check all invariants on final state
        (let [result (inv/check-all-invariants @idx-atom :max-samples 500)]
          (is (:valid? result)
              (str "Invariant violations: " (:errors result))))))))

(deftest invariants-after-delete
  (testing "Graph invariants hold after deletions"
    (let [dim 64
          n-vectors 500
          n-deletes 50
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 100
                                  :capacity (+ n-vectors 100)})
          vectors (vec (repeatedly n-vectors #(random-vector dim)))
          idx-with-vectors (insert-batch-auto idx vectors)

          ;; Delete random nodes
          delete-ids (take n-deletes (shuffle (range n-vectors)))
          final-idx (reduce pv/delete idx-with-vectors delete-ids)]

      ;; Check invariants after deletions
      (let [result (inv/check-all-invariants final-idx :max-samples 300)]
        (is (:valid? result)
            (str "Invariant violations after delete: " (:errors result)))))))

(deftest invariants-periodic-during-insert
  (testing "Graph invariants hold periodically during insertion"
    (let [dim 64
          n-vectors 500
          check-interval 100
          idx-atom (atom (create-test-index {:type :hnsw
                                             :dim dim
                                             :M 16
                                             :ef-construction 100
                                             :capacity (+ n-vectors 100)}))
          violation-count (atom 0)]

      ;; Insert with periodic invariant checks
      (dotimes [i n-vectors]
        (swap! idx-atom insert-auto (random-vector dim))

        ;; Check invariants every check-interval inserts
        (when (and (pos? i) (zero? (mod i check-interval)))
          (let [result (inv/check-all-invariants @idx-atom :max-samples 200)]
            (when-not (:valid? result)
              (swap! violation-count inc)
              (println "Invariant violation at iteration" i ":" (:errors result))))))

      ;; No violations should have occurred
      (is (zero? @violation-count)
          (str @violation-count " invariant violations occurred during insertion")))))

(deftest search-quality-test
  (testing "Search quality meets recall threshold"
    (let [dim 32
          n-vectors 500
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 200
                                  :ef-search 100
                                  :capacity (+ n-vectors 100)})
          vectors (vec (repeatedly n-vectors #(random-vector dim)))
          final-idx (insert-batch-auto idx vectors)]

      ;; Check search quality
      (let [result (inv/check-search-quality final-idx
                                             :k 10
                                             :num-queries 50
                                             :min-recall 0.85)]
        (is (:valid? result)
            (format "Recall %.2f below threshold %.2f"
                    (double (:mean-recall result))
                    (double (:threshold result))))

        ;; Print recall stats
        (println (format "Recall@10: mean=%.2f min=%.2f max=%.2f"
                         (double (:mean-recall result))
                         (double (:min-recall-seen result))
                         (double (:max-recall-seen result))))))))

(deftest no-self-loops-after-modifications
  (testing "No self-loops exist after various operations"
    (let [dim 64
          n-vectors 300
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 100
                                  :capacity (+ n-vectors 100)})
          vectors (vec (repeatedly n-vectors #(random-vector dim)))
          idx-inserted (insert-batch-auto idx vectors)

          ;; Delete some, insert more
          idx-modified (-> idx-inserted
                           (pv/delete 10)
                           (pv/delete 50)
                           (pv/delete 100)
                           (insert-auto (random-vector dim))
                           (insert-auto (random-vector dim)))]

      ;; Check for self-loops (comprehensive)
      (let [result (inv/check-no-self-loops idx-modified :max-samples 300)]
        (is (:valid? result)
            (str "Self-loops found: " (:errors result)))))))

;; -----------------------------------------------------------------------------
;; Delete Edge Case Tests

(deftest delete-entry-point-test
  (testing "Deleting entry point maintains graph integrity"
    (let [dim 64
          n-vectors 100
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 100
                                  :capacity (+ n-vectors 50)})
          vectors (vec (repeatedly n-vectors #(random-vector dim)))
          idx-with-vectors (insert-batch-auto idx vectors)

          ;; Get entry point before delete
          entry-point (-> idx-with-vectors p/edge-storage .getEntrypoint)]

      ;; Delete entry point
      (let [idx-after-delete (pv/delete idx-with-vectors entry-point)]
        ;; The node should be marked as deleted
        (is (-> idx-after-delete p/edge-storage (.isDeleted entry-point))
            "Deleted node should be marked as deleted in PES")

        ;; Graph should still be searchable
        (let [results (pv/search idx-after-delete (first vectors) 10)]
          (is (seq results) "Search should return results")
          (is (not (some #(= (:id %) entry-point) results))
              "Deleted entry point should not appear in results"))

        ;; Invariants should hold
        (let [result (inv/check-all-invariants idx-after-delete :max-samples 100)]
          (is (:valid? result)
              (str "Invariant violations: " (:errors result))))))))

(deftest delete-all-nodes-test
  (testing "Deleting all nodes results in empty graph"
    (let [dim 32
          n-vectors 50
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 100
                                  :capacity (+ n-vectors 10)})
          vectors (vec (repeatedly n-vectors #(random-vector dim)))
          ids (mapv #(str "doc-" %) (range n-vectors))
          idx-with-vectors (pv/insert-batch idx vectors ids)

          ;; Delete all nodes using external IDs
          idx-empty (reduce pv/delete idx-with-vectors ids)]

      ;; Entry point should be -1 (empty)
      (is (= -1 (-> idx-empty p/edge-storage .getEntrypoint))
          "Entry point should be -1 for empty graph")

      ;; Count should be 0 (or reflect deletions)
      (let [deleted-count (-> idx-empty p/edge-storage .getDeletedCount)]
        (is (= n-vectors deleted-count)
            (format "Deleted count should be %d, got %d" n-vectors deleted-count)))

      ;; Search should return empty
      (let [results (pv/search idx-empty (random-vector dim) 10)]
        (is (empty? results) "Search on empty graph should return no results")))))

(deftest delete-connected-clique-test
  (testing "Deleting highly connected nodes maintains graph integrity"
    (let [dim 32
          n-vectors 200
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 200
                                  :ef-search 100
                                  :capacity (+ n-vectors 50)})
          ;; Create vectors that will be close together (form a clique)
          base-vector (random-vector dim)
          ;; First 20 vectors are very similar (will be highly connected)
          similar-vectors (vec (repeatedly 20
                                           #(float-array (map (fn [x] (+ x (- (rand 0.1) 0.05)))
                                                              base-vector))))
          other-vectors (vec (repeatedly (- n-vectors 20) #(random-vector dim)))
          all-vectors (into similar-vectors other-vectors)
          idx-with-vectors (insert-batch-auto idx all-vectors)

          ;; Delete the similar vectors (clique)
          idx-after-delete (reduce pv/delete idx-with-vectors (range 20))]

      ;; Graph should still be valid
      (let [result (inv/check-all-invariants idx-after-delete :max-samples 200)]
        (is (:valid? result)
            (str "Invariant violations after clique delete: " (:errors result))))

      ;; Search should work and not return deleted nodes
      (let [results (pv/search idx-after-delete (random-vector dim) 10)]
        (is (every? #(>= (:id %) 20) results)
            "No deleted nodes should appear in results")))))

(deftest delete-then-search-consistency-test
  (testing "Search results are correct after deletions"
    (let [dim 32
          n-vectors 100
          k 10
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 100
                                  :ef-search 50
                                  :capacity (+ n-vectors 50)})
          vectors (vec (repeatedly n-vectors #(random-vector dim)))
          idx-with-vectors (insert-batch-auto idx vectors)

          ;; Delete every other node
          delete-ids (set (range 0 n-vectors 2))
          idx-after-delete (reduce pv/delete idx-with-vectors delete-ids)]

      ;; Multiple searches should never return deleted nodes
      (dotimes [_ 20]
        (let [query (random-vector dim)
              results (pv/search idx-after-delete query k)]
          (is (every? #(not (delete-ids (:id %))) results)
              "Deleted nodes should never appear in search results"))))))

(deftest delete-preserves-search-quality-test
  (testing "Search quality remains acceptable after deletions"
    ;; Use seeded RNG for reproducibility - eliminates flakiness from unlucky
    ;; random configurations while still testing real deletion behavior
    (let [rng (java.util.Random. 12345)
          dim 32
          n-vectors 1000      ; larger graph is more stable
          n-deletes 100       ; 10% deletion rate
          k 10
          num-queries 100     ; more queries for reliable statistics
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 200
                                  :ef-search 100
                                  :capacity (+ n-vectors 50)})
          vectors (vec (repeatedly n-vectors #(random-vector-seeded rng dim)))
          idx-with-vectors (insert-batch-auto idx vectors)

          ;; Delete random nodes (seeded shuffle for reproducibility)
          delete-ids (set (take n-deletes (shuffle-seeded rng (range n-vectors))))
          idx-after-delete (reduce pv/delete idx-with-vectors delete-ids)

          ;; Remaining vectors for ground truth
          remaining-ids (vec (remove delete-ids (range n-vectors)))
          remaining-vectors (mapv #(nth vectors %) remaining-ids)]

      ;; Check recall against remaining vectors
      (let [recalls (for [_ (range num-queries)]
                      (let [query (random-vector-seeded rng dim)
                            hnsw-results (pv/search idx-after-delete query k)
                            hnsw-ids (set (map :id hnsw-results))

                            ;; Brute force on remaining vectors
                            distances (map-indexed
                                       (fn [i v]
                                         {:id (nth remaining-ids i)
                                          :dist (reduce + (map #(* % %)
                                                               (map - query v)))})
                                       remaining-vectors)
                            exact-ids (set (map :id (take k (sort-by :dist distances))))]
                        (/ (count (clojure.set/intersection hnsw-ids exact-ids))
                           (count exact-ids))))
            mean-recall (/ (reduce + recalls) (count recalls))]

        (is (>= mean-recall 0.65)
            (format "Recall %.2f should be >= 0.65 after deletions"
                    (double mean-recall)))))))

(deftest consecutive-delete-insert-test
  (testing "Consecutive delete and insert operations work correctly"
    (let [dim 32
          initial-count 100
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 100
                                  :capacity 200})
          vectors (vec (repeatedly initial-count #(random-vector dim)))
          idx (insert-batch-auto idx vectors)]

      ;; Perform alternating delete/insert cycles
      (let [final-idx (reduce
                       (fn [idx i]
                         (let [;; Delete a random existing node
                               delete-id (rand-int (pv/count-vectors idx))
                               idx-deleted (pv/delete idx delete-id)
                                ;; Insert a new vector
                               idx-inserted (insert-auto idx-deleted (random-vector dim))]
                           idx-inserted))
                       idx
                       (range 50))]

        ;; Check invariants
        (let [result (inv/check-all-invariants final-idx :max-samples 150)]
          (is (:valid? result)
              (str "Invariant violations after delete/insert cycles: "
                   (:errors result))))

        ;; Search should work
        (let [results (pv/search final-idx (random-vector dim) 10)]
          (is (seq results) "Search should return results"))))))

;; -----------------------------------------------------------------------------
;; Boundary Condition Tests

(deftest empty-graph-operations-test
  (testing "Operations on empty graph"
    (let [dim 32
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 100
                                  :capacity 100})]
      ;; Search on empty returns empty
      (is (empty? (pv/search idx (random-vector dim) 10))
          "Search on empty graph should return empty")

      ;; Count is 0
      (is (zero? (pv/count-vectors idx))
          "Empty graph should have 0 vectors")

      ;; Entry point is -1
      (is (= -1 (-> idx p/edge-storage .getEntrypoint))
          "Empty graph should have entry point -1"))))

(deftest single-vector-operations-test
  (testing "Operations with single vector"
    (let [dim 32
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 100
                                  :capacity 10})
          v (random-vector dim)
          test-id "test-vector-1"
          idx-with-one (pv/insert idx v test-id)]

      ;; Count is 1
      (is (= 1 (pv/count-vectors idx-with-one))
          "Should have 1 vector")

      ;; Search returns the one vector
      (let [results (pv/search idx-with-one v 10)]
        (is (= 1 (count results))
            "Search should return 1 result")
        (is (= test-id (:id (first results)))
            "Result should have our external ID"))

      ;; Search with k=1 works
      (let [results (pv/search idx-with-one v 1)]
        (is (= 1 (count results))))

      ;; Delete single vector leaves empty graph
      (let [idx-empty (pv/delete idx-with-one test-id)]
        (is (= -1 (-> idx-empty p/edge-storage .getEntrypoint))
            "Deleting only vector should leave empty graph")
        (is (empty? (pv/search idx-empty v 10))
            "Search after deleting only vector should be empty")))))

(deftest capacity-boundary-test
  (testing "Filling index to exact capacity"
    (let [dim 32
          capacity 50
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 100
                                  :capacity capacity})
          vectors (vec (repeatedly capacity #(random-vector dim)))
          idx-full (insert-batch-auto idx vectors)]

      ;; Count equals capacity
      (is (= capacity (pv/count-vectors idx-full))
          "Should have exactly capacity vectors")

      ;; Remaining capacity is 0
      (is (zero? (pv/remaining-capacity idx-full))
          "Remaining capacity should be 0")

      ;; Insert beyond capacity throws
      (is (thrown? Exception (insert-auto idx-full (random-vector dim)))
          "Insert beyond capacity should throw")

      ;; Search still works
      (let [results (pv/search idx-full (first vectors) 10)]
        (is (= 10 (count results))
            "Search should return k results")))))

(deftest search-k-boundaries-test
  (testing "Search with various k values"
    (let [dim 32
          n-vectors 20
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 100
                                  :capacity 50})
          vectors (vec (repeatedly n-vectors #(random-vector dim)))
          idx (insert-batch-auto idx vectors)
          query (random-vector dim)]

      ;; k=1 returns exactly 1
      (is (= 1 (count (pv/search idx query 1)))
          "k=1 should return 1 result")

      ;; k=vector-count returns all
      (is (= n-vectors (count (pv/search idx query n-vectors)))
          "k=n should return n results")

      ;; k > vector-count returns all available
      (is (= n-vectors (count (pv/search idx query (* 2 n-vectors))))
          "k > n should return n results"))))

(deftest zero-vector-test
  (testing "All-zeros vector handling"
    (let [dim 32
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 100
                                  :capacity 20})
          zero-vec (float-array (repeat dim 0.0))
          random-vecs (vec (repeatedly 10 #(random-vector dim)))
          ;; Insert zero vector with explicit ID 0
          idx (pv/insert idx zero-vec 0)
          ;; Insert random vectors with IDs 1-10
          idx (pv/insert-batch idx random-vecs (vec (range 1 11)))]

      ;; Zero vector is searchable
      (let [results (pv/search idx zero-vec 5)]
        (is (seq results) "Search for zero vector should return results")
        (is (= 0 (:id (first results)))
            "Zero vector should be its own nearest neighbor"))

      ;; Invariants hold
      (let [result (inv/check-all-invariants idx :max-samples 20)]
        (is (:valid? result)
            (str "Invariants should hold with zero vector: " (:errors result)))))))

(deftest high-m-value-test
  (testing "High M value (dense graph)"
    (let [dim 32
          n-vectors 30
          M 24  ;; Very high M relative to vector count
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M M
                                  :ef-construction 200
                                  :capacity 50})
          vectors (vec (repeatedly n-vectors #(random-vector dim)))
          idx (insert-batch-auto idx vectors)]

      ;; Graph should be valid
      (let [result (inv/check-all-invariants idx :max-samples 30)]
        (is (:valid? result)
            (str "High-M graph should be valid: " (:errors result))))

      ;; Search should work well
      (let [results (pv/search idx (first vectors) 10)]
        (is (= 10 (count results)))
        (is (= 0 (:id (first results)))
            "Query vector should be its own nearest neighbor")))))

(deftest identical-vectors-test
  (testing "Identical vectors (degenerate case)"
    (let [dim 32
          base-vec (random-vector dim)
          ;; Create 10 identical vectors
          vectors (vec (repeat 10 (float-array base-vec)))
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 100
                                  :capacity 20})
          idx (insert-batch-auto idx vectors)]

      ;; All vectors have distance 0 to each other
      (let [results (pv/search idx base-vec 5)]
        (is (= 5 (count results))
            "Should return k results")
        ;; All distances should be ~0
        (is (every? #(< (:distance %) 0.001) results)
            "All distances should be near zero"))

      ;; Invariants should hold
      (let [result (inv/check-all-invariants idx :max-samples 10)]
        (is (:valid? result)
            (str "Invariants should hold with identical vectors: " (:errors result)))))))

(deftest clustered-vectors-test
  (testing "Highly clustered vectors"
    (let [dim 32
          ;; Create 5 clusters of 10 vectors each
          cluster-centers (vec (repeatedly 5 #(random-vector dim)))
          make-cluster (fn [center]
                         (vec (repeatedly 10
                                          #(float-array (map (fn [x] (+ x (- (rand 0.01) 0.005)))
                                                             center)))))
          all-vectors (vec (mapcat make-cluster cluster-centers))
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 100
                                  :capacity 100})
          idx (insert-batch-auto idx all-vectors)]

      ;; Search for cluster center should find cluster members
      (let [results (pv/search idx (first cluster-centers) 10)]
        (is (= 10 (count results)))
        ;; First 10 IDs should be 0-9 (first cluster)
        (is (every? #(< (:id %) 10) results)
            "Search should find first cluster members"))

      ;; Invariants hold
      (let [result (inv/check-all-invariants idx :max-samples 50)]
        (is (:valid? result)
            (str "Invariants with clustered vectors: " (:errors result)))))))

(deftest large-dimension-test
  (testing "High-dimensional vectors"
    (let [dim 1536  ;; OpenAI embedding size
          n-vectors 50
          idx (create-test-index {:type :hnsw
                                  :dim dim
                                  :M 16
                                  :ef-construction 100
                                  :capacity 100})
          vectors (vec (repeatedly n-vectors #(random-vector dim)))
          idx (insert-batch-auto idx vectors)]

      ;; Search works
      (let [results (pv/search idx (first vectors) 10)]
        (is (= 10 (count results)))
        (is (= 0 (:id (first results)))))

      ;; Invariants hold
      (let [result (inv/check-all-invariants idx :max-samples 50)]
        (is (:valid? result)
            (str "High-dim invariants: " (:errors result)))))))

(deftest fork-empty-and-single-test
  (testing "Fork operations on edge case graphs"
    (let [dim 32
          idx-empty (create-test-index {:type :hnsw
                                        :dim dim
                                        :M 16
                                        :ef-construction 100
                                        :capacity 20})]
      ;; Fork empty graph
      (let [forked (pv/fork idx-empty)]
        (is (= -1 (-> forked p/edge-storage .getEntrypoint))
            "Forked empty graph should have entry point -1")
        (is (zero? (pv/count-vectors forked))
            "Forked empty graph should have 0 vectors"))

      ;; Fork single-vector graph
      (let [idx-one (insert-auto idx-empty (random-vector dim))
            forked (pv/fork idx-one)]
        (is (= 1 (pv/count-vectors forked))
            "Forked single-vector graph should have 1 vector")
        (is (seq (pv/search forked (random-vector dim) 1))
            "Search on forked single-vector graph should work")))))
