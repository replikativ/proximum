(ns proximum.pes-stress-test
  "Low-level stress tests for PersistentEdgeStore.

   Tests CoW visibility, fork isolation, and concurrent operations
   directly at the PES level to verify thread safety and correctness
   of the chunked edge storage implementation.

   Key parameters to hit all branches:
   - CHUNK_SIZE = 1024 nodes per chunk
   - Multiple chunks require > 1024 vectors
   - Upper layers require nodes at higher levels (controlled by M, maxLevel)

   NOTE: After persistent semantics changes, all mutations require transient mode.
   Tests must call .asTransient() before mutations and .asPersistent() after."
  (:require [clojure.test :refer :all])
  (:import [proximum.internal PersistentEdgeStore]
           [java.util.concurrent CountDownLatch Executors TimeUnit]
           [java.util Random]))

;; -----------------------------------------------------------------------------
;; Test Configuration

(def ^:const CHUNK_SIZE 1024)  ; Must match PersistentEdgeStore.CHUNK_SIZE

;; Default parameters matching typical HNSW usage
(def default-M 16)
(def default-M0 32)  ; M * 2
(def default-max-level 5)

;; Scale: 1000 vectors/thread as requested
(def vectors-per-thread 1000)
(def default-n-threads 8)

;; -----------------------------------------------------------------------------
;; Test Utilities

(defn create-pes
  "Create a fresh PersistentEdgeStore with given capacity."
  ([max-nodes] (create-pes max-nodes default-max-level default-M default-M0))
  ([max-nodes max-level M M0]
   (PersistentEdgeStore. max-nodes max-level M M0)))

(defn random-neighbors
  "Generate random neighbor array of given size."
  [^Random rng size max-id]
  (int-array (repeatedly size #(.nextInt rng max-id))))

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

(defn print-seed
  "Print seed for reproducibility on failure."
  [seed test-name]
  (println (format "  [%s] Using seed: %d" test-name seed)))

(defmacro with-transient
  "Execute body with PES in transient mode, then switch back to persistent."
  [pes & body]
  `(do
     (.asTransient ~pes)
     (try
       ~@body
       (finally
         (.asPersistent ~pes)))))

;; -----------------------------------------------------------------------------
;; Test: CoW Visibility (Tests the bug fix in persistent mode)

(deftest cow-visibility-test
  (testing "Transient mode writes are immediately visible to readers"
    (let [seed (System/nanoTime)
          _ (print-seed seed "cow-visibility")
          rng (Random. seed)
          max-nodes 4096  ; 4 chunks
          pes (create-pes max-nodes)
          M0 default-M0]

      ;; Switch to transient mode for mutations
      (.asTransient pes)

      ;; Test nodes at different chunk boundaries
      (doseq [node-id [0                       ; First node, first chunk
                       (dec CHUNK_SIZE)        ; Last node, first chunk
                       CHUNK_SIZE              ; First node, second chunk
                       (+ CHUNK_SIZE 512)      ; Middle of second chunk
                       (* 2 CHUNK_SIZE)        ; Third chunk boundary
                       (dec max-nodes)]]       ; Last node
        (let [neighbors (random-neighbors rng (min M0 10) max-nodes)]
          ;; Set neighbors in transient mode
          (.setNeighbors pes 0 node-id neighbors)

          ;; Immediately read back - should see our write
          (let [read-back (.getNeighbors pes 0 node-id)]
            (is (some? read-back)
                (format "Node %d: getNeighbors returned nil after set" node-id))
            (when read-back
              (is (= (vec neighbors) (vec read-back))
                  (format "Node %d: neighbors mismatch" node-id))))))

      ;; Switch back to persistent
      (.asPersistent pes))))

(deftest cow-concurrent-visibility-test
  (testing "Concurrent writes on forked PES - each thread's fork is isolated"
    (let [seed (System/nanoTime)
          _ (print-seed seed "cow-concurrent-visibility")
          n-threads 8
          nodes-per-thread 100  ; Multiple chunks per thread
          max-nodes (* n-threads nodes-per-thread 2)
          base-pes (create-pes max-nodes)
          ;; Each thread gets its own fork - this is correct persistent semantics
          forks (mapv (fn [_] (.fork base-pes)) (range n-threads))]

      (let [results (run-concurrent n-threads
                                    (fn [thread-id]
                                      (let [rng (Random. (+ seed thread-id))
                                            my-fork (nth forks thread-id)
                                            start-node (* thread-id nodes-per-thread)
                                            errors (atom [])]
                          ;; Switch fork to transient mode
                                        (.asTransient my-fork)
                                        (try
                                          (dotimes [i nodes-per-thread]
                                            (let [node-id (+ start-node i)
                                                  neighbors (random-neighbors rng (min default-M0 10) max-nodes)]
                                ;; Set in our fork (transient mode)
                                              (.setNeighbors my-fork 0 node-id neighbors)
                                ;; Verify immediately in our fork
                                              (let [read-back (.getNeighbors my-fork 0 node-id)]
                                                (when (or (nil? read-back)
                                                          (not= (vec neighbors) (vec read-back)))
                                                  (swap! errors conj {:node node-id
                                                                      :expected (vec neighbors)
                                                                      :actual (when read-back (vec read-back))})))))
                                          (finally
                                            (.asPersistent my-fork)))
                                        @errors)))]

        ;; Check for any errors
        (doseq [{:keys [error result]} results]
          (is (nil? error) (str "Thread threw exception: " error))
          (is (empty? result)
              (format "Visibility errors: %s" (pr-str (take 5 result)))))))))

;; -----------------------------------------------------------------------------
;; Test: Fork Isolation

(deftest fork-isolation-test
  (testing "Fork creates isolated copy - modifications don't affect original"
    (let [seed (System/nanoTime)
          _ (print-seed seed "fork-isolation")
          rng (Random. seed)
          max-nodes 4096
          pes (create-pes max-nodes)
          test-nodes [0 100 1024 2048]]  ; Span multiple chunks

      ;; Initialize original with known values (in transient mode)
      (.asTransient pes)
      (doseq [node-id test-nodes]
        (let [neighbors (int-array (repeat 5 node-id))]  ; Recognizable pattern
          (.setNeighbors pes 0 node-id neighbors)))
      (.asPersistent pes)

      ;; Fork
      (let [fork (.fork pes)]

        ;; Modify fork with different values (in transient mode)
        (.asTransient fork)
        (doseq [node-id test-nodes]
          (let [new-neighbors (int-array (repeat 5 (+ node-id 10000)))]
            (.setNeighbors fork 0 node-id new-neighbors)))
        (.asPersistent fork)

        ;; Verify original unchanged
        (doseq [node-id test-nodes]
          (let [original-neighbors (.getNeighbors pes 0 node-id)]
            (is (= (vec (repeat 5 node-id)) (vec original-neighbors))
                (format "Original node %d was modified by fork!" node-id))))

        ;; Verify fork has new values
        (doseq [node-id test-nodes]
          (let [fork-neighbors (.getNeighbors fork 0 node-id)]
            (is (= (vec (repeat 5 (+ node-id 10000))) (vec fork-neighbors))
                (format "Fork node %d doesn't have new value" node-id))))))))

(deftest fork-isolation-stress-test
  (testing "Fork isolation under concurrent modifications"
    (let [seed (System/nanoTime)
          _ (print-seed seed "fork-isolation-stress")
          n-threads 8
          ops-per-thread 500
          max-nodes 8192  ; 8 chunks
          pes (create-pes max-nodes)]

      ;; Initialize all nodes with thread-id -1 pattern (in transient mode)
      (.asTransient pes)
      (dotimes [node-id (min 4096 max-nodes)]
        (.setNeighbors pes 0 node-id (int-array [node-id])))
      (.asPersistent pes)

      ;; Fork for each thread
      (let [forks (mapv (fn [_] (.fork pes)) (range n-threads))
            results (run-concurrent n-threads
                                    (fn [thread-id]
                                      (let [fork (nth forks thread-id)
                                            rng (Random. (+ seed thread-id))
                                            errors (atom [])]
                          ;; Switch fork to transient mode
                                        (.asTransient fork)
                                        (try
                            ;; Modify random nodes in our fork
                                          (dotimes [_ ops-per-thread]
                                            (let [node-id (.nextInt rng 4096)
                                                  marker (+ (* thread-id 10000) node-id)
                                                  neighbors (int-array [marker])]
                                              (.setNeighbors fork 0 node-id neighbors)))
                                          (finally
                                            (.asPersistent fork)))
                          ;; Verify original is untouched for some nodes
                                        (dotimes [_ 100]
                                          (let [node-id (.nextInt rng 4096)
                                                original (.getNeighbors pes 0 node-id)]
                                            (when (and original (not= (aget original 0) node-id))
                                              (swap! errors conj {:node node-id
                                                                  :expected node-id
                                                                  :actual (aget original 0)}))))
                                        @errors)))]

        (doseq [{:keys [error result]} results]
          (is (nil? error) (str "Thread threw: " error))
          (is (empty? result)
              (format "Original modified by fork: %s" (pr-str (take 5 result)))))))))

;; -----------------------------------------------------------------------------
;; Test: Multi-Index Parallel Operations

(deftest multi-index-parallel-test
  (testing "Multiple PES instances operate independently in parallel"
    (let [seed (System/nanoTime)
          _ (print-seed seed "multi-index-parallel")
          n-threads 8
          ops-per-thread 500
          max-nodes 4096
          ;; Each thread gets its own PES instance
          indices (mapv (fn [_] (create-pes max-nodes)) (range n-threads))]

      (let [results (run-concurrent n-threads
                                    (fn [thread-id]
                                      (let [rng (Random. (+ seed thread-id))
                              ;; Each thread has exclusive access to its index
                                            my-index (nth indices thread-id)
                                            thread-marker (* thread-id 100000)
                                            errors (atom [])]
                          ;; Switch to transient mode
                                        (.asTransient my-index)
                                        (try
                            ;; Write to my exclusive index
                                          (dotimes [i ops-per-thread]
                                            (let [node-id i
                                                  neighbors (int-array [(+ thread-marker node-id)])]
                                              (.setNeighbors my-index 0 node-id neighbors)))
                                          (finally
                                            (.asPersistent my-index)))
                          ;; Verify my writes
                                        (dotimes [i ops-per-thread]
                                          (let [node-id i
                                                read-back (.getNeighbors my-index 0 node-id)
                                                expected (+ thread-marker node-id)]
                                            (when (or (nil? read-back)
                                                      (not= expected (aget read-back 0)))
                                              (swap! errors conj {:node node-id
                                                                  :expected expected
                                                                  :actual (when read-back (aget read-back 0))}))))
                          ;; Verify other indices don't have my data (sample)
                                        (when (> thread-id 0)
                                          (let [other-index (nth indices (dec thread-id))
                                                sample-node 0
                                                other-data (.getNeighbors other-index 0 sample-node)]
                              ;; Other index should have different marker or nil
                                            (when (and other-data
                                                       (= (+ thread-marker sample-node) (aget other-data 0)))
                                              (swap! errors conj {:cross-contamination true
                                                                  :my-thread thread-id
                                                                  :other-thread (dec thread-id)}))))
                                        @errors)))]

        (doseq [{:keys [error result]} results]
          (is (nil? error) (str "Thread threw: " error))
          (is (empty? result)
              (format "Multi-index errors: %s" (pr-str (take 5 result)))))))))

;; -----------------------------------------------------------------------------
;; Test: Transient Mode Concurrent Operations

(deftest transient-mode-parallel-test
  (testing "Transient mode handles concurrent inserts correctly"
    (let [seed (System/nanoTime)
          _ (print-seed seed "transient-mode-parallel")
          n-threads 8
          ops-per-thread vectors-per-thread  ; 1000 as configured
          max-nodes (+ (* n-threads ops-per-thread) 1000)
          pes (create-pes max-nodes)]

      ;; Switch to transient mode
      (.asTransient pes)

      (let [results (run-concurrent n-threads
                                    (fn [thread-id]
                                      (let [rng (Random. (+ seed thread-id))
                              ;; Each thread gets a non-overlapping range
                                            start-node (* thread-id ops-per-thread)
                                            errors (atom [])]
                          ;; Insert many nodes - setNeighbors already uses striped locking
                                        (dotimes [i ops-per-thread]
                                          (let [node-id (+ start-node i)
                                                neighbors (random-neighbors rng (min default-M0 8) max-nodes)]
                                            (.setNeighbors pes 0 node-id neighbors)))
                          ;; Verify our writes
                                        (dotimes [i ops-per-thread]
                                          (let [node-id (+ start-node i)
                                                read-back (.getNeighbors pes 0 node-id)]
                                            (when (nil? read-back)
                                              (swap! errors conj {:node node-id :error "nil neighbors"}))))
                                        @errors)))]

        ;; Switch back to persistent
        (.asPersistent pes)

        ;; Verify no errors
        (doseq [{:keys [error result]} results]
          (is (nil? error) (str "Thread threw: " error))
          (is (empty? result)
              (format "Transient write errors: %s" (pr-str (take 5 result)))))

        ;; Verify total count
        (let [total-written (* n-threads ops-per-thread)
              count-found (atom 0)]
          (dotimes [i total-written]
            (when (.getNeighbors pes 0 i)
              (swap! count-found inc)))
          (is (= total-written @count-found)
              (format "Expected %d nodes, found %d" total-written @count-found)))))))

;; -----------------------------------------------------------------------------
;; Test: Chunk Boundary Stress

(deftest chunk-boundary-stress-test
  (testing "Operations at chunk boundaries are correct"
    (let [seed (System/nanoTime)
          _ (print-seed seed "chunk-boundary-stress")
          rng (Random. seed)
          ;; 4 full chunks plus partial
          max-nodes (+ (* 4 CHUNK_SIZE) 100)
          pes (create-pes max-nodes)
          ;; Test specific boundary nodes
          boundary-nodes [(dec CHUNK_SIZE)           ; End of chunk 0
                          CHUNK_SIZE                  ; Start of chunk 1
                          (dec (* 2 CHUNK_SIZE))     ; End of chunk 1
                          (* 2 CHUNK_SIZE)           ; Start of chunk 2
                          (dec (* 3 CHUNK_SIZE))     ; End of chunk 2
                          (* 3 CHUNK_SIZE)           ; Start of chunk 3
                          (dec (* 4 CHUNK_SIZE))     ; End of chunk 3
                          (* 4 CHUNK_SIZE)]]         ; Start of chunk 4

      ;; Switch to transient mode for mutations
      (.asTransient pes)

      ;; Write to all boundary nodes
      (doseq [node-id boundary-nodes]
        (let [neighbors (int-array (range (min 10 default-M0)))]
          (.setNeighbors pes 0 node-id neighbors)))

      ;; Switch back to persistent
      (.asPersistent pes)

      ;; Verify all boundary nodes
      (doseq [node-id boundary-nodes]
        (let [neighbors (.getNeighbors pes 0 node-id)]
          (is (some? neighbors)
              (format "Boundary node %d (chunk %d) has nil neighbors"
                      node-id (quot node-id CHUNK_SIZE)))
          (when neighbors
            (is (= (vec (range (min 10 default-M0))) (vec neighbors))
                (format "Boundary node %d has wrong neighbors" node-id))))))))

;; -----------------------------------------------------------------------------
;; Test: Upper Layer Operations

(deftest upper-layer-test
  (testing "Upper layer operations work correctly"
    (let [seed (System/nanoTime)
          _ (print-seed seed "upper-layer")
          rng (Random. seed)
          max-nodes 4096
          max-level 4
          pes (create-pes max-nodes max-level default-M default-M0)]

      ;; Switch to transient mode
      (.asTransient pes)

      ;; Write to different layers
      (doseq [layer (range 1 max-level)]
        (doseq [node-id [0 512 1024 2048]]
          (let [neighbors (int-array [layer node-id])]  ; Encode layer in neighbors
            (.setNeighbors pes layer node-id neighbors))))

      ;; Switch back to persistent
      (.asPersistent pes)

      ;; Verify all layers
      (doseq [layer (range 1 max-level)]
        (doseq [node-id [0 512 1024 2048]]
          (let [neighbors (.getNeighbors pes layer node-id)]
            (is (some? neighbors)
                (format "Layer %d node %d has nil neighbors" layer node-id))
            (when neighbors
              (is (= [layer node-id] (vec neighbors))
                  (format "Layer %d node %d has wrong neighbors" layer node-id)))))))))

(deftest upper-layer-fork-isolation-test
  (testing "Fork isolation works for upper layers"
    (let [max-nodes 4096
          max-level 3
          pes (create-pes max-nodes max-level default-M default-M0)]

      ;; Initialize layers 1 and 2 (in transient mode)
      (.asTransient pes)
      (doseq [layer [1 2]]
        (.setNeighbors pes layer 0 (int-array [layer])))
      (.asPersistent pes)

      ;; Fork and modify
      (let [fork (.fork pes)]
        ;; Modify fork in transient mode
        (.asTransient fork)
        (doseq [layer [1 2]]
          (.setNeighbors fork layer 0 (int-array [(+ layer 100)])))
        (.asPersistent fork)

        ;; Verify original unchanged
        (doseq [layer [1 2]]
          (let [neighbors (.getNeighbors pes layer 0)]
            (is (= [layer] (vec neighbors))
                (format "Original layer %d was modified" layer))))

        ;; Verify fork has new values
        (doseq [layer [1 2]]
          (let [neighbors (.getNeighbors fork layer 0)]
            (is (= [(+ layer 100)] (vec neighbors))
                (format "Fork layer %d not updated" layer))))))))

;; -----------------------------------------------------------------------------
;; Test: Random Operations (Property-based style)

(deftest random-operations-test
  (testing "Random operation sequences maintain consistency"
    (let [seed (System/nanoTime)
          _ (print-seed seed "random-operations")
          rng (Random. seed)
          max-nodes 4096
          pes (create-pes max-nodes)
          n-ops 10000
          ;; Track expected state
          expected (atom {})]

      ;; All mutations in transient mode
      (.asTransient pes)

      (dotimes [_ n-ops]
        (let [op (mod (.nextInt rng) 3)
              node-id (.nextInt rng max-nodes)]
          (case op
            ;; Write
            0 (let [neighbors (random-neighbors rng (min 5 default-M0) max-nodes)]
                (.setNeighbors pes 0 node-id neighbors)
                (swap! expected assoc node-id (vec neighbors)))
            ;; Read and verify
            1 (let [actual (.getNeighbors pes 0 node-id)
                    exp (get @expected node-id)]
                (if exp
                  (is (= exp (when actual (vec actual)))
                      (format "Node %d mismatch" node-id))
                  ;; Node not written yet - should be nil
                  (is (nil? actual)
                      (format "Node %d should be nil" node-id))))
            ;; Fork and verify isolation (fork inherits transient state, but we need to
            ;; switch to persistent first, then fork, then test)
            2 (do
                (.asPersistent pes)
                (let [fork (.fork pes)]
                  (.asTransient fork)
                  (let [new-neighbors (random-neighbors rng (min 5 default-M0) max-nodes)]
                    (.setNeighbors fork 0 node-id new-neighbors))
                  (.asPersistent fork)
                  ;; Original should be unchanged
                  (let [original-actual (.getNeighbors pes 0 node-id)
                        exp (get @expected node-id)]
                    (if exp
                      (is (= exp (when original-actual (vec original-actual)))
                          (format "Original node %d changed after fork write" node-id))
                      (is (nil? original-actual)))))
                ;; Switch back to transient for next ops
                (.asTransient pes)))))

      ;; Switch to persistent for final verification
      (.asPersistent pes)

      ;; Final verification pass
      (doseq [[node-id exp-neighbors] @expected]
        (let [actual (.getNeighbors pes 0 node-id)]
          (is (= exp-neighbors (when actual (vec actual)))
              (format "Final check: node %d mismatch" node-id)))))))
