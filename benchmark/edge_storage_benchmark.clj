(ns edge-storage-benchmark
  "Benchmark different edge storage designs for HNSW.

   Workload characteristics:
   - Random read by [layer, node-id] during search
   - Bidirectional insert during index building
   - Copy-on-write for versioning

   Designs tested:
   A. Clojure PHM: {[layer node-id] -> int[]}
   B. Chunked: PHM of chunks, each chunk holds N nodes
   C. PSS: Current persistent-sorted-set approach"
  (:require [clojure.core :as c]
            [me.tonsky.persistent-sorted-set :as pss]
            [criterium.core :as crit])
  (:import [java.util Random]))

;; =============================================================================
;; Test Parameters

(def ^:const M 16)        ; neighbors per node (upper layers)
(def ^:const M0 32)       ; neighbors at layer 0
(def ^:const N-NODES 10000)
(def ^:const CHUNK-SIZE 256)  ; nodes per chunk for Design B

;; =============================================================================
;; Design A: Plain Clojure PHM

(defn phm-create []
  {})

(defn phm-get-neighbors [phm layer node-id]
  (get phm [layer node-id]))

(defn phm-set-neighbors [phm layer node-id neighbors]
  (assoc phm [layer node-id] (int-array neighbors)))

;; =============================================================================
;; Design B: Chunked Storage

(defrecord ChunkedEdges [chunks ^int chunk-size ^int max-neighbors])

(defn chunked-create [chunk-size max-neighbors]
  (->ChunkedEdges {} chunk-size max-neighbors))

(defn- chunk-id ^long [^long node-id ^long chunk-size]
  (quot node-id chunk-size))

(defn- chunk-offset ^long [^long node-id ^long chunk-size ^long max-neighbors]
  (* (rem node-id chunk-size) max-neighbors))

(defn chunked-get-neighbors [^ChunkedEdges edges layer ^long node-id]
  (let [csize (long (.chunk-size edges))
        max-n (long (.max-neighbors edges))
        cid (chunk-id node-id csize)
        key [layer cid]]
    (when-let [^ints chunk (get (.chunks edges) key)]
      (let [offset (chunk-offset node-id csize max-n)
            ;; First int is count
            cnt (aget chunk (int offset))]
        (when (pos? cnt)
          (let [result (int-array cnt)]
            (System/arraycopy chunk (inc offset) result 0 cnt)
            result))))))

(defn chunked-set-neighbors [^ChunkedEdges edges layer ^long node-id neighbors]
  (let [csize (long (.chunk-size edges))
        max-n (long (.max-neighbors edges))
        cid (chunk-id node-id csize)
        key [layer cid]
        offset (int (chunk-offset node-id csize max-n))
        old-chunk (get (.chunks edges) key)
        ;; Clone or create chunk
        ;; Format: [count n0 n1 ... | count n0 n1 ... | ...]
        new-chunk (if old-chunk
                    (aclone ^ints old-chunk)
                    (int-array (* csize (inc max-n))))
        neighbors-arr (if (instance? (Class/forName "[I") neighbors)
                        neighbors
                        (int-array neighbors))
        cnt (alength ^ints neighbors-arr)]
    ;; Write count and neighbors
    (aset new-chunk offset cnt)
    (System/arraycopy neighbors-arr 0 new-chunk (inc offset) cnt)
    (->ChunkedEdges (assoc (.chunks edges) key new-chunk) csize max-n)))

;; =============================================================================
;; Design C: PSS (current approach)

(defn pss-comparator [a b]
  (let [[l1 n1] (if (map? a) (:key a) a)
        [l2 n2] (if (map? b) (:key b) b)
        c (Long/compare (long l1) (long l2))]
    (if (zero? c)
      (Long/compare (long n1) (long n2))
      c)))

(defn pss-create []
  (pss/sorted-set-by pss-comparator))

(defn pss-get-neighbors [edges layer node-id]
  (let [key [layer node-id]
        results (pss/slice edges {:key key} {:key [layer (inc node-id)]})]
    (when-let [entry (first results)]
      (when (= (:key entry) key)
        (:neighbors entry)))))

(defn pss-set-neighbors [edges layer node-id neighbors]
  (let [key [layer node-id]
        old-results (pss/slice edges {:key key} {:key [layer (inc node-id)]})
        old-entry (when-let [e (first old-results)]
                    (when (= (:key e) key) e))
        edges (if old-entry (c/disj edges old-entry) edges)]
    (c/conj edges {:key key :neighbors (vec neighbors)})))

;; =============================================================================
;; Workload Generation

(defn generate-random-neighbors [^Random rng ^long max-id ^long count]
  (let [result (int-array count)]
    (dotimes [i count]
      (aset result i (.nextInt rng (int max-id))))
    result))

(defn generate-workload
  "Generate test data: n-nodes with random neighbors."
  [n-nodes m0]
  (let [rng (Random. 42)]
    (vec (for [node-id (range n-nodes)]
           {:node-id node-id
            :neighbors (generate-random-neighbors rng n-nodes m0)}))))

;; =============================================================================
;; Benchmarks

(defn benchmark-sequential-insert
  "Benchmark building an index by inserting nodes sequentially."
  [workload impl-name create-fn set-fn]
  (println (format "\n=== %s: Sequential Insert ===" impl-name))
  (let [start (System/nanoTime)
        final-state (reduce (fn [state {:keys [node-id neighbors]}]
                              (set-fn state 0 node-id neighbors))
                            (create-fn)
                            workload)
        elapsed-ms (/ (- (System/nanoTime) start) 1e6)]
    (println (format "  Time: %.2f ms (%.0f inserts/sec)"
                     elapsed-ms
                     (/ (count workload) (/ elapsed-ms 1000))))
    {:state final-state :time-ms elapsed-ms}))

(defn benchmark-random-read
  "Benchmark random access pattern (like HNSW search)."
  [state n-nodes impl-name get-fn n-reads]
  (println (format "\n=== %s: Random Read ===" impl-name))
  (let [rng (Random. 123)
        ;; Pre-generate random node IDs to read
        read-ids (int-array n-reads)]
    (dotimes [i n-reads]
      (aset read-ids i (.nextInt rng (int n-nodes))))

    ;; Warmup
    (dotimes [i 1000]
      (get-fn state 0 (aget read-ids (rem i n-reads))))

    ;; Measure
    (let [start (System/nanoTime)]
      (dotimes [i n-reads]
        (get-fn state 0 (aget read-ids i)))
      (let [elapsed-ns (- (System/nanoTime) start)
            per-read-ns (/ elapsed-ns n-reads)]
        (println (format "  %d reads in %.2f ms" n-reads (double (/ elapsed-ns 1e6))))
        (println (format "  Per read: %.1f ns" (double per-read-ns)))
        {:time-ns elapsed-ns :per-read-ns per-read-ns}))))

(defn benchmark-cow-update
  "Benchmark copy-on-write update pattern."
  [state n-nodes impl-name set-fn get-fn n-updates]
  (println (format "\n=== %s: CoW Update ===" impl-name))
  (let [rng (Random. 456)
        neighbors (int-array M0)]
    (dotimes [i M0]
      (aset neighbors i (.nextInt rng (int n-nodes))))

    ;; Measure update + read pattern (like bidirectional insert)
    (let [start (System/nanoTime)
          final-state (loop [state state
                             i 0]
                        (if (< i n-updates)
                          (let [node-id (.nextInt rng (int n-nodes))
                                state' (set-fn state 0 node-id neighbors)
                                ;; Also read the updated node (verification)
                                _ (get-fn state' 0 node-id)]
                            (recur state' (inc i)))
                          state))
          elapsed-ns (- (System/nanoTime) start)
          per-op-ns (/ elapsed-ns n-updates)]
      (println (format "  %d updates in %.2f ms" n-updates (double (/ elapsed-ns 1e6))))
      (println (format "  Per update+read: %.1f ns" (double per-op-ns)))
      {:time-ns elapsed-ns :per-op-ns per-op-ns :final-state final-state})))

(defn benchmark-mixed-workload
  "Simulate HNSW search: many reads, occasional write."
  [state n-nodes impl-name get-fn set-fn n-ops read-ratio]
  (println (format "\n=== %s: Mixed Workload (%.0f%% reads) ===" impl-name (* 100 read-ratio)))
  (let [rng (Random. 789)
        neighbors (int-array M0)]
    (dotimes [i M0]
      (aset neighbors i (.nextInt rng (int n-nodes))))

    (let [start (System/nanoTime)
          final-state (loop [state state
                             i 0]
                        (if (< i n-ops)
                          (let [node-id (.nextInt rng (int n-nodes))]
                            (if (< (.nextDouble rng) read-ratio)
                              (do (get-fn state 0 node-id)
                                  (recur state (inc i)))
                              (recur (set-fn state 0 node-id neighbors) (inc i))))
                          state))
          elapsed-ns (- (System/nanoTime) start)
          per-op-ns (/ elapsed-ns n-ops)]
      (println (format "  %d ops in %.2f ms" n-ops (double (/ elapsed-ns 1e6))))
      (println (format "  Per op: %.1f ns" (double per-op-ns)))
      {:time-ns elapsed-ns :per-op-ns per-op-ns})))

;; =============================================================================
;; Main

(defn run-all-benchmarks
  "Run all benchmarks for all implementations."
  ([] (run-all-benchmarks N-NODES M0))
  ([n-nodes m0]
   (println "=" 60)
   (println "Edge Storage Benchmark")
   (println (format "  Nodes: %d, M0: %d, Chunk size: %d" n-nodes m0 CHUNK-SIZE))
   (println "=" 60)

   (let [workload (generate-workload n-nodes m0)

         ;; Build initial states
         phm-result (benchmark-sequential-insert workload "PHM"
                                                  phm-create phm-set-neighbors)
         chunked-result (benchmark-sequential-insert workload "Chunked"
                                                      #(chunked-create CHUNK-SIZE (inc m0))
                                                      chunked-set-neighbors)
         pss-result (benchmark-sequential-insert workload "PSS"
                                                  pss-create pss-set-neighbors)

         ;; Random read benchmarks
         n-reads 100000
         _ (benchmark-random-read (:state phm-result) n-nodes "PHM" phm-get-neighbors n-reads)
         _ (benchmark-random-read (:state chunked-result) n-nodes "Chunked" chunked-get-neighbors n-reads)
         _ (benchmark-random-read (:state pss-result) n-nodes "PSS" pss-get-neighbors n-reads)

         ;; CoW update benchmarks
         n-updates 10000
         _ (benchmark-cow-update (:state phm-result) n-nodes "PHM" phm-set-neighbors phm-get-neighbors n-updates)
         _ (benchmark-cow-update (:state chunked-result) n-nodes "Chunked" chunked-set-neighbors chunked-get-neighbors n-updates)
         _ (benchmark-cow-update (:state pss-result) n-nodes "PSS" pss-set-neighbors pss-get-neighbors n-updates)

         ;; Mixed workload (95% reads like search)
         n-mixed 50000
         _ (benchmark-mixed-workload (:state phm-result) n-nodes "PHM" phm-get-neighbors phm-set-neighbors n-mixed 0.95)
         _ (benchmark-mixed-workload (:state chunked-result) n-nodes "Chunked" chunked-get-neighbors chunked-set-neighbors n-mixed 0.95)
         _ (benchmark-mixed-workload (:state pss-result) n-nodes "PSS" pss-get-neighbors pss-set-neighbors n-mixed 0.95)]

     (println "\n" "=" 60)
     (println "Benchmark Complete")
     (println "=" 60))))

(defn -main [& args]
  (run-all-benchmarks))
