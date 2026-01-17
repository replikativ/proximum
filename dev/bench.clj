(ns bench
  "Benchmarks for proximum.

   Run with: clj -M:dev -e '(require (quote bench)) (bench/run-all)'

   Compares our CoW HNSW with hnswlib via exported vectors for Python comparison."
  (:require [proximum.core :as core]
            [proximum.vectors :as vectors]
            [proximum.distance :as dist]
            [clojure.set :as set]
            [clojure.java.io :as io])
  (:import [java.io File DataOutputStream FileOutputStream]
           [java.nio ByteBuffer ByteOrder]))

;; -----------------------------------------------------------------------------
;; Vector Generation

(defn random-vec
  "Generate a random float vector of given dimension."
  [dim]
  (float-array (repeatedly dim #(- (rand 2.0) 1.0))))

(defn random-vectors
  "Generate n random vectors of given dimension."
  [n dim]
  (vec (repeatedly n #(random-vec dim))))

;; -----------------------------------------------------------------------------
;; Ground Truth (Brute Force)

(defn brute-force-knn
  "Compute exact k-NN using brute force."
  [vectors query k distance-fn]
  (->> vectors
       (map-indexed (fn [i v]
                      {:id i :distance (dist/distance-squared distance-fn query v)}))
       (sort-by :distance)
       (take k)
       (mapv :id)))

;; -----------------------------------------------------------------------------
;; Recall Calculation

(defn recall
  "Calculate recall@k between approximumimate and exact results."
  [approximum exact]
  (let [approximum-set (set approximum)
        exact-set (set exact)]
    (/ (count (set/intersection approximum-set exact-set))
       (count exact-set))))

;; -----------------------------------------------------------------------------
;; Benchmarking Utilities

(defmacro bench
  "Benchmark an expression, return {:time-ms :result}."
  [expr]
  `(let [start# (System/nanoTime)
         result# ~expr
         end# (System/nanoTime)]
     {:time-ms (/ (- end# start#) 1e6)
      :result result#}))

(defn format-rate
  "Format operations per second."
  [count time-ms]
  (format "%.1f/sec" (/ count (/ time-ms 1000.0))))

;; -----------------------------------------------------------------------------
;; Export for Python hnswlib comparison

(defn export-vectors-binary
  "Export vectors to binary file for Python comparison.
   Format: [n:int32][dim:int32][v1:float32*dim][v2:float32*dim]..."
  [vectors path]
  (let [n (count vectors)
        dim (alength ^floats (first vectors))]
    (with-open [out (DataOutputStream. (FileOutputStream. path))]
      ;; Write header
      (.writeInt out n)
      (.writeInt out dim)
      ;; Write vectors (little-endian for numpy compatibility)
      (doseq [^floats v vectors]
        (let [bb (ByteBuffer/allocate (* dim 4))]
          (.order bb ByteOrder/LITTLE_ENDIAN)
          (doseq [f v]
            (.putFloat bb f))
          (.write out (.array bb)))))))

(defn export-queries-binary
  "Export query vectors to binary file."
  [queries path]
  (export-vectors-binary queries path))

(defn export-ground-truth
  "Export ground truth k-NN indices to binary file.
   Format: [n:int32][k:int32][q1_neighbors:int32*k][q2_neighbors:int32*k]..."
  [ground-truth path]
  (let [n (count ground-truth)
        k (count (first ground-truth))]
    (with-open [out (DataOutputStream. (FileOutputStream. path))]
      (.writeInt out n)
      (.writeInt out k)
      (doseq [neighbors ground-truth]
        (let [bb (ByteBuffer/allocate (* k 4))]
          (.order bb ByteOrder/LITTLE_ENDIAN)
          (doseq [id neighbors]
            (.putInt bb (int id)))
          (.write out (.array bb)))))))

;; -----------------------------------------------------------------------------
;; Main Benchmark

(defn run-insert-benchmark
  "Benchmark insert performance."
  [n dim M ef-construction]
  (println (format "\n=== Insert Benchmark: n=%d, dim=%d, M=%d, ef=%d ==="
                   n dim M ef-construction))
  (let [path (str (System/getProperty "java.io.tmpdir")
                  "/bench-vectors-" (System/currentTimeMillis) ".bin")
        vecs (random-vectors n dim)
        _ (println "Generated" n "random vectors")

        idx (core/create-index dim
                               :M M
                               :ef-construction ef-construction
                               :vectors-path path
                               :vectors-capacity (+ n 100))

        {:keys [time-ms result]} (bench (reduce core/insert idx vecs))
        final-idx result]

    (println (format "Insert time: %.1f ms (%s)" time-ms (format-rate n time-ms)))
    (println "Vectors in index:" (core/count-vectors final-idx))

    {:index final-idx
     :vectors vecs
     :path path
     :insert-time-ms time-ms
     :insert-rate (/ n (/ time-ms 1000.0))}))

(defn run-search-benchmark
  "Benchmark search performance and recall."
  [idx vectors n-queries k ef]
  (println (format "\n=== Search Benchmark: queries=%d, k=%d, ef=%d ==="
                   n-queries k ef))
  (let [queries (random-vectors n-queries (:dim idx))
        distance-fn (:distance-fn idx)

        ;; Compute ground truth
        ground-truth (mapv #(brute-force-knn vectors % k distance-fn) queries)

        ;; Run HNSW search
        {:keys [time-ms result]}
        (bench
          (mapv (fn [q] (mapv :id (core/search idx q k :ef ef))) queries))
        hnsw-results result

        ;; Calculate recall
        recalls (map recall hnsw-results ground-truth)
        mean-recall (/ (reduce + recalls) (count recalls))]

    (println (format "Search time: %.1f ms (%s)" time-ms (format-rate n-queries time-ms)))
    (println (format "Mean Recall@%d: %.2f%%" k (* 100.0 mean-recall)))

    {:queries queries
     :ground-truth ground-truth
     :hnsw-results hnsw-results
     :search-time-ms time-ms
     :search-qps (/ n-queries (/ time-ms 1000.0))
     :mean-recall mean-recall}))

(defn run-benchmark
  "Run full benchmark suite."
  [& {:keys [n dim M ef-construction n-queries k ef export-path]
      :or {n 10000
           dim 128
           M 16
           ef-construction 200
           n-queries 100
           k 10
           ef 50}}]

  (println "\n" (apply str (repeat 60 "=")))
  (println " Persistent proximum Benchmark")
  (println (apply str (repeat 60 "=")))

  (let [insert-result (run-insert-benchmark n dim M ef-construction)
        {:keys [index vectors path]} insert-result

        search-result (run-search-benchmark index vectors n-queries k ef)]

    ;; Export for Python comparison if requested
    (when export-path
      (println "\n=== Exporting for Python comparison ===")
      (let [vec-path (str export-path "/vectors.bin")
            query-path (str export-path "/queries.bin")
            gt-path (str export-path "/ground_truth.bin")]
        (.mkdirs (File. export-path))
        (export-vectors-binary vectors vec-path)
        (export-queries-binary (:queries search-result) query-path)
        (export-ground-truth (:ground-truth search-result) gt-path)
        (println "Exported to:" export-path)))

    ;; Cleanup
    (core/close! index)
    (.delete (File. path))

    ;; Summary
    (println "\n" (apply str (repeat 60 "=")))
    (println " Summary")
    (println (apply str (repeat 60 "=")))
    (println (format "Dataset: %d vectors, %d dimensions" n dim))
    (println (format "Parameters: M=%d, ef_construction=%d, ef_search=%d" M ef-construction ef))
    (println (format "Insert: %.1f vectors/sec" (:insert-rate insert-result)))
    (println (format "Search: %.1f QPS" (:search-qps search-result)))
    (println (format "Recall@%d: %.2f%%" k (* 100.0 (:mean-recall search-result))))

    {:insert insert-result
     :search search-result}))

(defn run-all
  "Run standard benchmark suite with multiple configurations."
  []
  ;; Small test
  (run-benchmark :n 1000 :dim 128 :M 16 :ef-construction 100 :n-queries 50 :k 10 :ef 50)

  ;; Medium test
  (run-benchmark :n 10000 :dim 128 :M 16 :ef-construction 200 :n-queries 100 :k 10 :ef 100)

  ;; Export for hnswlib comparison
  (run-benchmark :n 10000 :dim 128 :M 16 :ef-construction 200 :n-queries 100 :k 10 :ef 100
                 :export-path "/tmp/proximum-bench"))

(defn run-quick
  "Quick sanity check benchmark."
  []
  (run-benchmark :n 1000 :dim 64 :M 8 :ef-construction 50 :n-queries 20 :k 5 :ef 30))
