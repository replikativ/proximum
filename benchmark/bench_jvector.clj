(ns bench-jvector
  "Benchmark JVector (DataStax) on SIFT dataset.

   Usage:
     clj -M:benchmark -m bench-jvector sift10k
     clj -M:benchmark -m bench-jvector sift10k 16 200 100

   Outputs JSON results to stdout."
  (:require [clojure.data.json :as json]
            [clojure.set :as set])
  (:import [io.github.jbellis.jvector.graph GraphIndexBuilder GraphSearcher ListRandomAccessVectorValues]
           [io.github.jbellis.jvector.graph.similarity BuildScoreProvider]
           [io.github.jbellis.jvector.vector VectorSimilarityFunction VectorizationProvider]
           [io.github.jbellis.jvector.util Bits]
           [java.nio ByteBuffer ByteOrder]
           [java.io RandomAccessFile]
           [java.util.concurrent ForkJoinPool]))

;; -----------------------------------------------------------------------------
;; Dataset loading

(defn- parse-npy-header [^bytes header-bytes]
  (let [header-str (String. header-bytes "UTF-8")
        shape-match (re-find #"'shape':\s*\((\d+),\s*(\d+)\)" header-str)
        descr-match (re-find #"'descr':\s*'([^']+)'" header-str)]
    {:shape [(Long/parseLong (nth shape-match 1))
             (Long/parseLong (nth shape-match 2))]
     :dtype (nth descr-match 1)}))

(defn load-npy [^String path]
  (with-open [raf (RandomAccessFile. path "r")]
    (let [magic (byte-array 6)
          _ (.readFully raf magic)
          _ (assert (= (vec magic) [-109 78 85 77 80 89]) "Invalid npy magic")
          _major (.readByte raf)
          _minor (.readByte raf)
          header-len (let [b1 (.readByte raf) b2 (.readByte raf)]
                       (bit-or (bit-and b1 0xFF)
                               (bit-shift-left (bit-and b2 0xFF) 8)))
          header-bytes (byte-array header-len)
          _ (.readFully raf header-bytes)
          {:keys [shape dtype]} (parse-npy-header header-bytes)
          [rows cols] shape
          data-size (* rows cols 4)
          data-bytes (byte-array data-size)
          _ (.readFully raf data-bytes)
          buf (ByteBuffer/wrap data-bytes)]
      (.order buf ByteOrder/LITTLE_ENDIAN)
      (cond
        (= dtype "<f4")
        {:shape shape
         :data (vec (for [_ (range rows)]
                      (let [row (float-array cols)]
                        (dotimes [j cols]
                          (aset row j (.getFloat buf)))
                        row)))}
        (= dtype "<i4")
        {:shape shape
         :data (vec (for [_ (range rows)]
                      (let [row (int-array cols)]
                        (dotimes [j cols]
                          (aset row j (.getInt buf)))
                        row)))}))))

(defn load-dataset [dataset]
  (let [data-dir (str "benchmark/data/" dataset)]
    {:base (load-npy (str data-dir "/base.npy"))
     :queries (load-npy (str data-dir "/queries.npy"))
     :groundtruth (load-npy (str data-dir "/groundtruth.npy"))}))

;; -----------------------------------------------------------------------------
;; Benchmark utilities

(defn percentile [sorted-vals p]
  (let [n (count sorted-vals)
        idx (min (dec n) (int (* p n)))]
    (nth sorted-vals idx)))

(defn benchmark-stats [latencies-ns]
  (let [sorted (vec (sort latencies-ns))
        n (count sorted)
        mean (/ (reduce + sorted) n)
        p50 (percentile sorted 0.5)
        p99 (percentile sorted 0.99)]
    {:mean-us (/ mean 1e3)
     :p50-us (/ p50 1e3)
     :p99-us (/ p99 1e3)
     :qps (/ 1e9 mean)}))

(defn compute-recall [result-ids ground-truth-set]
  (let [result-set (set result-ids)
        overlap (count (set/intersection result-set ground-truth-set))]
    (/ overlap (count ground-truth-set))))

;; -----------------------------------------------------------------------------
;; JVector benchmark

(defn- glove-dataset? [dataset]
  (boolean (re-find #"glove" dataset)))

(defn run-benchmark [{:keys [dataset M ef-construction ef-search k]}]
  (let [use-cosine? (glove-dataset? dataset)
        similarity-fn (if use-cosine?
                        VectorSimilarityFunction/COSINE
                        VectorSimilarityFunction/EUCLIDEAN)]
    (println "\n=== JVector Benchmark ===")
    (println (format "  dataset=%s, M=%d, ef_c=%d, ef_s=%d, metric=%s"
                     dataset M ef-construction ef-search
                     (if use-cosine? "cosine" "euclidean")))

    ;; Initialize SIMD vectorization
    (VectorizationProvider/getInstance)

    (let [{:keys [base queries groundtruth]} (load-dataset dataset)
        vectors (:data base)
        query-vecs (:data queries)
        gt-data (:data groundtruth)
        n-vectors (count vectors)
        dim (alength ^floats (first vectors))
        n-queries (count query-vecs)
        ;; Get VectorTypeSupport for creating VectorFloat objects
        vts (.getVectorTypeSupport (VectorizationProvider/getInstance))
        ;; Wrap float[] in VectorFloat for JVector API
        wrapped-vectors (java.util.ArrayList. (map #(.createFloatVector vts ^floats %) vectors))
        wrapped-queries (vec (map #(.createFloatVector vts ^floats %) query-vecs))
        ;; Create RandomAccessVectorValues using ListRandomAccessVectorValues
        ravv (ListRandomAccessVectorValues. wrapped-vectors dim)]

    (println (format "  Loaded %d base vectors, dim=%d" n-vectors dim))
    (println (format "  Loaded %d queries" n-queries))

    ;; Build index with explicit parallelism
    (println "\nBuilding index...")
    (let [;; Use BuildScoreProvider for explicit control
          bsp (BuildScoreProvider/randomAccessScoreProvider ravv similarity-fn)
          ;; Use common pool for parallel build
          pool (ForkJoinPool/commonPool)
          builder (GraphIndexBuilder. bsp
                                      (int dim)
                                      (int M)
                                      (int ef-construction)
                                      (float 1.2)
                                      (float 1.2)
                                      pool  ;; simdExecutor
                                      pool) ;; parallelExecutor
          start (System/nanoTime)
          index (.build builder ravv)
          insert-time (/ (- (System/nanoTime) start) 1e9)]

      (println (format "  Insert: %.3fs (%.0f vec/sec)" insert-time (/ n-vectors insert-time)))

      ;; Create searcher instance for proper ef control via rerankK
      (let [searcher (GraphSearcher. index)]

        ;; Warmup
        (println "\nWarmup...")
        (dotimes [_ 10]
          (let [ssp (io.github.jbellis.jvector.graph.similarity.SearchScoreProvider/exact
                      (first wrapped-queries) similarity-fn ravv)]
            (.search searcher ssp k ef-search (float 0.0) (float 0.0) Bits/ALL)))

        ;; Benchmark search
        ;; JVector uses rerankK instead of ef - rerankK=ef-search for fair comparison
        (println (format "\nBenchmarking search (rerankK=%d, k=%d)..." ef-search k))
        (let [search-results (atom [])
              latencies (doall
                          (for [i (range n-queries)]
                            (let [q (nth wrapped-queries i)
                                  ssp (io.github.jbellis.jvector.graph.similarity.SearchScoreProvider/exact
                                        q similarity-fn ravv)
                                  start-time (System/nanoTime)
                                  ;; Use instance method with rerankK = ef-search for fair comparison
                                  results (.search searcher ssp k ef-search (float 0.0) (float 0.0) Bits/ALL)
                                  elapsed (- (System/nanoTime) start-time)
                                  result-ids (take k (map #(.node %) (seq (.getNodes results))))]
                              (swap! search-results conj result-ids)
                              elapsed)))
            stats (benchmark-stats latencies)]

        (println (format "  Search: mean=%.1fµs, p50=%.1fµs, p99=%.1fµs"
                         (:mean-us stats) (:p50-us stats) (:p99-us stats)))
        (println (format "  QPS: %.0f" (:qps stats)))

        ;; Compute recall
        (println "\nComputing recall...")
        (let [recalls (for [i (range n-queries)]
                        (let [result-ids (nth @search-results i)
                              gt-row (nth gt-data i)
                              gt-set (set (take k (seq gt-row)))]
                          (compute-recall result-ids gt-set)))
              mean-recall (/ (reduce + recalls) (count recalls))]

          (println (format "  Recall@%d: %.2f%%" k (* 100.0 mean-recall)))

          {:library "jvector"
           :dataset dataset
           :n_vectors n-vectors
           :dim dim
           :M M
           :ef_construction ef-construction
           :ef_search ef-search
           :k k
           :n_queries n-queries
           :insert_time_sec insert-time
           :insert_throughput (/ n-vectors insert-time)
           :search_latency_mean_us (:mean-us stats)
           :search_latency_p50_us (:p50-us stats)
           :search_latency_p99_us (:p99-us stats)
           :search_qps (:qps stats)
           :recall_at_k mean-recall})))))))

(defn -main [& args]
  (let [dataset (or (first args) "sift10k")
        M (if (second args) (Integer/parseInt (second args)) 16)
        ef-c (if (nth args 2 nil) (Integer/parseInt (nth args 2)) 200)
        ef-s (if (nth args 3 nil) (Integer/parseInt (nth args 3)) 100)
        k 10
        results (run-benchmark {:dataset dataset :M M :ef-construction ef-c :ef-search ef-s :k k})]
    (println (json/write-str results))))
