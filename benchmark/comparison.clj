(ns comparison
  "Benchmark comparison between proximum and hnswlib on SIFT dataset.

   Run hnswlib benchmark first to download SIFT and generate baseline:
     source benchmark-env/bin/activate
     python benchmark/hnswlib_bench.py sift10k 16 200 100

   Then run this:
     clj -M:dev -m comparison sift10k"
  (:require [proximum.core :as core]
            [proximum.distance :as dist]
            [libpython-clj2.python :as py]
            [libpython-clj2.require :refer [require-python]]
            [tech.v3.datatype :as dtype]
            [clojure.data.json :as json]))

;; Initialize Python with the benchmark-env
(py/initialize! :python-executable "benchmark-env/bin/python3")

(require-python '[numpy :as np])

;; -----------------------------------------------------------------------------
;; Dataset loading via numpy

(defn load-dataset [dataset]
  "Load SIFT dataset from numpy files."
  (let [data-dir (str "benchmark/data/" dataset)]
    {:base (np/load (str data-dir "/base.npy"))
     :queries (np/load (str data-dir "/queries.npy"))
     :groundtruth (np/load (str data-dir "/groundtruth.npy"))}))

(defn get-vector [np-array idx]
  "Extract vector at index as float array."
  (let [row (py/get-item np-array idx)]
    (float-array (dtype/->float-array row))))

(defn get-groundtruth-set [gt-array idx k]
  "Get ground truth neighbor IDs as a set."
  (let [row (py/get-item gt-array idx)]
    (set (take k (dtype/->int-array row)))))

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

(defn compute-recall [hnsw-results ground-truth-set]
  (let [hnsw-set (set (map :id hnsw-results))
        overlap (count (clojure.set/intersection hnsw-set ground-truth-set))]
    (/ overlap (count ground-truth-set))))

;; -----------------------------------------------------------------------------
;; Main benchmark

(defn run-benchmark [{:keys [dataset M ef-construction ef-search k]}]
  (println "\n=== proximum Benchmark ===")
  (println (format "  dataset=%s, M=%d, ef_c=%d, ef_s=%d"
                   dataset M ef-construction ef-search))

  ;; Load dataset via numpy
  (println (format "\nLoading dataset %s..." dataset))
  (let [{:keys [base queries groundtruth]} (load-dataset dataset)
        base-shape (vec (py/get-attr base "shape"))
        queries-shape (vec (py/get-attr queries "shape"))
        gt-shape (vec (py/get-attr groundtruth "shape"))
        n-vectors (first base-shape)
        dim (second base-shape)
        n-queries (first queries-shape)]

    (println (format "  Loaded %d base vectors, dim=%d" n-vectors dim))
    (println (format "  Loaded %d queries" n-queries))
    (println (format "  Ground truth shape: %s" (pr-str gt-shape)))

    ;; Create index
    (println "\nCreating index...")
    (let [path (str "/tmp/bench-vectors-" (System/currentTimeMillis) ".bin")
          idx (core/create-index dim
                                 :M M
                                 :ef-construction ef-construction
                                 :ef-search ef-search
                                 :vectors-path path
                                 :vectors-capacity (+ n-vectors 100))]

      (try
        ;; Benchmark insertion (using bulk insert with transient mode)
        (println "\nBenchmarking insertion...")
        (let [_ (println "  Loading vectors...")
              all-vectors (mapv #(get-vector base %) (range n-vectors))
              _ (println "  Inserting with bulk insert (transient mode)...")
              start (System/nanoTime)
              final-idx (core/insert-batch idx all-vectors)
              insert-time (/ (- (System/nanoTime) start) 1e9)]

          (println)
          (println (format "  Insert: %.3fs (%.0f vec/sec)"
                           insert-time (/ n-vectors insert-time)))

          ;; Warmup search
          (dotimes [_ 10]
            (core/search final-idx (get-vector queries 0) k))

          ;; Benchmark search
          (println (format "\nBenchmarking search (ef=%d, k=%d, %d queries)..."
                           ef-search k n-queries))
          (let [search-results (atom [])
                latencies (doall
                            (for [i (range n-queries)]
                              (let [q (get-vector queries i)
                                    start (System/nanoTime)
                                    results (core/search final-idx q k {:ef ef-search})
                                    elapsed (- (System/nanoTime) start)]
                                (swap! search-results conj results)
                                elapsed)))
                stats (benchmark-stats latencies)]

            (println (format "  Search: mean=%.1fµs, p50=%.1fµs, p99=%.1fµs"
                             (:mean-us stats) (:p50-us stats) (:p99-us stats)))
            (println (format "  QPS: %.0f" (:qps stats)))

            ;; Compute recall against ground truth
            (println "\nComputing recall against ground truth...")
            (let [recalls (for [i (range n-queries)]
                            (let [hnsw-results (nth @search-results i)
                                  gt-set (get-groundtruth-set groundtruth i k)]
                              (compute-recall hnsw-results gt-set)))
                  mean-recall (/ (reduce + recalls) (count recalls))]

              (println (format "  Recall@%d: %.2f%%" k (* 100.0 mean-recall)))

              ;; Return results
              {:library "proximum"
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
               :recall_at_k mean-recall})))

        (finally
          (core/close! idx)
          (.delete (java.io.File. path)))))))

(defn -main [& args]
  (let [dataset (or (first args) "sift10k")
        M (if (second args) (Integer/parseInt (second args)) 16)
        ef-c (if (nth args 2 nil) (Integer/parseInt (nth args 2)) 200)
        ef-s (if (nth args 3 nil) (Integer/parseInt (nth args 3)) 100)
        k 10]

    (println "╔══════════════════════════════════════════════════════════════════╗")
    (println "║     HNSW Benchmark: proximum on SIFT dataset          ║")
    (println "╚══════════════════════════════════════════════════════════════════╝")

    (let [results (run-benchmark {:dataset dataset
                                  :M M
                                  :ef-construction ef-c
                                  :ef-search ef-s
                                  :k k})]

      (println "\n=== Results (JSON) ===")
      (println (json/write-str results)))))
