(ns bench-ef-sweep
  "Benchmark recall vs latency across multiple ef values.

   This benchmark creates recall@ef curves and measures latency distributions
   (p50, p95, p99, p999) at each ef value to understand tail latency behavior.

   Usage:
     clj -M:dev -m bench-ef-sweep sift10k
     clj -M:dev -m bench-ef-sweep sift1m --ef-values 50,100,150,200,300,500

   Outputs JSON array for plotting recall-latency curves."
  (:require [proximum.core :as pv]
            [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.core.async :as a]
            [bench-proximum])
  (:import [java.nio ByteBuffer ByteOrder]
           [java.io RandomAccessFile File]))

;; -----------------------------------------------------------------------------
;; Enhanced statistics with p95 and p999

(defn percentile [sorted-vals p]
  (let [n (count sorted-vals)
        idx (min (dec n) (int (* p n)))]
    (nth sorted-vals idx)))

(defn benchmark-stats-detailed
  "Compute detailed latency statistics including p95 and p999."
  [latencies-ns]
  (let [sorted (vec (sort latencies-ns))
        n (count sorted)
        mean (/ (reduce + sorted) n)
        p50 (percentile sorted 0.5)
        p95 (percentile sorted 0.95)
        p99 (percentile sorted 0.99)
        p999 (percentile sorted 0.999)]
    {:mean-us (/ mean 1e3)
     :p50-us (/ p50 1e3)
     :p95-us (/ p95 1e3)
     :p99-us (/ p99 1e3)
     :p999-us (/ p999 1e3)
     :qps (/ 1e9 mean)
     :variance-ratio-p99-p50 (/ p99 (double p50))
     :variance-ratio-p999-p50 (/ p999 (double p50))}))

(defn compute-recall [result-ids ground-truth-ids k]
  (let [result-set (set (take k result-ids))
        gt-set (set (take k ground-truth-ids))
        overlap (count (set/intersection result-set gt-set))]
    (/ overlap (double k))))

;; -----------------------------------------------------------------------------
;; Per-query statistics for tail latency analysis

(defn compute-per-query-stats
  "Analyze individual query characteristics for tail latency debugging."
  [idx query-vecs gt-data k ef]
  (let [results (atom [])]
    (doseq [[i q] (map-indexed vector query-vecs)]
      (let [start (System/nanoTime)
            search-results (pv/search idx q k {:ef ef})
            elapsed (- (System/nanoTime) start)
            result-ids (mapv :id search-results)
            ground-truth (vec (aget ^"[[I" (into-array gt-data) i))
            recall (compute-recall result-ids ground-truth k)]
        (swap! results conj
               {:query-idx i
                :latency-ns elapsed
                :latency-us (/ elapsed 1e3)
                :recall recall
                :num-results (count search-results)})))
    @results))

;; -----------------------------------------------------------------------------
;; Main ef sweep benchmark

(defn run-ef-sweep
  "Benchmark recall vs latency across multiple ef values."
  [{:keys [dataset M ef-construction ef-values k num-threads distance]
    :or {distance :euclidean
         k 10
         ef-values [50 75 100 150 200 300 500]}}]

  (binding [*out* *err*]
    (println (format "\n=== proximum ef Sweep Benchmark ==="))
    (println (format "  dataset=%s, M=%d, ef_c=%d, k=%d" dataset M ef-construction k))
    (println (format "  ef values to test: %s" (str/join ", " ef-values))))

  (let [{:keys [base queries groundtruth]} (bench-proximum/load-dataset dataset)
        vectors-data (:data base)
        query-vecs (:data queries)
        gt-data (:data groundtruth)
        [n-vectors dim] (:shape base)
        n-queries (first (:shape queries))
        base-path (str "/tmp/pv-ef-sweep-" (System/currentTimeMillis))
        store-path (str base-path "/store")
        mmap-dir (str base-path "/mmap")
        max-nodes (+ n-vectors 1000)]

    (binding [*out* *err*]
      (println (format "  Base: %d vectors, dim=%d, queries=%d" n-vectors dim n-queries)))

    (.mkdirs (File. base-path))
    (.mkdirs (File. mmap-dir))

    (let [idx (pv/create-index {:type :hnsw
                                :dim dim
                                :M M
                                :ef-construction ef-construction
                                :ef-search (first ef-values) ; Initial value, will override
                                :distance distance
                                :store-config {:backend :file
                                               :path store-path
                                               :id (java.util.UUID/randomUUID)}
                                :mmap-dir mmap-dir
                                :capacity max-nodes})]
      (try
        ;; Build index once (parallel insertion)
        (binding [*out* *err*]
          (println (format "\nBuilding index (parallel %d threads)..." num-threads)))
        (let [external-ids (vec (range n-vectors))
              start (System/nanoTime)
              idx2 (pv/insert-batch idx vectors-data external-ids {:parallelism num-threads})
              insert-time (/ (- (System/nanoTime) start) 1e9)]

          (binding [*out* *err*]
            (println (format "  Build complete: %.2fs (%.0f vec/sec)"
                             insert-time (/ n-vectors insert-time))))

          ;; Warmup queries
          (binding [*out* *err*]
            (println "\nWarming up..."))
          (dotimes [_ 20]
            (pv/search idx2 (first query-vecs) k {:ef (first ef-values)}))

          ;; Test each ef value
          (binding [*out* *err*]
            (println (format "\nTesting %d ef values..." (count ef-values))))

          (let [results
                (doall
                  (for [ef ef-values]
                    (do
                      (binding [*out* *err*]
                        (println (format "\n  Testing ef=%d..." ef)))

                      ;; Collect latencies for all queries
                      (let [search-results (atom [])
                            latencies (doall
                                        (for [q query-vecs]
                                          (let [start (System/nanoTime)
                                                results (pv/search idx2 q k {:ef ef})
                                                elapsed (- (System/nanoTime) start)]
                                            (swap! search-results conj (mapv :id results))
                                            elapsed)))
                            stats (benchmark-stats-detailed latencies)
                            recalls (map-indexed
                                      (fn [i result-ids]
                                        (compute-recall result-ids (vec (aget ^"[[I" (into-array gt-data) i)) k))
                                      @search-results)
                            mean-recall (/ (reduce + recalls) (count recalls))

                            ;; Find slowest queries for debugging
                            query-stats (compute-per-query-stats idx2 query-vecs gt-data k ef)
                            sorted-by-latency (sort-by :latency-us > query-stats)
                            slowest-queries (take 5 sorted-by-latency)]

                        (binding [*out* *err*]
                          (println (format "    Recall@%d: %.2f%%" k (* 100.0 mean-recall)))
                          (println (format "    p50=%.1fus, p95=%.1fus, p99=%.1fus, p999=%.1fus"
                                           (:p50-us stats) (:p95-us stats) (:p99-us stats) (:p999-us stats)))
                          (println (format "    p99/p50 ratio: %.2fx, p999/p50 ratio: %.2fx"
                                           (:variance-ratio-p99-p50 stats)
                                           (:variance-ratio-p999-p50 stats)))
                          (println (format "    QPS: %.0f" (:qps stats)))
                          (println (format "    Slowest query: %.1fus (query #%d, recall=%.2f%%)"
                                           (:latency-us (first slowest-queries))
                                           (:query-idx (first slowest-queries))
                                           (* 100.0 (:recall (first slowest-queries))))))

                        ;; Return result for this ef value
                        (merge
                          {:ef ef
                           :k k
                           :n_queries n-queries
                           :recall_at_k mean-recall
                           :recall_pct (* 100.0 mean-recall)}
                          stats
                          {:slowest_query_idx (:query-idx (first slowest-queries))
                           :slowest_query_latency_us (:latency-us (first slowest-queries))
                           :slowest_query_recall (:recall (first slowest-queries))})))))]

            ;; Return complete result set
            {:library "proximum"
             :dataset dataset
             :n_vectors n-vectors
             :dim dim
             :M M
             :ef_construction ef-construction
             :k k
             :num_threads num-threads
             :distance (name distance)
             :insert_time_sec insert-time
             :ef_sweep_results results}))

        (finally
          (a/<!! (pv/close! idx))
          (doseq [f (reverse (file-seq (File. base-path)))]
            (.delete f)))))))

;; -----------------------------------------------------------------------------
;; Command-line interface

(defn parse-ef-values [s]
  (mapv #(Integer/parseInt (str/trim %)) (str/split s #",")))

(defn -main [& args]
  (let [;; Check for flags
        use-cosine? (some #{"--cosine"} args)
        args (remove #{"--cosine"} args)

        ;; Parse ef-values flag
        ef-flag-idx (.indexOf (vec args) "--ef-values")
        custom-ef-values (when (>= ef-flag-idx 0)
                           (parse-ef-values (nth args (inc ef-flag-idx))))
        args (if (>= ef-flag-idx 0)
               (concat (take ef-flag-idx args) (drop (+ ef-flag-idx 2) args))
               args)

        distance (if use-cosine? :cosine :euclidean)
        dataset (or (first args) "sift10k")
        M (if (second args) (Integer/parseInt (second args)) 16)
        ef-c (if (nth args 2 nil) (Integer/parseInt (nth args 2)) 200)
        num-threads (if (nth args 3 nil) (Integer/parseInt (nth args 3)) 8)

        ;; Default ef values based on dataset size
        default-ef-values (if (str/includes? dataset "1m")
                            [50 100 150 200 300 500 1000]
                            [25 50 75 100 150 200 300 500])
        ef-values (or custom-ef-values default-ef-values)

        params {:dataset dataset
                :M M
                :ef-construction ef-c
                :ef-values ef-values
                :k 10
                :num-threads num-threads
                :distance distance}]

    (binding [*out* *err*]
      (println (format "\nStarting ef sweep benchmark with values: %s"
                       (str/join ", " ef-values))))

    (let [results (run-ef-sweep params)]
      ;; Output JSON to stdout
      (println (json/write-str results :escape-slash false)))))
