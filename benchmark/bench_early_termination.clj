(ns bench-early-termination
  "Benchmark early termination controls: timeout, distance budget, and patience.

   This benchmark tests proximum's three latency control mechanisms:
   1. Hard timeout (timeoutNanos) - checked every iteration
   2. Distance computation budget (maxDistanceComputations)
   3. Patience-based termination (ECIR 2025) - monitors result stability

   Usage:
     clj -M:dev -m bench-early-termination sift1m
     clj -M:dev -m bench-early-termination sift1m --test timeout
     clj -M:dev -m bench-early-termination sift1m --test patience
     clj -M:dev -m bench-early-termination sift1m --test budget
     clj -M:dev -m bench-early-termination sift1m --test all

   Outputs JSON for plotting recall-latency-control curves."
  (:require [proximum.core :as pv]
            [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.core.async :as a]
            [bench-proximum])
  (:import [java.io File]))

;; -----------------------------------------------------------------------------
;; Statistics

(defn percentile [sorted-vals p]
  (let [n (count sorted-vals)
        idx (min (dec n) (int (* p n)))]
    (nth sorted-vals idx)))

(defn benchmark-stats
  "Compute detailed latency statistics."
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
     :p99-p50-ratio (/ p99 (double p50))
     :p999-p50-ratio (/ p999 (double p50))}))

(defn compute-recall [result-ids ground-truth-ids k]
  (let [result-set (set (take k result-ids))
        gt-set (set (take k ground-truth-ids))
        overlap (count (set/intersection result-set gt-set))]
    (/ overlap (double k))))

;; -----------------------------------------------------------------------------
;; Test configurations

(defn timeout-configs
  "Test various timeout values with high ef to see how timeout caps latency."
  []
  [{:name "baseline-ef200" :ef 200 :timeout-ms nil}
   {:name "timeout-5ms" :ef 500 :timeout-ms 5}
   {:name "timeout-10ms" :ef 500 :timeout-ms 10}
   {:name "timeout-20ms" :ef 500 :timeout-ms 20}
   {:name "timeout-50ms" :ef 500 :timeout-ms 50}
   {:name "timeout-100ms" :ef 500 :timeout-ms 100}])

(defn budget-configs
  "Test distance computation budgets."
  []
  [{:name "baseline-ef200" :ef 200 :budget nil}
   {:name "budget-500" :ef 500 :budget 500}
   {:name "budget-1000" :ef 500 :budget 1000}
   {:name "budget-2000" :ef 500 :budget 2000}
   {:name "budget-5000" :ef 500 :budget 5000}
   {:name "budget-10000" :ef 500 :budget 10000}])

(defn patience-configs
  "Test patience-based termination with varying ef values."
  []
  [{:name "baseline-ef100-no-patience" :ef 100 :patience false}
   {:name "patience-ef100" :ef 100 :patience true}
   {:name "baseline-ef200-no-patience" :ef 200 :patience false}
   {:name "patience-ef200" :ef 200 :patience true}
   {:name "baseline-ef300-no-patience" :ef 300 :patience false}
   {:name "patience-ef300" :ef 300 :patience true}
   {:name "baseline-ef500-no-patience" :ef 500 :patience false}
   {:name "patience-ef500" :ef 500 :patience true}])

(defn combined-configs
  "Test realistic combinations of controls."
  []
  [{:name "real-time" :ef 150 :timeout-ms 10 :patience true}
   {:name "balanced" :ef 200 :patience true}
   {:name "high-accuracy" :ef 300 :budget 5000 :patience true}
   {:name "ultra-low-latency" :ef 100 :timeout-ms 5 :patience true}
   {:name "batch-processing" :ef 500 :budget 10000}])

;; -----------------------------------------------------------------------------
;; Benchmark execution

(defn build-search-options
  "Build SearchOptions map from config."
  [{:keys [ef timeout-ms budget patience]}]
  (cond-> {:ef ef}
    timeout-ms (assoc :timeout-millis timeout-ms)
    budget (assoc :max-distance-computations budget)
    patience (assoc :patience true)))

(defn run-test-config
  "Run benchmark for a single configuration."
  [idx query-vecs gt-data k config]
  (let [opts (build-search-options config)
        _ (binding [*out* *err*]
            (println (format "  Testing: %s (opts=%s)" (:name config) opts)))

        ;; Run all queries
        search-results (atom [])
        latencies (doall
                    (for [q query-vecs]
                      (let [start (System/nanoTime)
                            results (pv/search idx q k opts)
                            elapsed (- (System/nanoTime) start)]
                        (swap! search-results conj (mapv :id results))
                        elapsed)))

        ;; Compute statistics
        stats (benchmark-stats latencies)
        recalls (map-indexed
                  (fn [i result-ids]
                    (compute-recall result-ids (vec (aget ^"[[I" (into-array gt-data) i)) k))
                  @search-results)
        mean-recall (/ (reduce + recalls) (count recalls))

        ;; Find queries that hit termination early
        incomplete-results (count (filter #(< (count %) k) @search-results))

        result {:config-name (:name config)
                :search-options opts
                :recall-at-k mean-recall
                :recall-pct (* 100.0 mean-recall)
                :incomplete-results incomplete-results
                :incomplete-pct (* 100.0 (/ incomplete-results (double (count query-vecs))))}]

    (binding [*out* *err*]
      (println (format "    Recall@%d: %.2f%% (%.1f%% incomplete)"
                       k (* 100.0 mean-recall) (:incomplete-pct result)))
      (println (format "    p50=%.1fμs, p95=%.1fμs, p99=%.1fμs, p999=%.1fμs"
                       (:p50-us stats) (:p95-us stats) (:p99-us stats) (:p999-us stats)))
      (println (format "    p99/p50=%.2fx, QPS=%.0f"
                       (:p99-p50-ratio stats) (:qps stats))))

    (merge result stats)))

(defn run-early-termination-benchmark
  "Main benchmark runner."
  [{:keys [dataset M ef-construction k num-threads test-type]
    :or {test-type :all}}]

  (binding [*out* *err*]
    (println "\n=== Proximum Early Termination Benchmark ===")
    (println (format "  dataset=%s, M=%d, ef_c=%d, k=%d, test=%s"
                     dataset M ef-construction k (name test-type))))

  (let [{:keys [base queries groundtruth]} (bench-proximum/load-dataset dataset)
        vectors-data (:data base)
        query-vecs (:data queries)
        gt-data (:data groundtruth)
        [n-vectors dim] (:shape base)
        n-queries (first (:shape queries))
        base-path (str "/tmp/pv-early-term-" (System/currentTimeMillis))
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
                                :ef-search 200  ;; Will override per test
                                :distance :euclidean
                                :store-config {:backend :file
                                               :path store-path
                                               :id (java.util.UUID/randomUUID)}
                                :mmap-dir mmap-dir
                                :capacity max-nodes})]
      (try
        ;; Build index
        (binding [*out* *err*]
          (println (format "\nBuilding index (parallel %d threads)..." num-threads)))
        (let [external-ids (vec (range n-vectors))
              start (System/nanoTime)
              idx2 (pv/insert-batch idx vectors-data external-ids {:parallelism num-threads})
              insert-time (/ (- (System/nanoTime) start) 1e9)]

          (binding [*out* *err*]
            (println (format "  Build complete: %.2fs (%.0f vec/sec)"
                             insert-time (/ n-vectors insert-time))))

          ;; Warmup
          (binding [*out* *err*]
            (println "\nWarming up..."))
          (dotimes [_ 20]
            (pv/search idx2 (first query-vecs) k {:ef 200}))

          ;; Select test configurations
          (let [configs (case test-type
                         :timeout (timeout-configs)
                         :budget (budget-configs)
                         :patience (patience-configs)
                         :combined (combined-configs)
                         :all (concat (timeout-configs)
                                      (budget-configs)
                                      (patience-configs)
                                      (combined-configs)))

                _ (binding [*out* *err*]
                    (println (format "\nTesting %d configurations...\n" (count configs))))

                results (doall
                          (for [config configs]
                            (run-test-config idx2 query-vecs gt-data k config)))]

            {:library "proximum"
             :dataset dataset
             :n-vectors n-vectors
             :dim dim
             :M M
             :ef-construction ef-construction
             :k k
             :num-threads num-threads
             :test-type (name test-type)
             :insert-time-sec insert-time
             :results results}))

        (finally
          (a/<!! (pv/close! idx))
          (doseq [f (reverse (file-seq (File. base-path)))]
            (.delete f)))))))

;; -----------------------------------------------------------------------------
;; CLI

(defn parse-test-type [s]
  (keyword (str/lower-case s)))

(defn -main [& args]
  (let [;; Parse --test flag
        test-flag-idx (.indexOf (vec args) "--test")
        test-type (if (>= test-flag-idx 0)
                    (parse-test-type (nth args (inc test-flag-idx)))
                    :all)
        args (if (>= test-flag-idx 0)
               (concat (take test-flag-idx args) (drop (+ test-flag-idx 2) args))
               args)

        dataset (or (first args) "sift10k")
        M (if (second args) (Integer/parseInt (second args)) 16)
        ef-c (if (nth args 2 nil) (Integer/parseInt (nth args 2)) 200)
        num-threads (if (nth args 3 nil) (Integer/parseInt (nth args 3)) 8)

        params {:dataset dataset
                :M M
                :ef-construction ef-c
                :k 10
                :num-threads num-threads
                :test-type test-type}]

    (binding [*out* *err*]
      (println (format "\nStarting early termination benchmark (test=%s)" (name test-type))))

    (let [results (run-early-termination-benchmark params)]
      ;; Output JSON to stdout
      (println (json/write-str results :escape-slash false)))))
