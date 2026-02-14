(ns runner
  "Pure Clojure benchmark runner for HNSW implementations.

   Usage:
     clj -M:benchmark -m runner --dataset sift10k
     clj -M:benchmark -m runner --dataset sift10k --only proximum
     clj -M:benchmark -m runner --dataset sift1m --runs 3

   Runs:
   - proximum (always)
   - jvector, lucene-hnsw, hnswlib-java, datalevin (JVM implementations)
   - hnswlib (C++ via Python - optional, only if Python env available)

   Results saved to benchmark/results/<dataset>.json"
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as str])
  (:import [java.io File]))

;; -----------------------------------------------------------------------------
;; Configuration

(def ^:dynamic *project-dir* (System/getProperty "user.dir"))
(def ^:dynamic *benchmark-dir* (str *project-dir* "/benchmark"))
(def ^:dynamic *results-dir* (str *benchmark-dir* "/results"))
(def ^:dynamic *data-dir* (str *benchmark-dir* "/data"))

(def datasets
  {:sift10k {:url "ftp://ftp.irisa.fr/local/texmex/corpus/siftsmall.tar.gz"
             :dir "siftsmall"
             :n-vectors 10000
             :dim 128
             :metric :euclidean}
   :sift1m {:url "ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz"
            :dir "sift"
            :n-vectors 1000000
            :dim 128
            :metric :euclidean}
   :glove100 {:url "http://ann-benchmarks.com/glove-100-angular.hdf5"
              :n-vectors 1183514
              :dim 100
              :metric :cosine}
   :glove10k {:subset-of :glove100
              :n-vectors 10000
              :dim 100
              :metric :cosine}
   :dbpedia-openai-100k {:source :huggingface
                         :repo "KShivendu/dbpedia-entities-openai-1M"
                         :n-vectors 100000
                         :dim 1536
                         :metric :cosine}
   :dbpedia-openai-1m {:source :huggingface
                       :repo "KShivendu/dbpedia-entities-openai-1M"
                       :n-vectors 1000000
                       :dim 1536
                       :metric :cosine}})

;; -----------------------------------------------------------------------------
;; Utilities

(defn println-stderr [& args]
  (binding [*out* *err*]
    (apply println args)))

(defn file-exists? [path]
  (.exists (io/file path)))

(defn ensure-dir [path]
  (.mkdirs (io/file path)))

(defn dataset-ready? [dataset-name]
  (let [data-path (str *data-dir* "/" (name dataset-name))]
    (and (file-exists? (str data-path "/base.npy"))
         (file-exists? (str data-path "/queries.npy"))
         (file-exists? (str data-path "/groundtruth.npy")))))

(defn download-dataset [dataset-name]
  "Download dataset using Python script (delegates to benchmark_datasets.py)"
  (println-stderr (format "Downloading dataset %s..." (name dataset-name)))
  (let [result (shell/sh "python3"
                         (str *benchmark-dir* "/benchmark_datasets.py")
                         (name dataset-name)
                         :dir *project-dir*)]
    (when-not (zero? (:exit result))
      (println-stderr "STDERR:" (:err result))
      (throw (ex-info "Dataset download failed"
                      {:dataset dataset-name
                       :exit (:exit result)
                       :stderr (:err result)})))
    (println-stderr "Dataset ready!")))

(defn ensure-dataset [dataset-name]
  (when-not (dataset-ready? dataset-name)
    (download-dataset dataset-name)))

;; -----------------------------------------------------------------------------
;; Benchmark runners

(defn parse-json-output [stdout]
  "Parse JSON from last line of stdout"
  (try
    (let [lines (str/split-lines stdout)
          json-line (last lines)]
      (json/read-str json-line :key-fn keyword))
    (catch Exception e
      (println-stderr "Failed to parse JSON from output:")
      (println-stderr stdout)
      nil)))

(defn run-clj-benchmark
  "Run a Clojure benchmark via clj command"
  [bench-ns dataset-name {:keys [M ef-construction ef-search threads warmup heap cosine?]
                          :or {M 16 ef-construction 200 ef-search 100 threads 8
                               warmup (if (str/includes? (name dataset-name) "1m") 1 2)
                               heap (if (str/includes? (name dataset-name) "1m") "8g" "4g")}}]
  (println-stderr (format "\n=== Running %s ===" bench-ns))
  (let [cmd ["clj"
             (str "-J-Xmx" heap)
             "-J--add-modules=jdk.incubator.vector"
             "-J--enable-native-access=ALL-UNNAMED"
             "-M:benchmark"
             "-m" bench-ns
             (name dataset-name)
             (str M)
             (str ef-construction)
             (str ef-search)
             (str threads)
             (str warmup)]
        cmd (if cosine? (conj cmd "--cosine") cmd)
        result (apply shell/sh (concat cmd [:dir *project-dir*]))]
    (if (zero? (:exit result))
      (parse-json-output (:out result))
      (do
        (println-stderr "FAILED:" bench-ns)
        (println-stderr "STDERR:" (:err result))
        nil))))

(defn run-proximum [dataset-name opts]
  (let [cosine? (= :cosine (get-in datasets [(keyword dataset-name) :metric]))]
    (run-clj-benchmark "bench-proximum" dataset-name (assoc opts :cosine? cosine?))))

(defn run-jvector [dataset-name opts]
  (run-clj-benchmark "bench-jvector" dataset-name opts))

(defn run-lucene [dataset-name opts]
  (run-clj-benchmark "bench-lucene" dataset-name opts))

(defn run-hnswlib-java [dataset-name opts]
  (run-clj-benchmark "bench-hnswlib-java" dataset-name opts))

(defn run-datalevin [dataset-name opts]
  (run-clj-benchmark "bench-datalevin" dataset-name opts))

(defn python-available? []
  (try
    (let [result (shell/sh "python3" "--version")]
      (zero? (:exit result)))
    (catch Exception _ false)))

(defn hnswlib-available? []
  (and (python-available?)
       (file-exists? (str *benchmark-dir* "/benchmark-env/bin/python"))))

(defn run-hnswlib [dataset-name {:keys [M ef-construction ef-search]
                                 :or {M 16 ef-construction 200 ef-search 100}}]
  (if (hnswlib-available?)
    (do
      (println-stderr "\n=== Running hnswlib (C++ via Python) ===")
      (let [python-exe (str *benchmark-dir* "/benchmark-env/bin/python")
            result (shell/sh python-exe
                             (str *benchmark-dir* "/hnswlib_bench.py")
                             (name dataset-name)
                             (str M)
                             (str ef-construction)
                             (str ef-search)
                             :dir *project-dir*)]
        (if (zero? (:exit result))
          (parse-json-output (:out result))
          (do
            (println-stderr "FAILED: hnswlib")
            (println-stderr "STDERR:" (:err result))
            nil))))
    (do
      (println-stderr "\n=== Skipping hnswlib (Python env not found) ===")
      (println-stderr "    To include hnswlib baseline:")
      (println-stderr "      python3 -m venv benchmark-env")
      (println-stderr "      source benchmark-env/bin/activate")
      (println-stderr "      pip install -r benchmark/requirements.txt")
      nil)))

;; -----------------------------------------------------------------------------
;; Aggregation

(defn aggregate-runs [results]
  "Aggregate multiple benchmark runs, computing mean and stddev"
  (if (<= (count results) 1)
    (first results)
    (let [template (first results)
          metrics [:insert_throughput :search_qps :search_latency_mean_us
                   :search_latency_p50_us :search_latency_p99_us :recall_at_k]
          aggregated (reduce
                       (fn [acc metric]
                         (let [values (keep metric results)
                               n (count values)]
                           (if (pos? n)
                             (let [mean (/ (reduce + values) n)
                                   variance (if (> n 1)
                                             (/ (reduce + (map #(* (- % mean) (- % mean)) values))
                                                (dec n))
                                             0)
                                   stddev (Math/sqrt variance)]
                               (assoc acc
                                      metric mean
                                      (keyword (str (name metric) "_stddev")) stddev
                                      (keyword (str (name metric) "_runs")) (vec values)))
                             acc)))
                       template
                       metrics)]
      (assoc aggregated :n_runs (count results)))))

(defn run-with-variance [run-fn dataset-name n-runs opts]
  "Run a benchmark multiple times and aggregate results"
  (loop [runs-left n-runs
         results []]
    (if (zero? runs-left)
      (aggregate-runs results)
      (do
        (println-stderr (format "\n--- Run %d/%d ---" (- n-runs runs-left -1) n-runs))
        (if-let [result (run-fn dataset-name opts)]
          (recur (dec runs-left) (conj results result))
          (recur (dec runs-left) results))))))

;; -----------------------------------------------------------------------------
;; Main runner

(defn run-suite [dataset-name {:keys [runs only skip-download]
                                :or {runs 1}
                                :as opts}]
  (println-stderr (apply str (repeat 60 "=")))
  (println-stderr "HNSW Benchmark Suite")
  (println-stderr (apply str (repeat 60 "=")))
  (println-stderr (format "Dataset: %s" (name dataset-name)))
  (println-stderr (format "Runs: %d" runs))

  ;; Ensure dataset
  (when-not skip-download
    (ensure-dataset dataset-name))

  ;; Run benchmarks
  (let [benchmarks (cond
                     (= only "proximum") [{:name "proximum" :fn run-proximum}]
                     (= only "jvector") [{:name "jvector" :fn run-jvector}]
                     (= only "lucene") [{:name "lucene" :fn run-lucene}]
                     (= only "hnswlib-java") [{:name "hnswlib-java" :fn run-hnswlib-java}]
                     (= only "datalevin") [{:name "datalevin" :fn run-datalevin}]
                     (= only "hnswlib") [{:name "hnswlib" :fn run-hnswlib}]
                     :else [{:name "proximum" :fn run-proximum}
                            {:name "jvector" :fn run-jvector}
                            {:name "lucene-hnsw" :fn run-lucene}
                            {:name "hnswlib-java" :fn run-hnswlib-java}
                            {:name "datalevin" :fn run-datalevin}
                            {:name "hnswlib" :fn run-hnswlib}])
        results (keep (fn [{:keys [name fn]}]
                        (println-stderr (format "\n\n### %s ###" name))
                        (run-with-variance fn dataset-name runs opts))
                      benchmarks)]

    ;; Save results
    (when (seq results)
      (ensure-dir *results-dir*)
      (let [results-file (str *results-dir* "/" (clojure.core/name dataset-name) ".json")]
        (spit results-file (json/write-str results :indent true))
        (println-stderr (format "\n%s\nResults saved to %s" (apply str (repeat 60 "=")) results-file))

        ;; Print summary
        (println-stderr (format "\n%s\nSUMMARY\n%s" (apply str (repeat 60 "=")) (apply str (repeat 60 "="))))
        (if (> runs 1)
          (do
            (println-stderr (format "\n%-25s %-18s %-18s %-10s"
                                   "Library" "Insert (vec/s)" "Search QPS" "Recall@10"))
            (println-stderr (apply str (repeat 71 "-")))
            (doseq [r results]
              (let [insert-str (format "%.0f±%.0f"
                                      (double (:insert_throughput r))
                                      (double (get r :insert_throughput_stddev 0)))
                    qps-str (format "%.0f±%.0f"
                                   (double (:search_qps r))
                                   (double (get r :search_qps_stddev 0)))]
                (println-stderr (format "%-25s %-18s %-18s %.2f%%"
                                       (:library r) insert-str qps-str
                                       (double (* 100 (:recall_at_k r))))))))
          (do
            (println-stderr (format "\n%-25s %-15s %-12s %-10s %-10s %-10s"
                                   "Library" "Insert (vec/s)" "Search QPS" "p50 (us)" "p99 (us)" "Recall@10"))
            (println-stderr (apply str (repeat 82 "-")))
            (doseq [r results]
              (println-stderr (format "%-25s %-15.0f %-12.0f %-10.1f %-10.1f %.2f%%"
                                     (:library r)
                                     (double (:insert_throughput r))
                                     (double (:search_qps r))
                                     (double (:search_latency_p50_us r))
                                     (double (:search_latency_p99_us r))
                                     (double (* 100 (:recall_at_k r))))))))))))

;; -----------------------------------------------------------------------------
;; CLI

(defn -main [& args]
  (let [parsed (loop [args args
                      opts {}]
                 (if (empty? args)
                   opts
                   (let [arg (first args)]
                     (cond
                       (= arg "--dataset")
                       (recur (drop 2 args) (assoc opts :dataset (keyword (second args))))

                       (= arg "--only")
                       (recur (drop 2 args) (assoc opts :only (second args)))

                       (= arg "--runs")
                       (recur (drop 2 args) (assoc opts :runs (Integer/parseInt (second args))))

                       (= arg "--M")
                       (recur (drop 2 args) (assoc opts :M (Integer/parseInt (second args))))

                       (= arg "--ef-construction")
                       (recur (drop 2 args) (assoc opts :ef-construction (Integer/parseInt (second args))))

                       (= arg "--ef-search")
                       (recur (drop 2 args) (assoc opts :ef-search (Integer/parseInt (second args))))

                       (= arg "--threads")
                       (recur (drop 2 args) (assoc opts :threads (Integer/parseInt (second args))))

                       (= arg "--skip-download")
                       (recur (rest args) (assoc opts :skip-download true))

                       ;; Positional argument - treat as dataset if no dataset set yet
                       (and (not (str/starts-with? arg "--"))
                            (not (:dataset opts)))
                       (recur (rest args) (assoc opts :dataset (keyword arg)))

                       :else
                       (do
                         (println-stderr "Unknown option:" arg)
                         (recur (rest args) opts))))))
        dataset (or (:dataset parsed) :sift10k)]

    (when-not (contains? datasets dataset)
      (println-stderr "Unknown dataset:" (name dataset))
      (println-stderr "Available:" (str/join ", " (map name (keys datasets))))
      (System/exit 1))

    (run-suite dataset parsed)))
