(ns bench-datalevin
  "Benchmark datalevin/usearch on SIFT dataset.

   This script is designed to run in a separate JVM to avoid dependency conflicts.

   Usage:
     clj -Sdeps '{:deps {datalevin/datalevin {:mvn/version \"0.9.27\"} org.clojure/data.json {:mvn/version \"2.5.1\"}}}' \\
         -J--enable-native-access=ALL-UNNAMED \\
         -M -m bench-datalevin sift10k

   Outputs JSON results to stdout."
  (:require [datalevin.core :as d]
            [clojure.data.json :as json]
            [clojure.set :as set])
  (:import [java.nio ByteBuffer ByteOrder]
           [java.io RandomAccessFile File]))

;; -----------------------------------------------------------------------------
;; Pure Clojure numpy loader

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
          _major (.readByte raf)
          _minor (.readByte raf)
          header-len (let [b1 (.readByte raf) b2 (.readByte raf)]
                       (bit-or (bit-and b1 0xFF)
                               (bit-shift-left (bit-and b2 0xFF) 8)))
          header-bytes (byte-array header-len)
          _ (.readFully raf header-bytes)
          {:keys [shape dtype]} (parse-npy-header header-bytes)
          [rows cols] shape
          data-bytes (byte-array (* rows cols 4))
          _ (.readFully raf data-bytes)
          buf (ByteBuffer/wrap data-bytes)]
      (.order buf ByteOrder/LITTLE_ENDIAN)
      (if (= dtype "<f4")
        {:shape shape
         :data (vec (for [_ (range rows)]
                      (let [row (float-array cols)]
                        (dotimes [j cols] (aset row j (.getFloat buf)))
                        row)))}
        {:shape shape
         :data (vec (for [_ (range rows)]
                      (let [row (int-array cols)]
                        (dotimes [j cols] (aset row j (.getInt buf)))
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

(defn compute-recall [result-ids gt-ids k]
  (let [result-set (set (take k result-ids))
        gt-set (set (take k (seq gt-ids)))]
    (/ (count (set/intersection result-set gt-set)) (double k))))

;; -----------------------------------------------------------------------------
;; Main benchmark

(defn- glove-dataset? [dataset]
  (boolean (re-find #"glove" dataset)))

(defn run-benchmark [{:keys [dataset M ef-construction ef-search k]}]
  (let [use-cosine? (glove-dataset? dataset)
        metric-type (if use-cosine? :cosine :euclidean)]
    (binding [*out* *err*]
      (println "\n=== datalevin/usearch Benchmark ===")
      (println (format "  dataset=%s, M=%d, ef_c=%d, ef_s=%d, k=%d, metric=%s"
                       dataset M ef-construction ef-search k (name metric-type))))

    ;; Load dataset
    (binding [*out* *err*]
      (println (format "\nLoading dataset %s..." dataset)))
    (let [{:keys [base queries groundtruth]} (load-dataset dataset)
          vectors (:data base)
          query-vecs (:data queries)
          gt-vecs (:data groundtruth)
          [n-vectors dim] (:shape base)
          n-queries (first (:shape queries))
          db-path (str "/tmp/dl-bench-" (System/currentTimeMillis))]

    (binding [*out* *err*]
      (println (format "  Base: %d vectors, dim=%d" n-vectors dim))
      (println (format "  Queries: %d" n-queries)))

    ;; Create LMDB and vector index
    (let [lmdb (d/open-kv db-path)
          vec-idx (atom nil)]
      (try
        (binding [*out* *err*]
          (println "\nCreating vector index..."))
        (let [idx (d/new-vector-index
                        lmdb
                        {:domain "bench"
                         :dimensions dim
                         :metric-type metric-type
                         :quantization :float
                         :connectivity M
                         :expansion-add ef-construction
                         :expansion-search ef-search})
              _ (reset! vec-idx idx)]

          ;; Benchmark insertion
          (binding [*out* *err*]
            (println "Benchmarking insertion..."))
          (let [start (System/nanoTime)
                _ (doseq [[i v] (map-indexed vector vectors)]
                    (d/add-vec idx i v))
                insert-time (/ (- (System/nanoTime) start) 1e9)]

            (binding [*out* *err*]
              (println (format "  Insert: %.2fs (%.0f vec/sec)"
                               insert-time (/ n-vectors insert-time))))

            ;; Warmup
            (dotimes [_ 10]
              (d/search-vec idx (first query-vecs) k))

            ;; Benchmark search
            (binding [*out* *err*]
              (println (format "\nBenchmarking search (k=%d)..." k)))
            (let [search-results (atom [])
                  latencies (doall
                              (for [q query-vecs]
                                (let [start (System/nanoTime)
                                      results (d/search-vec idx q k)
                                      elapsed (- (System/nanoTime) start)]
                                  (swap! search-results conj results)
                                  elapsed)))
                  stats (benchmark-stats latencies)]

              (binding [*out* *err*]
                (println (format "  Search: mean=%.1fus, p50=%.1fus, p99=%.1fus"
                                 (:mean-us stats) (:p50-us stats) (:p99-us stats)))
                (println (format "  QPS: %.0f" (:qps stats))))

              ;; Compute recall
              (binding [*out* *err*]
                (println "\nComputing recall..."))
              (let [recalls (map-indexed
                              (fn [i result-ids]
                                (compute-recall result-ids (nth gt-vecs i) k))
                              @search-results)
                    mean-recall (/ (reduce + recalls) (count recalls))]

                (binding [*out* *err*]
                  (println (format "  Recall@%d: %.2f%%" k (* 100.0 mean-recall))))

                ;; Return results
                {:library "datalevin/usearch"
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
                 :recall_at_k mean-recall}))))

        (finally
          ;; Important: close vector index BEFORE closing LMDB to prevent thread leak
          (try
            (when-let [idx @vec-idx]
              (d/close-vector-index idx))
            (catch Exception e
              (binding [*out* *err*]
                (println "Warning: failed to close vector index:" (.getMessage e)))))
          (d/close-kv lmdb)
          ;; Clean up
          (doseq [f (reverse (file-seq (File. db-path)))]
            (.delete f))))))))

(defn -main [& args]
  (let [dataset (or (first args) "sift10k")
        M (if (second args) (Integer/parseInt (second args)) 16)
        ef-c (if (nth args 2 nil) (Integer/parseInt (nth args 2)) 200)
        ef-s (if (nth args 3 nil) (Integer/parseInt (nth args 3)) 100)
        k 10
        results (run-benchmark {:dataset dataset
                                :M M
                                :ef-construction ef-c
                                :ef-search ef-s
                                :k k})]
    ;; Output JSON to stdout
    (println (json/write-str results))))
