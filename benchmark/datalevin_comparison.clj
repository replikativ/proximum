(ns datalevin-comparison
  "Benchmark comparison between proximum and datalevin/usearch.

   Usage:
     clj -M:dev:benchmark -m datalevin-comparison sift10k

   Requires SIFT data in benchmark/data/<dataset>/ (base.npy, queries.npy, groundtruth.npy)"
  (:require [proximum.core :as pv]
            [datalevin.core :as d]
            [clojure.java.io :as io]
            [clojure.data.json :as json])
  (:import [java.nio ByteBuffer ByteOrder]
           [java.io RandomAccessFile]))

;; -----------------------------------------------------------------------------
;; Simple numpy loader (handles float32 and int32 arrays)

(defn- parse-npy-header [^bytes header-bytes]
  "Parse numpy .npy header to extract shape and dtype."
  (let [header-str (String. header-bytes "UTF-8")
        ;; Extract shape tuple, e.g., "(10000, 128)"
        shape-match (re-find #"\('shape':\s*\((\d+),\s*(\d+)\)\)" header-str)
        descr-match (re-find #"'descr':\s*'([^']+)'" header-str)]
    {:shape [(Long/parseLong (nth shape-match 1))
             (Long/parseLong (nth shape-match 2))]
     :dtype (nth descr-match 1)}))

(defn load-npy
  "Load a numpy .npy file. Returns {:shape [rows cols] :data float-array-of-arrays}"
  [^String path]
  (with-open [raf (RandomAccessFile. path "r")]
    ;; Read magic and version
    (let [magic (byte-array 6)
          _ (.readFully raf magic)
          _ (assert (= (vec magic) [0x93 0x4E 0x55 0x4D 0x50 0x59]) "Invalid npy magic")
          major (.readByte raf)
          minor (.readByte raf)
          ;; Read header length (little-endian uint16 for v1.0)
          header-len (let [b1 (.readByte raf)
                           b2 (.readByte raf)]
                       (bit-or (bit-and b1 0xFF)
                               (bit-shift-left (bit-and b2 0xFF) 8)))
          header-bytes (byte-array header-len)
          _ (.readFully raf header-bytes)
          {:keys [shape dtype]} (parse-npy-header header-bytes)
          [rows cols] shape
          ;; Read data
          data-size (* rows cols (if (= dtype "<f4") 4 4))
          data-bytes (byte-array data-size)
          _ (.readFully raf data-bytes)
          buf (ByteBuffer/wrap data-bytes)]
      (.order buf ByteOrder/LITTLE_ENDIAN)
      (cond
        ;; Float32
        (= dtype "<f4")
        {:shape shape
         :data (vec (for [_ (range rows)]
                      (let [row (float-array cols)]
                        (dotimes [j cols]
                          (aset row j (.getFloat buf)))
                        row)))}
        ;; Int32
        (= dtype "<i4")
        {:shape shape
         :data (vec (for [_ (range rows)]
                      (let [row (int-array cols)]
                        (dotimes [j cols]
                          (aset row j (.getInt buf)))
                        row)))}
        :else
        (throw (ex-info "Unsupported dtype" {:dtype dtype}))))))

(defn load-dataset [dataset]
  "Load SIFT dataset from numpy files."
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

(defn compute-recall [result-ids ground-truth-ids k]
  (let [result-set (set (take k result-ids))
        gt-set (set (take k ground-truth-ids))
        overlap (count (clojure.set/intersection result-set gt-set))]
    (/ overlap (double k))))

;; -----------------------------------------------------------------------------
;; proximum benchmark

(defn bench-proximum [{:keys [base queries groundtruth M ef-construction ef-search k]}]
  (println "\n=== proximum Benchmark ===")
  (let [vectors (:data base)
        query-vecs (:data queries)
        gt-data (:data groundtruth)
        [n-vectors dim] (:shape base)
        n-queries (first (:shape queries))
        path (str "/tmp/pv-bench-" (System/currentTimeMillis) ".bin")]

    (println (format "  Vectors: %d x %d" n-vectors dim))
    (println (format "  Queries: %d" n-queries))

    ;; Create index
    (let [idx (pv/create-index dim
                               :M M
                               :ef-construction ef-construction
                               :ef-search ef-search
                               :vectors-path path
                               :vectors-capacity (+ n-vectors 100))]
      (try
        ;; Benchmark insertion
        (println "\n  Inserting vectors (batch mode)...")
        (let [start (System/nanoTime)
              final-idx (pv/insert-batch idx vectors)
              insert-time-s (/ (- (System/nanoTime) start) 1e9)]

          (println (format "  Insert: %.2fs (%.0f vec/sec)"
                           insert-time-s (/ n-vectors insert-time-s)))

          ;; Warmup
          (dotimes [_ 10]
            (pv/search final-idx (first query-vecs) k))

          ;; Benchmark search
          (println (format "\n  Searching (ef=%d, k=%d)..." ef-search k))
          (let [search-results (atom [])
                latencies (doall
                            (for [q query-vecs]
                              (let [start (System/nanoTime)
                                    results (pv/search final-idx q k :ef ef-search)
                                    elapsed (- (System/nanoTime) start)]
                                (swap! search-results conj (map :id results))
                                elapsed)))
                stats (benchmark-stats latencies)]

            (println (format "  Search: mean=%.1fµs, p50=%.1fµs, p99=%.1fµs"
                             (:mean-us stats) (:p50-us stats) (:p99-us stats)))
            (println (format "  QPS: %.0f" (:qps stats)))

            ;; Compute recall
            (let [recalls (map-indexed
                            (fn [i result-ids]
                              (compute-recall result-ids (vec (aget ^"[[I" (into-array gt-data) i)) k))
                            @search-results)
                  mean-recall (/ (reduce + recalls) (count recalls))]

              (println (format "  Recall@%d: %.2f%%" k (* 100.0 mean-recall)))

              {:library "proximum"
               :insert-time-s insert-time-s
               :insert-throughput (/ n-vectors insert-time-s)
               :search-stats stats
               :recall mean-recall})))

        (finally
          (pv/close! idx)
          (.delete (java.io.File. path)))))))

;; -----------------------------------------------------------------------------
;; datalevin/usearch benchmark

(defn bench-datalevin [{:keys [base queries groundtruth M ef-construction ef-search k]}]
  (println "\n=== datalevin/usearch Benchmark ===")
  (let [vectors (:data base)
        query-vecs (:data queries)
        gt-data (:data groundtruth)
        [n-vectors dim] (:shape base)
        n-queries (first (:shape queries))
        db-path (str "/tmp/datalevin-bench-" (System/currentTimeMillis))]

    (println (format "  Vectors: %d x %d" n-vectors dim))
    (println (format "  Queries: %d" n-queries))

    ;; Create datalevin connection with vector index
    (let [conn (d/get-conn db-path {})]
      (try
        ;; Create vector index
        (println "\n  Creating vector index...")
        (let [vec-idx (d/new-vector-index
                        (d/conn->db conn)
                        {:domain "bench"
                         :dimensions dim
                         :metric-type :euclidean
                         :quantization :float
                         :connectivity M
                         :expansion-add ef-construction
                         :expansion-search ef-search})]

          ;; Benchmark insertion
          (println "  Inserting vectors...")
          (let [start (System/nanoTime)
                _ (doseq [[i v] (map-indexed vector vectors)]
                    (d/add-vec vec-idx i v))
                insert-time-s (/ (- (System/nanoTime) start) 1e9)]

            (println (format "  Insert: %.2fs (%.0f vec/sec)"
                             insert-time-s (/ n-vectors insert-time-s)))

            ;; Warmup
            (dotimes [_ 10]
              (d/search-vec vec-idx (first query-vecs) {:top k}))

            ;; Benchmark search
            (println (format "\n  Searching (k=%d)..." k))
            (let [search-results (atom [])
                  latencies (doall
                              (for [q query-vecs]
                                (let [start (System/nanoTime)
                                      results (d/search-vec vec-idx q {:top k :display :refs})
                                      elapsed (- (System/nanoTime) start)]
                                  (swap! search-results conj results)
                                  elapsed)))
                  stats (benchmark-stats latencies)]

              (println (format "  Search: mean=%.1fµs, p50=%.1fµs, p99=%.1fµs"
                               (:mean-us stats) (:p50-us stats) (:p99-us stats)))
              (println (format "  QPS: %.0f" (:qps stats)))

              ;; Compute recall
              (let [recalls (map-indexed
                              (fn [i result-ids]
                                (compute-recall result-ids (vec (aget ^"[[I" (into-array gt-data) i)) k))
                              @search-results)
                    mean-recall (/ (reduce + recalls) (count recalls))]

                (println (format "  Recall@%d: %.2f%%" k (* 100.0 mean-recall)))

                {:library "datalevin/usearch"
                 :insert-time-s insert-time-s
                 :insert-throughput (/ n-vectors insert-time-s)
                 :search-stats stats
                 :recall mean-recall}))))

        (finally
          (d/close conn)
          ;; Clean up
          (doseq [f (file-seq (io/file db-path))]
            (.delete f)))))))

;; -----------------------------------------------------------------------------
;; Main

(defn -main [& args]
  (let [dataset (or (first args) "sift10k")
        M (if (second args) (Integer/parseInt (second args)) 16)
        ef-c (if (nth args 2 nil) (Integer/parseInt (nth args 2)) 200)
        ef-s (if (nth args 3 nil) (Integer/parseInt (nth args 3)) 100)
        k 10]

    (println "╔══════════════════════════════════════════════════════════════════╗")
    (println "║  HNSW Benchmark: proximum vs datalevin/usearch        ║")
    (println "╚══════════════════════════════════════════════════════════════════╝")
    (println (format "\nDataset: %s, M=%d, ef_construction=%d, ef_search=%d, k=%d"
                     dataset M ef-c ef-s k))

    ;; Load dataset
    (println "\nLoading dataset...")
    (let [data (load-dataset dataset)
          config {:base (:base data)
                  :queries (:queries data)
                  :groundtruth (:groundtruth data)
                  :M M
                  :ef-construction ef-c
                  :ef-search ef-s
                  :k k}]

      (println (format "  Base: %s" (pr-str (:shape (:base data)))))
      (println (format "  Queries: %s" (pr-str (:shape (:queries data)))))

      ;; Run benchmarks
      (let [pv-results (bench-proximum config)
            dl-results (bench-datalevin config)]

        (println "\n" (apply str (repeat 70 "=")))
        (println "SUMMARY")
        (println (apply str (repeat 70 "=")))
        (println (format "\n%-25s %20s %20s" "" "proximum" "datalevin/usearch"))
        (println (apply str (repeat 70 "-")))
        (println (format "%-25s %20.0f %20.0f" "Insert (vec/sec)"
                         (:insert-throughput pv-results)
                         (:insert-throughput dl-results)))
        (println (format "%-25s %20.0f %20.0f" "Search QPS"
                         (get-in pv-results [:search-stats :qps])
                         (get-in dl-results [:search-stats :qps])))
        (println (format "%-25s %20.1f %20.1f" "Search p50 (µs)"
                         (get-in pv-results [:search-stats :p50-us])
                         (get-in dl-results [:search-stats :p50-us])))
        (println (format "%-25s %20.1f %20.1f" "Search p99 (µs)"
                         (get-in pv-results [:search-stats :p99-us])
                         (get-in dl-results [:search-stats :p99-us])))
        (println (format "%-25s %20.2f%% %19.2f%%" "Recall@10"
                         (* 100.0 (:recall pv-results))
                         (* 100.0 (:recall dl-results))))))))
