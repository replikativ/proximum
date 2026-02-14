(ns bench-proximum
  "Benchmark proximum on SIFT dataset.

   Usage:
     clj -M:dev -m bench-proximum sift10k
     clj -M:dev -m bench-proximum sift10k 16 200 100
     clj -M:dev -m bench-proximum sift10k 16 200 100 8

   Outputs JSON results to stdout."
  (:require [proximum.core :as pv]
            [proximum.protocols :as p]
            [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.core.async :as a])
  (:import [java.nio ByteBuffer ByteOrder]
           [java.io RandomAccessFile File]
           [proximum.internal PersistentEdgeStore]))

;; -----------------------------------------------------------------------------
;; Memory utilities

(defn gc-and-get-heap-mb
  "Force GC and return used heap in MB."
  []
  (System/gc)
  (Thread/sleep 100)
  (System/gc)
  (let [rt (Runtime/getRuntime)]
    (/ (- (.totalMemory rt) (.freeMemory rt)) 1024.0 1024.0)))

(defn get-storage-size-mb
  "Calculate storage size: vectors (mmap) + edges."
  [n-vectors dim ^PersistentEdgeStore pes]
  (let [vector-bytes (* n-vectors dim 4)  ; float32
        edge-bytes (.getStorageBytes pes)]
    {:vector_storage_mb (/ vector-bytes 1024.0 1024.0)
     :edge_storage_mb (/ edge-bytes 1024.0 1024.0)
     :total_storage_mb (/ (+ vector-bytes edge-bytes) 1024.0 1024.0)}))

;; -----------------------------------------------------------------------------
;; Pure Clojure numpy loader (no libpython-clj dependency)

(defn- parse-npy-header [^bytes header-bytes]
  (let [header-str (String. header-bytes "UTF-8")
        shape-match (re-find #"'shape':\s*\((\d+),\s*(\d+)\)" header-str)
        descr-match (re-find #"'descr':\s*'([^']+)'" header-str)]
    {:shape [(Long/parseLong (nth shape-match 1))
             (Long/parseLong (nth shape-match 2))]
     :dtype (nth descr-match 1)}))

(defn load-npy
  "Load a numpy .npy file. Returns {:shape [rows cols] :data vec-of-arrays}"
  [^String path]
  (with-open [raf (RandomAccessFile. path "r")]
    (let [magic (byte-array 6)
          _ (.readFully raf magic)
          ;; 0x93 is -109 as signed byte, rest are ASCII "NUMPY"
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
                        row)))}
        :else
        (throw (ex-info "Unsupported dtype" {:dtype dtype}))))))

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

(defn compute-recall [result-ids ground-truth-ids k]
  (let [result-set (set (take k result-ids))
        gt-set (set (take k ground-truth-ids))
        overlap (count (set/intersection result-set gt-set))]
    (/ overlap (double k))))

;; -----------------------------------------------------------------------------
;; Main benchmark

(defn run-benchmark
  "Benchmark using full proximum API with PersistentEdgeStore."
  [{:keys [dataset M ef-construction ef-search k num-threads distance]
    :or {distance :euclidean}}]
  (binding [*out* *err*]
    (println (format "\n=== proximum Benchmark ==="))
    (println (format "  dataset=%s, M=%d, ef_c=%d, ef_s=%d, k=%d, threads=%d, dist=%s"
                     dataset M ef-construction ef-search k num-threads (name distance))))

  (let [{:keys [base queries groundtruth]} (load-dataset dataset)
        vectors-data (:data base)
        query-vecs (:data queries)
        gt-data (:data groundtruth)
        [n-vectors dim] (:shape base)
        n-queries (first (:shape queries))
        base-path (str "/tmp/pv-bench-" (System/currentTimeMillis))
        store-path (str base-path "/store")
        mmap-dir (str base-path "/mmap")
        max-nodes (+ n-vectors 1000)]

    (binding [*out* *err*]
      (println (format "  Base: %d vectors, dim=%d" n-vectors dim)))

    (.mkdirs (File. base-path))
    (.mkdirs (File. mmap-dir))

    (let [idx (pv/create-index {:type :hnsw
                                :dim dim
                                :M M
                                :ef-construction ef-construction
                                :ef-search ef-search
                                :distance distance
                                :store-config {:backend :file
                                               :path store-path
                                               :id (java.util.UUID/randomUUID)}
                                :mmap-dir mmap-dir
                                :capacity max-nodes})]
      (try
        ;; Benchmark parallel insertion
        (binding [*out* *err*]
          (println (format "\nBenchmarking insertion (parallel %d threads)..." num-threads)))
        (let [;; External IDs are indices 0..n-1 to match groundtruth
              external-ids (vec (range n-vectors))
              start (System/nanoTime)
              idx2 (pv/insert-batch idx vectors-data external-ids {:parallelism num-threads})
              insert-time (/ (- (System/nanoTime) start) 1e9)
              ^PersistentEdgeStore pes (p/edge-storage idx2)]

          (binding [*out* *err*]
            (println (format "  Insert: %.2fs (%.0f vec/sec)"
                             insert-time (/ n-vectors insert-time))))

          ;; Warmup
          (dotimes [_ 10]
            (pv/search idx2 (first query-vecs) k {:ef ef-search}))

          ;; Benchmark search
          (binding [*out* *err*]
            (println (format "\nBenchmarking search (ef=%d, k=%d)..." ef-search k)))
          (let [search-results (atom [])
                latencies (doall
                            (for [q query-vecs]
                              (let [start (System/nanoTime)
                                    results (pv/search idx2 q k {:ef ef-search})
                                    elapsed (- (System/nanoTime) start)]
                                (swap! search-results conj (mapv :id results))
                                elapsed)))
                stats (benchmark-stats latencies)
                recalls (map-indexed
                          (fn [i result-ids]
                            (compute-recall result-ids (vec (aget ^"[[I" (into-array gt-data) i)) k))
                          @search-results)
                mean-recall (/ (reduce + recalls) (count recalls))]

            ;; Collect storage and memory metrics
            (let [storage-stats (get-storage-size-mb n-vectors dim pes)
                  heap-mb (gc-and-get-heap-mb)]

              (binding [*out* *err*]
                (println (format "  Search: mean=%.1fus, p50=%.1fus, p99=%.1fus"
                                 (:mean-us stats) (:p50-us stats) (:p99-us stats)))
                (println (format "  QPS: %.0f" (:qps stats)))
                (println (format "  Recall@%d: %.2f%%" k (* 100.0 mean-recall)))
                (println (format "  Storage: %.1fMB vectors + %.1fMB edges = %.1fMB total"
                                 (:vector_storage_mb storage-stats)
                                 (:edge_storage_mb storage-stats)
                                 (:total_storage_mb storage-stats)))
                (println (format "  Heap: %.1fMB" heap-mb)))

              (merge
                {:library "proximum"
                 :dataset dataset
                 :n_vectors n-vectors
                 :dim dim
                 :M M
                 :ef_construction ef-construction
                 :ef_search ef-search
                 :k k
                 :n_queries n-queries
                 :num_threads num-threads
                 :insert_time_sec insert-time
                 :insert_throughput (/ n-vectors insert-time)
                 :search_latency_mean_us (:mean-us stats)
                 :search_latency_p50_us (:p50-us stats)
                 :search_latency_p99_us (:p99-us stats)
                 :search_qps (:qps stats)
                 :recall_at_k mean-recall
                 :heap_mb heap-mb}
                storage-stats))))

        (finally
          (a/<!! (pv/close! idx))
          (doseq [f (reverse (file-seq (File. base-path)))]
            (.delete f)))))))



(defn -main [& args]
  (let [;; Check for --cosine flag
        use-cosine? (some #{"--cosine"} args)
        args (remove #{"--cosine"} args)
        distance (if use-cosine? :cosine :euclidean)
        dataset (or (first args) "sift10k")
        M (if (second args) (Integer/parseInt (second args)) 16)
        ef-c (if (nth args 2 nil) (Integer/parseInt (nth args 2)) 200)
        ef-s (if (nth args 3 nil) (Integer/parseInt (nth args 3)) 100)
        num-threads (if (nth args 4 nil) (Integer/parseInt (nth args 4)) 8)
        warmup-runs (if (nth args 5 nil) (Integer/parseInt (nth args 5)) 0)
        k 10
        params {:dataset dataset
                :M M
                :ef-construction ef-c
                :ef-search ef-s
                :k k
                :num-threads num-threads
                :distance distance}]
    ;; Warmup runs (results discarded)
    (when (pos? warmup-runs)
      (binding [*out* *err*]
        (println (format "\n--- Warmup: %d runs ---" warmup-runs)))
      (dotimes [i warmup-runs]
        (binding [*out* *err*]
          (println (format "Warmup run %d/%d..." (inc i) warmup-runs)))
        (run-benchmark params)))
    ;; Actual benchmark run
    (binding [*out* *err*]
      (when (pos? warmup-runs)
        (println "\n--- Benchmark run ---")))
    (let [results (run-benchmark params)]
      ;; Output JSON to stdout
      (println (json/write-str results)))))
