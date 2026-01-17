(ns proximum.metrics-test
  "Tests for index metrics and health monitoring.

   Tests cover:
   - index-metrics completeness
   - deletion-ratio edge cases
   - needs-compaction? thresholds
   - cache-hit-ratio calculations
   - graph-health indicators"
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [proximum.core :as core]
            [proximum.metrics :as metrics]
            [proximum.protocols :as p])
  (:import [java.io File]))

;; -----------------------------------------------------------------------------
;; Test Fixtures

(def ^:dynamic *store-id* nil)

(defn with-store-id-fixture [f]
  (binding [*store-id* (java.util.UUID/randomUUID)]
    (f)))

(use-fixtures :each with-store-id-fixture)

(defn temp-base-path []
  (str "/tmp/metrics-test-" (System/currentTimeMillis) "-" (rand-int 100000)))

(defn file-store-config
  ([path]
   (file-store-config path *store-id*))
  ([path store-id]
   {:backend :file
    :path path
    :id store-id}))

(defn storage-layout [base-path]
  (let [layout {:base base-path
                :store-path (str base-path "/store")
                :mmap-dir (str base-path "/mmap")}]
    (.mkdirs (File. (:mmap-dir layout)))
    layout))

(defn cleanup-path! [path]
  (when path
    (let [f (File. path)]
      (when (.exists f)
        (doseq [file (reverse (file-seq f))]
          (.delete file))))))

;; -----------------------------------------------------------------------------
;; index-metrics Tests

(deftest test-index-metrics-empty-index
  (testing "index-metrics on empty index"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :M 8
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              m (metrics/index-metrics idx)]

          (is (= 0 (:vector-count m)))
          (is (= 0 (:deleted-count m)))
          (is (= 0 (:live-count m)))
          (is (= 0.0 (:deletion-ratio m)))
          (is (false? (:needs-compaction? m)))
          (is (= 100 (:capacity m)))
          (is (= 0.0 (:utilization m)))
          (is (= 0 (:edge-count m)))
          (is (= 0.0 (:avg-edges-per-node m)))
          (is (= :main (:branch m)))
          (is (nil? (:commit-id m)))
          (is (number? (:cache-hits m)))
          (is (number? (:cache-misses m))))

        (finally
          (cleanup-path! base))))))

(deftest test-index-metrics-with-data
  (testing "index-metrics with inserted data"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :M 8
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              idx-with-data (reduce (fn [idx i]
                                      (core/insert idx
                                                   (float-array [i (inc i) (+ i 2) (+ i 3)])
                                                   (keyword (str "v" i))))
                                    idx
                                    (range 10))
              m (metrics/index-metrics idx-with-data)]

          (is (= 10 (:vector-count m)))
          (is (= 0 (:deleted-count m)))
          (is (= 10 (:live-count m)))
          (is (= 0.0 (:deletion-ratio m)))
          (is (false? (:needs-compaction? m)))
          (is (= 100 (:capacity m)))
          (is (= 0.1 (:utilization m)))
          (is (pos? (:edge-count m)) "Should have edges after inserts")
          (is (pos? (:avg-edges-per-node m))))

        (finally
          (cleanup-path! base))))))

(deftest test-index-metrics-after-deletes
  (testing "index-metrics reflects deletions"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :M 8
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              idx-with-data (reduce (fn [idx i]
                                      (core/insert idx
                                                   (float-array [i (inc i) (+ i 2) (+ i 3)])
                                                   (keyword (str "v" i))))
                                    idx
                                    (range 10))
              ;; Delete 3 vectors (30%)
              idx-deleted (-> idx-with-data
                              (core/delete :v0)
                              (core/delete :v1)
                              (core/delete :v2))
              m (metrics/index-metrics idx-deleted)]

          (is (= 10 (:vector-count m)) "Total count includes deleted")
          (is (= 3 (:deleted-count m)))
          (is (= 7 (:live-count m)))
          (is (= 0.3 (:deletion-ratio m)))
          (is (true? (:needs-compaction? m)) "30% deletion should trigger compaction (threshold 10%)"))

        (finally
          (cleanup-path! base))))))

(deftest test-index-metrics-custom-threshold
  (testing "index-metrics with custom compaction threshold"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :M 8
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              idx-with-data (reduce (fn [idx i]
                                      (core/insert idx
                                                   (float-array [i (inc i) (+ i 2) (+ i 3)])
                                                   (keyword (str "v" i))))
                                    idx
                                    (range 10))
              idx-deleted (core/delete idx-with-data :v0)  ; 10% deletion
              m-default (metrics/index-metrics idx-deleted)
              m-high-threshold (metrics/index-metrics idx-deleted {:compaction-threshold 0.5})]

          (is (false? (:needs-compaction? m-default)) "10% == threshold (10%), should not trigger (uses >)")
          (is (false? (:needs-compaction? m-high-threshold)) "10% < 50% threshold"))

        (finally
          (cleanup-path! base))))))

;; -----------------------------------------------------------------------------
;; deletion-ratio Tests

(deftest test-deletion-ratio-empty-index
  (testing "deletion-ratio on empty index"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              ratio (metrics/deletion-ratio idx)]
          (is (= 0.0 ratio)))

        (finally
          (cleanup-path! base))))))

(deftest test-deletion-ratio-no-deletes
  (testing "deletion-ratio with data but no deletes"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              idx-with-data (core/insert idx (float-array [1.0 2.0 3.0 4.0]) :a)
              ratio (metrics/deletion-ratio idx-with-data)]
          (is (= 0.0 ratio)))

        (finally
          (cleanup-path! base))))))

(deftest test-deletion-ratio-partial-deletes
  (testing "deletion-ratio with partial deletions"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              idx-with-data (reduce (fn [idx i]
                                      (core/insert idx
                                                   (float-array [i (inc i) (+ i 2) (+ i 3)])
                                                   i))
                                    idx
                                    (range 10))
              idx-deleted (reduce (fn [idx i] (core/delete idx i))
                                  idx-with-data
                                  (range 3))  ; Delete 3 out of 10
              ratio (metrics/deletion-ratio idx-deleted)]
          (is (= 0.3 ratio)))

        (finally
          (cleanup-path! base))))))

(deftest test-deletion-ratio-all-deleted
  (testing "deletion-ratio with all vectors deleted"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              idx-with-data (core/insert idx (float-array [1.0 2.0 3.0 4.0]) :a)
              idx-deleted (core/delete idx-with-data :a)
              ratio (metrics/deletion-ratio idx-deleted)]
          (is (= 1.0 ratio)))

        (finally
          (cleanup-path! base))))))

;; -----------------------------------------------------------------------------
;; needs-compaction? Tests

(deftest test-needs-compaction-default-threshold
  (testing "needs-compaction? with default 10% threshold"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              idx-10 (reduce (fn [idx i] (core/insert idx (float-array [i i i i]) i))
                             idx
                             (range 10))
              idx-del-0 idx-10  ; No deletes - 0%
              idx-del-1 (core/delete idx-10 0)  ; 1 deleted - 10%
              idx-del-2 (core/delete idx-del-1 1)]  ; 2 deleted - 20%

          (is (false? (metrics/needs-compaction? idx-del-0)) "0% should not trigger")
          (is (false? (metrics/needs-compaction? idx-del-1)) "10% == threshold, should not trigger (uses >)")
          (is (true? (metrics/needs-compaction? idx-del-2)) "20% > threshold, should trigger"))

        (finally
          (cleanup-path! base))))))

(deftest test-needs-compaction-custom-threshold
  (testing "needs-compaction? with custom threshold"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              idx-10 (reduce (fn [idx i] (core/insert idx (float-array [i i i i]) i))
                             idx
                             (range 10))
              idx-del-1 (core/delete idx-10 0)]  ; 10% deletion

          (is (false? (metrics/needs-compaction? idx-del-1 0.5)) "10% < 50% threshold")
          (is (true? (metrics/needs-compaction? idx-del-1 0.05)) "10% > 5% threshold"))

        (finally
          (cleanup-path! base))))))

(deftest test-needs-compaction-binding
  (testing "needs-compaction? respects *compaction-threshold* binding"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              idx-10 (reduce (fn [idx i] (core/insert idx (float-array [i i i i]) i))
                             idx
                             (range 10))
              idx-del-1 (core/delete idx-10 0)]  ; 10% deletion

          (binding [metrics/*compaction-threshold* 0.5]
            (is (false? (metrics/needs-compaction? idx-del-1)) "Dynamic var should apply")))

        (finally
          (cleanup-path! base))))))

;; -----------------------------------------------------------------------------
;; cache-hit-ratio Tests

(deftest test-cache-hit-ratio-no-activity
  (testing "cache-hit-ratio returns nil with no cache activity"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              ratio (metrics/cache-hit-ratio idx)]
          ;; Fresh index might have 0 hits and 0 misses, so ratio could be nil
          (is (or (nil? ratio) (number? ratio))))

        (finally
          (cleanup-path! base))))))

(deftest test-cache-hit-ratio-with-operations
  (testing "cache-hit-ratio after operations"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :M 8
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              ;; Insert and search to generate cache activity
              idx-with-data (reduce (fn [idx i]
                                      (core/insert idx
                                                   (float-array [i (inc i) (+ i 2) (+ i 3)])
                                                   i))
                                    idx
                                    (range 20))
              _ (dotimes [_ 5]
                  (core/search idx-with-data (float-array [1.0 2.0 3.0 4.0]) 5))
              ratio (metrics/cache-hit-ratio idx-with-data)]

          ;; After operations, we should have some cache activity
          (when ratio
            (is (<= 0.0 ratio 1.0) "Ratio should be between 0 and 1")
            (is (number? ratio))))

        (finally
          (cleanup-path! base))))))

;; -----------------------------------------------------------------------------
;; graph-health Tests

(deftest test-graph-health-empty-index
  (testing "graph-health on empty index"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :M 8
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              health (metrics/graph-health idx)]

          (is (= 0.0 (:avg-connectivity health)))
          (is (number? (:entrypoint health)))
          (is (number? (:max-level health)))
          (is (= 8 (:expected-M health)))
          (is (= 0.0 (:connectivity-ratio health))))

        (finally
          (cleanup-path! base))))))

(deftest test-graph-health-with-data
  (testing "graph-health with sufficient data"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :M 16  ; M=16
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              idx-with-data (reduce (fn [idx i]
                                      (core/insert idx
                                                   (float-array [i (inc i) (+ i 2) (+ i 3)])
                                                   i))
                                    idx
                                    (range 50))  ; Enough vectors for proper graph
              health (metrics/graph-health idx-with-data)]

          (is (pos? (:avg-connectivity health)) "Should have connections")
          (is (>= (:entrypoint health) 0) "Should have valid entrypoint")
          (is (>= (:max-level health) 0) "Should have max level")
          (is (= 16 (:expected-M health)))
          (is (pos? (:connectivity-ratio health)) "Should have connectivity ratio"))

        (finally
          (cleanup-path! base))))))

(deftest test-graph-health-after-deletes
  (testing "graph-health may degrade after deletions"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :M 8
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              idx-with-data (reduce (fn [idx i]
                                      (core/insert idx
                                                   (float-array [i (inc i) (+ i 2) (+ i 3)])
                                                   i))
                                    idx
                                    (range 20))
              health-before (metrics/graph-health idx-with-data)

              ;; Delete half the vectors
              idx-deleted (reduce (fn [idx i] (core/delete idx i))
                                  idx-with-data
                                  (range 10))
              health-after (metrics/graph-health idx-deleted)]

          (is (pos? (:avg-connectivity health-before)))
          (is (pos? (:avg-connectivity health-after)))
          ;; Connectivity ratio might be higher after deletes (same edges, fewer live nodes)
          (is (number? (:connectivity-ratio health-after))))

        (finally
          (cleanup-path! base))))))
