(ns proximum.vector-compatibility-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [proximum.core :as core]
            [proximum.pgvector :as pg]
            [proximum.protocols :as p]
            [proximum.validation :as validation])
  (:import [java.nio.file Files]
           [proximum HnswIndex]))

(defn- test-index
  ([dim] (test-index dim :euclidean))
  ([dim distance]
   (core/create-index {:type :hnsw
                       :dim dim
                       :distance distance
                       :capacity 100
                       :seed 42
                       :store-config {:backend :memory :id (random-uuid)}})))

(defn- reason [f]
  (try
    (f)
    nil
    (catch clojure.lang.ExceptionInfo e
      (:reason (ex-data e)))))

(deftest strict-vector-validation-test
  (testing "validation requires exact dimensions and finite float32 components"
    (is (= :dimension-mismatch
           (reason #(validation/validate-vector [1.0 2.0 3.0] 2))))
    (is (= :dimension-mismatch
           (reason #(validation/validate-vector [1.0] 2))))
    (is (= :non-finite-component
           (reason #(validation/validate-vector [1.0 Double/NaN] 2))))
    (is (= :non-finite-component
           (reason #(validation/validate-vector [1.0 Double/POSITIVE_INFINITY] 2))))
    ;; Finite as a double, but not representable as finite float32.
    (is (= :non-finite-component
           (reason #(validation/validate-vector [1.0 Double/MAX_VALUE] 2))))))

(deftest close-is-idempotent-for-shared-index-values
  (let [idx (test-index 2)
        with-data (core/insert idx (float-array [1.0 0.0]) :one)
        first-close (core/close! with-data)
        second-close (core/close! idx)]
    (is (identical? first-close second-close)
        "index values sharing one VectorStore observe one cleanup")
    (is (nil? (a/<!! first-close)))
    (is (nil? (a/<!! second-close)))))

(deftest invalid-input-does-not-mutate-storage-test
  (let [idx (test-index 2)]
    (try
      (testing "single inserts and queries validate at the index boundary"
        (is (= :dimension-mismatch
               (reason #(core/insert idx (float-array [1.0 2.0 3.0]) :bad))))
        (is (= :non-finite-component
               (reason #(core/search idx (float-array [Float/NaN 0.0]) 1))))
        (is (zero? (p/vector-count-total idx))))

      (testing "the entire batch is checked before the first mmap append"
        (is (= :non-finite-component
               (reason #(core/insert-batch
                         idx
                         [(float-array [1.0 2.0])
                          (float-array [Float/POSITIVE_INFINITY 3.0])]
                         [:good :bad]))))
        (is (zero? (p/vector-count-total idx))))
      (finally
        (a/<!! (core/close! idx))))))

(deftest rejected-identity-input-does-not-mutate-storage-test
  (let [empty-idx (test-index 2)
        idx (core/insert empty-idx (float-array [1.0 0.0]) :existing)]
    (try
      (testing "a duplicate single ID is rejected before mmap append"
        (is (= :duplicate-external-id
               (reason #(core/insert idx (float-array [0.0 1.0]) :existing))))
        (is (= 1 (p/vector-count-total idx))))

      (testing "duplicate IDs within a batch are rejected before mmap append"
        (is (= :duplicate-external-id
               (reason #(core/insert-batch
                         idx
                         [(float-array [0.0 1.0]) (float-array [1.0 1.0])]
                         [:duplicate :duplicate]))))
        (is (= 1 (p/vector-count-total idx))))

      (testing "an ID already in the index rejects the whole batch before append"
        (is (= :duplicate-external-id
               (reason #(core/insert-batch
                         idx
                         [(float-array [0.0 1.0]) (float-array [1.0 1.0])]
                         [:new :existing]))))
        (is (= 1 (p/vector-count-total idx))))

      (testing "all public batch cardinalities are checked before append"
        (is (= :batch-cardinality-mismatch
               (reason #(core/insert-batch
                         idx [(float-array [0.0 1.0])] [:a :b]))))
        (is (= :batch-cardinality-mismatch
               (reason #(core/insert-batch
                         idx
                         [(float-array [0.0 1.0]) (float-array [1.0 1.0])]
                         [:a :b]
                         {:metadata [{:source :only-one}]}))))
        (is (= 1 (p/vector-count-total idx))))
      (finally
        (a/<!! (core/close! idx))))))

(deftest low-level-java-api-validates-before-append-test
  (let [path (Files/createTempDirectory "proximum-validation-"
                                        (make-array java.nio.file.attribute.FileAttribute 0))
        idx (HnswIndex/create path (int 2) (int 10) (int 4) (int 20))]
    (try
      (is (thrown? IllegalArgumentException
                   (.insert idx (float-array [1.0 2.0 3.0]))))
      (is (thrown? IllegalArgumentException
                   (.search idx (float-array [Float/NaN 0.0]) (int 1))))
      (is (thrown? IllegalArgumentException
                   (.insertBatch idx
                                 (into-array [(float-array [1.0 2.0])
                                              (float-array [Float/POSITIVE_INFINITY 3.0])]))))
      (is (zero? (.size idx)) "an invalid later batch element must prevent every append")
      (finally
        (.close idx)
        (doseq [file (reverse (file-seq (.toFile path)))]
          (.delete file))))))

(deftest pgvector-distance-translation-test
  (is (= 3.0 (pg/internal-distance->pg-distance :euclidean 9.0)))
  (is (= 0.25 (pg/internal-distance->pg-distance :cosine 0.25)))
  (is (= -4.0 (pg/internal-distance->pg-distance :inner-product -3.0)))
  (is (= 9.0 (pg/pg-distance->internal-distance :euclidean 3.0)))
  (is (= -3.0 (pg/pg-distance->internal-distance :inner-product -4.0))))

(deftest cosine-zero-vector-semantics-test
  (let [idx (test-index 2 :cosine)]
    (try
      (is (= :cosine-zero-vector-unindexable
             (reason #(core/insert idx (float-array [0.0 0.0]) :zero))))
      (finally
        (a/<!! (core/close! idx)))))
  (let [idx (test-index 2 :cosine)
        with-data (core/insert idx (float-array [1.0 0.0]) :unit)]
    (try
      (is (= :cosine-zero-query
             (reason #(core/search with-data (float-array [0.0 0.0]) 1))))
      (is (= 1 (core/count-vectors with-data)))
      (finally
        (a/<!! (core/close! with-data)))))
  (let [idx (test-index 2 :cosine)
        tiny (float-array [Float/MIN_VALUE 0.0])
        with-tiny (core/insert idx tiny :tiny)]
    (try
      (testing "tiny nonzero norms are valid and normalized"
        (is (= :cosine-zero-query
               (reason #(core/search with-tiny (float-array [0.0 0.0]) 1))))
        (is (= :tiny (:id (first (core/search with-tiny tiny 1))))))
      (finally
        (a/<!! (core/close! with-tiny))))))
