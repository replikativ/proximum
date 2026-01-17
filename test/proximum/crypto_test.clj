(ns proximum.crypto-test
  "Tests for crypto-hash and verification functionality.

   Tests cover:
   - Hash determinism (same input -> same hash)
   - Verification from cold storage
   - Corruption detection
   - Edge cases and error handling"
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [proximum.core :as core]
            [proximum.crypto :as crypto]
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
  (str "/tmp/crypto-test-" (System/currentTimeMillis) "-" (rand-int 100000)))

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
    ;; Create mmap directory (konserve will create store directory)
    (.mkdirs (File. (:mmap-dir layout)))
    layout))

(defn cleanup-path! [path]
  (when path
    (let [f (File. path)]
      (when (.exists f)
        (doseq [file (reverse (file-seq f))]
          (.delete file))))))

;; -----------------------------------------------------------------------------
;; Hash Determinism Tests

(deftest test-hash-determinism-same-inputs
  (testing "Same inputs produce same commit hash"
    (let [base1 (temp-base-path)
          base2 (temp-base-path)
          layout1 (storage-layout base1)
          layout2 (storage-layout base2)]
      (try
        ;; Create two identical indices
        (let [idx1 (core/create-index {:type :hnsw
                                       :dim 4
                                       :capacity 100
                                       :M 8
                                       :crypto-hash? true
                                       :store-config (file-store-config (:store-path layout1))
                                       :mmap-dir (:mmap-dir layout1)})
              idx2 (core/create-index {:type :hnsw
                                       :dim 4
                                       :capacity 100
                                       :M 8
                                       :crypto-hash? true
                                       :store-config (file-store-config (:store-path layout2))
                                       :mmap-dir (:mmap-dir layout2)})]

          ;; Insert identical data
          (let [v1 (float-array [1.0 2.0 3.0 4.0])
                v2 (float-array [5.0 6.0 7.0 8.0])
                idx1-v1 (core/insert idx1 v1 :a)
                idx1-v2 (core/insert idx1-v1 v2 :b)
                idx2-v1 (core/insert idx2 v1 :a)
                idx2-v2 (core/insert idx2-v1 v2 :b)]

            ;; Sync both to compute commit hashes
            (let [idx1-synced (p/sync! idx1-v2)
                  idx2-synced (p/sync! idx2-v2)
                  hash1 (crypto/get-commit-hash idx1-synced)
                  hash2 (crypto/get-commit-hash idx2-synced)]

              (is (some? hash1) "First index should have commit hash")
              (is (some? hash2) "Second index should have commit hash")
              (is (= hash1 hash2) "Identical content should produce identical commit hash"))))

        (finally
          (cleanup-path! base1)
          (cleanup-path! base2))))))

(deftest test-hash-determinism-different-data
  (testing "Different data produces different commit hash"
    (let [base1 (temp-base-path)
          base2 (temp-base-path)
          layout1 (storage-layout base1)
          layout2 (storage-layout base2)]
      (try
        (let [idx1 (core/create-index {:type :hnsw
                                       :dim 4
                                       :capacity 100
                                       :M 8
                                       :crypto-hash? true
                                       :store-config (file-store-config (:store-path layout1))
                                       :mmap-dir (:mmap-dir layout1)})
              idx2 (core/create-index {:type :hnsw
                                       :dim 4
                                       :capacity 100
                                       :M 8
                                       :crypto-hash? true
                                       :store-config (file-store-config (:store-path layout2))
                                       :mmap-dir (:mmap-dir layout2)})]

          ;; Insert different vectors
          (let [v1 (float-array [1.0 2.0 3.0 4.0])
                v2 (float-array [9.0 9.0 9.0 9.0])  ; Different!
                idx1-v1 (core/insert idx1 v1 :a)
                idx2-v1 (core/insert idx2 v2 :a)]

            (let [idx1-synced (p/sync! idx1-v1)
                  idx2-synced (p/sync! idx2-v1)
                  hash1 (crypto/get-commit-hash idx1-synced)
                  hash2 (crypto/get-commit-hash idx2-synced)]

              (is (some? hash1))
              (is (some? hash2))
              (is (not= hash1 hash2) "Different content should produce different commit hash"))))

        (finally
          (cleanup-path! base1)
          (cleanup-path! base2))))))

(deftest test-hash-chaining
  (testing "Commit hash chains through parent"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :M 8
                                      :crypto-hash? true
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})]

          ;; First commit
          (let [idx1 (core/insert idx (float-array [1.0 2.0 3.0 4.0]) :a)
                idx1-synced (p/sync! idx1)
                hash1 (crypto/get-commit-hash idx1-synced)]

            ;; Second commit with same data
            (let [idx2 (core/insert idx1-synced (float-array [5.0 6.0 7.0 8.0]) :b)
                  idx2-synced (p/sync! idx2)
                  hash2 (crypto/get-commit-hash idx2-synced)]

              (is (some? hash1))
              (is (some? hash2))
              (is (not= hash1 hash2) "Different commits should have different hashes due to parent chaining"))))

        (finally
          (cleanup-path! base))))))

;; -----------------------------------------------------------------------------
;; Verification Tests

(deftest test-verify-from-cold-valid-index
  (testing "Verify valid index from cold storage"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        ;; Create and populate index
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :M 8
                                      :crypto-hash? true
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              idx-with-data (reduce (fn [idx i]
                                      (core/insert idx
                                                   (float-array [i (inc i) (+ i 2) (+ i 3)])
                                                   (keyword (str "v" i))))
                                    idx
                                    (range 10))
              idx-synced (p/sync! idx-with-data)]

          ;; Close to release mmap (simulate cold start)
          (p/close! idx-synced)

          ;; Verify from cold storage
          (let [result (crypto/verify-from-cold (file-store-config (:store-path layout)) :main)]
            (is (:valid? result) "Index should be valid")
            (is (pos? (:vectors-verified result)) "Should verify vector chunks")
            (is (pos? (:edges-verified result)) "Should verify edge chunks")
            (is (some? (:commit-id result)) "Should return commit ID")))

        (finally
          (cleanup-path! base))))))

(deftest test-verify-from-cold-empty-index
  (testing "Verify empty index from cold storage"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :crypto-hash? true
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              idx-synced (p/sync! idx)]

          (p/close! idx-synced)

          (let [result (crypto/verify-from-cold (file-store-config (:store-path layout)) :main)]
            (is (:valid? result) "Empty index should be valid")
            (is (some? (:commit-id result)))))

        (finally
          (cleanup-path! base))))))

(deftest test-verify-from-cold-without-crypto-hash
  (testing "Verify index without crypto-hash enabled"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :crypto-hash? false  ; Disabled!
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              idx-with-data (core/insert idx (float-array [1.0 2.0 3.0 4.0]) :a)
              idx-synced (p/sync! idx-with-data)]

          (p/close! idx-synced)

          (let [result (crypto/verify-from-cold (file-store-config (:store-path layout)) :main)]
            (is (:valid? result) "Should succeed without verification")
            (is (= "Index not in crypto-hash mode, nothing to verify" (:note result)))))

        (finally
          (cleanup-path! base))))))

(deftest test-verify-from-cold-nonexistent-branch
  (testing "Verify nonexistent branch"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :crypto-hash? true
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              idx-synced (p/sync! idx)]

          (p/close! idx-synced)

          (let [result (crypto/verify-from-cold (file-store-config (:store-path layout)) :nonexistent)]
            (is (false? (:valid? result)))
            (is (= :branch-not-found (:error result)))
            (is (= :nonexistent (:branch result)))))

        (finally
          (cleanup-path! base))))))

;; -----------------------------------------------------------------------------
;; Corruption Detection Tests

;; TODO: Implement corruption detection test
;; Requires properly connecting to konserve store and corrupting chunks
;; This is complex and needs careful handling of store lifecycle

#_(deftest test-verify-detects-vector-corruption
    (testing "Verify detects corrupted vector chunk"
    ;; Test implementation pending - requires konserve store manipulation
      ))

;; -----------------------------------------------------------------------------
;; API Tests

(deftest test-crypto-hash-predicate
  (testing "crypto-hash? returns correct value"
    (let [base (temp-base-path)
          layout (storage-layout base)
          mmap-dir-2 (str (:mmap-dir layout) "-2")]
      (try
        ;; Create second mmap directory
        (.mkdirs (File. mmap-dir-2))

        (let [idx-with (core/create-index {:type :hnsw
                                           :dim 4
                                           :capacity 100
                                           :crypto-hash? true
                                           :store-config (file-store-config (:store-path layout))
                                           :mmap-dir (:mmap-dir layout)})
              idx-without (core/create-index {:type :hnsw
                                              :dim 4
                                              :capacity 100
                                              :crypto-hash? false
                                              :store-config (file-store-config (str (:store-path layout) "-2"))
                                              :mmap-dir mmap-dir-2})]

          (is (true? (crypto/crypto-hash? idx-with)))
          (is (false? (crypto/crypto-hash? idx-without))))

        (finally
          (cleanup-path! base))))))

(deftest test-get-commit-hash-before-sync
  (testing "get-commit-hash returns nil before first sync"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :crypto-hash? true
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})]
          (is (nil? (crypto/get-commit-hash idx)) "No commit hash before sync"))

        (finally
          (cleanup-path! base))))))

(deftest test-get-commit-hash-after-sync
  (testing "get-commit-hash returns hash after sync"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :crypto-hash? true
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              idx-synced (p/sync! idx)
              hash (crypto/get-commit-hash idx-synced)]

          (is (some? hash))
          (is (instance? java.util.UUID hash)))

        (finally
          (cleanup-path! base))))))
