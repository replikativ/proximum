(ns proximum.yggdrasil-test
  "Yggdrasil compliance tests for proximum adapter."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :as a]
            [proximum.yggdrasil :as py :refer [->ProximumSystem]]
            [proximum.protocols :as p]
            [proximum.api-impl :as api]
            [proximum.hnsw]  ;; register :hnsw multimethod
            [yggdrasil.compliance :as compliance]
            [yggdrasil.protocols :as yp])
  (:import [java.io File]))

(defn- delete-dir-recursive [path]
  (let [dir (File. path)]
    (when (.exists dir)
      (doseq [f (reverse (file-seq dir))]
        (.delete f)))))

(defn- temp-dir []
  (let [path (str "/tmp/proximum-ygg-test-" (System/nanoTime))]
    (.mkdirs (File. path))
    path))

(defn- random-vector [dim]
  (float-array (repeatedly dim #(float (- (rand) 0.5)))))

(def ^:private test-dim 4)

(defn- make-fixture
  "Create a compliance test fixture for a fresh proximum system."
  []
  {:create-system
   (fn []
     (let [base-path (temp-dir)
           store-path (str base-path "/store")
           mmap-dir (str base-path "/mmap")]
       ;; Only create mmap-dir; konserve creates its own store-path
       (.mkdirs (File. mmap-dir))
       (let [store-config {:backend :file
                           :path store-path
                           :id (java.util.UUID/randomUUID)}
             idx (p/create-index {:type :hnsw
                                  :dim test-dim
                                  :capacity 10000
                                  :M 8
                                  :store-config store-config
                                  :mmap-dir mmap-dir})]
         (py/create idx {:mmap-dir mmap-dir
                         :system-name "test-proximum"}))))

   :mutate
   (fn [sys]
     (let [idx (:idx sys)
           ext-id (str (java.util.UUID/randomUUID))
           idx (api/insert idx (random-vector test-dim) ext-id)]
       (->ProximumSystem idx (:mmap-dir sys) (:system-name sys))))

   :commit
   (fn [sys msg]
     (let [idx (a/<!! (p/sync! (:idx sys) {:message msg}))]
       (->ProximumSystem idx (:mmap-dir sys) (:system-name sys))))

   :close!
   (fn [sys]
     (try (p/close! (:idx sys)) (catch Exception _)))

   ;; Data consistency operations
   :write-entry
   (fn [sys key value]
     (let [idx (:idx sys)
           ;; Upsert: delete existing entry first (no-op if not found)
           idx (api/delete idx key)
           ;; Insert with key as external-id, dummy vector, value in metadata
           dummy-vec (float-array (repeat test-dim (float 1.0)))
           idx (api/insert idx dummy-vec key {:value value})]
       (->ProximumSystem idx (:mmap-dir sys) (:system-name sys))))

   :read-entry
   (fn [sys key]
     (let [idx (:idx sys)
           internal-id (api/lookup-internal-id idx key)]
       (when internal-id
         (:value (p/get-metadata idx internal-id)))))

   :count-entries
   (fn [sys]
     (count (:idx sys)))

   :delete-entry
   (fn [sys key]
     (let [idx (api/delete (:idx sys) key)]
       (->ProximumSystem idx (:mmap-dir sys) (:system-name sys))))

   :supports-concurrent? false})

;; ============================================================
;; Run all compliance tests
;; ============================================================

(deftest ^:compliance full-compliance-suite
  (testing "proximum passes yggdrasil compliance suite"
    (compliance/run-compliance-tests (make-fixture))))

;; ============================================================
;; Individual test groups (for targeted debugging)
;; ============================================================

(deftest system-identity-tests
  (let [fix (make-fixture)]
    (compliance/test-system-identity fix)))

(deftest snapshotable-tests
  (let [fix (make-fixture)]
    (compliance/test-snapshot-id-after-commit fix)
    (compliance/test-parent-ids-root-commit fix)
    (compliance/test-parent-ids-chain fix)
    (compliance/test-snapshot-meta fix)
    (compliance/test-as-of fix)))

(deftest branchable-tests
  (let [fix (make-fixture)]
    (compliance/test-initial-branches fix)
    (compliance/test-create-branch fix)
    (compliance/test-checkout fix)
    (compliance/test-branch-isolation fix)
    (compliance/test-delete-branch fix)))

(deftest graphable-tests
  (let [fix (make-fixture)]
    (compliance/test-history fix)
    (compliance/test-history-limit fix)
    (compliance/test-ancestors fix)
    (compliance/test-ancestor-predicate fix)
    (compliance/test-common-ancestor fix)
    (compliance/test-commit-graph fix)
    (compliance/test-commit-info fix)))

(deftest mergeable-tests
  (let [fix (make-fixture)]
    (compliance/test-merge fix)
    (compliance/test-merge-parent-ids fix)
    (compliance/test-conflicts-empty-for-compatible fix)))

(deftest data-consistency-tests
  (let [fix (make-fixture)]
    (compliance/test-write-read-roundtrip fix)
    (compliance/test-count-after-writes fix)
    (compliance/test-multiple-entries-readable fix)
    (compliance/test-branch-data-isolation fix)
    (compliance/test-merge-data-visibility fix)
    (compliance/test-as-of-data-consistency fix)
    (compliance/test-delete-entry-consistency fix)
    (compliance/test-overwrite-entry fix)))
