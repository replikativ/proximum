(ns proximum.gc-test
  "Tests for garbage collection (mark-and-sweep).

   Tests cover:
   - Basic GC operation with no garbage
   - Old commit removal (before remove-before date)
   - Multi-branch preservation
   - Orphaned data collection (after branch deletion)
   - Edge cases (in-memory index, empty index)
   - Whitelist preservation (:branches, :index/config)"
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :as a]
            [proximum.core :as core]
            [proximum.gc :as gc]
            [proximum.generations :as generations]
            [proximum.protocols :as p]
            [proximum.versioning :as versioning]
            [konserve.core :as k]
            [org.replikativ.persistent-sorted-set :as pss])
  (:import [java.io File]
           [java.util Date]))

;; -----------------------------------------------------------------------------
;; Test Fixtures

(def ^:dynamic *store-id* nil)

(defn with-store-id-fixture [f]
  (binding [*store-id* (java.util.UUID/randomUUID)]
    (f)))

(use-fixtures :each with-store-id-fixture)

(defn temp-base-path []
  (str "/tmp/gc-test-" (System/currentTimeMillis) "-" (rand-int 100000)))

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
    ;; Create mmap directory
    (.mkdirs (File. (:mmap-dir layout)))
    layout))

(defn cleanup-path! [path]
  (when path
    (let [f (File. path)]
      (when (.exists f)
        (doseq [file (reverse (file-seq f))]
          (.delete file))))))

;; -----------------------------------------------------------------------------
;; Basic GC Tests

(deftest detached-generations-require-and-honour-authoritative-roots
  (let [base (temp-base-path)
        layout (storage-layout base)
        config {:type :hnsw
                :dim 2
                :capacity 32
                :crypto-hash? true
                :store-config {:backend :memory :id *store-id*}
                :mmap-dir (:mmap-dir layout)}
        kept-builder (generations/begin-generation-from-config config)
        kept (atom nil)
        orphan (atom nil)
        reopened (atom nil)]
    (try
      (generations/put! kept-builder :kept (float-array [1.0 0.0]))
      (reset! kept (generations/seal! kept-builder))
      (generations/rooted! @kept)
      (let [orphan-builder
            (generations/begin-generation
             (generations/generation-index @kept))]
        (generations/put! orphan-builder :orphan (float-array [0.0 1.0]))
        (reset! orphan (generations/seal! orphan-builder))
        (generations/rooted! @orphan))

      (let [idx (generations/generation-index @kept)
            store (p/raw-storage idx)
            kept-id (generations/generation-id @kept)
            orphan-id (generations/generation-id @orphan)]
        (testing "a detached-only store cannot be swept without owner roots"
          (is (= :gc-missing-authoritative-roots
                 (try
                   (gc/gc! idx)
                   nil
                   (catch clojure.lang.ExceptionInfo e
                     (:reason (ex-data e))))))
          (is (some? (k/get store kept-id nil {:sync? true})))
          (is (some? (k/get store orphan-id nil {:sync? true}))))

        (testing "the supplied detached root survives while an orphan is swept"
          (let [deleted (gc/gc! idx (Date. 0)
                                {:generation-ids #{kept-id}})]
            (is (not (contains? deleted kept-id)))
            (is (contains? deleted orphan-id))
            (is (some? (k/get store kept-id nil {:sync? true})))
            (is (nil? (k/get store orphan-id nil {:sync? true}))))
          (reset! reopened (generations/open-generation config kept-id))
          (is (= [1.0 0.0]
                 (vec (core/get-vector
                       (generations/generation-index @reopened) :kept)))))

        (testing "reachability traversal failure aborts instead of under-marking"
          (let [failure (ex-info "injected PSS traversal failure"
                                 {:reason :injected-failure})]
            (is (identical?
                 failure
                 (try
                   (with-redefs [pss/restore-by (fn [& _] (throw failure))]
                     (generations/reachable-keys config kept-id))
                   nil
                   (catch Throwable e e)))))))
      (finally
        (when @reopened
          (a/<!! (generations/close-view! @reopened)))
        (when @orphan
          (a/<!! (generations/close-view! @orphan)))
        (when @kept
          (a/<!! (generations/close-view! @kept)))
        (cleanup-path! base)))))

(deftest test-gc-with-no-garbage
  (testing "GC with active index preserves all data"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :M 8
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              ;; Add some data
              idx-with-data (reduce (fn [idx i]
                                      (core/insert idx
                                                   (float-array [i (inc i) (+ i 2) (+ i 3)])
                                                   (keyword (str "v" i))))
                                    idx
                                    (range 10))
              idx-synced (a/<!! (p/sync! idx-with-data))]

          ;; Run GC with epoch (remove nothing by time)
          (let [deleted (gc/gc! idx-synced (Date. 0))]
            ;; Should delete nothing (all data is reachable from :main)
            (is (empty? deleted) "No garbage to collect with active index")))

        (finally
          (cleanup-path! base))))))

(deftest test-gc-in-memory-works
  (testing "GC on in-memory index works (no-op)"
    (let [idx (core/create-index {:type :hnsw
                                  :dim 4
                                  :capacity 100
                                  :store-config {:backend :memory
                                                 :id (java.util.UUID/randomUUID)}})
          idx-with-data (core/insert idx (float-array [1.0 2.0 3.0 4.0]) :a)
          idx-synced (a/<!! (p/sync! idx-with-data))
          deleted (gc/gc! idx-synced)]

      ;; Memory backend can be GC'd too
      (is (coll? deleted) "Returns collection of deleted keys"))))

;; -----------------------------------------------------------------------------
;; Old Commit Removal Tests

(deftest test-gc-removes-old-commits
  (testing "GC removes commits older than remove-before"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :M 8
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})]

          ;; Create first commit
          (let [idx1 (core/insert idx (float-array [1.0 2.0 3.0 4.0]) :a)
                idx1-synced (a/<!! (p/sync! idx1))
                commit1-id (p/current-commit idx1-synced)]

            ;; Wait a bit to ensure timestamp difference
            (Thread/sleep 100)

            ;; Capture cutoff time between commits
            (let [cutoff (Date.)]
              (Thread/sleep 100)

              ;; Create second commit
              (let [idx2 (core/insert idx1-synced (float-array [5.0 6.0 7.0 8.0]) :b)
                    idx2-synced (a/<!! (p/sync! idx2))
                    commit2-id (p/current-commit idx2-synced)]

                ;; GC with cutoff after commit1 but before commit2
                (let [deleted (gc/gc! idx2-synced cutoff)]

                  (is (some? commit1-id))
                  (is (some? commit2-id))
                  (is (not= commit1-id commit2-id))

                  ;; commit1 should be removed (older than cutoff)
                  ;; commit2 should be preserved (current HEAD)
                  (is (contains? deleted commit1-id) "Old commit should be deleted")
                  (is (not (contains? deleted commit2-id)) "Current commit preserved"))))))

        (finally
          (cleanup-path! base))))))

;; -----------------------------------------------------------------------------
;; Multi-Branch Preservation Tests

(deftest test-gc-preserves-all-branches
  (testing "GC preserves data from all active branches"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :M 8
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              ;; Create data on main
              idx1 (core/insert idx (float-array [1.0 2.0 3.0 4.0]) :a)
              idx1-synced (a/<!! (p/sync! idx1))]

          ;; Create feature branch
          (let [idx-feature (versioning/branch! idx1-synced :feature)
                idx-feature2 (core/insert idx-feature (float-array [5.0 6.0 7.0 8.0]) :b)
                idx-feature-synced (a/<!! (p/sync! idx-feature2))]

            ;; Get store to verify branches
            (let [store (p/raw-storage idx-feature-synced)
                  branches (k/get store :branches nil {:sync? true})]

              (is (contains? branches :main) "Main branch exists")
              (is (contains? branches :feature) "Feature branch exists")

              ;; Run GC
              (let [deleted (gc/gc! idx-feature-synced)]
                ;; Should preserve both branches
                (is (empty? deleted) "All data reachable from either branch")))))

        (finally
          (cleanup-path! base))))))

;; -----------------------------------------------------------------------------
;; Orphaned Data Tests

(deftest test-gc-collects-orphaned-branch-data
  (testing "GC collects data from deleted branches"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :M 8
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              ;; Create data on main
              idx1 (core/insert idx (float-array [1.0 2.0 3.0 4.0]) :a)
              idx1-synced (a/<!! (p/sync! idx1))]

          ;; Create temp branch with unique data
          (let [idx-temp (versioning/branch! idx1-synced :temp-branch)
                idx-temp2 (core/insert idx-temp (float-array [9.0 9.0 9.0 9.0]) :temp-data)
                idx-temp-synced (a/<!! (p/sync! idx-temp2))
                store (p/raw-storage idx-temp-synced)]

            ;; Verify temp branch exists
            (let [branches-before (k/get store :branches nil {:sync? true})]
              (is (contains? branches-before :temp-branch)))

            ;; Delete temp branch (from main's perspective)
            (versioning/delete-branch! idx1-synced :temp-branch)

            ;; Verify branch deleted
            (let [branches-after (k/get store :branches nil {:sync? true})]
              (is (not (contains? branches-after :temp-branch)) "Branch deleted"))

            ;; Run GC with far future cutoff to collect temp branch commits
            (let [far-future (Date. (+ (System/currentTimeMillis) 999999999))
                  deleted (gc/gc! idx1-synced far-future)]
              ;; Should collect orphaned temp-branch data
              ;; (temp branch commits are now unreachable and before cutoff)
              (is (pos? (count deleted)) "Orphaned data collected"))))

        (finally
          (cleanup-path! base))))))

;; -----------------------------------------------------------------------------
;; Empty Index Tests

(deftest test-gc-empty-index
  (testing "GC on empty index works"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              idx-synced (a/<!! (p/sync! idx))
              deleted (gc/gc! idx-synced)]

          ;; Empty index has minimal data, nothing to collect
          (is (empty? deleted) "No data to collect from empty index"))

        (finally
          (cleanup-path! base))))))

;; -----------------------------------------------------------------------------
;; Whitelist Preservation Tests

(deftest test-gc-preserves-global-keys
  (testing "GC always preserves :branches and :index/config"
    (let [base (temp-base-path)
          layout (storage-layout base)]
      (try
        (let [idx (core/create-index {:type :hnsw
                                      :dim 4
                                      :capacity 100
                                      :store-config (file-store-config (:store-path layout))
                                      :mmap-dir (:mmap-dir layout)})
              idx-synced (a/<!! (p/sync! idx))
              store (p/raw-storage idx-synced)]

          ;; Run GC with far future cutoff (would remove everything by time)
          (let [far-future (Date. (+ (System/currentTimeMillis) 999999999))
                deleted (gc/gc! idx-synced far-future)]

            ;; Verify global keys still exist
            (is (some? (k/get store :branches nil {:sync? true})) ":branches preserved")
            (is (some? (k/get store :index/config nil {:sync? true})) ":index/config preserved")

            ;; :branches and :index/config should never be in deleted set
            (is (not (contains? deleted :branches)))
            (is (not (contains? deleted :index/config)))))

        (finally
          (cleanup-path! base))))))

;; -----------------------------------------------------------------------------
;; Batch Size Tests

(deftest test-gc-with-custom-batch-size
  (testing "GC respects custom batch-size option"
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
                                    (range 5))
              idx-synced (a/<!! (p/sync! idx-with-data))]

          ;; Run GC with small batch size (should still work correctly)
          (let [deleted (gc/gc! idx-synced (Date. 0) {:batch-size 10})]
            ;; Should work same as default batch size
            (is (empty? deleted) "No garbage with small batch size")))

        (finally
          (cleanup-path! base))))))
