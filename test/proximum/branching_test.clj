(ns proximum.branching-test
  "Tests for branching, versioning, and garbage collection."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [clojure.core.async :as a]
            [proximum.core :as pv]
            [proximum.protocols :as p]
            [proximum.gc :as gc]
            [konserve.core :as k])
  (:import [java.io File]))

;; -----------------------------------------------------------------------------
;; Test Fixtures

(defn delete-dir [path]
  (when (.exists (File. path))
    (doseq [f (reverse (file-seq (File. path)))]
      (.delete f))))

(def ^:dynamic *test-path* nil)
(def ^:dynamic *store-id* nil)

(defn- storage-layout
  "Derive per-test directories for Konserve store and mmap cache.

  `storage-path` is treated as a base directory; Konserve files live under
  `.../store` and mmap cache files under `.../mmap`."
  [^String storage-path]
  {:base storage-path
   :store-path (str storage-path "/store")
   :mmap-dir (str storage-path "/mmap")})

(defn- ensure-dirs!
  [{:keys [^String base ^String mmap-dir]}]
  (.mkdirs (File. base))
  (.mkdirs (File. mmap-dir))
  nil)

(defn- store-config-for
  [^String storage-path store-id]
  (let [{:keys [store-path] :as layout} (storage-layout storage-path)]
    (ensure-dirs! layout)
    {:backend :file :path store-path :id store-id}))

(defn- mmap-dir-for
  [^String storage-path]
  (let [{:keys [mmap-dir] :as layout} (storage-layout storage-path)]
    (ensure-dirs! layout)
    mmap-dir))

(defn with-temp-storage [f]
  (let [path (str "/tmp/pvdb-branch-test-" (System/currentTimeMillis))]
    (try
      (binding [*test-path* path
                *store-id* (java.util.UUID/randomUUID)]
        (ensure-dirs! (storage-layout path))
        (f))
      (finally
        (delete-dir path)))))

(use-fixtures :each with-temp-storage)

;; -----------------------------------------------------------------------------
;; Helper Functions

(defn random-vector [dim]
  (float-array (repeatedly dim #(- (rand) 0.5))))

(defn create-test-index
  "Create an index with a stable Konserve store identity.

  Konserve requires a user-supplied UUID :id in store configs; for branching tests we
  bind a stable UUID per test via *store-id* so reconnect operations use the same store."
  [config]
  (let [store-id (or (:store-id config) *store-id*)
        storage-path (or (:storage-path config) *test-path*)
        store-config (or (:store-config config)
                         (store-config-for storage-path store-id))
        mmap-dir (or (:mmap-dir config)
                     (mmap-dir-for storage-path))]
    (proximum.core/create-index (-> config
                                    (dissoc :storage-path :store-id :vectors-path)
                                    (assoc :store-config store-config
                                           :mmap-dir mmap-dir)))))

;; Helper for immutable insert patterns - returns new index after inserting n vectors
(defn insert-n
  "Insert n random vectors with IDs into index, returning the new index.
   id-fn takes the iteration index and returns the external ID."
  ([idx n dim]
   (insert-n idx n dim identity))
  ([idx n dim id-fn]
   (reduce (fn [i j] (pv/insert i (random-vector dim) (id-fn j)))
           idx
           (range n))))

;; -----------------------------------------------------------------------------
;; Branching Tests

(deftest test-create-index-initializes-branches
  (testing "create-index initializes :branches set with :main"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        (is (= #{:main} (pv/branches idx)))
        (is (= :main (p/current-branch idx)))
        (is (nil? (p/current-commit idx)))
        (finally
          (pv/close! idx))))))

(deftest test-sync-creates-commit
  (testing "sync! creates a commit with proper structure"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        ;; Add some vectors and sync - use reduce for immutable index
        (let [idx (a/<!! (-> idx
                             (insert-n 10 32)
                             (pv/sync!)))]

          ;; Should have a commit ID now
          (is (some? (p/current-commit idx)))
          (is (uuid? (p/current-commit idx)))

          ;; Commit snapshot should record branch
          (let [commit-id (p/current-commit idx)
                edge-store (k/connect-store (store-config-for *test-path* *store-id*) {:sync? true})
                snapshot (k/get edge-store commit-id nil {:sync? true})]
            (is (= :main (:branch snapshot))))

          ;; Should be able to get history
          (let [hist (pv/history idx)]
            (is (= 1 (count hist)))
            (is (= (p/current-commit idx)
                   (:commit-id (first hist))))
            (is (= #{} (:parents (first hist))))))

        (finally
          (pv/close! idx))))))

(deftest test-load-commit-restores-historical-state
  (testing "load-commit restores an index at a specific commit"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        ;; Commit 1: 10 vectors
        (let [idx (a/<!! (-> idx (insert-n 10 32) (pv/sync!)))
              commit1 (p/current-commit idx)
              ;; Commit 2: +5 vectors
              idx (a/<!! (-> idx (insert-n 5 32 #(+ 10 %)) (pv/sync!)))]

          ;; Reconnect at commit1 and confirm per-commit count
          (pv/close! idx)
          (let [old (pv/load-commit (store-config-for *test-path* *store-id*)
                                    commit1
                                    :mmap-dir (mmap-dir-for *test-path*))]
            (try
              (is (= 10 (pv/count-vectors old)))
              (finally
                (pv/close! old)))))

        (finally
          (when idx
            (pv/close! idx)))))))

(deftest test-external-id-index
  (testing "external-id index supports lookup, update, delete, and persistence"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 16
                                  :storage-path *test-path*
                                  :capacity 100})]
      (try
        ;; Insert with external-id
        (let [idx2 (pv/insert idx (random-vector 16) "doc-1" {:k "v"})
              idx3 (pv/insert idx2 (random-vector 16) "doc-2")]

          ;; Verify vectors are retrievable by external ID
          (is (some? (pv/get-vector idx3 "doc-1")))
          (is (some? (pv/get-vector idx3 "doc-2")))
          (is (nil? (pv/get-vector idx3 "missing")))

          ;; Verify metadata is correct
          (is (= "v" (:k (pv/get-metadata idx3 "doc-1"))))

          ;; Update metadata (external-id is preserved internally)
          (let [idx4 (pv/with-metadata idx3 "doc-1" {:k "v2" :new "field"})]
            (is (= "v2" (:k (pv/get-metadata idx4 "doc-1"))))
            (is (= "field" (:new (pv/get-metadata idx4 "doc-1"))))

            ;; Delete removes vector
            (let [idx5 (pv/delete idx4 "doc-2")]
              (is (nil? (pv/get-vector idx5 "doc-2")))
              (is (some? (pv/get-vector idx5 "doc-1")))

              ;; Persist and reconnect
              (let [idx5-synced (a/<!! (pv/sync! idx5))
                    commit-id (p/current-commit idx5-synced)
                    reopened (pv/load (store-config-for *test-path* *store-id*)
                                      :mmap-dir (mmap-dir-for *test-path*))]
                (try
                  (is (some? (pv/get-vector reopened "doc-1")))
                  (is (nil? (pv/get-vector reopened "doc-2")))
                  ;; historical restore also contains mapping
                  (let [asof (pv/load-commit (store-config-for *test-path* *store-id*)
                                             commit-id
                                             :mmap-dir (mmap-dir-for *test-path*))]
                    (try
                      (is (some? (pv/get-vector asof "doc-1")))
                      (finally
                        (pv/close! asof))))
                  (finally
                    (pv/close! reopened)))))))
        (finally
          (pv/close! idx))))))

(deftest test-multiple-syncs-create-chain
  (testing "Multiple syncs create a commit chain"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        ;; First batch
        (let [idx (a/<!! (-> idx (insert-n 5 32) (pv/sync!)))
              commit1 (p/current-commit idx)
              ;; Second batch
              idx (a/<!! (-> idx (insert-n 5 32 #(+ 5 %)) (pv/sync!)))
              commit2 (p/current-commit idx)
              ;; Third batch
              idx (a/<!! (-> idx (insert-n 5 32 #(+ 10 %)) (pv/sync!)))
              commit3 (p/current-commit idx)]

          ;; History should have 3 commits
          (let [hist (pv/history idx)]
            (is (= 3 (count hist)))
            ;; Most recent first
            (is (= commit3 (:commit-id (first hist))))
            (is (= commit2 (:commit-id (second hist))))
            (is (= commit1 (:commit-id (nth hist 2))))

            ;; Check parent chain
            (is (= #{commit2} (:parents (first hist))))
            (is (= #{commit1} (:parents (second hist))))
            (is (= #{} (:parents (nth hist 2))))))

        (finally
          (pv/close! idx))))))

(deftest test-branch-creates-new-branch
  (testing "branch! creates a new branch from current state"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        ;; Add vectors and sync
        (let [idx (a/<!! (-> idx (insert-n 10 32) (pv/sync!)))
              main-commit (p/current-commit idx)
              main-count (pv/count-vectors idx)]

          ;; Create new branch
          (let [feature-idx (pv/branch! idx :feature-x)]
            (try
              ;; Check new branch state
              (is (= :feature-x (p/current-branch feature-idx)))
              (is (= #{:main :feature-x} (pv/branches feature-idx)))
              (is (= main-count (pv/count-vectors feature-idx)))

              ;; Add to new branch - note: vectors are shared (same mmap)
              ;; but edges are isolated (forked PES)
              (let [feature-idx (a/<!! (-> feature-idx
                                           (insert-n 5 32 #(str "feature-" %))
                                           (pv/sync!)))]

                ;; Branch should have more vectors
                (is (= (+ main-count 5) (pv/count-vectors feature-idx))))

              ;; Note: Vector store is shared, so count increases globally
              ;; This is expected behavior - branches isolate edges, not vectors
              ;; See BRANCHING_GC_DESIGN.md for details

              (finally
                (pv/close! feature-idx)))))

        (finally
          (pv/close! idx))))))

(deftest test-load-branch
  (testing "load can open specific branches"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        ;; Add to main
        (let [idx (a/<!! (-> idx (insert-n 10 32) (pv/sync!)))]

          ;; Create feature branch with more vectors
          (let [feature-idx (pv/branch! idx :feature-x)
                feature-idx (a/<!! (-> feature-idx
                                       (insert-n 5 32 #(str "feature-" %))
                                       (pv/sync!)))]
            (pv/close! feature-idx))

          (pv/close! idx)

          ;; Per-branch count tracking:
          ;; - main was synced with 10 vectors, so its count is 10
          ;; - feature inherited 10 from parent + added 5 = 15 vectors

          ;; Reopen main - sees only its own count (10)
          (let [main-idx (pv/load (store-config-for *test-path* *store-id*)
                                  :branch :main
                                  :mmap-dir (mmap-dir-for *test-path*))]
            (try
              (is (= 10 (pv/count-vectors main-idx)))  ; Per-branch count
              (is (= :main (p/current-branch main-idx)))
              (finally
                (pv/close! main-idx))))

          ;; Reopen feature - sees parent count + its own inserts (15)
          (let [feature-idx (pv/load (store-config-for *test-path* *store-id*)
                                     :branch :feature-x
                                     :mmap-dir (mmap-dir-for *test-path*))]
            (try
              (is (= 15 (pv/count-vectors feature-idx)))
              (is (= :feature-x (p/current-branch feature-idx)))
              (finally
                (pv/close! feature-idx)))))

        (catch Exception e
          (pv/close! idx)
          (throw e))))))

(deftest test-branch-requires-sync
  (testing "branch! requires sync first"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        ;; Add vectors but don't sync
        (let [idx (insert-n idx 10 32)]

          ;; branch! should fail because no commit exists
          (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                #"no commits"
                                (pv/branch! idx :feature-x))))
        (finally
          (pv/close! idx))))))

(deftest test-branch-duplicate-name-fails
  (testing "branch! with existing name fails"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        (let [idx (a/<!! (-> idx (insert-n 10 32) (pv/sync!)))]

          ;; Create first branch
          (let [b1 (pv/branch! idx :feature-x)]
            (pv/close! b1))

          ;; Trying to create same branch should fail
          (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                #"already exists"
                                (pv/branch! idx :feature-x))))
        (finally
          (pv/close! idx))))))

;; -----------------------------------------------------------------------------
;; GC Tests

(deftest test-gc-preserves-current-data
  (testing "GC preserves data referenced by current branch"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})
          id-counter (atom 0)]
      (try
        ;; Add vectors and sync multiple times - use reduce for immutable index
        (let [idx (reduce (fn [idx _]
                            (let [idx-with-batch (insert-n idx 10 32 (fn [_] (swap! id-counter inc)))]
                              (a/<!! (pv/sync! idx-with-batch))))
                          idx
                          (range 3))
              count-before (pv/count-vectors idx)
              query (random-vector 32)]

          ;; Run GC with very old cutoff (shouldn't delete anything active)
          (a/<!! (pv/gc! idx (java.util.Date. 0)))

          ;; All vectors should still be searchable
          (is (= count-before (pv/count-vectors idx)))
          (let [results (pv/search idx query 5)]
            (is (= 5 (count results)))))

        (finally
          (pv/close! idx))))))

(deftest test-gc-runs-without-error
  (testing "GC runs without error and preserves current data"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        ;; Create several commits
        (let [idx (a/<!! (-> idx (insert-n 5 32) (pv/sync!)))
              idx (a/<!! (-> idx (insert-n 5 32 #(+ 5 %)) (pv/sync!)))
              idx (a/<!! (-> idx (insert-n 5 32 #(+ 10 %)) (pv/sync!)))]

          ;; History should have 3 commits
          (is (= 3 (count (pv/history idx))))

          ;; GC with epoch date (delete nothing by time)
          ;; Note: konserve uses its own last-write timestamps, not our commit timestamps
          ;; In a fast test, all writes happen within ms, so time-based GC won't delete anything
          (let [deleted (a/<!! (pv/gc! idx (java.util.Date. 0)))]
            ;; Verify GC ran and returned a set (may be empty)
            (is (set? deleted)))

          ;; Current data should still work
          (is (= 15 (pv/count-vectors idx)))
          (let [results (pv/search idx (random-vector 32) 5)]
            (is (= 5 (count results))))

          ;; History should still be traversable
          (is (= 3 (count (pv/history idx)))))

        (finally
          (pv/close! idx))))))

(deftest test-gc-preserves-all-branches
  (testing "GC preserves data for all branches"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        ;; Main branch
        (let [idx (a/<!! (-> idx (insert-n 10 32) (pv/sync!)))]

          ;; Feature branch
          (let [feature-idx (pv/branch! idx :feature-x)
                feature-idx (a/<!! (-> feature-idx
                                       (insert-n 5 32 #(str "feature-" %))
                                       (pv/sync!)))]
            (pv/close! feature-idx))

          ;; Run GC
          (a/<!! (pv/gc! idx (java.util.Date. 0)))

          (pv/close! idx))

        ;; Both branches should still work with per-branch counts
        ;; main: 10 vectors, feature: 15 (10 inherited + 5 added)
        (let [main-idx (pv/load (store-config-for *test-path* *store-id*)
                                :branch :main
                                :mmap-dir (mmap-dir-for *test-path*))]
          (is (= 10 (pv/count-vectors main-idx)))  ; Per-branch count
          ;; Search should work correctly
          (is (seq (pv/search main-idx (random-vector 32) 5)))
          (pv/close! main-idx))

        (let [feature-idx (pv/load (store-config-for *test-path* *store-id*)
                                   :branch :feature-x
                                   :mmap-dir (mmap-dir-for *test-path*))]
          (is (= 15 (pv/count-vectors feature-idx)))  ; Per-branch count
          (is (seq (pv/search feature-idx (random-vector 32) 5)))
          (pv/close! feature-idx))

        (catch Exception e
          (pv/close! idx)
          (throw e))))))

(deftest test-history-function
  (testing "history returns commit chain"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        ;; No history before first sync
        (is (empty? (pv/history idx)))

        ;; Create commits
        (let [idx (a/<!! (-> idx
                             (pv/insert (random-vector 32) "v1")
                             (pv/sync!)))
              hist1 (pv/history idx)
              _ (is (= 1 (count hist1)))
              _ (is (some? (:commit-id (first hist1))))
              _ (is (some? (:created-at (first hist1))))
              idx (a/<!! (-> idx
                             (pv/insert (random-vector 32) "v2")
                             (pv/sync!)))
              hist2 (pv/history idx)]
          (is (= 2 (count hist2))))

        (finally
          (pv/close! idx))))))

;; -----------------------------------------------------------------------------
;; Advanced Branching Tests

(deftest test-branch-edge-isolation
  (testing "Branches have isolated edge graphs"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :M 4
                                  :capacity 1000})
          ;; Known query vector
          query (float-array (repeat 32 0.5))]
      (try
        ;; Add vectors close to query on main
        (let [idx (reduce (fn [idx i]
                            (pv/insert idx (float-array (repeat 32 (+ 0.4 (* i 0.02)))) i))
                          idx (range 5))
              idx (a/<!! (pv/sync! idx))]

          ;; Main should find these vectors
          (let [main-results (pv/search idx query 5)]
            (is (= 5 (count main-results))))

          ;; Create feature branch
          (let [feature-idx (pv/branch! idx :feature)
                ;; Add very different vectors to feature branch
                feature-idx (reduce (fn [idx i]
                                      (pv/insert idx (float-array (repeat 32 -0.9)) (str "feature-" i)))
                                    feature-idx (range 10))
                feature-idx (a/<!! (pv/sync! feature-idx))]

            ;; Feature branch search should now include the new vectors
            (let [feature-results (pv/search feature-idx query 5)]
              (is (= 5 (count feature-results))))

            (pv/close! feature-idx))

          ;; Reopen main - search should still work with original edges
          (let [main-idx (pv/load (store-config-for *test-path* *store-id*)
                                  :branch :main
                                  :mmap-dir (mmap-dir-for *test-path*))]
            (let [results (pv/search main-idx query 5)]
              (is (= 5 (count results))))
            (pv/close! main-idx)))

        (finally
          (pv/close! idx))))))

(deftest test-deep-branch-chain
  (testing "Branches can be created from branches"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        ;; Main branch
        (let [idx (a/<!! (-> idx (insert-n 5 32) (pv/sync!)))]

          ;; Feature-a from main
          (let [feature-a (pv/branch! idx :feature-a)
                feature-a (a/<!! (-> feature-a
                                     (insert-n 5 32 #(str "a-" %))
                                     (pv/sync!)))]

            ;; Feature-b from feature-a
            (let [feature-b (pv/branch! feature-a :feature-b)
                  feature-b (a/<!! (-> feature-b
                                       (insert-n 5 32 #(str "b-" %))
                                       (pv/sync!)))]

              ;; All three branches should exist
              (is (= #{:main :feature-a :feature-b} (pv/branches feature-b)))

              ;; feature-b has 10 inherited + 5 added = 15
              (is (= 15 (pv/count-vectors feature-b)))

              (pv/close! feature-b))
            (pv/close! feature-a)))

        ;; All branches should be openable with per-branch counts:
        ;; main: 5, feature-a: 10, feature-b: 15
        (let [expected-counts {:main 5, :feature-a 10, :feature-b 15}]
          (doseq [branch [:main :feature-a :feature-b]]
            (let [b-idx (pv/load (store-config-for *test-path* *store-id*)
                                 :branch branch
                                 :mmap-dir (mmap-dir-for *test-path*))]
              (is (= (expected-counts branch) (pv/count-vectors b-idx)))
              (is (seq (pv/search b-idx (random-vector 32) 3)))
              (pv/close! b-idx))))

        (finally
          (pv/close! idx))))))

(deftest test-delete-on-branch-in-memory
  (testing "Delete on branch doesn't affect original branch IN MEMORY (via forked PES)"
    ;; Note: This tests in-memory branch isolation via PES fork.
    ;; Persistence of branch isolation after close/reopen is a known limitation
    ;; when branches share chunk addresses. See BRANCHING_GC_DESIGN.md.
    (let [base-idx (create-test-index {:type :hnsw
                                       :dim 32
                                       :storage-path *test-path*
                                       :M 16
                                       :ef-construction 100
                                       :capacity 1000})
          ;; Use deterministic vectors for reliable graph
          vectors (vec (for [i (range 10)]
                         (float-array (repeat 32 (float (/ i 10.0))))))]
      (try
        ;; Add 10 vectors to main with external IDs matching indices
        ;; Use reduce to properly capture the updated index
        (let [idx (reduce (fn [idx [i v]] (pv/insert idx v i))
                          base-idx
                          (map-indexed vector vectors))
              _ (a/<!! (pv/sync! idx))

              ;; Verify all 10 are searchable on main before branching
              main-results-before (pv/search idx (first vectors) 10)
              _ (is (= 10 (count main-results-before)))

              ;; Create feature branch
              feature-idx (pv/branch! idx :feature)
              ;; Delete on feature branch (using external IDs), capturing each result
              feature-idx (-> feature-idx
                              (pv/delete 0)
                              (pv/delete 1)
                              (pv/delete 2))]

          ;; Feature should have fewer results (in memory, via forked PES)
          (let [feature-results (pv/search feature-idx (first vectors) 10)]
            (is (< (count feature-results) 10)
                "Feature branch should have fewer results after delete"))

          ;; IMPORTANT: Main (idx) should still have all 10 IN MEMORY
          ;; because the PES was forked, not shared
          (let [main-results (pv/search idx (first vectors) 10)]
            (is (= 10 (count main-results))
                "Main branch should still have all 10 vectors (in-memory isolation)"))

          (pv/close! feature-idx)
          (pv/close! idx))

        (finally
          nil)))))

(deftest test-delete-on-branch-persistence
  (testing "Delete on branch persists independently with COW addressing"
    ;; This tests that branches have full persistence isolation via COW chunk addressing.
    ;; Each branch maintains its own position->storage-address mapping, so when
    ;; feature branch modifies chunks, it gets new storage addresses without affecting main.
    (let [storage-path (str *test-path* "-cow-isolation")
          base-idx (create-test-index {:type :hnsw
                                       :dim 32
                                       :storage-path storage-path
                                       :M 16
                                       :ef-construction 100
                                       :capacity 1000})
          ;; Use deterministic vectors for reliable graph
          vectors (vec (for [i (range 10)]
                         (float-array (repeat 32 (float (/ i 10.0))))))]
      (try
        ;; Add 10 vectors to main with external IDs matching indices
        ;; Use reduce to properly capture the updated index
        (let [idx (reduce (fn [idx [i v]] (pv/insert idx v i))
                          base-idx
                          (map-indexed vector vectors))
              _ (a/<!! (pv/sync! idx))

              ;; Verify all 10 are searchable on main before branching
              main-results-before (pv/search idx (first vectors) 10)
              _ (is (= 10 (count main-results-before)))

              ;; Create feature branch
              feature-idx (pv/branch! idx :feature)
              ;; Delete on feature branch (using external IDs), capturing each result
              feature-idx (-> feature-idx
                              (pv/delete 0)
                              (pv/delete 1)
                              (pv/delete 2))]

          ;; Sync feature branch - this should create new chunk addresses
          (a/<!! (pv/sync! feature-idx))

          ;; Close both
          (pv/close! feature-idx)
          (pv/close! idx)

          ;; Now reopen both branches and verify isolation
          (let [main-reopened (pv/load (store-config-for storage-path *store-id*)
                                       :branch :main
                                       :mmap-dir (mmap-dir-for storage-path))
                feature-reopened (pv/load (store-config-for storage-path *store-id*)
                                          :branch :feature
                                          :mmap-dir (mmap-dir-for storage-path))]
            (try
              ;; Main should still have all 10 searchable
              (let [main-results (pv/search main-reopened (first vectors) 10)]
                (is (= 10 (count main-results))
                    "Main branch should still have all 10 vectors after reopen (COW isolation)"))

              ;; Feature should have fewer (deleted 3)
              (let [feature-results (pv/search feature-reopened (first vectors) 10)]
                (is (< (count feature-results) 10)
                    "Feature branch should have fewer results after reopen"))

              (finally
                (pv/close! main-reopened)
                (pv/close! feature-reopened)))))

        (finally
          ;; Cleanup is handled by test fixture
          nil)))))

(deftest test-concurrent-branch-inserts
  (testing "Concurrent inserts on different branches"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        ;; Initial data
        (let [idx (a/<!! (-> (reduce (fn [i j] (pv/insert i (random-vector 32) j))
                                     idx (range 10))
                             (pv/sync!)))
              ;; Create two branches
              branch-a (pv/branch! idx :branch-a)
              branch-b (pv/branch! idx :branch-b)
              ;; Atoms to capture final branch states
              branch-a-result (atom nil)
              branch-b-result (atom nil)
              ;; Concurrent inserts using reduce for proper chaining
              futures [(future
                         (let [result (a/<!! (-> (reduce (fn [b i]
                                                           (pv/insert b (random-vector 32) (str "a-" i)))
                                                         branch-a (range 20))
                                                 (pv/sync!)))]
                           (reset! branch-a-result result)))
                       (future
                         (let [result (a/<!! (-> (reduce (fn [b i]
                                                           (pv/insert b (random-vector 32) (str "b-" i)))
                                                         branch-b (range 20))
                                                 (pv/sync!)))]
                           (reset! branch-b-result result)))]]

          (doseq [f futures] @f)

          (let [final-a @branch-a-result
                final-b @branch-b-result]
            ;; Both branches should work with per-branch counts
            ;; Each branch: 10 inherited + 20 added = 30
            (is (= 30 (pv/count-vectors final-a)))
            (is (= 30 (pv/count-vectors final-b)))

            ;; Both should be searchable
            (is (seq (pv/search final-a (random-vector 32) 10)))
            (is (seq (pv/search final-b (random-vector 32) 10)))

            (pv/close! final-a)
            (pv/close! final-b))

          (pv/close! idx))

        (finally nil)))))

;; -----------------------------------------------------------------------------
;; Advanced GC Tests

(deftest test-gc-with-many-commits
  (testing "GC handles many commits correctly"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        ;; Create 20 commits
        (let [idx (reduce (fn [idx i]
                            (a/<!! (-> idx
                                       (pv/insert (random-vector 32) i)
                                       (pv/sync!))))
                          idx (range 20))]

          (is (= 20 (count (pv/history idx))))
          (is (= 20 (pv/count-vectors idx)))

          ;; GC should preserve all current data
          (a/<!! (pv/gc! idx (java.util.Date. 0)))

          ;; Everything should still work
          (is (= 20 (pv/count-vectors idx)))
          (is (= 10 (count (pv/search idx (random-vector 32) 10)))))

        (finally
          (pv/close! idx))))))

(deftest test-gc-with-multiple-branches
  (testing "GC preserves all branch data correctly"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        ;; Main with commits - insert and sync each vector
        (let [idx (reduce (fn [idx i]
                            (a/<!! (-> idx
                                       (pv/insert (random-vector 32) i)
                                       (pv/sync!))))
                          idx (range 5))]

          ;; Create 3 branches from main
          (let [branches (doall
                          (for [n [:dev :staging :prod]]
                            (let [b (pv/branch! idx n)
                                  b (a/<!! (-> b
                                               (insert-n 3 32 #(str (name n) "-" %))
                                               (pv/sync!)))]
                              b)))]

            ;; GC
            (a/<!! (pv/gc! idx (java.util.Date. 0)))

            ;; Close branches
            (doseq [b branches]
              (pv/close! b)))

          (pv/close! idx))

        ;; All 4 branches should work with per-branch counts
        ;; main: 5 vectors, each other branch: 5 inherited + 3 added = 8
        (let [expected-counts {:main 5, :dev 8, :staging 8, :prod 8}]
          (doseq [branch [:main :dev :staging :prod]]
            (let [b-idx (pv/load (store-config-for *test-path* *store-id*)
                                 :branch branch
                                 :mmap-dir (mmap-dir-for *test-path*))]
              (is (= (expected-counts branch) (pv/count-vectors b-idx))
                  (str "Branch " branch " should have " (expected-counts branch) " vectors"))
              (is (seq (pv/search b-idx (random-vector 32) 5))
                  (str "Branch " branch " should be searchable"))
              (pv/close! b-idx))))

        (catch Exception e
          (pv/close! idx)
          (throw e))))))

(deftest test-gc-after-delete-operations
  (testing "GC handles deleted vectors correctly"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :M 16
                                  :ef-construction 100
                                  :capacity 1000})
          ;; Use deterministic vectors
          vectors (vec (for [i (range 10)]
                         (float-array (repeat 32 (float (/ i 10.0))))))]
      (try
        ;; Add and sync with external IDs matching indices
        (let [idx (reduce (fn [i [j v]] (pv/insert i v j))
                          idx
                          (map-indexed vector vectors))
              idx (a/<!! (pv/sync! idx))
              ;; Verify all 10 searchable
              _ (is (= 10 (count (pv/search idx (first vectors) 10))))
              ;; Delete some (using external IDs)
              idx (-> idx
                      (pv/delete 0)
                      (pv/delete 5)
                      (pv/delete 9))
              idx (a/<!! (pv/sync! idx))
              ;; GC
              _ (a/<!! (pv/gc! idx (java.util.Date. 0)))
              ;; Search should return fewer results (deletes removed from graph)
              results (pv/search idx (first vectors) 10)]
          (is (< (count results) 10)
              "Search should return fewer results after deletes")
          ;; History should still work
          (is (= 2 (count (pv/history idx))))
          (pv/close! idx))

        (finally nil)))))

(deftest test-gc-concurrent-with-writes
  (testing "GC runs safely concurrent with writes"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})
          id-counter (atom 0)
          idx-atom (atom nil)]
      (try
        ;; Initial data
        (let [idx (a/<!! (-> idx
                             (insert-n 20 32 (fn [_] (swap! id-counter inc)))
                             (pv/sync!)))]
          (reset! idx-atom idx)

          ;; Run GC and writes concurrently using atom for mutable tracking
          (let [write-future (future
                               (dotimes [_ 30]
                                 (swap! idx-atom (fn [i] (pv/insert i (random-vector 32) (swap! id-counter inc))))
                                 (Thread/sleep 5))
                               (swap! idx-atom #(a/<!! (pv/sync! %))))
                gc-future (future
                            (Thread/sleep 50)  ; Start GC mid-writes
                            (a/<!! (pv/gc! @idx-atom (java.util.Date. 0))))]

            @write-future
            @gc-future)

          ;; Should have 50 vectors
          (is (= 50 (pv/count-vectors @idx-atom)))

          ;; Search should work
          (is (seq (pv/search @idx-atom (random-vector 32) 10))))

        (finally
          (when @idx-atom (pv/close! @idx-atom)))))))

(deftest test-gc-repeated-cycles
  (testing "Multiple GC cycles don't corrupt data"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})
          id-counter (atom 0)]
      (try
        ;; Add, sync, GC multiple times
        (let [idx (reduce (fn [idx _cycle]
                            (let [idx (insert-n idx 5 32 (fn [_] (swap! id-counter inc)))
                                  idx (a/<!! (pv/sync! idx))]
                              (a/<!! (pv/gc! idx (java.util.Date. 0)))
                              idx))
                          idx (range 5))]

          ;; Should have 25 vectors
          (is (= 25 (pv/count-vectors idx)))

          ;; All history should be preserved (epoch date = keep all)
          (is (= 5 (count (pv/history idx))))

          ;; Search should work
          (let [results (pv/search idx (random-vector 32) 10)]
            (is (= 10 (count results)))))

        (finally
          (pv/close! idx))))))

(deftest test-reopen-after-gc
  (testing "Index reopens correctly after GC"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})
          id-counter (atom 0)]
      (try
        ;; Add data and create commits
        (let [idx (reduce (fn [idx _]
                            (a/<!! (-> idx
                                       (insert-n 10 32 (fn [_] (swap! id-counter inc)))
                                       (pv/sync!))))
                          idx (range 3))]

          ;; Run GC
          (a/<!! (pv/gc! idx (java.util.Date. 0)))

          (pv/close! idx))

        ;; Reopen and verify
        (let [idx2 (pv/load (store-config-for *test-path* *store-id*)
                            :mmap-dir (mmap-dir-for *test-path*))]
          (is (= 30 (pv/count-vectors idx2)))
          (is (= 3 (count (pv/history idx2))))
          (is (seq (pv/search idx2 (random-vector 32) 10)))
          (pv/close! idx2))

        (catch Exception e
          (pv/close! idx)
          (throw e))))))

;; -----------------------------------------------------------------------------
;; COW-Specific GC Tests (verify whitelist actually works)

(deftest test-gc-with-future-date-preserves-whitelisted
  (testing "GC with future date still preserves whitelisted data"
    ;; This is the critical test - using a future date means sweep! will
    ;; consider ALL non-whitelisted keys for deletion. If the whitelist
    ;; is wrong (e.g., missing edge chunks), data will be lost.
    (let [storage-path (str *test-path* "-gc-future")
          idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path storage-path
                                  :M 16
                                  :ef-construction 100
                                  :capacity 1000})
          test-vectors (vec (for [i (range 20)]
                              (float-array (repeat 32 (float (/ i 20.0))))))]
      (try
        ;; Add vectors with external IDs matching indices
        (let [idx (reduce (fn [i [j v]] (pv/insert i v j))
                          idx
                          (map-indexed vector test-vectors))
              idx (a/<!! (pv/sync! idx))
              ;; Verify data before GC
              _ (is (= 20 (pv/count-vectors idx)))
              results-before (pv/search idx (first test-vectors) 10)
              _ (is (= 10 (count results-before)))
              ;; Run GC with FUTURE date - this forces sweep! to evaluate all keys
              ;; Only whitelisted keys should survive
              future-date (java.util.Date. (+ (System/currentTimeMillis) 86400000))
              _ (a/<!! (pv/gc! idx future-date))]
          ;; Data should still be accessible (whitelist preserved it)
          (is (= 20 (pv/count-vectors idx)))
          (let [results-after (pv/search idx (first test-vectors) 10)]
            (is (= 10 (count results-after))
                "Search should still work after GC with future date"))
          ;; Close and reopen to verify persistence
          (pv/close! idx)
          (let [idx2 (pv/load (store-config-for storage-path *store-id*)
                              :mmap-dir (mmap-dir-for storage-path))]
            (is (= 20 (pv/count-vectors idx2))
                "Vector count should survive reopen after GC")
            (let [results-reopened (pv/search idx2 (first test-vectors) 10)]
              (is (= 10 (count results-reopened))
                  "Search should work after reopen post-GC"))
            (pv/close! idx2)))

        (finally
          nil)))))

(deftest test-gc-cow-branch-isolation
  (testing "GC preserves both old and new COW chunk addresses across branches"
    ;; This tests the core COW+GC interaction:
    ;; 1. Main branch has chunks at storage addresses A, B, C
    ;; 2. Feature branch modifies chunks -> new addresses A', B', C'
    ;; 3. GC runs with future date
    ;; 4. Both branches should still work (A,B,C for main, A',B',C' for feature)
    (let [storage-path (str *test-path* "-gc-cow")
          idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path storage-path
                                  :M 16
                                  :ef-construction 100
                                  :capacity 1000})
          test-vectors (vec (for [i (range 10)]
                              (float-array (repeat 32 (float (/ i 10.0))))))]
      (try
        ;; Add vectors to main with external IDs matching indices
        (let [idx (reduce (fn [i [j v]] (pv/insert i v j))
                          idx
                          (map-indexed vector test-vectors))
              idx (a/<!! (pv/sync! idx))
              ;; Create feature branch and modify
              feature (pv/branch! idx :feature)
              ;; Delete some vectors (modifies chunks, gets new COW addresses)
              feature (-> feature
                          (pv/delete 0)
                          (pv/delete 1)
                          (pv/delete 2))
              ;; Add new vectors (may create new chunks)
              feature (reduce (fn [f i] (pv/insert f (random-vector 32) (str "feature-" i)))
                              feature
                              (range 5))
              feature (a/<!! (pv/sync! feature))
              ;; Run GC with future date on main
              future-date (java.util.Date. (+ (System/currentTimeMillis) 86400000))
              _ (a/<!! (pv/gc! idx future-date))]
          (pv/close! feature)
          (pv/close! idx))

        ;; Reopen BOTH branches and verify they work
        (let [main-reopened (pv/load (store-config-for storage-path *store-id*)
                                     :branch :main
                                     :mmap-dir (mmap-dir-for storage-path))
              feature-reopened (pv/load (store-config-for storage-path *store-id*)
                                        :branch :feature
                                        :mmap-dir (mmap-dir-for storage-path))]
          (try
            ;; Note: Branches share VectorStore (mmap), so vector count includes all vectors
            ;; from both branches. But the EDGE GRAPH should be isolated - main's graph
            ;; should only have edges for its original 10 vectors.

            ;; Main branch search should return 10 results (its original vectors)
            ;; because the edge graph is isolated via COW
            (let [main-results (pv/search main-reopened (first test-vectors) 10)]
              (is (= 10 (count main-results))
                  "Main branch search should return 10 results (edge graph isolation)"))

            ;; Feature should search successfully (edge chunks preserved)
            (let [feature-results (pv/search feature-reopened (nth test-vectors 5) 10)]
              (is (pos? (count feature-results))
                  "Feature branch search should work after GC"))

            (finally
              (pv/close! main-reopened)
              (pv/close! feature-reopened))))

        (finally
          nil)))))

;; -----------------------------------------------------------------------------
;; Versioning API Tests

(deftest test-parents
  (testing "parents returns parent commit IDs"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        ;; First commit (root) has no parents
        (let [idx (a/<!! (-> idx (insert-n 5 32) (pv/sync!)))
              parents1 (pv/parents idx)]
          (is (= #{} parents1) "Root commit has empty parents")

          ;; Second commit has first commit as parent
          (let [idx (a/<!! (-> idx (insert-n 5 32 #(+ 5 %)) (pv/sync!)))
                parents2 (pv/parents idx)
                commit1 (:commit-id (second (pv/history idx)))]
            (is (= #{commit1} parents2) "Second commit should have first commit as parent")))

        (finally
          (pv/close! idx))))))

(deftest test-ancestors
  (testing "ancestors returns all ancestor commit IDs"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        ;; Create chain of 4 commits
        (let [idx (a/<!! (-> idx (insert-n 5 32) (pv/sync!)))
              commit1 (p/current-commit idx)
              idx (a/<!! (-> idx (insert-n 5 32 #(+ 5 %)) (pv/sync!)))
              commit2 (p/current-commit idx)
              idx (a/<!! (-> idx (insert-n 5 32 #(+ 10 %)) (pv/sync!)))
              commit3 (p/current-commit idx)
              idx (a/<!! (-> idx (insert-n 5 32 #(+ 15 %)) (pv/sync!)))
              commit4 (p/current-commit idx)
              ancestors (pv/ancestors idx)]

          ;; Should have all 4 commits, most recent first
          (is (= 4 (count ancestors)))
          (is (= commit4 (first ancestors)))
          (is (= commit1 (last ancestors))))

        (finally
          (pv/close! idx))))))

(deftest test-ancestor?
  (testing "ancestor? checks ancestry relationship"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        ;; Create chain: commit1 -> commit2 -> commit3
        (let [idx (a/<!! (-> idx (insert-n 5 32) (pv/sync!)))
              commit1 (p/current-commit idx)
              idx (a/<!! (-> idx (insert-n 5 32 #(+ 5 %)) (pv/sync!)))
              commit2 (p/current-commit idx)
              idx (a/<!! (-> idx (insert-n 5 32 #(+ 10 %)) (pv/sync!)))
              commit3 (p/current-commit idx)]

          ;; commit1 is ancestor of commit3
          (is (pv/ancestor? idx commit1 commit3))
          ;; commit2 is ancestor of commit3
          (is (pv/ancestor? idx commit2 commit3))
          ;; commit3 is NOT ancestor of commit1
          (is (not (pv/ancestor? idx commit3 commit1))))

        (finally
          (pv/close! idx))))))

(deftest test-common-ancestor
  (testing "common-ancestor finds merge base"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        ;; Create: main: c1 -> c2
        ;;         feature: c1 -> branch-commit -> c3
        ;; Note: branch! creates an intermediate commit (branch-commit)
        (let [idx (a/<!! (-> idx (insert-n 5 32) (pv/sync!)))
              commit1 (p/current-commit idx)

              ;; Branch before adding more to main
              feature-idx (pv/branch! idx :feature)
              branch-commit (p/current-commit feature-idx)  ;; branch! creates a new commit

              ;; Add to main
              idx (a/<!! (-> idx (insert-n 5 32 #(+ 5 %)) (pv/sync!)))
              commit2 (p/current-commit idx)

              ;; Add to feature
              feature-idx (a/<!! (-> feature-idx (insert-n 5 32 #(str "f-" %)) (pv/sync!)))
              commit3 (p/current-commit feature-idx)

              ;; Find common ancestor of commit2 and commit3
              ;; commit2's ancestors: [commit2, commit1]
              ;; commit3's ancestors: [commit3, branch-commit, commit1]
              ;; Common ancestor: commit1
              common (pv/common-ancestor idx commit2 commit3)]

          (is (= commit1 common) "Common ancestor should be commit1")

          (pv/close! feature-idx))

        (finally
          (pv/close! idx))))))

(deftest test-commit-info
  (testing "commit-info returns commit metadata"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        (let [idx (a/<!! (-> idx (insert-n 10 32) (pv/sync!)))
              commit-id (p/current-commit idx)
              info (pv/commit-info idx commit-id)]

          (is (= commit-id (:commit-id info)))
          (is (= #{} (:parents info)))
          (is (= :main (:branch info)))
          (is (= 10 (:vector-count info)))
          (is (inst? (:created-at info))))

        (finally
          (pv/close! idx))))))

(deftest test-commit-graph
  (testing "commit-graph returns full DAG"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        ;; Create: main: c1 -> c2
        ;;         feature: c1 -> branch-commit -> c3
        ;; Note: branch! creates an intermediate commit (branch-commit)
        (let [idx (a/<!! (-> idx (insert-n 5 32) (pv/sync!)))
              commit1 (p/current-commit idx)
              feature-idx (pv/branch! idx :feature)
              branch-commit (p/current-commit feature-idx)
              idx (a/<!! (-> idx (insert-n 5 32 #(+ 5 %)) (pv/sync!)))
              commit2 (p/current-commit idx)
              feature-idx (a/<!! (-> feature-idx (insert-n 5 32 #(str "f-" %)) (pv/sync!)))
              commit3 (p/current-commit feature-idx)
              graph (pv/commit-graph idx)]

          ;; Should have 4 nodes (c1, branch-commit, c2, c3)
          (is (= 4 (count (:nodes graph))))
          ;; Should have 2 branches
          (is (= #{:main :feature} (set (keys (:branches graph)))))
          ;; Should have 1 root (commit1)
          (is (= #{commit1} (:roots graph)))
          ;; Branch heads should be correct
          (is (= commit2 (:main (:branches graph))))
          (is (= commit3 (:feature (:branches graph))))

          (pv/close! feature-idx))

        (finally
          (pv/close! idx))))))

(deftest test-delete-branch
  (testing "delete-branch! removes branch"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        (let [idx (a/<!! (-> idx (insert-n 10 32) (pv/sync!)))
              feature-idx (pv/branch! idx :feature)]

          ;; Should have 2 branches
          (is (= #{:main :feature} (pv/branches feature-idx)))

          ;; Delete feature branch (from main's perspective)
          (pv/delete-branch! idx :feature)

          ;; Should only have main now
          (is (= #{:main} (pv/branches idx)))

          ;; Cannot delete current branch
          (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                #"Cannot delete current branch"
                                (pv/delete-branch! idx :main)))

          (pv/close! feature-idx))

        (finally
          (pv/close! idx))))))

(deftest test-reset!
  (testing "reset! moves branch to different commit"
    (let [idx (create-test-index {:type :hnsw
                                  :dim 32
                                  :storage-path *test-path*
                                  :capacity 1000})]
      (try
        ;; Create commits: c1 (5 vecs) -> c2 (10 vecs) -> c3 (15 vecs)
        (let [idx (a/<!! (-> idx (insert-n 5 32) (pv/sync!)))
              commit1 (p/current-commit idx)
              idx (a/<!! (-> idx (insert-n 5 32 #(+ 5 %)) (pv/sync!)))
              commit2 (p/current-commit idx)
              idx (a/<!! (-> idx (insert-n 5 32 #(+ 10 %)) (pv/sync!)))
              commit3 (p/current-commit idx)]

          ;; Current state: 15 vectors at commit3
          (is (= 15 (pv/count-vectors idx)))
          (is (= commit3 (p/current-commit idx)))

          ;; Reset to commit1
          (let [reset-idx (pv/reset! idx commit1)]
            (try
              (is (= 5 (pv/count-vectors reset-idx)))
              (is (= commit1 (p/current-commit reset-idx)))
              (finally
                (pv/close! reset-idx))))

          ;; Reset to invalid commit should throw
          (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                #"Commit not found"
                                (pv/reset! idx (java.util.UUID/randomUUID)))))

        (finally
          (pv/close! idx))))))
