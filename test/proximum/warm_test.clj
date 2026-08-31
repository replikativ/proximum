(ns proximum.warm-test
  "The parallel restore fetch and the tree warm.

   The restore assertions are EQUIVALENCE assertions: a parallel fetch must
   produce an index indistinguishable from the serial one — same edge count,
   same entrypoint, same search results — because the parallelism is confined
   to the konserve reads and every mutation stays on the restoring thread.
   `:fetch-width 1` IS the old serial loop (proximum.fetch degrades to it), so
   the control is not a mock but the actual previous behaviour."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :as a]
            [proximum.core :as core]
            [proximum.warm :as warm]
            [proximum.protocols :as p])
  (:import [java.io File]
           [proximum.internal PersistentEdgeIndex]))

(def ^:dynamic *store-id* nil)

(defn with-store-id-fixture [f]
  (binding [*store-id* (java.util.UUID/randomUUID)]
    (f)))

(use-fixtures :each with-store-id-fixture)

(defn- cleanup [path]
  (let [f (File. ^String path)]
    (when (.exists f)
      (doseq [^File file (reverse (file-seq f))]
        (.delete file)))))

(defn- random-vec [dim]
  (float-array (repeatedly dim #(- (rand 2.0) 1.0))))

(defn- base-paths [tag]
  (let [base (str "/tmp/proximum-warm-test-" tag "-" (System/nanoTime))]
    {:store {:backend :file :path (str base "/store") :id *store-id*}
     :mmap  (str base "/mmap")
     :base  base}))

(defn- build-and-sync!
  "An index with enough vectors to spread across several edge and vector
   chunks, synced to a file store. Returns the query used for comparisons."
  [{:keys [store mmap]} n dim]
  (let [idx  (core/create-index {:type :hnsw :dim dim :M 8 :ef-construction 50
                                 :capacity (* 2 n)
                                 :store-config store :mmap-dir mmap})
        vecs (vec (repeatedly n #(random-vec dim)))
        idx  (core/insert-batch idx vecs (range n))
        idx  (a/<!! (core/sync! idx))]
    (a/<!! (core/close! idx))
    vecs))

(deftest parallel-restore-is-indistinguishable-from-serial
  (let [{:keys [store mmap base] :as paths} (base-paths "eq")
        dim  32
        vecs (build-and-sync! paths 300 dim)
        q    (nth vecs 17)]
    (try
      (let [serial   (core/load store :mmap-dir mmap :fetch-width 1)
            s-edges  (.countEdges ^PersistentEdgeIndex (p/edge-storage serial))
            s-entry  (.getEntrypoint ^PersistentEdgeIndex (p/edge-storage serial))
            ;; Explore the complete small graph. At a finite ANN breadth even a
            ;; query copied from the corpus is not contractually guaranteed to
            ;; find itself, which made this restore-equivalence test random.
            s-hits   (mapv :id (core/search serial q 10 {:ef 300}))
            _        (a/<!! (core/close! serial))
            ;; a DIFFERENT mmap dir, so the parallel load cannot inherit the
            ;; serial load's materialized cache and pass by reuse
            parallel (core/load store :mmap-dir (str base "/mmap2") :fetch-width 64)
            p-edges  (.countEdges ^PersistentEdgeIndex (p/edge-storage parallel))
            p-entry  (.getEntrypoint ^PersistentEdgeIndex (p/edge-storage parallel))
            p-hits   (mapv :id (core/search parallel q 10 {:ef 300}))]
        (is (pos? s-edges) "the fixture actually built a graph")
        (is (= s-edges p-edges) "same edge count")
        (is (= s-entry p-entry) "same entrypoint")
        (is (= s-hits p-hits) "identical search results, order included")
        (is (= 17 (first p-hits)) "and the query finds its own vector first")
        (a/<!! (core/close! parallel)))
      (finally (cleanup base)))))

(deftest warm-covers-the-lazy-trees
  ;; 1500 entries: past the PSS branching factor, so the id/metadata trees have
  ;; leaves BELOW an interior root and the walk has something to fetch. (At a
  ;; few hundred entries a tree is one leaf, the walk correctly reports 0, and
  ;; this test would be vacuous.)
  (let [{:keys [store mmap base] :as paths} (base-paths "warm")]
    (build-and-sync! paths 1500 16)
    (try
      (let [idx (core/load store :mmap-dir mmap)
            r   (warm/warm! idx {:depth :with-leaves :budget 100000})]
        (is (pos? (:fetched r)) "a restored index has lazy tree nodes to warm")
        (is (contains? (:by-index r) :external-id-index))
        (is (contains? (:by-index r) :metadata))
        (is (false? (:budget-exhausted? r)))
        (testing "the budget is a hard ceiling here too"
          (let [idx2 (core/load store :mmap-dir (str base "/mmap3"))
                r2   (warm/warm! idx2 {:depth :with-leaves :budget 2})]
            (is (= 2 (:fetched r2)))
            (is (true? (:budget-exhausted? r2)))
            (a/<!! (core/close! idx2))))
        (testing "id lookups work on the warmed index"
          (is (some? (core/get-metadata idx 17))))
        (a/<!! (core/close! idx)))
      (finally (cleanup base)))))
