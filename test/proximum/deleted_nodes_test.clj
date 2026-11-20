(ns proximum.deleted-nodes-test
  "Hand-wired graphs for what a deleted node may and may not do to a traversal.

   `delete` clears the deleted node's edges and removes it from the lists of its
   OWN neighbors. Edges are directed, so a node that pointed at it without being
   pointed back at keeps a stale edge to a node that now has none. Each test
   wires exactly the edges that produce one such situation, so it fails every
   time rather than in a few runs out of a hundred."
  (:require [clojure.test :refer [deftest is testing]]
            [proximum.core :as core]
            [proximum.hnsw.internal :as hi])
  (:import [proximum.internal PersistentEdgeIndex]))

(defn- index-of
  "An index over 2-d points, inserted in order: point i has internal and external id i."
  [points]
  (reduce (fn [idx [i [x y]]] (core/insert idx (float-array [x y]) i))
          (core/create-index {:type :hnsw :dim 2 :distance :euclidean :capacity 100
                              :seed 42
                              :store-config {:backend :memory :id (random-uuid)}})
          (map-indexed vector points)))

(defn- wire!
  "Replace the index's graph: `layers` is {layer {node [neighbors]}}; every node
   not mentioned has no edges on that layer."
  [idx n {:keys [entry max-level layers]}]
  (let [^PersistentEdgeIndex pes (hi/edges idx)]
    (.asTransient pes)
    (try
      (doseq [layer (range (inc (.getCurrentMaxLevel pes)))
              node (range n)]
        (.setNeighbors pes (int layer) (int node) (int-array 0)))
      (doseq [[layer nodes] layers
              [node neighbors] nodes]
        (.setNeighbors pes (int layer) (int node) (int-array neighbors)))
      (.setCurrentMaxLevel pes (int max-level))
      (.setEntrypoint pes (int entry))
      (finally (.asPersistent pes)))
    idx))

(defn- ids [idx query k]
  (set (map :id (core/search idx (float-array query) k))))

(defn- scan-ids [idx query]
  (loop [scan (core/start-candidate-scan idx (float-array query)
                                         {:candidate-limit 10 :page-size 10 :ef 30
                                          :primary-snapshot-id :test-snapshot})
         acc #{} remaining 20]
    (let [page (core/candidate-page scan)
          acc (into acc (map :id) (:candidates page))]
      (if (or (:exhausted? page) (zero? remaining))
        acc
        (recur (:continuation page) acc (dec remaining))))))

;; 0 is the entry point and, on layer 1, points at 1 - which does not point
;; back, so deleting 1 leaves that edge in place. 1 is the nearest node to the
;; query: the descent steps onto it.
(def ^:private stale-upper-edge
  {:points [[0 0] [10 0] [1 0] [2 0]]
   :graph {:entry 0 :max-level 1
           :layers {1 {0 [1]}
                    0 {0 [2] 2 [0 3] 3 [2]}}}})

(deftest the-descent-does-not-step-onto-a-deleted-node
  (let [{:keys [points graph]} stale-upper-edge
        idx (-> (index-of points) (wire! 4 graph) (dissoc 1))]
    (try
      (is (= #{0 2 3} (ids idx [10 0] 10))
          "every live vector is found; it returned nothing")
      (is (= #{0 2 3} (scan-ids idx [10 0]))
          "and by the candidate cursor, which descends on its own")
      (testing "a vector inserted afterwards joins the same graph"
        (let [idx' (core/insert idx (float-array [9 0]) 9)]
          (is (= #{0 2 3 9} (ids idx' [10 0] 10)))))
      (finally (core/close! idx)))))

;; 0 -> 1 -> 2 -> 1. Deleting 1 leaves 2 without a single edge; deleting the
;; entry point 0 then finds no live neighbor and no live node WITH edges.
(deftest a-live-vector-without-edges-becomes-the-entry-point
  (let [idx (-> (index-of [[0 0] [1 0] [2 0]])
                (wire! 3 {:entry 0 :max-level 0
                          :layers {0 {0 [1] 1 [2] 2 [1]}}})
                (dissoc 1)
                (dissoc 0))]
    (try
      (is (= 2 (.getEntrypoint ^PersistentEdgeIndex (hi/edges idx)))
          "the graph was declared empty with a live vector in it")
      (is (= #{2} (ids idx [0 0] 10)))
      (let [idx' (core/insert idx (float-array [3 0]) 3)]
        (is (= #{2 3} (ids idx' [0 0] 10))
            "and the next insert links to it instead of starting a new graph"))
      (finally (core/close! idx)))))

;; The entry point is the only node on layer 1. Its replacement lives on
;; layer 0, and the hierarchy is then one layer tall.
(deftest the-hierarchy-is-as-tall-as-the-replacement-entry-point
  (let [idx (-> (index-of [[0 0] [1 0] [2 0]])
                (wire! 3 {:entry 0 :max-level 1
                          :layers {1 {0 []}
                                   0 {0 [1] 1 [0 2] 2 [1]}}})
                (dissoc 0))
        ^PersistentEdgeIndex pes (hi/edges idx)]
    (try
      (is (= 1 (.getEntrypoint pes)))
      (is (= 0 (.getCurrentMaxLevel pes)))
      (is (= #{1 2} (ids idx [0 0] 10)))
      (finally (core/close! idx)))))

;; 5 points at both 6, deleted earlier, and 7. Deleting 7 must not hand the
;; stale 6 to 7's other neighbor as a repair candidate, nor keep it in 5's list.
(deftest repair-does-not-reconnect-to-an-older-deleted-node
  (let [idx (-> (index-of [[0 0] [1 0] [2 0] [3 0] [4 0] [5 0] [6 0] [7 0]])
                (wire! 8 {:entry 0 :max-level 0
                          :layers {0 {0 [5] 5 [0 6 7] 6 [] 7 [5 6 4] 4 [7]}}})
                (dissoc 6)
                (dissoc 7))
        ^PersistentEdgeIndex pes (hi/edges idx)
        neighbors (fn [node] (set (.getNeighbors pes 0 (int node))))]
    (try
      (is (not-any? #{6 7} (neighbors 5)))
      (is (not-any? #{6 7} (neighbors 4)))
      (is (= #{0 4 5} (ids idx [7 0] 10)))
      (finally (core/close! idx)))))
