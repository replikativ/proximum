(ns proximum.batch-insert-test
  "A parallel batch insert must leave the graph as connected as inserting the
   same vectors one at a time. Three races used to cost it about one vector in a
   hundred, unreachable by any search:

   - two inserts adding a back-link to the same neighbor each read its list,
     appended, and wrote it back; the second write dropped the first link;
   - a node wrote its own neighbor list over the back-links that concurrent
     inserts had already put there;
   - a node was linked on its upper layers first, so another insert could
     descend onto it and search the layer below from a node without edges.

   And two that left a vector with no edges at all, or with the same neighbor
   twice:

   - the first insert published the entry point and the height as two writes;
     an insert that saw the entry point but not yet the height linked itself
     on no layer;
   - a candidate that was both in flight and already in the graph was selected
     twice when its vector equalled the new one."
  (:require [clojure.test :refer [deftest is]]
            [proximum.core :as core]
            [proximum.hnsw.internal :as hi])
  (:import [proximum.internal PersistentEdgeIndex]))

(defn- random-vector [dim]
  (float-array (repeatedly dim #(- (rand 2.0) 1.0))))

(defn- unreachable-after-batch [n dim]
  (let [idx (core/insert-batch
             (core/create-index {:type :hnsw :dim dim :M 8 :ef-construction 50
                                 :capacity (* 2 n)
                                 :store-config {:backend :memory :id (random-uuid)}})
             (vec (repeatedly n #(random-vector dim)))
             (range n))]
    (try
      (:unreachable-count (core/reachability idx))
      (finally (core/close! idx)))))

(deftest a-parallel-batch-leaves-the-graph-connected
  ;; 18,000 vectors over 60 builds. The races left about 170 of them
  ;; unreachable; pruning alone, which a sequential build has too, leaves about
  ;; 2. The bound sits far from both, so this does not fail by chance.
  (let [unreachable (reduce + (repeatedly 60 #(unreachable-after-batch 300 32)))]
    (is (<= unreachable 30)
        (str unreachable " of 18000 batch-inserted vectors cannot be reached"))))

(deftest a-node-keeps-the-back-links-that-arrived-before-its-own-list
  ;; Points 0..3 at x = 1..4. A concurrent insert that picked the next node (4)
  ;; as a neighbor has already written its back-link 4 -> 3. Node 4 is then
  ;; inserted at x = 0: it selects 0, and 3 is not among its own choices.
  (let [base (reduce (fn [idx i] (core/insert idx (float-array [(float (inc i)) 0.0]) i))
                     (core/create-index {:type :hnsw :dim 2 :distance :euclidean
                                         :capacity 100 :seed 42
                                         :store-config {:backend :memory :id (random-uuid)}})
                     (range 4))
        ^PersistentEdgeIndex pes (hi/edges base)]
    (try
      (.asTransient pes)
      (.setNeighbors pes 0 4 (int-array [3]))
      (.asPersistent pes)
      (let [idx (core/insert base (float-array [0.0 0.0]) 4)
            neighbors (set (.getNeighbors ^PersistentEdgeIndex (hi/edges idx) 0 4))]
        (is (contains? neighbors 0) "its own choice")
        (is (contains? neighbors 3) "and the link that was already there"))
      (finally (core/close! base)))))

(defn- layer-0-lists [vectors]
  (let [n (count vectors)
        idx (core/insert-batch
             (core/create-index {:type :hnsw :dim (alength ^floats (first vectors))
                                 :distance :euclidean :M 8 :ef-construction 40
                                 :capacity (* 2 n)
                                 :store-config {:backend :memory :id (random-uuid)}})
             vectors
             (range n))
        ^PersistentEdgeIndex pes (hi/edges idx)]
    (try
      (mapv #(vec (.getNeighbors pes 0 (int %))) (range n))
      (finally (core/close! idx)))))

(deftest every-vector-of-a-batch-into-an-empty-index-is-linked
  ;; 202 vectors split into two tasks that start together on an empty graph.
  ;; About one such build in 150 used to leave a vector without any edge.
  (let [edgeless (reduce + (repeatedly 600
                                       #(count (filter empty?
                                                       (layer-0-lists (vec (repeatedly 202 (fn [] (random-vector 16)))))))))]
    (is (zero? edgeless) (str edgeless " vectors were left without edges"))))

(deftest no-neighbor-is-listed-twice
  ;; Identical vectors: the diversity check compares 0 < 0 and lets the second
  ;; copy of a candidate through.
  (let [v (random-vector 16)
        twice (reduce + (repeatedly 30
                                    #(count (remove (fn [l] (= (count l) (count (distinct l))))
                                                    (layer-0-lists (vec (repeatedly 300 (fn [] (aclone ^floats v)))))))))]
    (is (zero? twice) (str twice " neighbor lists hold the same id twice"))))

(deftest an-ef-construction-above-the-buffer-cap-is-honoured
  ;; The insert search buffers were capped at 4096 entries while the result
  ;; heap grew to ef-construction: with more reachable vectors than the cap,
  ;; the next result was written past the end of the array.
  (let [n 4300
        idx (core/insert-batch
             (core/create-index {:type :hnsw :dim 2 :distance :euclidean :M 8
                                 :ef-construction 4500 :capacity (* 2 n)
                                 :store-config {:backend :memory :id (random-uuid)}})
             (vec (repeatedly n #(random-vector 2)))
             (range n)
             {:parallelism 1})]
    (try
      (is (= n (core/count-vectors idx)))
      (finally (core/close! idx)))))
