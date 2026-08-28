(ns proximum.candidate-cursor-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is]]
            [proximum.core :as core]
            [proximum.protocols :as p]))

(defn- test-index []
  (core/create-index {:type :hnsw
                      :dim 2
                      :distance :euclidean
                      :capacity 100
                      :seed 42
                      :store-config {:backend :memory :id (random-uuid)}}))

(defn- reason [f]
  (try
    (f)
    nil
    (catch clojure.lang.ExceptionInfo e
      (:reason (ex-data e)))))

(deftest stable-candidate-pagination-test
  (let [base (test-index)
        idx (reduce (fn [i n]
                      (core/insert i (float-array [(float n) 0.0]) n))
                    base
                    (range 8))]
    (try
      (let [scan (core/start-candidate-scan
                  idx (float-array [0.0 0.0])
                  {:candidate-limit 6 :page-size 2 :ef 30
                   :primary-snapshot-id :test-snapshot})
            page-1 (core/candidate-page scan)
            ;; Mutate a descendant after materialization. Continuation owns the
            ;; frozen candidates and must remain byte-for-byte stable.
            _mutated (core/insert idx (float-array [0.1 0.0]) :later)
            page-2 (core/candidate-page (:continuation page-1))
            page-3 (core/candidate-page (:continuation page-2))]
        (is (= [2 2 2] (mapv (comp count :candidates) [page-1 page-2 page-3])))
        (is (false? (:exhausted? page-1)))
        (is (true? (:exhausted? page-3)))
        (is (nil? (:continuation page-3)))
        (is (= :approximate (get-in page-1 [:metadata :recall])))
        (is (false? (get-in page-1 [:metadata :complete-recall?])))
        (is (= :fixed-ann-candidate-set (:exhaustion-scope page-3)))
        (is (= [:rank-distance :internal-id]
               (get-in page-1 [:metadata :ordering])))
        (is (= :test-snapshot
               (get-in page-1 [:metadata :primary-snapshot-id])))
        (is (nil? (get-in page-1 [:metadata :index-commit-id])))
        (is (uuid? (get-in page-1 [:metadata :query-id])))
        (is (true? (get-in page-1 [:metadata :mutation-stable?])))
        (is (every? :distance-exact?
                    (mapcat :candidates [page-1 page-2 page-3])))
        (is (not-any? #(= :later (:id %))
                      (mapcat :candidates [page-1 page-2 page-3]))))
      (finally
        (a/<!! (core/close! idx))))))

(deftest candidate-identity-requirements-test
  (let [base (test-index)
        with-id (core/insert base (float-array [1.0 0.0]) :one)]
    (try
      (is (= :missing-primary-snapshot-id
             (reason #(core/start-candidate-scan
                       with-id (float-array [1.0 0.0])))))
      (is (= :index-commit-mismatch
             (reason #(core/start-candidate-scan
                       with-id (float-array [1.0 0.0])
                       {:expected-index-commit-id :not-this-commit
                        :primary-snapshot-id :snapshot}))))
      (let [scan-a (core/start-candidate-scan
                    with-id (float-array [1.0 0.0])
                    {:primary-snapshot-id :snapshot})
            scan-b (core/start-candidate-scan
                    with-id (float-array [1.0 0.0])
                    {:primary-snapshot-id :snapshot})]
        (is (= (get-in scan-a [:metadata :query-id])
               (get-in scan-b [:metadata :query-id]))))
      (finally
        (a/<!! (core/close! base)))))
  (let [base (test-index)
        without-id (p/insert base (float-array [1.0 0.0]) nil)]
    (try
      (is (= :missing-external-id
             (reason #(core/start-candidate-scan
                       without-id (float-array [1.0 0.0])
                       {:primary-snapshot-id :snapshot}))))
      (finally
        (a/<!! (core/close! base))))))

(deftest candidate-beam-preserves-index-default-test
  (let [idx (test-index)
        calls (atom [])
        query (float-array [0.0 0.0])]
    (try
      (with-redefs [p/search (fn [_idx _query _k options]
                               (swap! calls conj options)
                               [])]
        (core/start-candidate-scan
         idx query {:candidate-limit 6 :primary-snapshot-id :snapshot})
        (core/start-candidate-scan
         idx query {:candidate-limit 6 :ef 30 :primary-snapshot-id :snapshot})
        (core/start-candidate-scan
         idx query {:candidate-limit 6 :ef 2 :primary-snapshot-id :snapshot}))
      (is (= [{} {:ef 30} {:ef 6}] @calls)
          "an omitted :ef delegates to the index generation's beam")
      (finally
        (a/<!! (core/close! idx))))))
