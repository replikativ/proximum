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

(defn- drain-pages [scan]
  (loop [scan scan pages [] remaining 1000]
    (when (zero? remaining)
      (throw (ex-info "Candidate scan did not terminate" {})))
    (let [page (core/candidate-page scan)
          pages (conj pages page)]
      (if (:exhausted? page)
        pages
        (recur (:continuation page) pages (dec remaining))))))

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

(deftest iterative-candidate-scan-resumes-native-frontier-test
  (let [base (test-index)
        idx (reduce (fn [i n]
                      (core/insert i (float-array [(float n) 0.0]) n))
                    base
                    (range 80))]
    (try
      (let [scan (core/start-candidate-scan
                  idx (float-array [0.0 0.0])
                  {:mode :iterative :page-size 5 :ef 8
                   :strict-order? false
                   :primary-snapshot-id :iterative-test})
            pages (drain-pages scan)
            candidates (vec (mapcat :candidates pages))
            ids (mapv :id candidates)
            continuations (keep :continuation pages)
            last-page (peek pages)]
        (is (> (count ids) 8)
            "resuming explores beyond the first ef candidates")
        (is (= (count ids) (count (distinct ids))))
        (is (> (count pages) 2))
        (is (= (count continuations) (count (distinct continuations))))
        (is (= :resumable-hnsw-frontier (:exhaustion-scope last-page)))
        (is (= :frontier-empty (:stop-reason last-page)))
        (is (= (count ids) (get-in last-page [:metadata :emitted-count])))
        (is (>= (get-in last-page [:metadata :visited-count]) (count ids))))

      (let [pages (drain-pages
                   (core/start-candidate-scan
                    idx (float-array [0.0 0.0])
                    {:mode :iterative :page-size 3 :ef 6
                     :strict-order? true
                     :primary-snapshot-id :strict-test}))
            distances (mapv :rank-distance (mapcat :candidates pages))]
        (is (apply <= distances)
            "strict mode never emits a distance regression across pages")
        (is (= (count distances) (count (distinct (map :id (mapcat :candidates pages)))))))

      (let [pages (drain-pages
                   (core/start-candidate-scan
                    idx (float-array [0.0 0.0])
                    {:mode :iterative :page-size 5 :ef 8
                     :max-visited 16
                     :primary-snapshot-id :budget-test}))
            last-page (peek pages)]
        (is (= :visited-budget (:stop-reason last-page)))
        (is (<= (get-in last-page [:metadata :visited-count]) 16)))
      (finally
        (a/<!! (core/close! idx))))))

(deftest iterative-continuations-are-affine-and-retryable-test
  (let [base (test-index)
        idx (reduce (fn [i n]
                      (core/insert i (float-array [(float n) 0.0]) n))
                    base
                    (range 30))]
    (try
      (let [scan (core/start-candidate-scan
                  idx (float-array [0.0 0.0])
                  {:mode :iterative :page-size 3 :ef 6
                   :primary-snapshot-id :affine})
            page-1 (core/candidate-page scan)
            continuation-1 (:continuation page-1)
            page-2 (core/candidate-page continuation-1)]
        (is (thrown-with-msg? IllegalStateException #"stale or out-of-order"
                              (core/candidate-page scan)))
        (is (thrown-with-msg? IllegalStateException #"stale or out-of-order"
                              (core/candidate-page continuation-1)))
        (core/close-candidate-scan! (:continuation page-2)))

      (let [scan (core/start-candidate-scan
                  idx (float-array [0.0 0.0])
                  {:mode :iterative :page-size 3 :ef 6
                   :primary-snapshot-id :retry})]
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo #"conversion failed"
             (with-redefs [p/get-metadata
                           (fn [_ _]
                             (throw (ex-info "conversion failed" {})))]
               (core/candidate-page scan))))
        (let [page (core/candidate-page scan)]
          (is (= 3 (count (:candidates page)))
              "the same staged native page is available after conversion failure")
          (core/close-candidate-scan! (:continuation page))))
      (finally
        (a/<!! (core/close! idx))))))

(deftest iterative-cursor-leases-vector-store-test
  (let [base (test-index)
        idx (core/insert base (float-array [0.0 0.0]) :one)
        scan (core/start-candidate-scan
              idx (float-array [0.0 0.0])
              {:mode :iterative :page-size 1 :ef 2
               :primary-snapshot-id :lease})
        close-result (core/close! idx)
        timeout (a/timeout 50)]
    (is (= timeout (second (a/alts!! [close-result timeout])))
        "index close waits for an active cursor lease")
    (is (= [:one] (mapv :id (:candidates (core/candidate-page scan))))
        "a close request cannot invalidate the cursor's MemorySegment")
    (is (nil? (a/<!! close-result)))))

(deftest iterative-frontier-memory-budget-test
  (let [base (test-index)
        idx (reduce (fn [i n]
                      (core/insert i (float-array [(float n) 0.0]) n))
                    base
                    (range 30))]
    (try
      (let [pages (drain-pages
                   (core/start-candidate-scan
                    idx (float-array [0.0 0.0])
                    {:mode :iterative :page-size 2 :ef 2
                     :max-frontier-nodes 2
                     :primary-snapshot-id :memory-budget}))]
        (is (= :memory-budget (:stop-reason (peek pages)))))
      (finally
        (a/<!! (core/close! idx))))))

(deftest transport-page-size-does-not-change-traversal-batches-test
  (let [base (test-index)
        idx (reduce (fn [i n]
                      (core/insert i (float-array [(float n) 0.0]) n))
                    base
                    (range 40))]
    (try
      (let [options {:mode :iterative :ef 4 :strict-order? false
                     :primary-snapshot-id :page-size}
            small (->> (core/start-candidate-scan
                        idx (float-array [0.0 0.0])
                        (assoc options :page-size 2))
                       drain-pages
                       (mapcat :candidates)
                       (take 20)
                       (mapv :internal-id))
            large-page (core/candidate-page
                        (core/start-candidate-scan
                         idx (float-array [0.0 0.0])
                         (assoc options :page-size 20)))
            large (mapv :internal-id (:candidates large-page))]
        (is (= small large)
            "transport batching does not widen ef or change traversal order")
        (core/close-candidate-scan! (:continuation large-page)))
      (finally
        (a/<!! (core/close! idx))))))

(deftest cursor-cancellation-covers-unread-and-pending-pages-test
  (let [base (test-index)
        idx (reduce (fn [i n]
                      (core/insert i (float-array [(float n) 0.0]) n))
                    base
                    (range 10))]
    (try
      (let [unread (core/start-candidate-scan
                    idx (float-array [0.0 0.0])
                    {:mode :iterative :page-size 2 :ef 4
                     :primary-snapshot-id :unread})]
        (core/close-candidate-scan! unread)
        (is (thrown-with-msg? IllegalStateException #"closed"
                              (core/candidate-page unread))))

      (let [pending (core/start-candidate-scan
                     idx (float-array [0.0 0.0])
                     {:mode :iterative :page-size 2 :ef 4
                      :primary-snapshot-id :pending})
            cursor (:cursor pending)]
        (.page ^proximum.internal.HnswCandidateCursor cursor 0 2)
        (core/close-candidate-scan! pending)
        (is (thrown-with-msg? IllegalStateException #"closed"
                              (.page ^proximum.internal.HnswCandidateCursor
                               cursor 0 2))))
      (finally
        (a/<!! (core/close! idx))))))

(deftest close-publication-rejects-new-cursor-leases-test
  (let [base (test-index)
        idx (core/insert base (float-array [0.0 0.0]) :one)
        active (core/start-candidate-scan
                idx (float-array [0.0 0.0])
                {:mode :iterative :page-size 1 :ef 2
                 :primary-snapshot-id :active})
        close-result (core/close! idx)]
    (is (= :vector-store-closing
           (reason #(core/start-candidate-scan
                     idx (float-array [0.0 0.0])
                     {:mode :iterative :page-size 1 :ef 2
                      :primary-snapshot-id :too-late}))))
    (core/close-candidate-scan! active)
    (is (nil? (a/<!! close-result)))))

(deftest native-search-breaks-distance-ties-by-internal-id-test
  (let [base (test-index)
        idx (reduce (fn [i n]
                      (core/insert i (float-array [1.0 0.0]) n))
                    base
                    (range 10))]
    (try
      (is (= (range 5)
             (map :id (core/search idx (float-array [0.0 0.0]) 5 {:ef 20}))))
      (is (= (range 5)
             (map :id (core/search-filtered
                       idx (float-array [0.0 0.0]) 5 (set (range 10))
                       {:ef 20}))))
      (let [pages (drain-pages
                   (core/start-candidate-scan
                    idx (float-array [0.0 0.0])
                    {:mode :iterative :page-size 2 :ef 4
                     :strict-order? true
                     :primary-snapshot-id :ties}))
            internal-ids (mapv :internal-id (mapcat :candidates pages))]
        (is (apply <= internal-ids)
            "strict iterative ordering includes the internal-id tie-break"))
      (finally
        (a/<!! (core/close! idx))))))
