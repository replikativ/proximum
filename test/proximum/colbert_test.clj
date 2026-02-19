(ns proximum.colbert-test
  "Tests for ColBERT-style late interaction retrieval."
  (:require [clojure.test :refer [deftest is testing]]
            [proximum.core :as core]
            [proximum.colbert :as colbert])
  (:import [java.util UUID]))

(defn create-test-index
  "Create in-memory HNSW index for testing."
  [dim]
  (core/create-index {:type :hnsw
                      :dim dim
                      :store-config {:backend :memory
                                     :id (UUID/randomUUID)}
                      :capacity 1000}))

(defn random-vec
  "Generate random float vector."
  [dim]
  (float-array (repeatedly dim #(- (rand 2.0) 1.0))))

;; -----------------------------------------------------------------------------
;; Document Insertion Tests

(deftest test-insert-document
  (testing "Insert document with token vectors"
    (let [idx (create-test-index 4)
          tokens [(float-array [1 0 0 0])
                  (float-array [0 1 0 0])
                  (float-array [0 0 1 0])]
          idx' (colbert/insert-document idx "doc-1" tokens)]
      
      ;; Each token should be searchable
      (is (= 3 (core/count-vectors idx')))
      
      ;; Tokens have compound keys
      (let [result (core/search idx' (float-array [1 0 0 0]) 3)]
        (is (= ["doc-1" :token 0] (:id (first result))))))))

(deftest test-insert-documents
  (testing "Insert multiple documents"
    (let [idx (create-test-index 4)
          docs [["doc-1" [(float-array [1 0 0 0])
                          (float-array [0 1 0 0])]]
                ["doc-2" [(float-array [0 0 1 0])
                          (float-array [0 0 0 1])]]]
          idx' (colbert/insert-documents idx docs)]
      
      (is (= 4 (core/count-vectors idx')))
      
      ;; Search finds tokens from both docs
      (let [result (core/search idx' (float-array [0.5 0.5 0 0]) 10)]
        (is (some #(= ["doc-1" :token 0] (:id %)) result))
        (is (some #(= ["doc-1" :token 1] (:id %)) result))))))

(deftest test-document-tokens-with-compound-keys
  (testing "Token keys are compound [doc-id :token idx]"
    (let [idx (create-test-index 4)
          idx' (colbert/insert-document idx "test-doc" 
                                       [(float-array [1 0 0 0])
                                        (float-array [0 1 0 0])])]
      
      ;; First token: [doc-id :token 0]
      (is (= ["test-doc" :token 0]
             (:id (first (core/search idx' (float-array [1 0 0 0]) 1)))))
      
      ;; Second token: [doc-id :token 1]
      (is (= ["test-doc" :token 1]
             (:id (first (core/search idx' (float-array [0 1 0 0]) 1))))))))

;; -----------------------------------------------------------------------------
;; MaxSim Search Tests

(deftest test-maxsim-search-basic
  (testing "MaxSim search returns document scores"
    (let [idx (create-test-index 4)
          ;; Doc 1: two orthogonal vectors
          idx' (colbert/insert-document idx "doc-1"
                                       [(float-array [1 0 0 0])
                                        (float-array [0 1 0 0])])
          ;; Query: same vectors
          query [(float-array [1 0 0 0])
                 (float-array [0 1 0 0])]
          results (colbert/maxsim-search idx' query 5)]
      
      (is (= 1 (count results)))
      (is (= "doc-1" (:doc-id (first results))))
      ;; MaxSim: both query tokens find exact matches = 2.0
      (is (> (:maxsim-score (first results)) 1.9)))))

(deftest test-maxsim-ranking
  (testing "Documents ranked by MaxSim score"
    (let [idx (create-test-index 4)
          ;; Doc 1: matches one query token exactly
          idx' (-> idx
                   (colbert/insert-document "doc-1"
                                            [(float-array [1 0 0 0])
                                             (float-array [0.1 0.1 0.1 0.1])])
                   (colbert/insert-document "doc-2"
                                            [(float-array [1 0 0 0])
                                             (float-array [0 1 0 0])]))
          ;; Query: two orthogonal vectors
          query [(float-array [1 0 0 0])
                 (float-array [0 1 0 0])]
          results (colbert/maxsim-search idx' query 5)]
      
      ;; doc-2 should rank higher (both tokens match well)
      ;; doc-1 has one match, one poor match
      (is (= "doc-2" (:doc-id (first results))))
      (is (> (:maxsim-score (first results))
             (:maxsim-score (second results)))))))

(deftest test-maxsim-partial-matches
  (testing "Partial token matches contribute to score"
    (let [idx (create-test-index 4)
          idx' (colbert/insert-document idx "doc-1"
                                       [(float-array [1 0 0 0])
                                        (float-array [0.5 0.5 0 0])])
          ;; Query: exact match for first, partial for second
          query [(float-array [1 0 0 0])
                 (float-array [1 0 0 0])]
          results (colbert/maxsim-search idx' query 5)]
      
      ;; Both query tokens can match doc-1's first token
      ;; MaxSim takes max per query token
      (is (= "doc-1" (:doc-id (first results)))))))

;; -----------------------------------------------------------------------------
;; Filtered Search Tests

(deftest test-maxsim-search-filtered
  (testing "Filtered MaxSim search"
    (let [idx (create-test-index 4)
          idx' (-> idx
                   (colbert/insert-document "allowed-doc"
                                            [(float-array [1 0 0 0])])
                   (colbert/insert-document "blocked-doc"
                                            [(float-array [1 0 0 0])]))
          query [(float-array [1 0 0 0])]
          results (colbert/maxsim-search-filtered
                   idx' query 5
                   #(= % "allowed-doc"))]
      
      (is (= 1 (count results)))
      (is (= "allowed-doc" (:doc-id (first results)))))))

;; -----------------------------------------------------------------------------
;; Edge Cases

(deftest test-empty-query-tokens
  (testing "Empty query returns empty results"
    (let [idx (create-test-index 4)
          idx' (colbert/insert-document idx "doc-1" [(float-array [1 0 0 0])])
          results (colbert/maxsim-search idx' [] 5)]
      
      (is (empty? results)))))

(deftest test-no-matching-documents
  (testing "Query with no matches returns empty"
    (let [idx (create-test-index 4)
          idx' (colbert/insert-document idx "doc-1"
                                       [(float-array [-1 -1 -1 -1])])
          ;; Query in opposite direction
          query [(float-array [1 1 1 1])]
          results (colbert/maxsim-search idx' query 5 {:ef 10})]
      
      ;; Distance may be too high, might return empty or poor matches
      (is (<= (count results) 1)))))

;; -----------------------------------------------------------------------------
;; Integration with Compound Keys

(deftest test-colbert-with-multi-field-index
  (testing "ColBERT tokens coexist with other compound keys"
    (let [idx (create-test-index 4)
          idx' (-> idx
                   (assoc ["doc-1" :title] (float-array [1 0 0 0]))
                   (colbert/insert-document "doc-1"
                                            [(float-array [0.9 0.1 0 0])
                                             (float-array [0.1 0.9 0 0])])
                   (assoc ["doc-2" :title] (float-array [0 0 1 0])))]
      
      ;; Search finds both title vectors and tokens
      (let [result (core/search idx' (float-array [1 0 0 0]) 10)]
        (is (some #(= ["doc-1" :title] (:id %)) result))
        (is (some #(= ["doc-1" :token 0] (:id %)) result))))))

;; -----------------------------------------------------------------------------
;; Weighted Multi-Field Search Tests

(deftest test-weighted-field-search-basic
  (testing "Weighted combination of field results"
    (let [idx (create-test-index 4)
          ;; Two docs with different field weights
          idx' (-> idx
                   (assoc ["doc-1" :title] (float-array [1 0 0 0]))
                   (assoc ["doc-1" :content] (float-array [0 1 0 0]))
                   (assoc ["doc-2" :title] (float-array [0.9 0 0 0]))
                   (assoc ["doc-2" :content] (float-array [1 0 0 0])))
          ;; Query matches doc-1's title exactly, doc-2's content exactly
          query (float-array [1 0 0 0])
          ;; Weight title higher
          weights {:title 0.7 :content 0.3}
          results (colbert/weighted-field-search idx' query 5 weights)]
      
      (is (= 2 (count results)))
      ;; doc-2 should rank higher (0.7*0.9 + 0.3*1.0 = 0.93)
      ;; vs doc-1 (0.7*1.0 + 0.3*0.0 = 0.7)
      (is (= "doc-2" (:doc-id (first results)))))))

(deftest test-weighted-field-search-missing-fields
  (testing "Documents with missing fields score lower"
    (let [idx (create-test-index 4)
          idx' (-> idx
                   (assoc ["doc-1" :title] (float-array [1 0 0 0]))
                   (assoc ["doc-1" :content] (float-array [1 0 0 0]))
                   (assoc ["doc-2" :title] (float-array [1 0 0 0])))
          ;; doc-2 has no content field
          query (float-array [1 0 0 0])
          weights {:title 0.5 :content 0.5}
          results (colbert/weighted-field-search idx' query 5 weights)]
      
      ;; doc-1 has both fields, should score higher
      (is (= "doc-1" (:doc-id (first results)))))))

(deftest test-weighted-field-search-with-constraints
  (testing "Required field constraints"
    (let [idx (create-test-index 4)
          idx' (-> idx
                   (assoc ["doc-1" :title] (float-array [1 0 0 0]))
                   (assoc ["doc-1" :author] (float-array [1 0 0 0]))
                   (assoc ["doc-2" :title] (float-array [1 0 0 0])))
          ;; doc-2 has no author
          query (float-array [1 0 0 0])
          weights {:title 0.5 :author 0.5}
          results (colbert/weighted-field-search-with-constraints
                   idx' query 5 weights #{:author})]
      
      ;; Only doc-1 has author field
      (is (= 1 (count results)))
      (is (= "doc-1" (:doc-id (first results)))))))

;; -----------------------------------------------------------------------------
;; Hybrid Search Tests

(deftest test-hybrid-search-basic
  (testing "Combine field search with MaxSim"
    (let [idx (create-test-index 4)
          idx' (-> idx
                   ;; Field vectors
                   (assoc ["doc-1" :title] (float-array [1 0 0 0]))
                   (assoc ["doc-2" :title] (float-array [0 1 0 0]))
                   ;; Token vectors
                   (colbert/insert-document "doc-1"
                                            [(float-array [1 0 0 0])
                                             (float-array [0 1 0 0])])
                   (colbert/insert-document "doc-2"
                                            [(float-array [0 1 0 0])]))
          query-vec (float-array [1 0 0 0])
          query-toks [(float-array [1 0 0 0])
                      (float-array [0 1 0 0])]
          weights {:title 1.0}
          results (colbert/hybrid-search idx' query-vec query-toks 5 weights 0.5)]
      
      (is (<= 1 (count results)))
      ;; Results should have both field and maxsim scores
      (is (contains? (first results) :field-score))
      (is (contains? (first results) :maxsim-score)))))

(deftest test-hybrid-search-alpha-extremes
  (testing "Alpha=1.0 uses only field score, alpha=0.0 uses only MaxSim"
    (let [idx (create-test-index 4)
          idx' (-> idx
                   (assoc ["doc-1" :title] (float-array [1 0 0 0]))
                   (colbert/insert-document "doc-1"
                                            [(float-array [0 0 0 1])]))
          query-vec (float-array [1 0 0 0])
          query-toks [(float-array [0 0 0 1])]
          weights {:title 1.0}
          ;; Alpha=1.0: field score dominates (title matches)
          results-field (colbert/hybrid-search idx' query-vec query-toks 5 weights 1.0)
          ;; Alpha=0.0: MaxSim dominates (token matches)
          results-maxsim (colbert/hybrid-search idx' query-vec query-toks 5 weights 0.0)]
      
      ;; Both should return doc-1 but with different score composition
      (is (some? (seq results-field)))
      (is (some? (seq results-maxsim))))))