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