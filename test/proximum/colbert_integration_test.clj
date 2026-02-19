(ns proximum.colbert-integration-test
  "Integration test using real embeddings from sentence-transformers.
   
   This test simulates ColBERT token-level embeddings by chunking text
   and embedding each chunk separately."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [proximum.core :as core]
            [proximum.colbert :as colbert])
  (:import [java.util UUID]))

(defn read-test-data []
  (let [path "/tmp/colbert_data.json"]
    (when (.exists (io/file path))
      (json/read-str (slurp path) :key-fn keyword))))

(defn create-index-for-dim [dim]
  (let [path (str "/tmp/proximum-colbert-test-" (System/currentTimeMillis))]
    (.mkdirs (io/file (str path "/mmap")))
    (core/create-index {:type :hnsw
                        :dim dim
                        :store-config {:backend :file
                                       :path (str path "/store")
                                       :id (UUID/randomUUID)}
                        :mmap-dir (str path "/mmap")
                        :capacity 10000})))

(defn float-array-from-list [coll]
  (float-array (map float coll)))

(deftest ^:integration test-colbert-with-real-embeddings
  (testing "ColBERT search with real sentence-transformer embeddings"
    (let [data (read-test-data)]
      (when data
        (let [dim (:dim data)
              idx (create-index-for-dim dim)
              
              ;; Insert documents with token embeddings
              idx' (reduce
                    (fn [idx doc]
                      (let [doc-id (:id doc)
                            tokens (:tokens doc)
                            token-vecs (map #(float-array-from-list (:embedding %)) tokens)]
                        (colbert/insert-document idx doc-id token-vecs)))
                    idx
                    (:docs data))
              
              ;; Create query token vectors
              query-toks (map #(float-array-from-list (:embedding %))
                              (:query_chunks data))
              
              ;; Run MaxSim search
              results (colbert/maxsim-search idx' query-toks 5 {:ef 100})]
          
          (is (<= 1 (count results)) "Should find at least 1 document")
          (is (every? :doc-id results) "All results should have doc-id")
          (is (every? :maxsim-score results) "All results should have maxsim-score")
          
          ;; doc-1 should be most relevant (about machine learning)
          (is (= "doc-1" (:doc-id (first results)))
              "Query 'What is machine learning?' should match doc-1 first"))))))

(deftest ^:integration test-weighted-field-with-real-embeddings
  (testing "Weighted field search with real embeddings"
    (let [data (read-test-data)]
      (when data
        (let [dim (:dim data)
              idx (create-index-for-dim dim)
              
              ;; Use first token of each doc as "title" embedding
              idx' (reduce
                    (fn [idx doc]
                      (let [doc-id (:id doc)
                            title-emb (-> doc :tokens first :embedding)]
                        (assoc idx [doc-id :title] (float-array-from-list title-emb))))
                    idx
                    (:docs data))
              
              ;; Query with first chunk
              query-vec (-> data :query_chunks first :embedding float-array-from-list)
              
              ;; Weighted search
              results (colbert/weighted-field-search idx' query-vec 5 {:title 1.0})]
          
          (is (<= 1 (count results)))
          (is (every? :doc-id results)))))))