#!/usr/bin/env clojure
(ns examples.semantic-search
  "Semantic Search Example for Proximum

  Demonstrates a simple RAG (Retrieval-Augmented Generation) pattern:
  - Indexing document chunks
  - Semantic search with metadata
  - Filtering results by metadata
  - Branching for experiments

  This example uses mock embeddings. In production, you'd use:
  - OpenAI API for embeddings
  - FastEmbed for local embeddings
  - Sentence Transformers via libpython-clj

  Run from REPL:
    clj -M:dev
    (load-file \"examples/clojure/semantic_search.clj\")
    (in-ns 'examples.semantic-search)
    ;; Then evaluate the forms in the comment block"
  (:require [proximum.core :as prox]
            [clojure.string :as str]))

(comment
  ;; =============================================================================
  ;; Mock Embedding Function
  ;; =============================================================================

  "In production, replace this with actual embeddings from OpenAI, FastEmbed, etc."

  (defn mock-embed
    "Generate a deterministic embedding from text (for demo purposes).
     In production, use: OpenAI API, FastEmbed, or Sentence Transformers"
    [text]
    (let [hash (.hashCode text)]
      (float-array (for [i (range 384)]
                     (Math/sin (+ hash (* i 0.01)))))))


  ;; =============================================================================
  ;; Sample Documents
  ;; =============================================================================

  (def documents
    [{:id "doc-1"
      :title "Introduction to Vector Databases"
      :text "Vector databases are specialized systems designed to store and query high-dimensional vectors efficiently. They use algorithms like HNSW for fast approximate nearest neighbor search."
      :category "technology"
      :author "Alice"}

     {:id "doc-2"
      :title "Machine Learning Embeddings"
      :text "Embeddings are dense vector representations of data like text or images. Modern language models create embeddings that capture semantic meaning, enabling similarity search."
      :category "ai"
      :author "Bob"}

     {:id "doc-3"
      :title "RAG Architecture Patterns"
      :text "Retrieval-Augmented Generation combines vector search with large language models. The retriever finds relevant documents, and the generator uses them to produce informed responses."
      :category "ai"
      :author "Alice"}

     {:id "doc-4"
      :title "Clojure Persistent Data Structures"
      :text "Clojure's persistent data structures use structural sharing to provide efficient immutable collections. Each operation returns a new version while sharing unchanged parts."
      :category "programming"
      :author "Charlie"}

     {:id "doc-5"
      :title "Git Version Control"
      :text "Git uses a directed acyclic graph to track commits. Branches are lightweight pointers to commits, and merging combines histories from different branches."
      :category "technology"
      :author "Bob"}])


  ;; =============================================================================
  ;; Part 1: Index Documents with Metadata
  ;; =============================================================================

  "Create an index and add documents with rich metadata"

  (def idx (prox/create-index {:type :hnsw
                                :dim 384
                                :M 16
                                :ef-construction 200
                                :store-config {:backend :memory
                                              :id (java.util.UUID/randomUUID)}
                                :capacity 10000}))

  ;; Index all documents with metadata
  (def idx-with-docs
    (into idx
          (map (fn [{:keys [id title text category author]}]
                 [id {:vector (mock-embed text)
                      :metadata {:title title
                                :category category
                                :author author
                                :text text}}])
               documents)))

  (count idx-with-docs)  ; => 5


  ;; =============================================================================
  ;; Part 2: Semantic Search
  ;; =============================================================================

  "Search for documents by semantic similarity"

  (def query1 "How do vector databases work?")
  (def results1 (prox/search idx-with-docs (mock-embed query1) 3))

  (println "\nQuery: How do vector databases work?")
  (doseq [{:keys [id distance]} results1]
    (let [meta (prox/get-metadata idx-with-docs id)]
      (printf "  [%.4f] %s: %s\n" distance id (:title meta))))
  ; Expected: doc-1 (vector databases), doc-2 (embeddings), etc.


  (def query2 "What are persistent data structures?")
  (def results2 (prox/search idx-with-docs (mock-embed query2) 3))

  (println "\nQuery: What are persistent data structures?")
  (doseq [{:keys [id distance]} results2]
    (let [meta (prox/get-metadata idx-with-docs id)]
      (printf "  [%.4f] %s: %s\n" distance id (:title meta))))
  ; Expected: doc-4 (Clojure), doc-5 (Git), etc.


  ;; =============================================================================
  ;; Part 3: Filtered Search by Metadata
  ;; =============================================================================

  "Search within a specific category or by specific author"

  ;; Get only AI-related documents
  (def ai-doc-ids
    (->> documents
         (filter #(= (:category %) "ai"))
         (map :id)
         set))

  (def query3 "machine learning")
  (def results3 (prox/search-filtered idx-with-docs
                                      (mock-embed query3)
                                      3
                                      ai-doc-ids))

  (println "\nQuery: machine learning (AI category only)")
  (doseq [{:keys [id distance]} results3]
    (let [meta (prox/get-metadata idx-with-docs id)]
      (printf "  [%.4f] %s: %s\n" distance id (:title meta))))
  ; Will only return doc-2 and doc-3 (AI category)


  ;; Search by author
  (def alice-doc-ids
    (->> documents
         (filter #(= (:author %) "Alice"))
         (map :id)
         set))

  (def results4 (prox/search-filtered idx-with-docs
                                      (mock-embed "vector search")
                                      2
                                      alice-doc-ids))

  (println "\nQuery: vector search (Alice's docs only)")
  (doseq [{:keys [id distance]} results4]
    (let [meta (prox/get-metadata idx-with-docs id)]
      (printf "  [%.4f] %s by %s\n" distance (:title meta) (:author meta))))


  ;; =============================================================================
  ;; Part 4: RAG Pattern - Retrieve and Generate
  ;; =============================================================================

  "Simulate a RAG workflow: search, retrieve, format context"

  (defn retrieve-context
    "Search for relevant documents and format them as context"
    [idx query k]
    (let [results (prox/search idx (mock-embed query) k)]
      (str/join "\n\n"
                (map (fn [{:keys [id]}]
                       (let [{:keys [title text]} (prox/get-metadata idx id)]
                         (str "# " title "\n" text)))
                     results))))

  (def context (retrieve-context idx-with-docs "How does semantic search work?" 2))

  (println "\n=== Retrieved Context ===")
  (println context)

  "In production, you'd send this context + query to an LLM:

   (llm/chat {:system \"Answer using only the provided context.\"
              :context context
              :user query})"


  ;; =============================================================================
  ;; Part 5: Experiment with Different Embeddings (Branching)
  ;; =============================================================================

  "Use branching to test different embedding models or chunking strategies"

  ;; Save current version
  (def main-idx idx-with-docs)

  ;; Create experimental branch
  (def experiment (prox/fork main-idx))

  ;; Add new documents or re-embed existing ones with a different model
  (def experiment2
    (assoc experiment "doc-6" {:vector (mock-embed "New experimental document")
                               :metadata {:title "Experimental Doc"
                                         :category "test"
                                         :author "Experimenter"}}))

  ;; Search both versions
  (def query "experimental features")
  (prox/search main-idx (mock-embed query) 3)      ; Original
  (prox/search experiment2 (mock-embed query) 3)   ; With new doc

  "Both indices are independent - perfect for A/B testing!"


  ;; =============================================================================
  ;; Part 6: Multi-Tenant Pattern
  ;; =============================================================================

  "Store documents for multiple users in one index, search per-user"

  ;; Add user ID to metadata
  (def multi-tenant-docs
    [{:id "user1-doc1" :user-id "user-1" :text "User 1's private document about AI"}
     {:id "user1-doc2" :user-id "user-1" :text "User 1's notes on vector databases"}
     {:id "user2-doc1" :user-id "user-2" :text "User 2's research on embeddings"}
     {:id "user2-doc2" :user-id "user-2" :text "User 2's RAG implementation guide"}])

  (def mt-idx
    (into idx
          (map (fn [{:keys [id user-id text]}]
                 [id {:vector (mock-embed text)
                      :metadata {:user-id user-id
                                :text text}}])
               multi-tenant-docs)))

  ;; Search only User 1's documents
  (defn search-for-user [idx query user-id k]
    (let [user-doc-ids (->> multi-tenant-docs
                            (filter #(= (:user-id %) user-id))
                            (map :id)
                            set)]
      (prox/search-filtered idx (mock-embed query) k user-doc-ids)))

  (def user1-results (search-for-user mt-idx "AI embeddings" "user-1" 2))

  (println "\nUser 1's search results:")
  (doseq [{:keys [id distance]} user1-results]
    (let [{:keys [user-id text]} (prox/get-metadata mt-idx id)]
      (printf "  [%.4f] %s (user: %s)\n" distance id user-id)))
  ; Will only return user-1's documents


  ;; =============================================================================
  ;; Part 7: Persistence and Versioning
  ;; =============================================================================

  "For production use, persist to disk and track versions"

  (def persistent-idx
    (prox/create-index {:type :hnsw
                        :dim 384
                        :M 16
                        :ef-construction 200
                        :store-config {:backend :file
                                      :path "/tmp/semantic-search-demo"
                                      :id (java.util.UUID/randomUUID)}
                        :mmap-dir "/tmp/semantic-search-demo-mmap"
                        :capacity 100000}))

  ;; Add documents
  (def persistent-idx2
    (into persistent-idx
          (map (fn [{:keys [id text title category author]}]
                 [id {:vector (mock-embed text)
                      :metadata {:title title
                                :category category
                                :author author}}])
               documents)))

  ;; Persist to disk (creates a commit)
  (prox/sync! persistent-idx2)

  ;; Get commit ID
  (def commit1 (prox/get-commit-id persistent-idx2))
  (println "\nCommit ID:" commit1)

  ;; Later: load from disk
  (def reloaded (prox/load {:backend :file
                            :path "/tmp/semantic-search-demo"}))

  (count reloaded)  ; => 5 (same as persistent-idx2)

  ;; Clean up
  (prox/close! reloaded)
  (clojure.java.shell/sh "rm" "-rf" "/tmp/semantic-search-demo" "/tmp/semantic-search-demo-mmap")


  ;; =============================================================================
  ;; Summary
  ;; =============================================================================

  "Proximum is perfect for RAG applications:

   ✅ Fast semantic search (HNSW algorithm)
   ✅ Rich metadata for filtering
   ✅ Multi-tenant support (search-filtered)
   ✅ Branching for A/B testing embeddings
   ✅ Versioning for reproducible results
   ✅ Pure Clojure, immutable semantics

   Use it to build:
   - Document search engines
   - Question-answering systems
   - Recommendation engines
   - Multi-tenant SaaS applications"

  ) ;; end comment
