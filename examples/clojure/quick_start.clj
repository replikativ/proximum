#!/usr/bin/env clojure
(ns examples.quick-start
  "Quick Start Example for Proximum

  Demonstrates:
  - Creating an index
  - Adding vectors
  - Searching for nearest neighbors
  - Branching for experiments
  - Persisting changes

  Run from REPL:
    clj -M:dev
    (load-file \"examples/clojure/quick_start.clj\")
    (in-ns 'examples.quick-start)
    ;; Then evaluate the forms in the comment block"
  (:require [proximum.core :as prox]))

(comment
  ;; =============================================================================
  ;; Quick Start with Proximum
  ;; =============================================================================

  ;; 1. Create an index for OpenAI embeddings (1536 dimensions)
  (def idx (prox/create-index {:type :hnsw
                                :dim 1536
                                :M 16
                                :ef-construction 200
                                :store-config {:backend :file
                                              :path "/tmp/prox-quickstart"
                                              :id (java.util.UUID/randomUUID)}
                                :mmap-dir "/tmp/prox-quickstart-mmap"
                                :capacity 100000}))

  ;; 2. Add vectors with external IDs
  (def embedding1 (float-array (repeatedly 1536 rand)))
  (def idx2 (prox/insert idx embedding1 "doc-1"))

  ;; Or use the collection protocol (feels like Clojure!)
  (def idx3 (assoc idx2 "doc-2" (float-array (repeatedly 1536 rand))))

  (count idx3)  ; => 2

  ;; 3. Add multiple vectors at once
  (def batch-vectors [(float-array (repeatedly 1536 rand))
                      (float-array (repeatedly 1536 rand))
                      (float-array (repeatedly 1536 rand))])

  (def batch-ids ["doc-3" "doc-4" "doc-5"])

  (def idx4 (prox/insert-batch idx3 batch-vectors batch-ids))

  (count idx4)  ; => 5

  ;; 4. Search for nearest neighbors
  (def query-vec (float-array (repeatedly 1536 rand)))
  (def results (prox/search idx4 query-vec 3))

  ;; Results are maps with :id and :distance
  (println "Top 3 results:")
  (doseq [{:keys [id distance]} results]
    (printf "  ID: %s, Distance: %.4f\n" id distance))

  ;; 5. Get vectors by ID (ILookup protocol)
  (def retrieved-vec (get idx4 "doc-1"))
  (type retrieved-vec)  ; => [F (float array)

  ;; 6. Add vectors with metadata
  (def idx5 (assoc idx4 "doc-6" {:vector (float-array (repeatedly 1536 rand))
                                  :metadata {:title "My Document"
                                            :category :science
                                            :timestamp (System/currentTimeMillis)}}))

  ;; Retrieve metadata
  (prox/get-metadata idx5 "doc-6")
  ; => {:title "My Document", :category :science, :timestamp 1234567890}

  ;; 7. Create a branch for experiments
  (def main-idx idx5)

  ;; Sync creates a commit (like git commit)
  (prox/sync! main-idx)

  ;; Branch creates a fork (like git branch)
  (def experiment (prox/branch! main-idx "experiment"))

  ;; Current branch
  (prox/get-branch experiment)  ; => :experiment

  ;; Add to experiment without affecting main
  (def experiment2 (assoc experiment "exp-doc-1" (float-array (repeatedly 1536 rand))))

  (count main-idx)      ; => 6 (unchanged)
  (count experiment2)   ; => 7 (has the new vector)

  ;; 8. Search on specific branch
  (prox/search main-idx query-vec 3)      ; Search main branch
  (prox/search experiment2 query-vec 3)   ; Search experiment branch

  ;; 9. Delete vectors (soft delete)
  (def idx6 (prox/delete main-idx "doc-1"))
  (count idx6)  ; Still 6, but doc-1 is marked deleted

  ;; Check metrics
  (def metrics (prox/index-metrics idx6))
  (select-keys metrics [:live-count :deleted-count :deletion-ratio])
  ; => {:live-count 5, :deleted-count 1, :deletion-ratio 0.166...}

  ;; 10. Compact to reclaim space
  (when (prox/needs-compaction? idx6)
    (def compacted (prox/compact idx6 {:store-config {:backend :file
                                                       :path "/tmp/prox-compacted"
                                                       :id (java.util.UUID/randomUUID)}
                                        :mmap-dir "/tmp/prox-compacted-mmap"}))
    (count compacted)  ; => 5 (deleted vectors removed)
    (prox/close! compacted))

  ;; 11. Persist and close
  (prox/sync! idx6)
  (prox/close! idx6)
  (prox/close! experiment2)

  ;; 12. Reconnect later
  (def reloaded (prox/load {:backend :file
                            :path "/tmp/prox-quickstart"}))

  (count reloaded)  ; => 6 (persisted state)

  ;; Clean up
  (prox/close! reloaded)
  (clojure.java.shell/sh "rm" "-rf" "/tmp/prox-quickstart" "/tmp/prox-quickstart-mmap")

  ;; =============================================================================
  ;; Summary
  ;; =============================================================================

  "Proximum provides:

   ✅ Clojure collection protocols (assoc, get, into)
   ✅ Immutable, persistent semantics (snapshot isolation)
   ✅ Fast vector search (HNSW algorithm)
   ✅ Git-like branching and versioning
   ✅ Optional persistence with sync!
   ✅ Metadata support
   ✅ Compaction and maintenance

   It feels like working with native Clojure data structures!"

  ) ;; end comment
