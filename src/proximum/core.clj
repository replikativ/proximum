(ns proximum.core
  "Persistent Vector Database - Main API entry point.

   This namespace is generated from proximum.specification.
   See proximum.specification for the authoritative API definition.

   Architecture:
   - proximum.protocols: VectorIndex protocol and multimethods
   - proximum.hnsw: HNSW index implementation
   - proximum.writing: sync!, load, load-commit (durability)
   - proximum.versioning: branch!, history, merge!, reset! (git-like versioning)
   - proximum.compaction: compact, online compaction
   - proximum.api-impl: Helper functions (with-metadata, etc.)
   - proximum.crypto: Crypto-hash utilities

   Public API:
     VectorIndex protocol - insert, insert-batch, search, delete, count-vectors,
                           get-vector, get-metadata, fork, flush!, close!,
                           index-type, index-config
     create-index multimethod - dispatches on :type (:hnsw)
     restore-index multimethod - restore from snapshot (type-specific)

   Example:
     (def idx (create-index {:type :hnsw :dim 128
                             :store-config {:backend :file :path \"/tmp/idx\" :id (random-uuid)}
                             :mmap-dir \"/tmp/mmap\"}))
     (def idx2 (insert idx (float-array [1.0 2.0 ...])))
     (def results (search idx2 query-vec 10))
     (sync! idx2)

   With metadata (for Datahike):
     (def idx2 (insert idx vector {:entity-id 123 :attr :person/embedding}))
     (get-metadata idx2 0)  ; => {:entity-id 123 :attr :person/embedding}

   Batch insert with metadata:
     (def idx2 (insert-batch idx vectors {:metadata [m1 m2 m3]}))

   Note: This namespace shadows clojure.core/load, reset!, parents, and ancestors
   to provide index operations with those names."
  (:refer-clojure :exclude [load reset! parents ancestors])
  (:require
   ;; Core abstractions - must be loaded first
   [proximum.protocols :as p]
   ;; Implementation namespaces - required for emit-api macro resolution
   [proximum.api-impl]
   [proximum.compaction]
   [proximum.crypto]
   [proximum.gc]
   [proximum.hnsw]
   [proximum.metrics]
   [proximum.writing]
   [proximum.versioning]
   ;; Codegen
   [proximum.codegen.clojure :as codegen]
   ;; Specification
   [proximum.specification :as spec]))

;; =============================================================================
;; Protocol Re-export (for satisfies? checks)
;; =============================================================================

(def VectorIndex
  "Protocol for vector index operations.
   All implementations provide persistent (immutable) semantics."
  p/VectorIndex)

;; =============================================================================
;; Generated API (from proximum.specification)
;; =============================================================================

(codegen/emit-api proximum.specification/api-specification)
