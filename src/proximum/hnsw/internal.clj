(ns proximum.hnsw.internal
  "Internal API for accessing HnswIndex implementation details.

   This namespace is for:
   - Advanced users who need direct access to internal structures
   - Debugging and introspection
   - Internal implementation code

   WARNING: This API is UNSTABLE. Functions here may change without notice
   and will NOT trigger major version bumps. Use the stable public API in
   proximum.core and proximum.protocols whenever possible.

   The stable collection protocol interface (get, assoc, seq, etc.) operates
   on external IDs and vectors. This namespace exposes the raw internal state
   for cases where you need it."
  (:require [proximum.protocols :as p]))

(defn vectors
  "Get the VectorStore from an HnswIndex.

   The VectorStore manages the memory-mapped vector data and provides
   low-level access to vector storage."
  [idx]
  (.-vectors idx))

(defn edges
  "Get the PersistentEdgeStore from an HnswIndex.

   The PersistentEdgeStore manages the HNSW graph structure with
   copy-on-write semantics."
  [idx]
  (.-pes-edges idx))

(defn metadata-pss
  "Get the metadata persistent sorted set from an HnswIndex.

   Contains metadata indexed by internal node ID."
  [idx]
  (p/metadata-index idx))

(defn external-id-index
  "Get the external-id index persistent sorted set from an HnswIndex.

   Maps external IDs to internal node IDs."
  [idx]
  (p/external-id-index idx))

(defn storage
  "Get the Konserve CachedStorage from an index.

   Uses IndexState protocol - works for any index type."
  [idx]
  (p/storage idx))

(defn edge-store
  "Get the Konserve edge store from an HnswIndex.

   Used for persisting edge chunks."
  [idx]
  (:edge-store (.-state idx)))

(defn address-map
  "Get the edge address map from an HnswIndex.

   Maps chunk positions to storage addresses."
  [idx]
  (:address-map (.-state idx)))

(defn branch
  "Get the current branch name from an index.

   Uses IndexState protocol - works for any index type."
  [idx]
  (p/current-branch idx))

(defn commit-id
  "Get the current commit UUID from an index.

   Uses IndexState protocol - works for any index type."
  [idx]
  (p/current-commit idx))

(defn vector-count
  "Get the total vector count (including deleted) from an index.

   Uses IndexState protocol - works for any index type."
  [idx]
  (p/vector-count-total idx))

(defn deleted-count
  "Get the deleted vector count from an index.

   Uses IndexState protocol - works for any index type."
  [idx]
  (p/deleted-count-total idx))

(defn pending-edge-writes
  "Get the pending edge writes atom from an HnswIndex.

   This atom tracks async edge write operations."
  [idx]
  (:pending-edge-writes (.-state idx)))

(defn state
  "Get the entire internal state map from an HnswIndex.

   Returns the raw state map containing all internal fields.
   Use with extreme caution - directly modifying this can break invariants."
  [idx]
  (.-state idx))

