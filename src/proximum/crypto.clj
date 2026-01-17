(ns proximum.crypto
  "Crypto-hash utilities for index auditability.

   When :crypto-hash? is enabled, each sync! computes a commit hash that
   chains with previous commits (like git). This enables:
   - Auditing index integrity after storage/transmission
   - Verifying backups before restore
   - Detecting corruption or tampering

   The commit hash is computed from:
   - Parent commit hash (chaining)
   - Vector chunks commit hash
   - Edge chunks commit hash

   Public API:
   - hash-index-commit: Compute commit hash from components
   - get-commit-hash: Get current commit hash from index
   - crypto-hash?: Check if index uses crypto-hash mode
   - verify-from-cold: Verify index integrity from cold storage"
  (:require [hasch.core :as hasch]
            [konserve.core :as k]
            [proximum.protocols :as p]
            [proximum.storage :as storage]
            [proximum.vectors :as vectors]
            [proximum.edges :as edges]))

;; -----------------------------------------------------------------------------
;; Hash Computation

(defn hash-index-commit
  "Compute combined index commit hash from vector and edge commit hashes.
   index-commit-hash = hash(parent + vectors-hash + edges-hash)
   This creates a single hash representing the complete index state."
  [parent-hash vectors-hash edges-hash]
  (hasch/uuid {:parent parent-hash
               :vectors vectors-hash
               :edges edges-hash}))

;; -----------------------------------------------------------------------------
;; Index Accessors

(defn get-commit-hash
  "Get the current commit hash for the index.

   When :crypto-hash? is enabled, the commit-id IS the merkle hash.
   This hash is computed from:
   - Parent commit hash (chaining like git)
   - Vector chunks commit hash
   - Edge chunks commit hash

   Returns nil if:
   - Index is not in crypto-hash mode
   - No commits have been made yet

   Example:
     (def idx (create-index {:type :hnsw
                             :dim 128
                             :store-config {:backend :file :path \"/tmp/idx-store\" :id #uuid \"550e8400-e29b-41d4-a716-446655440000\"}
                             :mmap-dir \"/tmp/idx-mmap\"
                             :crypto-hash? true}))
     (def idx (insert idx (float-array (repeat 128 1.0))))
     (sync! idx)
     (get-commit-hash idx)  ; => #uuid \"...\""
  [idx]
  (when (p/crypto-hash? idx)
    (p/current-commit idx)))

(defn crypto-hash?
  "Check if index is in crypto-hash mode.

   When enabled, each sync! computes a commit hash that can be used
   for auditability and verification from cold storage."
  [idx]
  (boolean (p/crypto-hash? idx)))

;; -----------------------------------------------------------------------------
;; Cold Storage Verification

(defn verify-from-cold
  "Verify index integrity from cold storage (konserve only).

   Reads all vector and edge chunks from konserve, recomputes their hashes,
   and verifies the commit chain. Does not use mmap cache.

   Args:
     store-config - Konserve store config map (must include :id)
     branch       - Branch to verify (default :main)

   Returns:
     {:valid? true/false :vectors-verified N :edges-verified N ...}"
  ([store-config]
   (verify-from-cold store-config :main))
  ([store-config branch & {:keys [store]}]
   (let [store-config (when store-config (storage/normalize-store-config store-config))
         edge-store (or store
                        (when store-config (storage/connect-store-sync store-config))
                        (throw (ex-info "verify-from-cold requires store-config or :store"
                                        {:hint "Pass a Konserve store-config including :id"})))
         config (k/get edge-store :index/config nil {:sync? true})
         {:keys [crypto-hash?]} config
         snapshot (k/get edge-store branch nil {:sync? true})]
     (if-not snapshot
       {:valid? false :error :branch-not-found :branch branch}
       (if-not crypto-hash?
         {:valid? true :note "Index not in crypto-hash mode, nothing to verify"}
         ;; When crypto-hash? enabled, commit-id IS the merkle hash
         (let [{:keys [commit-id vector-count vectors-addr-pss-root edges-addr-pss-root]} snapshot
               pss-storage (storage/create-storage edge-store {:crypto-hash? crypto-hash?})
               vectors-addr-map (if vectors-addr-pss-root
                                  (storage/address-pss-to-map
                                   (storage/restore-address-pss vectors-addr-pss-root pss-storage))
                                  {})
               edges-addr-map (if edges-addr-pss-root
                                (storage/address-pss-to-map
                                 (storage/restore-address-pss edges-addr-pss-root pss-storage))
                                {})
               vectors-result (vectors/verify-vectors-from-cold edge-store vectors-addr-map vector-count)
               edges-result (edges/verify-edges-from-cold edge-store edges-addr-map)]
           (cond
             (not (:valid? vectors-result))
             {:valid? false :error :vectors-invalid :vectors-result vectors-result}

             (not (:valid? edges-result))
             {:valid? false :error :edges-invalid :edges-result edges-result}

             :else
             {:valid? true
              :vectors-verified (:chunks-verified vectors-result)
              :edges-verified (:chunks-verified edges-result)
              :commit-id commit-id})))))))
