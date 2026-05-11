(ns proximum.audit
  "Audit-chain verification for `:crypto-hash? true` proximum indexes.

   Wraps `proximum.crypto/verify-from-cold` (which re-reads vector +
   edge chunks from konserve and verifies hashes) into the same
   protocol shape as `datahike.index.audit`, `stratum.audit`, and
   `scriptum.audit`, so cross-repo audit results share one shape.

   `verify-chain` walks the parent chain of a branch HEAD: each commit
   snapshot points at its parents via `:parents`, so we walk that DAG,
   ask each commit's pss-roots to be re-verified end-to-end, and report
   per-commit status.

   The protocol contract is identical across repos:

     IAuditable
     (-merkle-root [this])
       Cheap. Returns nil when no commit / no crypto-hash. Never throws.

     (-recompute-merkle-root [this])
       Expensive. Returns a result map; never throws on mismatch:
         {:status :ok          :root <uuid>}
         {:status :mismatch    :root <recomputed?>
                               :errors [{:type, :address, :expected,
                                         :recomputed, :details}]}
         {:status :unsupported :reason <kw>}"
  (:require [konserve.core :as k]
            [proximum.crypto :as pcrypto]
            [proximum.hnsw]
            [proximum.protocols :as p]
            [proximum.storage :as storage])
  (:import [proximum.hnsw HnswIndex]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Protocol — same shape as datahike.index.audit/IAuditable
;; ============================================================================

(defprotocol IAuditable
  (-merkle-root [this]
    "Cheap. Returns the cached/known content-addressed UUID. Returns
     nil when unsynced or `:crypto-hash?` is off. Never throws.")

  (-recompute-merkle-root [this]
    "Expensive. Returns a result map; never throws on mismatch.
     See ns docstring for the contract."))

;; ============================================================================
;; verify-from-cold result → unified result map
;; ============================================================================

(defn- translate-vfc
  "Translate `proximum.crypto/verify-from-cold`'s
   `{:valid? bool :error :reason ...}` shape into the unified result
   map keyed by `:status`."
  [stored-cid r]
  (cond
    (and (:valid? r) (:note r))
    {:status :unsupported :reason :crypto-hash-disabled}

    (:valid? r)
    {:status :ok :root (or (:commit-id r) stored-cid)}

    :else
    {:status :mismatch :root nil
     :errors [{:type :audit/merkle-mismatch
               :address stored-cid
               :expected stored-cid
               :details (dissoc r :valid?)}]}))

;; ============================================================================
;; Single-commit verification
;; ============================================================================

(defn- verify-commit-from-cold
  "Verify the named branch's HEAD by delegating to
   `proximum.crypto/verify-from-cold` and translating the result. The
   stored commit-id (the `:commit-id` field of the snapshot under the
   branch key) is used as `:address` so callers can correlate."
  [store branch]
  (let [snapshot (k/get store branch nil {:sync? true})
        stored-cid (:commit-id snapshot)]
    (cond
      (nil? snapshot)
      {:status :mismatch :root nil
       :errors [{:type :audit/snapshot-missing
                 :address branch}]}

      :else
      (translate-vfc stored-cid
                     (pcrypto/verify-from-cold nil branch :store store)))))

;; ============================================================================
;; Public chain entrypoint
;; ============================================================================

(defn verify-chain
  "Walk the commit DAG anchored at `branch`'s HEAD on `store` and
   verify each reachable commit. Returns the unified shape:

     {:head <head-commit-uuid>
      :status :ok | :mismatch | :advisory | :incomplete
      :commits [{:cid, :status [, :errors :reason]}]
      :mismatches [...]
      :missing []}

   Options:
     :branch  — branch keyword (default :main)
     :limit   — max commits to walk (default unbounded)

   For now the deep PSS walk runs only on the head snapshot — older
   parents are surfaced as `:advisory` because re-running
   `verify-from-cold` per-commit would require loading each commit's
   own pss-roots and re-hashing every chunk, which is much more
   expensive than the layer-1 check datahike+stratum do for parents.
   Extending parent walks is straightforward when there's demand."
  ([store] (verify-chain store {}))
  ([store {:keys [branch limit]
           :or {branch :main
                limit Long/MAX_VALUE}}]
   (let [head-snapshot (k/get store branch nil {:sync? true})]
     (when-not head-snapshot
       (throw (ex-info "verify-chain: branch not found"
                       {:type :audit/no-head :branch branch})))
     (let [head-cid (:commit-id head-snapshot)
           commits (volatile! [])
           missing (volatile! [])
           visited (volatile! #{})
           head-result (verify-commit-from-cold store branch)
           _ (vswap! commits conj
                     (cond-> {:cid head-cid :status (:status head-result)}
                       (:errors head-result) (assoc :errors (:errors head-result))
                       (:reason head-result) (assoc :reason (:reason head-result))
                       :always (assoc :parents (or (:parents head-snapshot) #{}))))
           _ (vswap! visited conj head-cid)]
       ;; Walk parents — layer-1 only (commit-id is a content hash,
       ;; recomputable from {:parent :vectors-hash :edges-hash} stored
       ;; in the snapshot).
       (loop [frontier (vec (:parents head-snapshot)) n 1]
         (when (and (seq frontier) (< n limit))
           (let [cid (first frontier) rest-f (rest frontier)]
             (if (contains? @visited cid)
               (recur (vec rest-f) n)
               (do (vswap! visited conj cid)
                   (if-let [snap (k/get store cid nil {:sync? true})]
                     (let [recomputed (pcrypto/hash-index-commit
                                       (first (:parents snap))
                                       (or (:vectors-commit-hash snap) nil)
                                       (or (:edges-commit-hash snap) nil))
                           ok? (= cid recomputed)]
                       (vswap! commits conj
                               (cond-> {:cid cid
                                        :recomputed recomputed
                                        :parents (or (:parents snap) #{})
                                        :status (if ok? :ok :mismatch)}
                                 (not ok?)
                                 (assoc :errors
                                        [{:type :audit/merkle-mismatch
                                          :address cid
                                          :expected cid
                                          :recomputed recomputed}])))
                       (recur (into (vec rest-f)
                                    (remove @visited (:parents snap)))
                              (inc n)))
                     (do (vswap! missing conj cid)
                         (recur (vec rest-f) n))))))))
       (let [es @commits
             mism (filterv #(= :mismatch (:status %)) es)
             miss @missing
             adv  (some #(= :unsupported (:status %)) es)
             status (cond (seq mism) :mismatch
                          (seq miss) :incomplete
                          adv        :advisory
                          :else      :ok)]
         {:head head-cid
          :status status
          :commits es
          :mismatches mism
          :missing miss})))))

(defn ok? [report] (= :ok (:status report)))

;; ============================================================================
;; Live-instance protocol extension — bridges hold a live HNSW index
;; and call `-recompute-merkle-root` to verify the current branch.
;; ============================================================================

(extend-protocol IAuditable
  HnswIndex
  (-merkle-root [idx]
    (when (p/crypto-hash? idx)
      (p/current-commit idx)))
  (-recompute-merkle-root [idx]
    (let [store (p/raw-storage idx)
          branch (p/current-branch idx)]
      (cond
        (not (p/crypto-hash? idx))
        {:status :unsupported :reason :crypto-hash-disabled}

        (nil? store)
        {:status :unsupported :reason :no-store}

        :else
        (verify-commit-from-cold store branch)))))
