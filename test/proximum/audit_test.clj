(ns proximum.audit-test
  "Tests for `proximum.audit/verify-chain` and the unified `IAuditable`
   protocol. Mirrors the test surface of `stratum.audit-test` and
   `scriptum.audit-test` so the three audit shapes stay coordinated.

     - clean walk reports `:ok`
     - tampering an edge or vector chunk on disk → `:mismatch` with
       `:audit/merkle-mismatch` and the underlying
       `:chunk-content-tampered` detail
     - `:crypto-hash?` off → `:advisory`
     - `IAuditable` on a live `HnswIndex` returns the unified shape"
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [proximum.audit :as audit]
            [proximum.core :as core]
            [proximum.protocols :as p])
  (:import [java.util UUID]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Helpers
;; ============================================================================

(defn- temp-store-path []
  (str (System/getProperty "java.io.tmpdir") "/proximum-audit-" (UUID/randomUUID)))

(defn- file-store-config []
  {:backend :file :path (temp-store-path) :id (UUID/randomUUID)})

(defn- fresh-index
  "Build a small in-memory file-backed HNSW index, insert a few
   vectors, sync, return the synced index."
  [{:keys [crypto-hash?] :or {crypto-hash? true}}]
  (let [cfg {:type :hnsw
             :dim 4
             :capacity 100
             :M 8
             :store-config (file-store-config)
             :crypto-hash? crypto-hash?}
        idx (core/create-index cfg)
        idx (reduce (fn [i n]
                      (core/insert i (float-array [(double n) (inc (double n)) 0.0 0.0])
                                   (keyword (str "v" n))))
                    idx (range 5))]
    (a/<!! (p/sync! idx))))

(defn- tamper-first-edge-chunk!
  "Find an [:edges :chunk uuid] key in `store` and overwrite its value
   with a bogus byte-array. Returns the storage address of the tampered
   chunk."
  [store]
  (let [all-keys (mapv :key (k/keys store {:sync? true}))
        edge-key (first (filter #(and (vector? %) (= :edges (first %))) all-keys))]
    (assert edge-key "expected at least one edge chunk in the store")
    (k/assoc store edge-key (byte-array (range 64)) {:sync? true})
    (last edge-key)))

(defn- tamper-first-vector-chunk!
  "Find a [:vectors :chunk uuid] key (or fall back to any non-edge,
   non-snapshot UUID-keyed entry that looks like a vector chunk) and
   overwrite it. Returns the storage address."
  [store]
  (let [all-keys (mapv :key (k/keys store {:sync? true}))
        v-key (or (first (filter #(and (vector? %) (= :vectors (first %))) all-keys))
                  ;; Some proximum versions store vector chunks as flat UUID keys.
                  (first (filter (fn [k]
                                   (and (instance? UUID k)
                                        (let [v (k/get store k nil {:sync? true})]
                                          (bytes? v))))
                                 all-keys)))]
    (assert v-key "expected at least one vector chunk in the store")
    (k/assoc store v-key (byte-array (range 64)) {:sync? true})
    (if (vector? v-key) (last v-key) v-key)))

;; ============================================================================
;; verify-chain
;; ============================================================================

(deftest clean-chain-verifies-ok
  (let [idx (fresh-index {:crypto-hash? true})
        store (p/raw-storage idx)
        r (audit/verify-chain store {:branch :main})]
    (is (= :ok (:status r)))
    (is (audit/ok? r))
    (is (= 1 (count (:commits r))))
    (is (= :ok (-> r :commits first :status)))))

(deftest tamper-on-edge-chunk-is-detected
  (let [idx (fresh-index {:crypto-hash? true})
        store (p/raw-storage idx)
        _ (tamper-first-edge-chunk! store)
        r (audit/verify-chain store {:branch :main})
        first-err (-> r :commits first :errors first)]
    (is (= :mismatch (:status r)))
    (is (= :audit/merkle-mismatch (:type first-err)))
    (is (= :chunk-content-tampered
           (-> first-err :details :edges-result :error)))))

(deftest no-crypto-hash-is-advisory
  (let [idx (fresh-index {:crypto-hash? false})
        store (p/raw-storage idx)
        r (audit/verify-chain store {:branch :main})]
    (is (= :advisory (:status r)))
    (is (= :unsupported (-> r :commits first :status)))
    (is (= :crypto-hash-disabled (-> r :commits first :reason)))))

;; ============================================================================
;; IAuditable on a live HnswIndex
;; ============================================================================

(deftest iauditable-clean
  (let [idx (fresh-index {:crypto-hash? true})
        root (audit/-merkle-root idx)
        r (audit/-recompute-merkle-root idx)]
    (is (uuid? root))
    (is (= :ok (:status r)))
    (is (= root (:root r))
        "recomputed root must equal -merkle-root on a clean store")))

(deftest iauditable-detects-tampering
  (let [idx (fresh-index {:crypto-hash? true})
        store (p/raw-storage idx)
        _ (tamper-first-edge-chunk! store)
        r (audit/-recompute-merkle-root idx)]
    (is (= :mismatch (:status r)))
    (is (= :audit/merkle-mismatch (-> r :errors first :type)))))

(deftest iauditable-no-crypto-hash
  (let [idx (fresh-index {:crypto-hash? false})]
    (is (nil? (audit/-merkle-root idx)))
    (let [r (audit/-recompute-merkle-root idx)]
      (is (= :unsupported (:status r)))
      (is (= :crypto-hash-disabled (:reason r))))))
