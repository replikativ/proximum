(ns proximum.edges
  "Konserve-backed edge storage for PersistentEdgeStore.

   Dual storage model (like vectors.clj):
   - PES (Java): Runtime performance with chunked CoW arrays
   - Konserve: Source of truth for durability

   Storage layout (COW addressing):
     :edges/metadata       -> {:M :M0 :max-nodes :max-level :entrypoint :current-max-level
                               :address-map {encoded-position -> storage-address}}
     :edges/chunk/<storage-addr> -> byte[] (serialized chunk data)

   Merkle mode (crypto-hash? = true):
   - storage-address = hash(chunk-content) (content-addressed)
   - address-map tracks {position -> hash-uuid}
   - PES passes encoded-position to ChunkStorage, which looks up actual UUID
   - Enables automatic integrity verification and deduplication

   Copy-on-Write Semantics:
   - Each chunk gets a unique storage address when first persisted
   - When a chunk is modified, its storage address is invalidated (-1)
   - On sync, dirty chunks get NEW storage addresses (never reuse old ones)
   - This enables branch isolation: each branch tracks its own position->address mapping
   - Old unreferenced storage addresses can be garbage collected

   Operations:
   - On open: Load chunks lazily from Konserve via ChunkStorage interface
   - On flush: Generate new addresses for dirty chunks, fire async writes
   - On sync: Wait for channels, update address mapping, optionally softify chunks
   - Clear dirty set after successful sync

   Lazy Loading (larger-than-RAM support):
   - ChunkStorage interface allows PES to reload evicted chunks
   - SoftReferences let JVM evict chunks under memory pressure
   - Only hot chunks stay in memory, cold chunks reload on demand"
  (:require [konserve.core :as k]
            [clojure.core.async :as a]
            [hasch.core :as hasch])
  (:import [proximum.internal PersistentEdgeStore ChunkStorage]
           [java.nio ByteBuffer ByteOrder]
           [java.util UUID]))

;; -----------------------------------------------------------------------------
;; Chunk Serialization

(defn- chunk-to-bytes
  "Serialize an int[] chunk to byte array."
  ^bytes [^ints chunk]
  (when chunk
    (let [n (alength chunk)
          buf (ByteBuffer/allocate (* n 4))]
      (.order buf ByteOrder/LITTLE_ENDIAN)
      (dotimes [i n]
        (.putInt buf (aget chunk i)))
      (.array buf))))

(defn bytes-to-chunk
  "Deserialize byte array to int[] chunk."
  ^ints [^bytes data]
  (when data
    (let [buf (ByteBuffer/wrap data)
          _ (.order buf ByteOrder/LITTLE_ENDIAN)
          n (quot (alength data) 4)
          chunk (int-array n)]
      (dotimes [i n]
        (aset chunk i (.getInt buf)))
      chunk)))

;; -----------------------------------------------------------------------------
;; Storage Address Generation (COW)

(defn- generate-storage-address
  "Generate a unique storage address for a new chunk.
   Uses UUID most-significant bits for globally unique addresses."
  ^long []
  (.getMostSignificantBits (UUID/randomUUID)))

;; -----------------------------------------------------------------------------
;; Content-Based Hashing (for :crypto-hash? mode)
;; Uses hasch library which provides SHA-512 based content hashing

(defn hash-chunk
  "Hash a chunk's bytes to UUID using hasch (SHA-512 based)."
  [^bytes data]
  (hasch/uuid data))

(defn hash-commit
  "Compute commit hash from parent hash and new chunk hashes.
   commit-hash = hash(parent-hash + new-chunk-hashes)"
  [parent-hash new-chunk-hashes]
  (hasch/uuid {:parent parent-hash
               :chunks (vec new-chunk-hashes)}))

;; -----------------------------------------------------------------------------
;; Metadata & Keys

(defn- chunk-key
  "Generate Konserve key for a storage address.
   Accepts either long (random mode) or UUID (merkle mode)."
  [storage-addr]
  (if (instance? java.util.UUID storage-addr)
    [:edges :chunk storage-addr]
    [:edges :chunk (keyword (str storage-addr))]))

;; -----------------------------------------------------------------------------
;; ChunkStorage Implementation (Lazy Loading)

(defn create-chunk-storage
  "Create a ChunkStorage implementation backed by Konserve.
   This enables lazy loading of chunks - they can be evicted from memory
   and reloaded on demand when accessed.

   The storage uses blocking reads (sync? true) since chunk access
   is typically on the critical path during search.

   address-map-atom contains position->UUID mappings.
   PES passes encoded positions, we look up the actual UUID from address-map.
   Returns nil if position is not in address-map (chunk not persisted)."
  ^ChunkStorage [store address-map-atom]
  (reify ChunkStorage
    (restore [_ encoded-position]
      ;; PES passes encoded position, look up storage address from address-map
      ;; Returns nil if not in map (chunk not persisted yet)
      (when-let [storage-addr (get @address-map-atom encoded-position)]
        (when-let [data (k/get store (chunk-key storage-addr) nil {:sync? true})]
          (bytes-to-chunk data))))

    (accessed [_ address]
      ;; Could track LRU here, but JVM SoftReference handles eviction
      nil)

    (store [_ encoded-address chunk]
      ;; Store chunk synchronously and return the address
      (k/assoc store (chunk-key encoded-address) (chunk-to-bytes chunk) {:sync? true})
      encoded-address)))

;; -----------------------------------------------------------------------------
;; Async Flush & Sync

(defn flush-dirty-chunks-async!
  "Fire async writes for all dirty chunks with COW semantics.
   Generates new unique storage addresses for each dirty chunk.
   Returns {:channels #{...} :address-map {...} :new-addresses {...} :chunk-hashes [...]}
   with pending write channels, merged address map, new addresses for dirty chunks,
   and chunk hashes for crypto-hash mode.
   Returns nil if no dirty chunks.

   Options:
     :crypto-hash? - If true, compute and return chunk hashes for commit computation"
  ([store pes existing-address-map]
   (flush-dirty-chunks-async! store pes existing-address-map {}))
  ([store ^PersistentEdgeStore pes existing-address-map {:keys [crypto-hash?]}]
   (when (.hasDirtyChunks pes)
     (let [dirty-positions (.getDirtyChunks pes)
           ;; Start with existing address map (from previous sync or load)
           ;; Remove dirty positions (they'll get new addresses)
           base-map (reduce dissoc (or existing-address-map {}) dirty-positions)
           ;; Process dirty chunks: get bytes, compute address, fire async write
           ;; In crypto-hash mode: address = hash(content) (merkle)
           ;; In normal mode: address = random long
           result (reduce
                   (fn [{:keys [channels new-addrs chunk-hashes]} pos]
                     (if-let [chunk (.getChunkByAddress pes pos)]
                       (let [chunk-bytes (chunk-to-bytes chunk)
                             ;; In merkle mode, address IS the hash
                             storage-addr (if crypto-hash?
                                            (hash-chunk chunk-bytes)
                                            (generate-storage-address))]
                         {:channels (conj channels (k/assoc store (chunk-key storage-addr) chunk-bytes))
                          :new-addrs (assoc new-addrs pos storage-addr)
                          ;; In merkle mode, address IS the hash - no separate tracking needed
                          ;; but we keep chunk-hashes for commit hash computation
                          :chunk-hashes (if crypto-hash?
                                          (conj chunk-hashes storage-addr)
                                          chunk-hashes)})
                       {:channels channels :new-addrs new-addrs :chunk-hashes chunk-hashes}))
                   {:channels #{} :new-addrs {} :chunk-hashes []}
                   dirty-positions)
           new-addresses (:new-addrs result)
           address-map (merge base-map new-addresses)]
       {:channels (:channels result)
        :address-map address-map
        :new-addresses new-addresses
        :chunk-hashes (:chunk-hashes result)}))))

(defn sync-edges!
  "Wait for all pending edge write channels, then write metadata with {:sync? true}.
   Clears dirty set after successful sync.

   Arguments:
     store - Konserve store
     pes - PersistentEdgeStore
     pending-channels - Set of async write channels to wait for
     address-map - Complete position->storage-address map
     new-addresses - Map of just the new addresses assigned to dirty chunks

   Options:
     :softify? - If true, convert persisted chunks to SoftReference (default: false)
                 Enable this for larger-than-RAM operation.
     :chunk-hashes - Vector of hashes for chunks written in this sync (for crypto-hash)
     :parent-commit-hash - Parent commit hash (for crypto-hash chain)

   Returns:
     Channel that delivers {:address-map ... :commit-hash ...} when sync completes."
  ([store pes pending-channels address-map new-addresses]
   (sync-edges! store pes pending-channels address-map new-addresses {}))
  ([store ^PersistentEdgeStore pes pending-channels address-map new-addresses
    {:keys [softify? chunk-hashes parent-commit-hash] :or {softify? false}}]
   ;; Wait for all pending chunk writes asynchronously
   (a/go
     (try
       (doseq [ch pending-channels]
         (a/<! ch))

       ;; Compute commit hash if we have new chunk hashes
       (let [new-commit-hash (when (seq chunk-hashes)
                               (hash-commit parent-commit-hash chunk-hashes))
             commit-hash (or new-commit-hash parent-commit-hash)]
         ;; Note: We no longer write [:edges :metadata] here - that was a global key
         ;; that caused multi-branch conflicts. Per-branch state is stored in snapshots
         ;; by persistence/sync!

         ;; Optionally convert chunks to soft references for memory reclamation
         (when softify?
           (doseq [pos (keys new-addresses)]
             (.softifyChunk pes pos)))

         ;; Clear dirty set after successful sync
         (.clearDirty pes)

         {:address-map address-map
          :commit-hash commit-hash})
       (catch Exception e
         (throw e))))))

(defn flush-dirty-chunks!
  "Persist all dirty chunks to Konserve store (blocking).
   Convenience wrapper that calls async flush then sync.

   Options:
     :crypto-hash? - If true, compute and return commit hash
     :parent-commit-hash - Parent commit hash for chaining

   Returns {:address-map ... :commit-hash ...}."
  ([store pes existing-address-map]
   (flush-dirty-chunks! store pes existing-address-map {}))
  ([store ^PersistentEdgeStore pes existing-address-map
    {:keys [crypto-hash? parent-commit-hash] :as opts}]
   (if-let [{:keys [channels address-map new-addresses chunk-hashes]}
            (flush-dirty-chunks-async! store pes existing-address-map opts)]
     (sync-edges! store pes channels address-map new-addresses
                  {:chunk-hashes chunk-hashes
                   :parent-commit-hash parent-commit-hash})
     {:address-map existing-address-map
      :commit-hash parent-commit-hash})))

;; -----------------------------------------------------------------------------
;; Verification (for crypto-hash mode)

(defn verify-edges-from-cold
  "Verify edges from cold storage (konserve only).
   Reads all chunks by their storage addresses and verifies they exist.

   Args:
     store       - Konserve store
     address-map - Map of position->storage-address

   Returns {:valid? true, :chunks-verified N} or {:valid? false, :error ...}"
  [store address-map]
  (if (empty? address-map)
    {:valid? true :chunks-verified 0 :note "No chunks to verify"}
    (let [;; Read and hash all chunks by their storage addresses
          chunk-hashes (loop [positions (keys address-map)
                              hashes []]
                         (if (empty? positions)
                           hashes
                           (let [pos (first positions)
                                 storage-addr (get address-map pos)
                                 storage-key (if (instance? java.util.UUID storage-addr)
                                               (chunk-key storage-addr)
                                               (chunk-key storage-addr))
                                 chunk-data (k/get store storage-key nil {:sync? true})]
                             (if chunk-data
                               (recur (rest positions) (conj hashes (hash-chunk chunk-data)))
                               {:error :chunk-not-found :position pos :storage-addr storage-addr}))))]
      (if (map? chunk-hashes)
        ;; Error case
        {:valid? false :error (:error chunk-hashes)
         :position (:position chunk-hashes) :storage-addr (:storage-addr chunk-hashes)}
        ;; All chunks read successfully
        {:valid? true
         :chunks-verified (count chunk-hashes)
         :chunk-hashes chunk-hashes}))))
