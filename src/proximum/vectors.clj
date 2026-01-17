(ns proximum.vectors
  "Konserve-backed vector storage with mmap cache.

   Dual storage model:
   - Konserve: source of truth, distributed via konserve-sync
   - Local mmap: runtime cache for fast SIMD access

   Async write pipeline:
   - append! writes to mmap immediately (fast path)
   - Chunks written to konserve asynchronously on completion
   - sync! waits for all pending writes, then writes metadata

   On open: loads chunks from konserve into local mmap (with optional reuse)

   Storage layout (konserve):
     [:vectors :chunk uuid] - byte[] of vectors (uuid = chunk-id or merkle hash)
     (Note: Per-branch metadata stored in snapshots by persistence/sync!)
     ...

   Merkle mode (crypto-hash? = true):
   - Chunk storage key = hash(chunk-content) (content-addressed)
   - chunk-address-map tracks {chunk-id -> hash-uuid}
   - Enables automatic integrity verification and deduplication

   Mmap header format (64 bytes):
     Bytes 0-3:   magic 'PVDB'
     Bytes 4-7:   version (int) = 1
     Bytes 8-15:  count (long) - last synced count
     Bytes 16-23: dim (long)
     Bytes 24-31: chunk-size (long)
     Bytes 32-63: reserved
     Byte 64+:    vector data"
  (:require [konserve.core :as k]
            [proximum.distance :as dist]
            [clojure.core.async :as a]
            [hasch.core :as hasch])
  (:import [jdk.incubator.vector FloatVector VectorOperators]
           [java.nio ByteBuffer ByteOrder MappedByteBuffer]
           [java.nio.channels FileChannel FileChannel$MapMode]
           [java.io RandomAccessFile File]
           [java.lang.foreign Arena MemorySegment ValueLayout ValueLayout$OfFloat]))

;; Default chunk size: 1000 vectors per chunk
(def ^:const DEFAULT-CHUNK-SIZE 1000)

;; Default initial capacity for mmap (10M vectors, sparse allocation)
(def ^:const DEFAULT-CAPACITY 10000000)

;; Mmap header constants
(def ^:const HEADER-SIZE 64)
(def ^:const HEADER-VERSION 1)
(def ^{:private true :tag "[B"} HEADER-MAGIC (.getBytes "PVDB"))

;; Header field offsets
(def ^:const OFFSET-MAGIC 0)
(def ^:const OFFSET-VERSION 4)
(def ^:const OFFSET-COUNT 8)
(def ^:const OFFSET-DIM 16)
(def ^:const OFFSET-CHUNK-SIZE 24)

;; -----------------------------------------------------------------------------
;; Content-Based Hashing (for :crypto-hash? mode)
;; Uses hasch library which provides SHA-512 based content hashing

(defn hash-chunk
  "Hash a chunk's bytes to UUID using hasch (SHA-512 based)."
  [^bytes data]
  (hasch/uuid data))

(defn hash-commit
  "Compute commit hash from parent hash and new chunk hashes.
   commit-hash = hash(parent-hash + new-chunk-hashes)
   This chains commits like git, enabling incremental verification."
  [parent-hash new-chunk-hashes]
  (hasch/uuid {:parent parent-hash
               :chunks (vec new-chunk-hashes)}))

(defn- generate-chunk-address
  "Generate a unique storage address for a vector chunk.
   In non-crypto mode, uses random UUID."
  []
  (java.util.UUID/randomUUID))

(defn- chunk-key
  "Generate konserve key for a vector chunk.
   Always uses [:vectors :chunk <uuid>] format for consistency."
  [addr]
  [:vectors :chunk addr])

(defrecord VectorStore
           [store          ;; konserve store
            dim            ;; vector dimensionality
            chunk-size     ;; vectors per chunk (for konserve)
            count-atom     ;; atom holding current count (may be ahead of konserve)
            write-buffer   ;; atom: vectors pending write to current chunk
            pending-writes ;; atom: #{channels} from async k/assoc calls
   ;; Mmap cache fields
            mmap-path      ;; path to mmap file (user-provided or temp)
            mmap-buf       ;; MappedByteBuffer
            mem-segment    ;; MemorySegment for SIMD
            capacity       ;; current mmap capacity
   ;; Crypto-hash fields (for auditability / merkle)
            crypto-hash?        ;; boolean: enable content-based hashing
            commit-hash         ;; atom: current commit hash (chained from parent + new chunks)
            pending-chunk-hashes ;; atom: [hash ...] for chunks written since last sync
            chunk-address-map   ;; atom: {chunk-id -> hash-uuid} for merkle addressing
            ])

(defn- chunk-id
  "Get chunk ID for a vector index."
  ^long [^long idx ^long chunk-size]
  (quot idx chunk-size))

(defn- idx-in-chunk
  "Get position within chunk for a vector index."
  ^long [^long idx ^long chunk-size]
  (rem idx chunk-size))

(defn- vector-offset
  "Calculate byte offset for vector at given index in mmap."
  ^long [^long dim ^long idx]
  (+ HEADER-SIZE (* idx dim 4)))

(defn- vectors->bytes
  "Convert vector of float arrays to byte array."
  ^bytes [vectors ^long dim]
  (let [n (count vectors)
        buf (ByteBuffer/allocate (* n dim 4))]
    (.order buf ByteOrder/LITTLE_ENDIAN)
    (doseq [^floats v vectors]
      (dotimes [i dim]
        (.putFloat buf (aget v i))))
    (.array buf)))

(defn- bytes->vectors
  "Convert byte array to vector of float arrays."
  [^bytes data ^long dim]
  (let [buf (ByteBuffer/wrap data)
        _ (.order buf ByteOrder/LITTLE_ENDIAN)
        n (quot (alength data) (* dim 4))]
    (vec (for [_ (range n)]
           (let [v (float-array dim)]
             (dotimes [i dim]
               (aset v i (.getFloat buf)))
             v)))))

;; -----------------------------------------------------------------------------
;; Mmap Header Operations

(defn- write-header!
  "Write full header to mmap buffer."
  [^MappedByteBuffer buf ^long count ^long dim ^long chunk-size]
  (locking buf
    (.position buf 0)
    (.put buf HEADER-MAGIC)
    (.putInt buf HEADER-VERSION)
    (.putLong buf count)
    (.putLong buf dim)
    (.putLong buf chunk-size)))

(defn update-header-count!
  "Update only the count field in header (at fixed offset)."
  [^MappedByteBuffer buf ^long count]
  (.putLong buf OFFSET-COUNT count))

(defn- read-header
  "Read header from mmap buffer. Returns nil if invalid."
  [^MappedByteBuffer buf]
  (locking buf
    (.position buf 0)
    (let [magic (byte-array 4)
          _ (.get buf magic)
          magic-str (String. magic)]
      (when (= magic-str "PVDB")
        (let [version (.getInt buf)]
          (when (= version HEADER-VERSION)
            {:version version
             :count (.getLong buf)
             :dim (.getLong buf)
             :chunk-size (.getLong buf)}))))))

;; -----------------------------------------------------------------------------
;; Mmap File Operations

(defn- create-mmap-file
  "Create and map a new mmap file for vectors.
   Writes header with initial count=0."
  [^String path ^long dim ^long chunk-size ^long capacity]
  (let [file (File. path)
        file-size (+ HEADER-SIZE (* capacity dim 4))
        raf (RandomAccessFile. file "rw")
        _ (.setLength raf file-size)
        channel (.getChannel raf)
        mmap-buf (.map channel FileChannel$MapMode/READ_WRITE 0 file-size)
        mem-seg (.map channel FileChannel$MapMode/READ_WRITE 0 file-size (Arena/global))]
    (.order mmap-buf ByteOrder/LITTLE_ENDIAN)
    (.close raf)
    ;; Write initial header
    (write-header! mmap-buf 0 dim chunk-size)
    {:mmap-buf mmap-buf
     :mem-segment mem-seg
     :capacity capacity}))

(defn- open-existing-mmap
  "Open an existing mmap file. Returns nil if file doesn't exist or invalid header."
  [^String path]
  (let [file (File. path)]
    (when (.exists file)
      (let [raf (RandomAccessFile. file "rw")
            file-size (.length raf)
            channel (.getChannel raf)
            mmap-buf (.map channel FileChannel$MapMode/READ_WRITE 0 file-size)
            mem-seg (.map channel FileChannel$MapMode/READ_WRITE 0 file-size (Arena/global))]
        (.order mmap-buf ByteOrder/LITTLE_ENDIAN)
        (.close raf)
        (when-let [header (read-header mmap-buf)]
          {:mmap-buf mmap-buf
           :mem-segment mem-seg
           :header header
           :capacity (quot (- file-size HEADER-SIZE) (* (:dim header) 4))})))))

(defn- ensure-mmap-capacity
  "Ensure mmap has enough capacity, creating new file if needed."
  [existing-mmap path dim chunk-size required-capacity]
  (if (and existing-mmap (>= (:capacity existing-mmap) required-capacity))
    existing-mmap
    (create-mmap-file path dim chunk-size required-capacity)))

(defn- write-vector-to-mmap!
  "Write a single vector to the mmap buffer."
  [^MappedByteBuffer buf ^long dim ^long idx ^floats vector]
  (let [offset (int (vector-offset dim idx))]
    (locking buf
      (.position buf offset)
      (dotimes [i dim]
        (.putFloat buf (aget vector i))))))

(defn- load-chunk-bytes-to-mmap!
  "Load a chunk's bytes directly into mmap at correct offset."
  [^MappedByteBuffer buf dim chunk-size chunk-id ^bytes data]
  (let [start-idx (* (long chunk-id) (long chunk-size))
        offset (int (vector-offset (long dim) start-idx))
        src-buf (ByteBuffer/wrap data)]
    (.order src-buf ByteOrder/LITTLE_ENDIAN)
    (locking buf
      (.position buf offset)
      ;; Copy bytes directly
      (let [n-bytes (alength data)]
        (dotimes [i n-bytes]
          (.put buf (.get src-buf)))))))

(defn create-store*
  "Internal: Create a new vector store with mmap cache.
   Called only from create-index. All params required."
  [store dim chunk-size capacity mmap-path crypto-hash?]
  (let [actual-mmap-path (or mmap-path
                             (str (System/getProperty "java.io.tmpdir")
                                  "/vectors-" (java.util.UUID/randomUUID) ".mmap"))
        {:keys [mmap-buf mem-segment]} (create-mmap-file actual-mmap-path dim chunk-size capacity)]
    (->VectorStore
     store
     dim
     chunk-size
     (atom 0)
     (atom [])           ;; write-buffer
     (atom #{})          ;; pending-writes
     actual-mmap-path
     mmap-buf
     mem-segment
     capacity
     crypto-hash?
     (when crypto-hash? (atom nil))   ;; commit-hash
     (when crypto-hash? (atom []))    ;; pending-chunk-hashes
     (atom {}))))                      ;; chunk-address-map (always, for PSS storage)

(defn open-store*
  "Internal: Open an existing vector store. All params required.
   Called only from restore-index-from-snapshot and branch!."
  [store dim chunk-size crypto-hash? mmap-path address-map vector-count commit-hash]
  (let [;; Try to open existing mmap if path provided
        existing-mmap (when mmap-path (open-existing-mmap mmap-path))
        ;; Check if existing mmap is compatible
        mmap-compatible? (and existing-mmap
                              (= dim (get-in existing-mmap [:header :dim]))
                              (= chunk-size (get-in existing-mmap [:header :chunk-size])))
        mmap-header-count (if mmap-compatible?
                            (get-in existing-mmap [:header :count])
                            0)
        ;; Determine capacity needed
        extra-capacity 10000
        required-capacity (+ vector-count extra-capacity)
        ;; Create or reuse mmap
        actual-mmap-path (or mmap-path
                             (str (System/getProperty "java.io.tmpdir")
                                  "/vectors-" (System/currentTimeMillis) ".mmap"))
        {:keys [mmap-buf mem-segment capacity]}
        (if (and mmap-compatible? (>= (:capacity existing-mmap) required-capacity))
          existing-mmap
          (create-mmap-file actual-mmap-path dim chunk-size required-capacity))
        ;; Load chunks from konserve that mmap doesn't have
        start-chunk (if mmap-compatible?
                      (chunk-id mmap-header-count chunk-size)
                      0)
        end-chunk (if (zero? vector-count)
                    0
                    (inc (chunk-id (dec vector-count) chunk-size)))]
    ;; Load missing chunks using address-map
    (doseq [cid (range start-chunk end-chunk)]
      (when-let [addr (get address-map cid)]
        (let [data (k/get store (chunk-key addr) nil {:sync? true})]
          (when data
            (load-chunk-bytes-to-mmap! mmap-buf dim chunk-size cid data)))))
    (->VectorStore
     store
     dim
     chunk-size
     (atom vector-count)
     (atom [])
     (atom #{})
     actual-mmap-path
     mmap-buf
     mem-segment
     capacity
     crypto-hash?
     (when crypto-hash? (atom commit-hash))
     (when crypto-hash? (atom []))
     (atom (or address-map {})))))  ;; Always create chunk-address-map

(defn flush-write-buffer-async!
  "Flush pending vectors to konserve asynchronously.
   Fires async k/assoc and adds channel to pending-writes.
   Does NOT wait or update metadata - that happens on sync!.

   Thread-safe: caller must hold lock on vs.

   When crypto-hash? is true:
   - Computes content hash for each chunk
   - Tracks hashes in pending-chunk-hashes for commit computation"
  [^VectorStore vs]
  ;; Note: Assumes caller holds lock on vs (called from append! and sync!)
  (let [buffer @(:write-buffer vs)]
    (when (seq buffer)
      (let [store (:store vs)
            dim (:dim vs)
            chunk-size (:chunk-size vs)
            crypto-hash? (:crypto-hash? vs)
            current-count @(:count-atom vs)
            start-idx (- current-count (count buffer))
            cid (chunk-id start-idx chunk-size)
            ;; Load existing chunk if partial (need current address)
            existing-addr (get @(:chunk-address-map vs) cid)
            existing-chunk (when existing-addr
                             (k/get store (chunk-key existing-addr) nil {:sync? true}))
            existing-vecs (if existing-chunk
                            (bytes->vectors existing-chunk dim)
                            [])
            ;; Append new vectors
            new-vecs (into existing-vecs buffer)
            ;; Compute chunk bytes and address
            chunk-bytes (vectors->bytes new-vecs dim)
            ;; In merkle mode: address = hash(content)
            ;; In normal mode: address = random UUID
            new-addr (if crypto-hash?
                       (hash-chunk chunk-bytes)
                       (generate-chunk-address))
            store-key (chunk-key new-addr)
            ;; Store chunk - ASYNC
            ch (k/assoc store store-key chunk-bytes)]
        ;; Track hash (merkle mode only) and update address map (always)
        (when crypto-hash?
          (swap! (:pending-chunk-hashes vs) conj new-addr))
        (swap! (:chunk-address-map vs) assoc cid new-addr)
        ;; Add channel to pending writes
        (swap! (:pending-writes vs) conj ch)
        ;; Clear buffer
        (reset! (:write-buffer vs) [])))))

(defn append!
  "Append a vector to the store.

   Writes to mmap immediately (fast path).
   Buffers for konserve, flushes chunk asynchronously on completion.
   Call sync! to ensure all writes are committed.

   Thread-safe: locks on the VectorStore to ensure slot allocation and
   buffer writes happen atomically, maintaining correct order for
   konserve persistence.

   Args:
     vs     - VectorStore
     vector - float array

   Returns:
     Index of the appended vector"
  [^VectorStore vs ^floats vector]
  (locking vs
    (let [count-atom (:count-atom vs)
          capacity (:capacity vs)
          dim (:dim vs)
          chunk-size (:chunk-size vs)
          idx (long @count-atom)]
      (when (>= idx capacity)
        (throw (ex-info "Mmap capacity exceeded" {:capacity capacity :idx idx})))
      ;; Write to mmap immediately (for fast reads)
      (write-vector-to-mmap! (:mmap-buf vs) dim idx vector)
      ;; Add to write buffer for konserve (order now matches slot order)
      (swap! (:write-buffer vs) conj vector)
      ;; Increment count
      (swap! count-atom inc)
      ;; Flush to konserve ASYNC if chunk is complete
      (when (zero? (idx-in-chunk (inc idx) chunk-size))
        (flush-write-buffer-async! vs))
      idx)))

(defn get-vector
  "Retrieve a vector by index.

   Reads directly from mmap cache (fast path).

   Args:
     vs  - VectorStore
     idx - Vector index

   Returns:
     float array"
  ^floats [^VectorStore vs ^long idx]
  (let [dim (long (:dim vs))
        offset (vector-offset dim idx)
        ^MappedByteBuffer buf (:mmap-buf vs)
        result (float-array dim)
        ^MappedByteBuffer local-buf (.duplicate buf)]
    (.order local-buf ByteOrder/LITTLE_ENDIAN)
    (.position local-buf (int offset))
    (let [^java.nio.FloatBuffer fbuf (.asFloatBuffer local-buf)]
      (.get fbuf result))
    result))

(defn count-vectors
  "Get the number of stored vectors."
  ^long [^VectorStore vs]
  @(:count-atom vs))

(defn- throw-crash!
  "Throw a simulated crash exception."
  [point]
  (throw (ex-info (str "Simulated mmap crash at " (name point))
                  {:type :mmap-crash :point point})))

(defn sync!
  "Sync all pending writes to konserve.

   1. Flush any partial chunk (async)
   2. Force mmap to disk
   3. Update mmap header with current count
   4. Force mmap again
   5. Wait for all pending konserve writes
   6. Write metadata to konserve (sync)
   7. Clear pending writes

   After sync!, konserve is consistent with mmap state.

   Options (for testing):
     :crash-point - Simulate crash at specific point:
       :after-flush-buffer    - After flushing write buffer
       :after-first-force     - After first mmap force (data on disk)
       :after-header-update   - After header count update (not forced)
       :after-second-force    - After second mmap force (header on disk)
       :after-pending-writes  - After waiting for konserve chunk writes
       :after-metadata-write  - After writing metadata to konserve"
  ([^VectorStore vs] (sync! vs nil))
  ([^VectorStore vs {:keys [crash-point]}]
   ;; Lock to prevent concurrent appends during sync
   (locking vs
     (let [store (:store vs)
           dim (:dim vs)
           chunk-size (:chunk-size vs)
           current-count @(:count-atom vs)
           ^MappedByteBuffer mmap-buf (:mmap-buf vs)]
       ;; 1. Flush partial chunk if any (async)
       (flush-write-buffer-async! vs)
       (when (= crash-point :after-flush-buffer) (throw-crash! crash-point))

       ;; 2. Force mmap to disk
       (.force mmap-buf)
       (when (= crash-point :after-first-force) (throw-crash! crash-point))

       ;; 3. Update mmap header with current count
       (update-header-count! mmap-buf current-count)
       (when (= crash-point :after-header-update) (throw-crash! crash-point))

       ;; 4. Force mmap again (commits header)
       (.force mmap-buf)
       (when (= crash-point :after-second-force) (throw-crash! crash-point))

       ;; 5. Wait for all pending konserve writes (atomic removal pattern)
       ;; Capture channels first, then wait, then remove only what we waited on.
       ;; This prevents losing channels added by concurrent append! calls.
       (let [channels-to-wait @(:pending-writes vs)]
         (doseq [ch channels-to-wait]
           (a/<!! ch))
         (when (= crash-point :after-pending-writes) (throw-crash! crash-point))

         ;; 6. Compute commit hash (metadata is stored per-branch in snapshots now)
         ;; Note: We no longer write :vectors/meta - that was a global key that
         ;; caused multi-branch conflicts. Per-branch state is in snapshots.
         (let [crypto-hash? (:crypto-hash? vs)
               pending-hashes (when crypto-hash? @(:pending-chunk-hashes vs))
               parent-hash (when crypto-hash? @(:commit-hash vs))
               new-commit-hash (when (and crypto-hash? (seq pending-hashes))
                                 (hash-commit parent-hash pending-hashes))]
           ;; Update commit-hash atom and atomically remove processed hashes
           (when crypto-hash?
             (when new-commit-hash
               (reset! (:commit-hash vs) new-commit-hash))
             ;; Atomic removal: drop only the hashes we processed, keep any new ones
             (swap! (:pending-chunk-hashes vs)
                    #(vec (drop (count pending-hashes) %)))))
         (when (= crash-point :after-metadata-write) (throw-crash! crash-point))

         ;; 7. Atomically remove only the channels we waited on
         (swap! (:pending-writes vs) #(reduce disj % channels-to-wait)))))
   vs))

(defn flush!
  "Flush any pending writes. Alias for sync! for backwards compatibility."
  [^VectorStore vs]
  (sync! vs))

(defn close!
  "Close the store and release resources.
   Calls sync! first to ensure all writes are committed."
  [^VectorStore vs]
  (sync! vs)
  ;; Clean up temp file (only if it's a temp file, not user-provided)
  (let [mmap-path (:mmap-path vs)
        temp-dir (System/getProperty "java.io.tmpdir")]
    (when (.startsWith ^String mmap-path temp-dir)
      (let [f (File. ^String mmap-path)]
        (when (.exists f)
          (.delete f)))))
  nil)

(defn get-segment
  "Get the MemorySegment for direct SIMD access."
  ^MemorySegment [^VectorStore vs]
  (:mem-segment vs))

(defn capacity
  "Get the capacity (max vectors) of the store."
  ^long [^VectorStore vs]
  (:capacity vs))

(defn get-commit-hash
  "Get the current commit hash (for crypto-hash mode).
   Returns nil if not in crypto-hash mode or no commits yet.
   Used by core.clj to include in snapshot."
  [^VectorStore vs]
  (when-let [ch (:commit-hash vs)]
    @ch))

(defn crypto-hash?
  "Check if store is in crypto-hash mode."
  [^VectorStore vs]
  (:crypto-hash? vs))

;; -----------------------------------------------------------------------------
;; Verification (for crypto-hash mode)

(defn verify-vectors-from-cold
  "Verify vectors from cold storage (konserve only, no mmap).
   Reads all chunks using address map, recomputes their hashes, and verifies.

   Args:
     store       - Konserve store
     address-map - Map of chunk-id -> storage-address (UUID)
     vector-count - Total vector count (for info only)

   Returns {:valid? true, :chunks-verified N} or {:valid? false, :error ...}"
  [store address-map vector-count]
  (if (empty? address-map)
    {:valid? true :chunks-verified 0 :note "No chunks to verify"}
    (let [;; Read and hash all chunks by their storage addresses
          chunk-hashes (loop [cids (keys address-map)
                              hashes []]
                         (if (empty? cids)
                           hashes
                           (let [cid (first cids)
                                 storage-addr (get address-map cid)
                                 chunk-data (k/get store (chunk-key storage-addr) nil {:sync? true})]
                             (if chunk-data
                               (recur (rest cids) (conj hashes (hash-chunk chunk-data)))
                               {:error :chunk-not-found :chunk-id cid :storage-addr storage-addr}))))]
      (if (map? chunk-hashes)
        ;; Error case
        {:valid? false :error (:error chunk-hashes)
         :chunk-id (:chunk-id chunk-hashes) :storage-addr (:storage-addr chunk-hashes)}
        ;; All chunks read successfully
        {:valid? true
         :chunks-verified (count chunk-hashes)
         :chunk-hashes chunk-hashes}))))

;; -----------------------------------------------------------------------------
;; Distance computation using MemorySegment (storage-specific SIMD)

(def ^:private ^ValueLayout$OfFloat FLOAT_LE
  (.withOrder ValueLayout/JAVA_FLOAT ByteOrder/LITTLE_ENDIAN))

(defn distance-squared-to-node
  "Compute squared L2 distance from query to stored vector.
   Zero-copy SIMD: reads directly from memory-mapped segment.

   Args:
     vs      - VectorStore
     node-id - Index of the stored vector
     query   - Query vector (float array)

   Returns:
     Squared Euclidean distance (double)"
  ^double [^VectorStore vs ^long node-id ^floats query]
  (let [dim (long (:dim vs))
        ^MemorySegment seg (:mem-segment vs)
        base-offset (long (+ HEADER-SIZE (* node-id dim 4)))
        species-len (long dist/SPECIES_LENGTH)
        upper-bound (- dim (rem dim species-len))]
    ;; SIMD loop - read directly from mmap
    (loop [i (long 0)
           ^FloatVector sum-vec (.zero dist/FLOAT_SPECIES)]
      (if (< i upper-bound)
        (let [offset (+ base-offset (* i 4))
              ^FloatVector va (FloatVector/fromMemorySegment dist/FLOAT_SPECIES seg offset ByteOrder/LITTLE_ENDIAN)
              ^FloatVector vb (FloatVector/fromArray dist/FLOAT_SPECIES query (int i))
              ^FloatVector diff (.sub va vb)
              ^FloatVector sq (.mul diff diff)]
          (recur (+ i species-len) (.add sum-vec sq)))
        ;; Reduce SIMD vector and handle tail
        (let [simd-sum (.reduceLanes sum-vec VectorOperators/ADD)]
          (loop [j (long upper-bound)
                 tail-sum (double simd-sum)]
            (if (< j dim)
              (let [offset (+ base-offset (* j 4))
                    stored (.get seg FLOAT_LE offset)
                    q (aget query (int j))
                    d (- stored q)]
                (recur (inc j) (+ tail-sum (* d d))))
              tail-sum)))))))

;; -----------------------------------------------------------------------------
;; Reflink (COW copy) support for branch isolation

(defn test-reflink-support
  "Test if the filesystem at the given directory supports reflink copies.
   Creates a small test file, attempts reflink copy, and cleans up.

   Returns true if reflink is supported, false otherwise."
  [^String dir-path]
  (let [test-src (str dir-path "/.reflink-test-" (System/currentTimeMillis))
        test-dst (str test-src ".copy")]
    (try
      ;; Create a small test file
      (spit test-src "reflink test")
      ;; Try reflink copy
      (let [proc (.start (ProcessBuilder. ["cp" "--reflink=always" test-src test-dst]))
            exit-code (.waitFor proc)]
        (= 0 exit-code))
      (catch Exception _
        false)
      (finally
        ;; Cleanup test files
        (let [src-file (File. test-src)
              dst-file (File. test-dst)]
          (when (.exists src-file) (.delete src-file))
          (when (.exists dst-file) (.delete dst-file)))))))

(defn copy-mmap-for-branch!
  "Copy mmap file for a new branch using reflink if supported.

   Args:
     src-path        - Source mmap file path
     dst-path        - Destination mmap file path
     reflink-supported - Boolean indicating if reflink is supported

   Returns:
     {:copied true, :reflink true/false} on success
     Throws on failure."
  [^String src-path ^String dst-path reflink-supported]
  (let [src-file (File. src-path)]
    (when-not (.exists src-file)
      (throw (ex-info "Source mmap file does not exist" {:src-path src-path})))

    (let [cp-args (if reflink-supported
                    ["cp" "--reflink=auto" src-path dst-path]
                    ["cp" src-path dst-path])
          proc (.start (ProcessBuilder. ^java.util.List cp-args))
          exit-code (.waitFor proc)]
      (when-not (zero? exit-code)
        (throw (ex-info "Failed to copy mmap file"
                        {:src-path src-path
                         :dst-path dst-path
                         :exit-code exit-code})))
      {:copied true :reflink reflink-supported})))

(defn branch-mmap-path
  "Generate an mmap file path for a specific branch.

  This is intentionally independent of the Konserve backend. The mmap files are
  local runtime artifacts (cache + SIMD-friendly access) and should live in a
  dedicated directory that is not scanned by Konserve.

  Format: {mmap-dir}/vectors-{branch-name}.bin"
  [^String mmap-dir branch]
  (str mmap-dir "/vectors-" (name branch) ".bin"))
