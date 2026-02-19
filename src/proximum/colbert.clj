(ns proximum.colbert
  "ColBERT-style late interaction retrieval.
   
   ColBERT represents each document as a matrix of token embeddings rather than
   a single vector. At search time, it computes MaxSim scores:
   
   MaxSim(query, doc) = Σ_i max_j (dot(query_i, doc_j))
   
   This enables fine-grained semantic matching that captures token-level
   interactions, outperforming single-vector models on complex queries.
   
   Implementation:
   - Uses compound keys [doc-id :token idx] for each token embedding
   - HNSW finds nearest tokens for each query token
   - MaxSim aggregates token matches into document scores
   
   References:
   - ColBERT (SIGIR'20): https://arxiv.org/abs/2004.12832
   - ColBERTv2 (NAACL'22): https://arxiv.org/abs/2112.01488
   - PLAID (CIKM'22): https://arxiv.org/abs/2205.09707"
  (:require [proximum.core :as core]))

;; -----------------------------------------------------------------------------
;; Document Insertion

(defn insert-document
  "Insert all token vectors for a document.
   
   Each token gets a compound key [doc-id :token idx] enabling:
   - Retrieval of individual tokens via HNSW
   - Aggregation of token matches by doc-id for MaxSim scoring
   
   Args:
     idx        - HNSW index
     doc-id     - Document identifier (string, keyword, UUID, etc.)
     token-vecs - Sequence of token embedding vectors (float arrays)
   
   Returns:
     Updated index with all token vectors inserted.
   
   Example:
     (insert-document idx \"doc-1\" [tok0 tok1 tok2])"
  [idx doc-id token-vecs]
  (reduce-kv
    (fn [idx' i tok-vec]
      (assoc idx' [doc-id :token i] tok-vec))
    idx
    (into [] token-vecs)))

(defn insert-documents
  "Insert multiple documents with their token vectors.
   
   Args:
     idx      - HNSW index
     docs     - Sequence of [doc-id token-vecs] pairs
   
   Returns:
     Updated index.
   
   Example:
     (insert-documents idx [[\"doc-1\" [toks...]] [\"doc-2\" [toks...]]])"
  [idx docs]
  (reduce
    (fn [idx' [doc-id token-vecs]]
      (insert-document idx' doc-id token-vecs))
    idx
    docs))

;; -----------------------------------------------------------------------------
;; MaxSim Scoring

(defn- compute-maxsim
  "Compute MaxSim score for a document from token match results.
   
   MaxSim = Σ_i max_j (similarity(query_i, doc_j))
   
   Args:
     query-count - Number of query tokens
     matches     - Sequence of {:id [doc-id :token idx] :distance d}
                   for all token matches across all query tokens
   
   Returns:
     {:doc-id ... :maxsim-score ... :matched-tokens count}"
  [query-count matches]
  (let [;; Group matches by doc-id
        by-doc (group-by (fn [{:keys [id]}]
                           (when (vector? id) (first id)))
                         matches)
        ;; For each doc, compute MaxSim: sum of max similarity per query token
        scores (for [[doc-id doc-matches] by-doc
                     :when doc-id]
                 (let [;; Group by query token index (implicit in order)
                       ;; Each query token's matches contribute one max
                       ;; We use similarity = 1 - distance (for cosine/L2 normalized)
                       query-groups (group-by :query-idx doc-matches)
                       maxsim (reduce +
                                     (for [[_ tokens] query-groups]
                                       (apply max (map #(- 1 (:distance %)) tokens))))]
                   {:doc-id doc-id
                    :maxsim-score maxsim
                    :matched-tokens (count doc-matches)}))]
    (sort-by :maxsim-score > scores)))

;; -----------------------------------------------------------------------------
;; MaxSim Search

(defn maxsim-search
  "Search using ColBERT MaxSim late interaction.
   
   Algorithm:
   1. For each query token, find k nearest document tokens via HNSW
   2. Group token matches by document ID
   3. Compute MaxSim score for each document
   4. Return top documents ranked by MaxSim
   
   Args:
     idx         - HNSW index with token embeddings
     query-toks  - Sequence of query token vectors (float arrays)
     k           - Number of documents to return
     opts        - Options:
                   :ef - HNSW beam width (default: 50)
                   :token-k - Token matches per query token (default: k * 10)
   
   Returns:
     Sequence of {:doc-id ... :maxsim-score ... :matched-tokens count}
   
   Example:
     (maxsim-search idx [q-tok0 q-tok1 q-tok2] 10)
     ;; => ({:doc-id \"doc-1\" :maxsim-score 2.5 :matched-tokens 15} ...)"
  ([idx query-toks k]
   (maxsim-search idx query-toks k {}))
  ([idx query-toks k opts]
   (let [ef (:ef opts 50)
         token-k (:token-k opts (* k 10))
         ;; For each query token, find nearest document tokens
         query-results (map-indexed
                        (fn [q-idx q-vec]
                          (map #(assoc % :query-idx q-idx)
                               (core/search idx q-vec token-k {:ef ef})))
                        query-toks)
         ;; Flatten all token matches with query index
         all-matches (apply concat query-results)
         ;; Compute MaxSim scores by document
         scored (compute-maxsim (count query-toks) all-matches)]
     (take k scored))))

;; -----------------------------------------------------------------------------
;; Filtered MaxSim Search

(defn maxsim-search-filtered
  "MaxSim search with document ID filtering.
   
   pred-fn: (fn [doc-id] boolean) - return true to include document.
   
   Useful for:
   - Multi-tenant search
   - Filtering by metadata stored externally
   - Category/domain constraints"
  ([idx query-toks k pred-fn]
   (maxsim-search-filtered idx query-toks k pred-fn {}))
  ([idx query-toks k pred-fn opts]
   (let [results (maxsim-search idx query-toks (* k 2) opts)
         filtered (filter #(pred-fn (:doc-id %)) results)]
     (take k filtered))))

;; -----------------------------------------------------------------------------
;; Utility Functions

(defn count-document-tokens
  "Count how many tokens are indexed for a document.
   
   Scans for compound keys [doc-id :token *]."
  [idx doc-id]
  (let [;; Access the external-id-index to count matching keys
        state (.-state idx)
        ext-idx (:external-id-index state)]
    (count (filter #(and (vector? %)
                         (= (first %) doc-id)
                         (= (second %) :token))
                   ext-idx))))

(defn get-document-token-ids
  "Get all token compound keys for a document.
   
   Returns sequence of [doc-id :token idx] keys."
  [idx doc-id]
  (let [state (.-state idx)
        ext-idx (:external-id-index state)
        ;; Find all keys matching [doc-id :token *]
        prefix [doc-id :token]]
    (filter #(and (vector? %)
                  (= (subvec % 0 2) prefix))
            ext-idx)))