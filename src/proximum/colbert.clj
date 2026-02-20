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

;; -----------------------------------------------------------------------------
;; Weighted Multi-Field Search

(defn- group-by-field
  "Group search results by field (second element of compound key)."
  [results]
  (group-by (fn [{:keys [id]}]
              (when (vector? id) (second id)))
            results))

(defn weighted-field-search
  "Search across multiple semantic fields with weighted scoring.
   
   Combines results from different attribute embeddings (title, content, etc.)
   with user-specified weights. This is NOT ColBERT MaxSim - it combines
   single-vector field results.
   
   Algorithm:
   1. Search index with query vector
   2. Group results by field (from compound key [doc-id :field ...])
   3. For each document, compute weighted sum: Σ_field weight * similarity
   4. Return top documents by combined score
   
   Args:
     idx           - HNSW index with compound keys
     query-vec     - Query vector (float array)
     k             - Number of documents to return
     field-weights - Map of {:field-name weight} (weights should sum to 1.0)
     opts          - Options:
                     :ef - HNSW beam width (default: 50)
   
   Returns:
     Sequence of {:doc-id ... :score ... :field-scores {:field score ...}}
   
   Example:
     (weighted-field-search idx query 10 {:title 0.5 :content 0.3 :metadata 0.2})"
  ([idx query-vec k field-weights]
   (weighted-field-search idx query-vec k field-weights {}))
  ([idx query-vec k field-weights opts]
   (let [ef (:ef opts 50)
         ;; Search with enough results to cover all fields
         results (core/search idx query-vec (* k (count field-weights)) {:ef ef})
         ;; Group by field
         by-field (group-by-field results)
         ;; Group by doc-id across all fields
         by-doc (group-by (fn [{:keys [id]}]
                            (when (vector? id) (first id)))
                          results)
         ;; Compute weighted scores per document
         scored (for [[doc-id matches] by-doc
                      :when doc-id]
                  (let [;; For each field, get best match and apply weight
                        field-scores (into {}
                                           (for [[field field-matches] (group-by-field matches)]
                                             [field (apply max (map #(- 1 (:distance %)) field-matches))]))
                       ;; Weighted sum: only include fields with weights
                        weighted-score (reduce +
                                               (for [[field weight] field-weights
                                                     :let [sim (get field-scores field 0)]]
                                                 (* weight sim)))]
                    {:doc-id doc-id
                     :score weighted-score
                     :field-scores field-scores}))]
     (take k (sort-by :score > scored)))))

(defn weighted-field-search-with-constraints
  "Weighted field search with mandatory field constraints.
   
   Requires matches in specified fields, then applies weighted scoring.
   Useful for 'must match title' type queries.
   
   Args:
     idx           - HNSW index
     query-vec     - Query vector
     k             - Number of results
     field-weights - Map of {:field weight}
     required-fields - Set of fields that must have matches
     opts          - Options
   
   Returns:
     Sequence of {:doc-id ... :score ... :field-scores {...}}
     Only documents with matches in ALL required fields are returned."
  ([idx query-vec k field-weights required-fields]
   (weighted-field-search-with-constraints idx query-vec k field-weights required-fields {}))
  ([idx query-vec k field-weights required-fields opts]
   (let [results (weighted-field-search idx query-vec (* k 2) field-weights opts)]
     (->> results
          (filter (fn [{:keys [field-scores]}]
                    (every? #(contains? field-scores %) required-fields)))
          (take k)))))

;; -----------------------------------------------------------------------------
;; Hybrid: Weighted Fields + ColBERT MaxSim

(defn hybrid-search
  "Combine weighted field search with ColBERT MaxSim.
   
   For each document:
   - Field score: weighted combination of single-vector field matches
   - MaxSim score: token-level semantic matching
   - Final score: α * field_score + (1-α) * maxsim_score
   
   Args:
     idx           - HNSW index with both field vectors and token vectors
     query-vec     - Single vector for field search
     query-toks    - Token vectors for MaxSim search
     k             - Number of results
     field-weights - Map of {:field weight} for field search
     alpha         - Weight for field score (0-1, MaxSim gets 1-alpha)
     opts          - Options:
                     :ef - HNSW beam width
                     :token-k - Token matches per query token
   
   Returns:
     Sequence of {:doc-id ... :score ... :field-score ... :maxsim-score ...}"
  ([idx query-vec query-toks k field-weights alpha]
   (hybrid-search idx query-vec query-toks k field-weights alpha {}))
  ([idx query-vec query-toks k field-weights alpha opts]
   (let [;; Get field search results
         field-results (weighted-field-search idx query-vec (* k 2) field-weights opts)
         field-by-doc (into {} (map (juxt :doc-id identity) field-results))
         ;; Get MaxSim results
         maxsim-results (maxsim-search idx query-toks (* k 2) opts)
         maxsim-by-doc (into {} (map (juxt :doc-id identity) maxsim-results))
         ;; Combine all doc-ids
         all-doc-ids (into (set (keys field-by-doc))
                           (keys maxsim-by-doc))
         ;; Normalize MaxSim scores to 0-1 range (rough approximation)
         maxsim-max (apply max (map :maxsim-score maxsim-results))
         maxsim-min (apply min (map :maxsim-score maxsim-results))
         maxsim-range (- maxsim-max maxsim-min)
         ;; Compute hybrid scores
         scored (for [doc-id all-doc-ids]
                  (let [field-rec (get field-by-doc doc-id)
                        maxsim-rec (get maxsim-by-doc doc-id)
                        field-score (:score field-rec 0)
                        raw-maxsim (:maxsim-score maxsim-rec 0)
                       ;; Normalize maxsim to 0-1
                        maxsim-norm (if (pos? maxsim-range)
                                      (/ (- raw-maxsim maxsim-min) maxsim-range)
                                      0.5)
                       ;; Hybrid score
                        hybrid-score (+ (* alpha field-score)
                                        (* (- 1 alpha) maxsim-norm))]
                    {:doc-id doc-id
                     :score hybrid-score
                     :field-score field-score
                     :maxsim-score raw-maxsim
                     :maxsim-norm maxsim-norm
                     :field-scores (:field-scores field-rec)
                     :matched-tokens (:matched-tokens maxsim-rec 0)}))]
     (take k (sort-by :score > scored)))))