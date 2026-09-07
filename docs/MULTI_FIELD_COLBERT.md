# Multi-Field Indexing & ColBERT Support

Proximum supports two complementary approaches to multi-vector document representation:

1. **Compound Keys (Cozo-style)** - Multiple vectors per document, each keyed by field path
2. **ColBERT Late Interaction** - Token-level embeddings with MaxSim scoring

## Compound Keys

### Concept

External IDs can be any Clojure collection, not just strings. This enables multi-field indexing:

```clojure
;; Instead of:
(assoc idx "doc-1" doc-vec)

;; Use compound keys:
(assoc idx ["doc-1" :title] title-vec)
(assoc idx ["doc-1" :content 0] content-vec-0)
(assoc idx ["doc-1" :metadata :author] author-vec)
```

### API

```clojure
;; Insert with compound keys (standard Clojure assoc)
(def idx' (-> idx
              (assoc ["doc-1" :title] title-vec)
              (assoc ["doc-1" :content 0] content-vec-0)
              (assoc ["doc-2" :title] title-vec-2)))

;; Search returns compound keys
(search idx' query 10)
;; => ({:id ["doc-1" :title], :distance 0.1}
;;     {:id ["doc-1" :content 0], :distance 0.15}
;;     {:id ["doc-2" :title], :distance 0.3})

;; Filter by field
(filter #(= (second (:id %)) :title) (search idx' query 100))

;; Mix compound and simple keys
(assoc idx' "simple-id" vec)
```

### Implementation

The `external-id-index` PSS uses lexicographic comparison for compound keys:

```clojure
(compare-coll ["doc-1" :title] ["doc-1" :content])  ; => negative (title < content)
(compare-coll ["doc-1" :title] ["doc-2" :title])    ; => negative (doc-1 < doc-2)
(compare-coll ["doc-1"] ["doc-1" :title])           ; => negative (shorter is less)
```

This enables efficient range queries and filtering.

## Weighted Multi-Field Search

Combine results from different semantic fields with user-specified weights:

```clojure
(require '[proximum.colbert :as colbert])

;; Search with field weights
(colbert/weighted-field-search 
  idx query-vec 10 
  {:title 0.5 :content 0.3 :metadata 0.2})

;; => ({:doc-id "doc-1", 
;;      :score 0.85,
;;      :field-scores {:title 1.0, :content 0.7}}
;;     ...)
```

### Required Field Constraints

```clojure
;; Only return documents with matches in ALL specified fields
(colbert/weighted-field-search-with-constraints
  idx query 10 
  {:title 0.5 :content 0.5}
  #{:title :content})  ; Both fields must have matches
```

## ColBERT Late Interaction

### Concept

ColBERT represents each document as a **matrix of token embeddings** rather than a single vector:

```
Document: "Machine learning is AI"
Tokens:   [tok0] [tok1] [tok2] [tok3]
Vectors:  [v0]   [v1]   [v2]   [v3]   ; 4 vectors per document
```

At search time, **MaxSim scoring** aggregates token-level matches:

```
MaxSim(query, doc) = Σ_i max_j (similarity(query_i, doc_j))
```

This captures fine-grained semantic interactions that single-vector models miss.

### API

```clojure
(require '[proximum.colbert :as colbert])

;; Insert document with token embeddings
(def idx' (colbert/insert-document 
           idx "doc-1" 
           [tok0-vec tok1-vec tok2-vec]))

;; Batch insert
(def idx'' (colbert/insert-documents
            idx'
            [["doc-1" [toks...]]
             ["doc-2" [toks...]]]))

;; MaxSim search
(colbert/maxsim-search idx' query-tokens 10)
;; => ({:doc-id "doc-1", 
;;      :maxsim-score 2.5, 
;;      :matched-tokens 15}
;;     ...)

;; Filtered search (multi-tenant)
(colbert/maxsim-search-filtered
  idx' query-tokens 10
  #(= "tenant-a" (namespace %)))
```

### Implementation

Token embeddings use compound keys `[doc-id :token idx]`:

```clojure
;; Internally, insert-document does:
(assoc idx [doc-id :token 0] tok0-vec)
(assoc idx [doc-id :token 1] tok1-vec)
...
```

MaxSim search:
1. For each query token, find k nearest document tokens via HNSW
2. Group matches by doc-id
3. Compute MaxSim score per document
4. Return ranked documents

## Hybrid Search

Combine field-level and token-level matching:

```clojure
;; α controls balance: 1.0 = field only, 0.0 = MaxSim only
(colbert/hybrid-search 
  idx 
  query-vec           ; For field search
  query-tokens        ; For MaxSim search
  10                  ; k results
  {:title 0.5 :content 0.5}  ; Field weights
  0.5)                ; alpha (50% each)

;; => ({:doc-id "doc-1",
;;      :score 0.75,
;;      :field-score 0.8,
;;      :maxsim-score 2.5}
;;     ...)
```

## Testing

### Unit Tests

Unit tests use synthetic embeddings and run without Python:

```bash
clojure -M:test
```

### Integration Tests with Real Embeddings

Integration tests use `sentence-transformers` for real semantic vectors.

#### Option 1: Use Committed Test Data (CI/CD)

A minimal test file is committed at `test/data/colbert_minimal.json`. Tests will use this automatically if available.

#### Option 2: Generate Real Embeddings

```bash
# Create Python venv
python -m venv .venv
source .venv/bin/activate  # or: .venv/bin/activate.fish

# Install dependencies
pip install sentence-transformers

# Generate test data
python test/generate_colbert_data.py > /tmp/colbert_data.json

# Run tests
clojure -M:test
```

### What Integration Tests Validate

1. **MaxSim scoring works with real semantic vectors** (384-dim from all-MiniLM-L6-v2)
2. **Semantic ranking is correct** - Query "What is machine learning?" returns ML document first
3. **Weighted field search** combines field scores correctly

**Note**: These tests use simulated token embeddings (chunked text embeddings), not actual ColBERT model tokenization. The ColBERT model requires C++ extension compilation.

## References

- [ColBERT (SIGIR'20)](https://arxiv.org/abs/2004.12832) - Original late interaction paper
- [ColBERTv2 (NAACL'22)](https://arxiv.org/abs/2112.01488) - Residual compression
- [PLAID (CIKM'22)](https://arxiv.org/abs/2205.09707) - Efficient retrieval engine
- [RAGatouille](https://github.com/AnswerDotAI/RAGatouille) - Easy ColBERT integration
- [answerai-colbert-small-v1](https://huggingface.co/answerdotai/answerai-colbert-small-v1) - Best small ColBERT model