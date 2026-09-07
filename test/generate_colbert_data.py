"""
Test Proximum's ColBERT functionality with real embeddings.

Since we can't easily load ColBERT models (requires C++ extensions),
we simulate token-level embeddings by splitting text into chunks.
"""

import numpy as np
from sentence_transformers import SentenceTransformer
import json
import sys

def get_token_embeddings(text, model, chunk_size=20):
    """Split text into chunks and embed each chunk (simulating tokens)."""
    words = text.split()
    chunks = []
    for i in range(0, len(words), chunk_size):
        chunk = " ".join(words[i:i+chunk_size])
        if chunk:
            chunks.append(chunk)
    
    if not chunks:
        chunks = [text]  # Fallback to full text
    
    embeddings = model.encode(chunks)
    return embeddings, chunks

def main():
    import sys
    print("Loading model...", file=sys.stderr)
    model = SentenceTransformer('all-MiniLM-L6-v2')
    dim = 384
    
    # Test documents
    documents = [
        "Machine learning is a field of artificial intelligence that uses statistical techniques to give computer systems the ability to learn from data.",
        "Python is a high-level programming language known for its clear syntax and readability.",
        "Neural networks are computing systems inspired by biological neural networks in animal brains."
    ]
    
    # Query
    query = "What is machine learning?"
    
    # Generate embeddings
    print(f"Encoding {len(documents)} documents...", file=sys.stderr)
    
    result = {
        "dim": dim,
        "query_chunks": [],
        "docs": []
    }
    
    # Encode query
    query_embedding, query_chunks = get_token_embeddings(query, model, chunk_size=3)
    for i, (emb, chunk) in enumerate(zip(query_embedding, query_chunks)):
        result["query_chunks"].append({
            "id": i,
            "text": chunk,
            "embedding": emb.tolist()
        })
    
    # Encode documents
    for doc_idx, doc in enumerate(documents):
        doc_embedding, doc_chunks = get_token_embeddings(doc, model, chunk_size=10)
        doc_data = {
            "id": f"doc-{doc_idx + 1}",
            "text": doc,
            "tokens": []
        }
        for tok_idx, (emb, chunk) in enumerate(zip(doc_embedding, doc_chunks)):
            doc_data["tokens"].append({
                "id": tok_idx,
                "text": chunk,
                "embedding": emb.tolist()
            })
        result["docs"].append(doc_data)
    
    # Output as JSON
    print(json.dumps(result))

if __name__ == "__main__":
    main()