#!/usr/bin/env python3
"""
Download and prepare benchmark datasets.

SIFT datasets from http://corpus-texmex.irisa.fr/
- SIFT10K: 10,000 base vectors, 100 queries, 128 dimensions
- SIFT1M: 1,000,000 base vectors, 10,000 queries, 128 dimensions

GloVe dataset from ann-benchmarks.com
- GloVe-100: 1,183,514 base vectors, 10,000 queries, 100 dimensions (angular/cosine)

DBpedia-OpenAI from HuggingFace
- dbpedia-openai-1m: 1,000,000 vectors, 1,536 dimensions (OpenAI ada-002)
- dbpedia-openai-100k: 100,000 vectors subset for faster testing

Usage:
    python benchmark/datasets.py sift10k
    python benchmark/datasets.py sift1m
    python benchmark/datasets.py glove100
    python benchmark/datasets.py dbpedia-openai-100k
    python benchmark/datasets.py dbpedia-openai-1m
"""

import os
import sys
import struct
import tarfile
import numpy as np
from pathlib import Path
from urllib.request import urlretrieve

DATASETS_DIR = Path(__file__).parent / "data"

DATASETS = {
    "sift10k": {
        "url": "ftp://ftp.irisa.fr/local/texmex/corpus/siftsmall.tar.gz",
        "dir": "siftsmall",
        "base": "siftsmall_base.fvecs",
        "query": "siftsmall_query.fvecs",
        "groundtruth": "siftsmall_groundtruth.ivecs",
        "dim": 128,
        "n_base": 10000,
        "n_query": 100,
    },
    "sift1m": {
        "url": "ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz",
        "dir": "sift",
        "base": "sift_base.fvecs",
        "query": "sift_query.fvecs",
        "groundtruth": "sift_groundtruth.ivecs",
        "dim": 128,
        "n_base": 1000000,
        "n_query": 10000,
    },
}

# HDF5 datasets from ann-benchmarks.com (different format)
HDF5_DATASETS = {
    "glove100": {
        "url": "http://ann-benchmarks.com/glove-100-angular.hdf5",
        "filename": "glove-100-angular.hdf5",
        "metric": "angular",  # cosine similarity
        "dim": 100,
        "n_base": 1183514,
        "n_query": 10000,
    },
    "glove10k": {
        "url": "http://ann-benchmarks.com/glove-100-angular.hdf5",
        "filename": "glove-100-angular.hdf5",
        "metric": "angular",  # cosine similarity
        "dim": 100,
        "n_base": 10000,  # Subset of first 10k vectors
        "n_query": 100,   # Subset of first 100 queries
        "subset": True,   # Flag to indicate this is a subset
    },
}

# HuggingFace datasets
HUGGINGFACE_DATASETS = {
    "dbpedia-openai-1m": {
        "repo": "KShivendu/dbpedia-entities-openai-1M",
        "embedding_column": "openai",
        "metric": "angular",  # cosine similarity (normalized embeddings)
        "dim": 1536,
        "n_base": 1000000,
        "n_query": 1000,  # Sample 1000 queries from base
    },
    "dbpedia-openai-100k": {
        "repo": "KShivendu/dbpedia-entities-openai-1M",
        "embedding_column": "openai",
        "metric": "angular",
        "dim": 1536,
        "n_base": 100000,  # Subset for faster testing
        "n_query": 1000,
        "subset": True,
    },
}


def read_fvecs(filename):
    """Read .fvecs file format (float vectors)."""
    with open(filename, "rb") as f:
        vectors = []
        while True:
            dim_bytes = f.read(4)
            if not dim_bytes:
                break
            dim = struct.unpack("i", dim_bytes)[0]
            vec = struct.unpack(f"{dim}f", f.read(dim * 4))
            vectors.append(vec)
        return np.array(vectors, dtype=np.float32)


def read_ivecs(filename):
    """Read .ivecs file format (integer vectors, for ground truth)."""
    with open(filename, "rb") as f:
        vectors = []
        while True:
            dim_bytes = f.read(4)
            if not dim_bytes:
                break
            dim = struct.unpack("i", dim_bytes)[0]
            vec = struct.unpack(f"{dim}i", f.read(dim * 4))
            vectors.append(vec)
        return np.array(vectors, dtype=np.int32)


def download_progress(block_num, block_size, total_size):
    """Progress callback for urlretrieve."""
    downloaded = block_num * block_size
    percent = min(100, downloaded * 100 // total_size)
    bar_len = 40
    filled = int(bar_len * percent // 100)
    bar = "=" * filled + "-" * (bar_len - filled)
    print(f"\r  [{bar}] {percent}%", end="", flush=True)


def download_hdf5_dataset(name):
    """Download an HDF5 dataset from ann-benchmarks."""
    try:
        import h5py
    except ImportError:
        print("h5py required for HDF5 datasets. Install with: pip install h5py")
        sys.exit(1)

    info = HDF5_DATASETS[name]
    DATASETS_DIR.mkdir(parents=True, exist_ok=True)

    hdf5_file = DATASETS_DIR / info["filename"]
    if not hdf5_file.exists():
        print(f"Downloading {name} from {info['url']}...")
        urlretrieve(info["url"], hdf5_file, download_progress)
        print()

    print(f"Dataset ready at {hdf5_file}")
    return hdf5_file


def compute_groundtruth_cosine(base, queries, k=100):
    """Compute ground truth for cosine similarity using brute force."""
    # Normalize vectors for cosine similarity
    base_norm = base / np.linalg.norm(base, axis=1, keepdims=True)
    queries_norm = queries / np.linalg.norm(queries, axis=1, keepdims=True)

    # Compute all cosine similarities (dot product of normalized vectors)
    similarities = np.dot(queries_norm, base_norm.T)

    # Get indices of top-k most similar (highest dot product)
    groundtruth = np.argsort(-similarities, axis=1)[:, :k]
    return groundtruth.astype(np.int32)


def load_huggingface_dataset(name):
    """Load dataset from HuggingFace. Returns (base, queries, groundtruth)."""
    try:
        from datasets import load_dataset as hf_load
    except ImportError:
        print("datasets library required. Install with: pip install datasets")
        sys.exit(1)

    info = HUGGINGFACE_DATASETS[name]
    print(f"Loading {name} from HuggingFace ({info['repo']})...", file=sys.stderr)
    print("  This may take a while for large datasets...", file=sys.stderr)

    # Load from HuggingFace
    ds = hf_load(info["repo"], split="train")

    # Extract embeddings
    n_base = info["n_base"]
    n_query = info["n_query"]

    print(f"  Extracting {n_base} embeddings...", file=sys.stderr)

    # For subset, only load what we need
    if info.get("subset"):
        ds = ds.select(range(n_base + n_query))

    # Convert to numpy array
    embeddings = np.array(ds[info["embedding_column"]], dtype=np.float32)

    # Split into base and queries
    # Use last n_query vectors as queries (different from base)
    if len(embeddings) > n_base:
        base = embeddings[:n_base]
        # Sample queries from remaining vectors or random from base
        remaining = embeddings[n_base:]
        if len(remaining) >= n_query:
            queries = remaining[:n_query]
        else:
            # Random sample from base as queries
            query_indices = np.random.choice(n_base, n_query, replace=False)
            queries = base[query_indices]
    else:
        base = embeddings[:n_base]
        # Random sample from base as queries
        query_indices = np.random.choice(n_base, n_query, replace=False)
        queries = base[query_indices]

    print(f"  Computing ground truth (k=100)...", file=sys.stderr)
    groundtruth = compute_groundtruth_cosine(base, queries, k=100)

    print(f"  Base vectors: {base.shape}", file=sys.stderr)
    print(f"  Query vectors: {queries.shape}", file=sys.stderr)
    print(f"  Ground truth: {groundtruth.shape}", file=sys.stderr)

    return base, queries, groundtruth


def load_hdf5_dataset(name):
    """Load HDF5 dataset from ann-benchmarks. Returns (base, queries, groundtruth)."""
    try:
        import h5py
    except ImportError:
        print("h5py required for HDF5 datasets. Install with: pip install h5py")
        sys.exit(1)

    info = HDF5_DATASETS[name]
    hdf5_file = DATASETS_DIR / info["filename"]

    if not hdf5_file.exists():
        download_hdf5_dataset(name)

    print(f"Loading {name} dataset...", file=sys.stderr)
    with h5py.File(hdf5_file, "r") as f:
        base = np.array(f["train"])
        queries = np.array(f["test"])
        groundtruth = np.array(f["neighbors"])

    # Handle subset datasets (e.g., glove10k from glove100)
    if info.get("subset"):
        n_base = info["n_base"]
        n_query = info["n_query"]
        print(f"  Creating subset: {n_base} base, {n_query} queries...", file=sys.stderr)
        base = base[:n_base]
        queries = queries[:n_query]
        # Recompute ground truth for subset (original GT references full dataset)
        print(f"  Computing ground truth for subset...", file=sys.stderr)
        groundtruth = compute_groundtruth_cosine(base, queries, k=100)

    print(f"  Base vectors: {base.shape}", file=sys.stderr)
    print(f"  Query vectors: {queries.shape}", file=sys.stderr)
    print(f"  Ground truth: {groundtruth.shape}", file=sys.stderr)

    return base, queries, groundtruth


def download_dataset(name):
    """Download and extract a dataset."""
    if name in HUGGINGFACE_DATASETS:
        # HuggingFace datasets are downloaded on first load
        print(f"Dataset {name} will be downloaded from HuggingFace on first load")
        return None

    if name in HDF5_DATASETS:
        return download_hdf5_dataset(name)

    if name not in DATASETS:
        all_datasets = list(DATASETS.keys()) + list(HDF5_DATASETS.keys()) + list(HUGGINGFACE_DATASETS.keys())
        print(f"Unknown dataset: {name}")
        print(f"Available: {', '.join(all_datasets)}")
        sys.exit(1)

    info = DATASETS[name]
    DATASETS_DIR.mkdir(parents=True, exist_ok=True)

    dataset_dir = DATASETS_DIR / info["dir"]
    if dataset_dir.exists():
        print(f"Dataset {name} already exists at {dataset_dir}")
        return dataset_dir

    # Download
    tarball = DATASETS_DIR / f"{name}.tar.gz"
    if not tarball.exists():
        print(f"Downloading {name} from {info['url']}...")
        urlretrieve(info["url"], tarball, download_progress)
        print()

    # Extract
    print(f"Extracting to {DATASETS_DIR}...")
    with tarfile.open(tarball, "r:gz") as tar:
        tar.extractall(DATASETS_DIR)

    # Clean up tarball
    tarball.unlink()

    print(f"Dataset ready at {dataset_dir}")
    return dataset_dir


def load_dataset(name):
    """Load dataset, downloading if necessary. Returns (base, queries, groundtruth)."""
    if name in HUGGINGFACE_DATASETS:
        return load_huggingface_dataset(name)

    if name in HDF5_DATASETS:
        return load_hdf5_dataset(name)

    info = DATASETS[name]
    dataset_dir = DATASETS_DIR / info["dir"]

    if not dataset_dir.exists():
        download_dataset(name)

    print(f"Loading {name} dataset...", file=sys.stderr)
    base = read_fvecs(dataset_dir / info["base"])
    queries = read_fvecs(dataset_dir / info["query"])
    groundtruth = read_ivecs(dataset_dir / info["groundtruth"])

    print(f"  Base vectors: {base.shape}", file=sys.stderr)
    print(f"  Query vectors: {queries.shape}", file=sys.stderr)
    print(f"  Ground truth: {groundtruth.shape}", file=sys.stderr)

    return base, queries, groundtruth


def export_for_clojure(name):
    """Export dataset as numpy .npy files for easy loading in Clojure."""
    base, queries, groundtruth = load_dataset(name)

    out_dir = DATASETS_DIR / name
    out_dir.mkdir(parents=True, exist_ok=True)

    np.save(out_dir / "base.npy", base)
    np.save(out_dir / "queries.npy", queries)
    np.save(out_dir / "groundtruth.npy", groundtruth)

    print(f"Exported to {out_dir}/")
    return out_dir


if __name__ == "__main__":
    all_datasets = list(DATASETS.keys()) + list(HDF5_DATASETS.keys()) + list(HUGGINGFACE_DATASETS.keys())
    if len(sys.argv) < 2:
        print("Usage: python datasets.py <dataset_name>")
        print(f"Available datasets: {', '.join(all_datasets)}")
        sys.exit(1)

    name = sys.argv[1].lower()
    export_for_clojure(name)
