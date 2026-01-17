(ns proximum.distance
  "SIMD constants for vector operations.

   This namespace provides Java Vector API constants used by vectors.clj
   for SIMD-accelerated distance computations in Clojure.

   Note: The main HNSW hot path uses Java's Distance.java directly.
   These constants are used for Clojure-side SIMD operations like
   compute-raw-distance-simd in vectors.clj."
  (:import [jdk.incubator.vector FloatVector VectorSpecies]))

;; SIMD Vector Species - use preferred width for this CPU
;; AVX-512: 16 floats, AVX: 8 floats, SSE: 4 floats
(def ^VectorSpecies FLOAT_SPECIES (FloatVector/SPECIES_PREFERRED))
(def ^:const SPECIES_LENGTH (.length FLOAT_SPECIES))
