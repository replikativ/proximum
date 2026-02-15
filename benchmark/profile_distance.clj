(ns profile-distance
  "Profile and compare distance computation implementations."
  (:require [clj-async-profiler.core :as prof])
  (:import [jdk.incubator.vector FloatVector VectorOperators VectorSpecies]
           [java.lang.foreign MemorySegment ValueLayout Arena]
           [java.nio ByteOrder]))

;; SIMD setup
(def ^VectorSpecies FLOAT_SPECIES (FloatVector/SPECIES_PREFERRED))
(def ^:const SPECIES_LENGTH (.length FLOAT_SPECIES))
(def FLOAT_LE (ValueLayout/JAVA_FLOAT))

(println (format "Vector species: %s (length=%d)" FLOAT_SPECIES SPECIES_LENGTH))

;; Original implementation (current)
(defn distance-squared-original
  ^double [^MemorySegment seg ^long offset ^floats query ^long dim]
  (let [upper-bound (- dim (mod dim SPECIES_LENGTH))]
    (loop [i 0
           sum-vec (FloatVector/zero FLOAT_SPECIES)]
      (if (< i upper-bound)
        (let [va (FloatVector/fromMemorySegment FLOAT_SPECIES seg
                   (+ offset (* i 4)) ByteOrder/LITTLE_ENDIAN)
              vb (FloatVector/fromArray FLOAT_SPECIES query (int i))
              diff (.sub va vb)]
          (recur (+ i SPECIES_LENGTH)
                 (.add sum-vec (.mul diff diff))))
        ;; Reduce and add tail
        (let [sum (.reduceLanes sum-vec VectorOperators/ADD)]
          (loop [j upper-bound
                 s sum]
            (if (< j dim)
              (let [a (.get seg FLOAT_LE (+ offset (* j 4)))
                    d (- a (aget query j))]
                (recur (inc j) (+ s (* d d))))
              s)))))))

;; FMA implementation
(defn distance-squared-fma
  ^double [^MemorySegment seg ^long offset ^floats query ^long dim]
  (let [upper-bound (- dim (mod dim SPECIES_LENGTH))]
    (loop [i 0
           sum-vec (FloatVector/zero FLOAT_SPECIES)]
      (if (< i upper-bound)
        (let [va (FloatVector/fromMemorySegment FLOAT_SPECIES seg
                   (+ offset (* i 4)) ByteOrder/LITTLE_ENDIAN)
              vb (FloatVector/fromArray FLOAT_SPECIES query (int i))
              diff (.sub va vb)]
          (recur (+ i SPECIES_LENGTH)
                 (.fma diff diff sum-vec)))  ;; FMA: diff*diff + sum
        ;; Reduce and add tail
        (let [sum (.reduceLanes sum-vec VectorOperators/ADD)]
          (loop [j upper-bound
                 s sum]
            (if (< j dim)
              (let [a (.get seg FLOAT_LE (+ offset (* j 4)))
                    d (- a (aget query j))]
                (recur (inc j) (+ s (* d d))))
              s)))))))

;; Unrolled 2x with FMA
(defn distance-squared-unrolled-2x
  ^double [^MemorySegment seg ^long offset ^floats query ^long dim]
  (let [stride (* 2 SPECIES_LENGTH)
        upper-bound-2x (- dim (mod dim stride))
        upper-bound (- dim (mod dim SPECIES_LENGTH))]
    ;; Main loop: 2x unrolled
    (loop [i 0
           sum-vec1 (FloatVector/zero FLOAT_SPECIES)
           sum-vec2 (FloatVector/zero FLOAT_SPECIES)]
      (if (< i upper-bound-2x)
        (let [;; First vector
              va1 (FloatVector/fromMemorySegment FLOAT_SPECIES seg
                    (+ offset (* i 4)) ByteOrder/LITTLE_ENDIAN)
              vb1 (FloatVector/fromArray FLOAT_SPECIES query (int i))
              diff1 (.sub va1 vb1)
              ;; Second vector
              i2 (+ i SPECIES_LENGTH)
              va2 (FloatVector/fromMemorySegment FLOAT_SPECIES seg
                    (+ offset (* i2 4)) ByteOrder/LITTLE_ENDIAN)
              vb2 (FloatVector/fromArray FLOAT_SPECIES query (int i2))
              diff2 (.sub va2 vb2)]
          (recur (+ i stride)
                 (.fma diff1 diff1 sum-vec1)
                 (.fma diff2 diff2 sum-vec2)))
        ;; Merge accumulators and handle remainder
        (let [sum-vec (.add sum-vec1 sum-vec2)]
          ;; Handle 1x remainder if needed
          (let [sum-vec-final
                (if (< upper-bound-2x upper-bound)
                  (let [va (FloatVector/fromMemorySegment FLOAT_SPECIES seg
                            (+ offset (* upper-bound-2x 4)) ByteOrder/LITTLE_ENDIAN)
                        vb (FloatVector/fromArray FLOAT_SPECIES query (int upper-bound-2x))
                        diff (.sub va vb)]
                    (.fma diff diff sum-vec))
                  sum-vec)
                sum (.reduceLanes sum-vec-final VectorOperators/ADD)]
            ;; Scalar tail
            (loop [j upper-bound
                   s sum]
              (if (< j dim)
                (let [a (.get seg FLOAT_LE (+ offset (* j 4)))
                      d (- a (aget query j))]
                  (recur (inc j) (+ s (* d d))))
                s))))))))

(defn benchmark-impl [name f seg query dim iterations]
  (let [offset 0]
    ;; Warmup
    (dotimes [_ 1000]
      (f seg offset query dim))
    ;; Timed
    (let [start (System/nanoTime)]
      (dotimes [_ iterations]
        (f seg offset query dim))
      (let [elapsed (/ (- (System/nanoTime) start) 1e6)]
        (println (format "  %s: %.2f ms (%.0f ns/call)"
                         name elapsed (/ (* elapsed 1e6) iterations)))))))

(defn -main [& args]
  (let [dim 128
        iterations 1000000
        ;; Create test data
        arena (Arena/ofConfined)
        seg (.allocate arena (* dim 4))
        query (float-array dim)]

    ;; Initialize with random data
    (dotimes [i dim]
      (.set seg FLOAT_LE (* i 4) (float (- (rand 2.0) 1.0)))
      (aset query i (float (- (rand 2.0) 1.0))))

    (println (format "\n=== Distance Computation Benchmark ==="))
    (println (format "dim=%d, iterations=%d\n" dim iterations))

    ;; Verify correctness
    (let [r1 (distance-squared-original seg 0 query dim)
          r2 (distance-squared-fma seg 0 query dim)
          r3 (distance-squared-unrolled-2x seg 0 query dim)]
      (println (format "Correctness check:"))
      (println (format "  Original: %.6f" r1))
      (println (format "  FMA:      %.6f (diff: %.2e)" r2 (Math/abs (- r1 r2))))
      (println (format "  Unroll2x: %.6f (diff: %.2e)\n" r3 (Math/abs (- r1 r3)))))

    ;; Benchmark each
    (println "Benchmarking (lower is better):")
    (benchmark-impl "Original     " distance-squared-original seg query dim iterations)
    (benchmark-impl "FMA          " distance-squared-fma seg query dim iterations)
    (benchmark-impl "Unrolled 2x  " distance-squared-unrolled-2x seg query dim iterations)

    (.close arena)
    (println "\nDone.")))
