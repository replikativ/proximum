(ns user
  "REPL development utilities."
  (:require [proximum.distance :as dist]
            [proximum.vectors :as vec]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]))

(defn random-vector
  "Generate a random float vector of given dimension."
  [dim]
  (float-array (repeatedly dim #(- (rand 2.0) 1.0))))

(defn random-vectors
  "Generate n random vectors of given dimension."
  [n dim]
  (vec (repeatedly n #(random-vector dim))))

(comment
  ;; Quick tests

  ;; Test distance functions
  (let [v1 (float-array [1.0 0.0 0.0])
        v2 (float-array [0.0 1.0 0.0])]
    (println "L2 distance:" (dist/distance dist/euclidean v1 v2))
    (println "L2² distance:" (dist/distance-squared dist/euclidean v1 v2)))

  ;; Test vector store
  (let [path "/tmp/test-vectors.bin"
        _ (when (.exists (java.io.File. path))
            (.delete (java.io.File. path)))
        store (vec/create-store path 128 1000)]
    (println "Created store with capacity 1000, dim 128")

    ;; Add some vectors
    (dotimes [i 10]
      (vec/append! store (random-vector 128)))

    (println "Added 10 vectors, count:" (vec/count-vectors store))

    ;; Read back
    (let [v0 (vec/get-vector store 0)]
      (println "Vector 0 first 5 elements:" (take 5 (seq v0))))

    ;; Flush and close
    (vec/close! store)
    (println "Closed store"))

  )
