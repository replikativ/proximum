(ns proximum.validation
  "Validation at the public boundary of a vector index.

   HNSW's SIMD loops are dimension-driven, so accepting a longer array would
   silently ignore its tail. Converting arbitrary numbers to float can also
   turn a finite double into an infinity. Keep both checks in one place and
   perform them before any mmap or graph mutation."
  (:import [java.lang Float]))

(def ^:private float-array-class (Class/forName "[F"))

(defn finite-float?
  "True when x is representable as a finite IEEE-754 float32 value."
  [x]
  (and (number? x)
       (try
         (Float/isFinite (float x))
         (catch IllegalArgumentException _ false))))

(defn validate-vector
  "Return a defensive float-array copy of `vector` after strict validation.

   `dimension` must match exactly. Every component must be numeric and remain
   finite after float32 conversion. Throws ExceptionInfo with machine-readable
   :reason and position data on invalid input."
  ^floats [vector dimension]
  (when (nil? vector)
    (throw (ex-info "Vector must not be nil"
                    {:reason :nil-vector :expected-dimension dimension})))
  (let [actual (try
                 (count vector)
                 (catch Throwable _ ::not-countable))]
    (when (= ::not-countable actual)
      (throw (ex-info "Vector must be a finite collection or float array"
                      {:reason :invalid-vector-type
                       :expected-dimension dimension
                       :type (type vector)})))
    (when-not (= dimension actual)
      (throw (ex-info (str "Vector dimension mismatch: expected " dimension
                           ", got " actual)
                      {:reason :dimension-mismatch
                       :expected-dimension dimension
                       :actual-dimension actual})))
    (let [values (if (instance? float-array-class vector)
                   (seq ^floats vector)
                   vector)]
      (doseq [[position value] (map-indexed clojure.core/vector values)]
        (when-not (finite-float? value)
          (throw (ex-info (str "Vector component at position " position
                               " is not a finite float32 value")
                          {:reason :non-finite-component
                           :position position
                           :value value
                           :expected-dimension dimension}))))
      (if (instance? float-array-class vector)
        (aclone ^floats vector)
        (float-array values)))))

(defn validate-vector-for-metric
  "Strictly validate a vector. Metric-specific indexability is decided by the
   operation boundary, since pgvector stores cosine zero vectors canonically
   while omitting them from its ANN index."
  ^floats [vector dimension _metric]
  (validate-vector vector dimension))

(defn zero-vector?
  "True only for an exact all-zero float vector. Tiny nonzero values are valid."
  [^floats vector]
  (every? #(== 0.0 (double %)) vector))
