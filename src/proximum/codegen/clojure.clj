(ns proximum.codegen.clojure
  "Code generation for Clojure API from specification.

   This namespace provides macros to emit the public API from the
   specification, ensuring documentation and arglists are consistent
   with the spec."
  (:require [proximum.specification :as spec]
            [malli.core :as m]))

;; =============================================================================
;; Arglist Extraction from Malli Schemas
;; =============================================================================

;; Primitive schema type keywords that should NOT become arg names
(def ^:private primitive-types
  #{:any :map :int :string :keyword :boolean :double :uuid :nil
    :nat-int? :pos-int? :number :symbol :qualified-keyword})

(defn- arg-name-from-schema
  "Extract a meaningful argument name from a schema element.
   Returns positional name (argN) for primitive types."
  [i arg]
  (cond
    ;; Optional arg [:? Type]
    (and (sequential? arg) (= :? (first arg)))
    (symbol (str "arg" i))

    ;; Primitive type keyword - use positional name
    (and (keyword? arg) (contains? primitive-types arg))
    (symbol (str "arg" i))

    ;; Named type keyword (custom schemas like StoreConfig, Vector, etc.)
    (keyword? arg)
    (symbol (name arg))

    ;; Schema with name like [:sequential Vector]
    (and (sequential? arg) (keyword? (first arg)))
    (let [first-kw (first arg)]
      (if (contains? primitive-types first-kw)
        (symbol (str "arg" i))
        (symbol (name first-kw))))

    :else
    (symbol (str "arg" i))))

(defn- schema->arglist
  "Convert a malli function schema to an arglist for defn metadata.
   Handles :=> schemas with :cat/:alt for arguments."
  [schema]
  (when schema
    (let [schema-form (if (m/schema? schema) (m/form schema) schema)]
      (cond
        ;; Function schema [:=> [:cat ...] output]
        (and (sequential? schema-form)
             (= :=> (first schema-form)))
        (let [[_ input-schema _output-schema] schema-form
              input-form (if (m/schema? input-schema)
                           (m/form input-schema)
                           input-schema)]
          (cond
            ;; [:cat Type1 Type2 ...]
            (and (sequential? input-form)
                 (= :cat (first input-form)))
            (let [args (rest input-form)]
              (list (vec (map-indexed arg-name-from-schema args))))

            ;; [:alt [:cat ...] [:cat ...]] - multiple arities
            (and (sequential? input-form)
                 (= :alt (first input-form)))
            (for [alt-form (rest input-form)]
              (let [cat-form (if (and (sequential? alt-form)
                                      (= :cat (first alt-form)))
                               (rest alt-form)
                               [alt-form])]
                (vec (map-indexed arg-name-from-schema cat-form))))

            :else
            '([& args])))

        :else
        '([& args])))))

(defn- extract-arglists
  "Extract arglists from a spec entry.
   Uses :args field (malli function schema)."
  [entry]
  (let [arglists (schema->arglist (:args entry))]
    (if (and (sequential? arglists)
             (sequential? (first arglists))
             (not (vector? (first arglists))))
      ;; Multiple arities
      (list (vec arglists))
      ;; Single arity
      arglists)))

;; =============================================================================
;; API Emission
;; =============================================================================

(defn operation-names
  "Get all operation names from the specification."
  []
  (keys spec/api-specification))

(defn operation-entry
  "Get the spec entry for an operation."
  [op-key]
  (get spec/api-specification op-key))

(defmacro def-api-fn
  "Define a single API function from the specification.

   Usage:
     (def-api-fn :insert)

   This will create a def that references the implementation
   with proper docstring and arglists metadata."
  [op-key]
  (let [entry (operation-entry op-key)
        fn-name (symbol (name op-key))
        impl-sym (:impl entry)
        doc-str (:doc entry)
        arglists (extract-arglists entry)]
    (if entry
      `(do
         (def ~fn-name ~doc-str ~impl-sym)
         (alter-meta! (var ~fn-name) assoc :arglists (quote ~arglists)))
      (throw (ex-info (str "Unknown operation: " op-key)
                      {:op-key op-key})))))

(defmacro emit-api
  "Emit all API functions from the specification.

   This macro generates def statements for each operation in the spec,
   with proper docstrings and arglists.

   Usage in proximum.core:
     (emit-api proximum.specification/api-specification)"
  [spec-sym]
  (let [spec-val (if (symbol? spec-sym)
                   @(resolve spec-sym)
                   spec-sym)
        defs (for [[op-key entry] spec-val
                   :let [fn-name (symbol (name op-key))
                         impl-sym (:impl entry)
                         doc-str (:doc entry)
                         arglists (extract-arglists entry)]]
               ;; Generate: (def name "doc" impl) then alter-meta! for arglists
               ;; This avoids issues with metadata evaluation
               `(do
                  (def ~fn-name ~doc-str ~impl-sym)
                  (alter-meta! (var ~fn-name) assoc :arglists (quote ~arglists))))]
    `(do ~@defs)))

;; =============================================================================
;; Validation
;; =============================================================================

(defn validate-implementations
  "Validate that all implementation symbols in the spec resolve to actual functions.
   Returns a map of {:valid [...] :invalid [...]}."
  []
  ;; First, require all the namespaces
  (doseq [[_ entry] spec/api-specification
          :let [impl-sym (:impl entry)]
          :when impl-sym]
    (try
      (require (symbol (namespace impl-sym)))
      (catch Exception _)))

  (let [results (for [[op-key entry] spec/api-specification
                      :let [impl-sym (:impl entry)
                            resolved (when impl-sym (resolve impl-sym))]]
                  {:op op-key
                   :impl impl-sym
                   :resolved? (some? resolved)
                   :var resolved})]
    {:valid (vec (filter :resolved? results))
     :invalid (vec (remove :resolved? results))}))

(defn missing-implementations
  "Return list of operations with missing implementations."
  []
  (:invalid (validate-implementations)))

(defn print-api-summary
  "Print a summary of the API specification."
  []
  (println "Proximum API Specification Summary")
  (println "==============================")
  (println)
  (println "Total operations:" (count spec/api-specification))
  (println "Pure (referentially transparent):" (count (spec/pure-operations)))
  (println "I/O (side effects):" (count (spec/io-operations)))
  (println)
  (println "Remote-accessible:" (count (spec/remote-operations)))
  (println "Local-only:" (count (spec/local-only-operations)))
  (println)
  (let [validation (validate-implementations)
        invalid (:invalid validation)]
    (if (empty? invalid)
      (println "All implementations resolve correctly.")
      (do
        (println "WARNING: Missing implementations:")
        (doseq [{:keys [op impl]} invalid]
          (println "  -" op "->" impl))))))

;; =============================================================================
;; Development Helpers
;; =============================================================================

(defn spec->defn
  "Generate a defn form for an operation (for inspection/debugging).
   Does not evaluate, just returns the form."
  [op-key]
  (let [entry (operation-entry op-key)
        fn-name (symbol (name op-key))
        impl-sym (:impl entry)
        doc-str (:doc entry)
        arglists (extract-arglists entry)]
    `(def ~(with-meta fn-name
             {:doc doc-str
              :arglists (quote ~arglists)})
       ~impl-sym)))

(defn generate-core-ns
  "Generate the full proximum.core namespace source code from the specification.
   Returns a string that can be written to a file."
  []
  (let [header "(ns proximum.core
  \"Persistent Vector Database - Main API entry point.

   This namespace is generated from proximum.specification.
   See proximum.specification for the authoritative API definition.\"
  (:require
   [proximum.protocols :as p]
   [proximum.hnsw :as hnsw]
   [proximum.writing :as writing]
   [proximum.versioning :as versioning]
   [proximum.compaction :as compact]
   [proximum.api-impl :as api-impl]
   [proximum.crypto :as crypto]
   [proximum.gc :as gc]
   [proximum.metrics :as metrics])
  (:import [proximum.internal ArrayBitSet]))

;; =============================================================================
;; Generated API (from proximum.specification)
;; =============================================================================
"
        defs (for [[op-key entry] (sort-by first spec/api-specification)
                   :let [fn-name (name op-key)
                         impl-sym (:impl entry)
                         doc-str (or (:doc entry) "")
                         ;; Escape quotes in docstring
                         escaped-doc (clojure.string/replace doc-str "\"" "\\\"")]]
               (str "(def " fn-name "\n"
                    "  \"" escaped-doc "\"\n"
                    "  " impl-sym ")\n"))]
    (str header
         (clojure.string/join "\n" defs))))
