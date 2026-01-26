(ns proximum.codegen.java-source
  "Java source code generation from specification.

   This namespace generates complete Java source code from the API specification.
   The generated code provides a clean 1-to-1 mapping with the Clojure API.

   Build pipeline:
   1. Run (generate-java-source) at build time
   2. Output written to src-java-generated/
   3. Java compiler compiles generated + hand-written sources
   4. JAR includes both

   Usage:
     (generate-java-source \"src-java-generated\")
     ;; Writes ProximumVectorStore.java to the output directory"
  (:require [proximum.specification :as spec]
            [proximum.codegen.java :as java]
            [clojure.string :as str]
            [clojure.java.io :as io]))

;; =============================================================================
;; Type Mapping: Malli -> Java
;; =============================================================================

(def ^:private malli->java-type
  "Map malli schema types to Java types."
  {;; Semantic types (defined in spec)
   :VectorIndex "ProximumVectorStore"
   :Vector "float[]"
   :Metadata "Map<String, Object>"
   :SearchResult "SearchResult"
   :SearchOptions "SearchOptions"
   :StoreConfig "Map<String, Object>"
   :IndexConfig "Map<String, Object>"
   :ConnectOptions "Map<String, Object>"
   :CompactTarget "Map<String, Object>"
   ;; ID types
   :InternalId "int"
   :ExternalId "Object"
   :CommitId "UUID"
   :BranchName "String"
   ;; Primitive types
   :boolean "boolean"
   :int "int"
   :pos-int? "int"
   :nat-int? "int"
   :double "double"
   :string "String"
   :keyword "String"
   :uuid "UUID"
   :any "Object"
   :map "Map<String, Object>"
   :nil "void"
   ;; Sequences
   :sequential "List"
   :vector "List"
   :set "Set"})

(defn- box-type
  "Box primitive types for generics."
  [java-type]
  (case java-type
    "int" "Integer"
    "long" "Long"
    "boolean" "Boolean"
    "double" "Double"
    "float" "Float"
    "float[]" "float[]"
    java-type))

(defn- schema-to-java-type
  "Convert a malli schema element to Java type string."
  [schema]
  (cond
    ;; Direct keyword lookup
    (keyword? schema)
    (get malli->java-type schema "Object")

    ;; Symbol lookup (spec uses symbols like VectorIndex, InternalId, etc.)
    (symbol? schema)
    (get malli->java-type (keyword schema) "Object")

    ;; Async/Channel wrapper - maps to CompletableFuture
    (and (sequential? schema) (= :async (first schema)))
    (let [inner-type (schema-to-java-type (second schema))]
      (str "CompletableFuture<" (box-type inner-type) ">"))

    ;; Vector/sequential with element type
    (and (sequential? schema)
         (#{:sequential :vector} (first schema)))
    (let [elem-type (schema-to-java-type (second schema))]
      (str "List<" (box-type elem-type) ">"))

    ;; Set with element type
    (and (sequential? schema) (= :set (first schema)))
    (let [elem-type (schema-to-java-type (second schema))]
      (str "Set<" (box-type elem-type) ">"))

    ;; Maybe (nullable) - same type, just nullable in Java
    (and (sequential? schema) (= :maybe (first schema)))
    (let [inner (schema-to-java-type (second schema))]
      (box-type inner))  ;; Box primitives for nullable

    ;; Optional [:? Type] in function args
    (and (sequential? schema) (= :? (first schema)))
    (schema-to-java-type (second schema))

    ;; Enum
    (and (sequential? schema) (= :enum (first schema)))
    "String"

    ;; Map with specific structure
    (and (sequential? schema) (= :map (first schema)))
    "Map<String, Object>"

    ;; Map-of
    (and (sequential? schema) (= :map-of (first schema)))
    "Map<String, Object>"

    ;; Multi dispatch
    (and (sequential? schema) (= :multi (first schema)))
    "Map<String, Object>"

    ;; Function schema - extract return type
    (and (sequential? schema) (= :=> (first schema)))
    (schema-to-java-type (nth schema 2))

    :else "Object"))

(defn- vector-index?
  "Check if a schema element represents VectorIndex (keyword or symbol)."
  [x]
  (or (= x :VectorIndex)
      (= x 'VectorIndex)))

(defn- async-schema?
  "Check if a schema is wrapped in [:async ...]."
  [schema]
  (and (sequential? schema)
       (= :async (first schema))))

(defn- unwrap-async
  "Unwrap [:async T] to get T. Returns schema unchanged if not async."
  [schema]
  (if (async-schema? schema)
    (second schema)
    schema))

(defn- schema-element-name
  "Get the name/type of a schema element for naming purposes."
  [param]
  (cond
    (keyword? param) param
    (symbol? param) (keyword param)
    (and (sequential? param) (= :? (first param)))
    (schema-element-name (second param))
    (and (sequential? param) (keyword? (first param))) (first param)
    (and (sequential? param) (symbol? (first param))) (keyword (first param))
    :else :any))

(defn- extract-java-params
  "Extract Java parameter list from malli :args schema.
   Returns vector of {:name \"paramName\" :type \"JavaType\"}.
   Ensures unique parameter names when multiple params have the same type."
  [args-schema]
  (when args-schema
    (let [schema-form (if (sequential? args-schema) args-schema [args-schema])]
      (when (and (= :=> (first schema-form))
                 (sequential? (second schema-form))
                 (= :cat (first (second schema-form))))
        (let [params (rest (second schema-form))
              ;; Skip first param if it's VectorIndex (instance method)
              instance-method? (vector-index? (first params))
              effective-params (if instance-method? (rest params) params)
              ;; First pass: get base names
              base-names (map-indexed
                          (fn [i param]
                            (let [type-kw (schema-element-name param)]
                              (cond
                                (= type-kw :Vector) "vector"
                                (= type-kw :Metadata) "metadata"
                                (= type-kw :SearchOptions) "options"
                                (= type-kw :StoreConfig) "storeConfig"
                                (= type-kw :IndexConfig) "config"
                                (= type-kw :ConnectOptions) "options"
                                (= type-kw :CompactTarget) "target"
                                (= type-kw :BranchName) "branchName"
                                (= type-kw :CommitId) "commitId"
                                (= type-kw :InternalId) "internalId"
                                (= type-kw :ExternalId) "id"
                                (= type-kw :int) "k"
                                (= type-kw :pos-int?) "k"
                                (= type-kw :map) "opts"
                                :else (str "arg" i))))
                          effective-params)
              ;; Count occurrences for uniquification
              name-counts (reduce (fn [m n] (update m n (fnil inc 0))) {} base-names)
              ;; Track seen names for suffix assignment
              seen-counts (atom {})]
          (map-indexed
           (fn [i param]
             (let [type-kw (schema-element-name param)
                   java-type (schema-to-java-type param)
                   base-name (nth base-names i)
                   ;; Add numeric suffix if name appears multiple times
                   param-name (if (> (get name-counts base-name) 1)
                                (let [n (get (swap! seen-counts update base-name (fnil inc 0)) base-name)]
                                  (str base-name n))
                                base-name)
                   optional? (and (sequential? param) (= :? (first param)))]
               {:name param-name
                :type java-type
                :optional? optional?}))
           effective-params))))))

(defn- returns-index?
  "Check if operation returns a VectorIndex."
  [entry]
  (let [ret (:ret entry)]
    (or (= ret :VectorIndex)
        (= ret 'VectorIndex))))

;; =============================================================================
;; Java Method Generation
;; =============================================================================

(defn- generate-method-signature
  "Generate Java method signature for an operation."
  [op-key entry]
  (let [method-name (java/java-method-name op-key)
        static? (java/static-operation? op-key)
        params (extract-java-params (:args entry))
        ret-schema (:ret entry)
        async? (async-schema? ret-schema)
        unwrapped-ret (unwrap-async ret-schema)
        ;; Special case: close! is async in Clojure but blocks in Java (AutoCloseable)
        close-special? (= op-key 'close!)
        ;; Use unwrapped type for returns-index? check
        base-return-type (if (returns-index? {:ret unwrapped-ret})
                           "ProximumVectorStore"
                           (schema-to-java-type unwrapped-ret))
        ;; Full return type is wrapped in CompletableFuture if async (except close!)
        return-type (if (and async? (not close-special?))
                      (str "CompletableFuture<" base-return-type ">")
                      base-return-type)
        param-str (str/join ", "
                            (map #(str (:type %) " " (:name %))
                                 (remove :optional? params)))]
    {:method-name method-name
     :static? static?
     :return-type return-type
     :base-return-type base-return-type  ; Track unwrapped type
     :async? (and async? (not close-special?))  ; close! is not treated as async in Java
     :close-special? close-special?  ; Mark close! for special handling
     :params params
     :param-str param-str
     :doc (:doc entry)
     :op-key op-key}))

(defn- format-javadoc
  "Format a docstring as Javadoc."
  [doc-str]
  (when doc-str
    (let [lines (str/split-lines doc-str)
          formatted (map #(str "     * " %) lines)]
      (str "    /**\n"
           (str/join "\n" formatted)
           "\n     */"))))

(defn- op-key->java-var
  "Convert operation key to valid Java variable name (camelCase)."
  [op-key]
  (let [s (name op-key)
        ;; Remove trailing ! and ?
        clean (str/replace s #"[!?]$" "")
        parts (str/split clean #"-")]
    (apply str (first parts)
           (map str/capitalize (rest parts)))))

(defn- param-conversion
  "Generate conversion code for a parameter if needed."
  [param-name param-type]
  (cond
    ;; Convert Java Set to Clojure set
    (str/starts-with? param-type "Set")
    (str "toClojureSet((java.util.Set<?>) " param-name ")")
    ;; Convert Java Map to Clojure map with keyword keys (for config/target/metadata params)
    (and (str/starts-with? param-type "Map")
         (or (= param-name "target")
             (= param-name "config")
             (= param-name "options")
             (= param-name "opts")
             (= param-name "storeConfig")
             (= param-name "metadata")))
    (str "toClojureMap(" param-name ")")
    ;; For Object params that might be Sets (like filter params), use runtime check
    (and (= param-type "Object")
         (or (str/includes? param-name "arg")  ;; generic arg names
             (= param-name "filter")
             (= param-name "allowedIds")))
    (str "convertFilterArg(" param-name ")")
    ;; No conversion needed
    :else param-name))

(defn- generate-result-converter
  "Generate lambda expression to convert channel result to Java type."
  [base-return-type static?]
  (cond
    (= base-return-type "ProximumVectorStore")
    (if static?
      "result -> new ProximumVectorStore(result, null)"
      "result -> { this.clojureIndex = result; return this; }")

    (= base-return-type "int")
    "result -> ((Number) result).intValue()"

    (= base-return-type "long")
    "result -> ((Number) result).longValue()"

    (= base-return-type "double")
    "result -> ((Number) result).doubleValue()"

    (= base-return-type "List<SearchResult>")
    "result -> toSearchResults((Iterable<Object>) result)"

    (= base-return-type "String")
    "result -> (result instanceof clojure.lang.Keyword) ? ((clojure.lang.Keyword) result).getName() : (String) result"

    (= base-return-type "Map<String, Object>")
    "result -> result == null ? null : convertClojureMap((Map<Object, Object>) result)"

    (= base-return-type "List<Map<String, Object>>")
    "result -> convertClojureSeqToMapList(result)"

    (= base-return-type "Set<String>")
    "result -> convertKeywordSetToStrings((Set<Object>) result)"

    (str/starts-with? base-return-type "Set<")
    (str "result -> (Set<Object>) result")

    :else
    (str "result -> (" base-return-type ") result")))

(defn- generate-async-method-body
  "Generate method body for async operations that return CompletableFuture."
  [{:keys [base-return-type static? op-key params include-optional?]}]
  (let [java-var-name (op-key->java-var op-key)
        required-params (if include-optional?
                          params
                          (remove :optional? params))
        param-exprs (map (fn [{:keys [name type]}]
                           (param-conversion name type))
                         required-params)
        invoke-args (if static?
                      param-exprs
                      (cons "clojureIndex" param-exprs))
        invoke-str (str/join ", " invoke-args)
        converter (generate-result-converter base-return-type static?)]
    (str "        Object channel = " java-var-name "Fn.invoke(" invoke-str ");\n"
         "        return channelToCompletableFuture(channel, " converter ");\n")))

(defn- generate-sync-method-body
  "Generate method body for synchronous operations."
  [{:keys [return-type static? op-key params include-optional?]}]
  (let [java-var-name (op-key->java-var op-key)
        required-params (if include-optional?
                          params
                          (remove :optional? params))
        param-exprs (map (fn [{:keys [name type]}]
                           (param-conversion name type))
                         required-params)
        invoke-args (if static?
                      param-exprs
                      (cons "clojureIndex" param-exprs))
        invoke-str (str/join ", " invoke-args)
        returns-void? (= return-type "void")
        returns-store? (= return-type "ProximumVectorStore")
        returns-search-results? (= return-type "List<SearchResult>")
        returns-map? (= return-type "Map<String, Object>")
        returns-list-of-maps? (= return-type "List<Map<String, Object>>")
        returns-string? (= return-type "String")
        returns-set-of-strings? (= return-type "Set<String>")]
    (cond
      returns-void?
      (str "        " java-var-name "Fn.invoke(" invoke-str ");\n")

      returns-store?
      (if static?
        (str "        Object result = " java-var-name "Fn.invoke(" invoke-str ");\n"
             "        return new ProximumVectorStore(result, null);\n")
        ;; For instance methods that return store, update internal state and return this
        ;; This makes methods like sync() work as mutable convenience methods
        (str "        Object newIdx = " java-var-name "Fn.invoke(" invoke-str ");\n"
             "        this.clojureIndex = newIdx;\n"
             "        return this;\n"))

      ;; Primitive numeric types need special handling - Clojure returns boxed types
      (= return-type "int")
      (str "        return ((Number) " java-var-name "Fn.invoke(" invoke-str ")).intValue();\n")

      (= return-type "long")
      (str "        return ((Number) " java-var-name "Fn.invoke(" invoke-str ")).longValue();\n")

      (= return-type "double")
      (str "        return ((Number) " java-var-name "Fn.invoke(" invoke-str ")).doubleValue();\n")

      ;; Search results need conversion from Clojure maps to SearchResult objects
      returns-search-results?
      (str "        @SuppressWarnings(\"unchecked\")\n"
           "        Iterable<Object> results = (Iterable<Object>) " java-var-name "Fn.invoke(" invoke-str ");\n"
           "        return toSearchResults(results);\n")

      ;; String return type - may get a Clojure keyword, convert to string
      returns-string?
      (str "        Object result = " java-var-name "Fn.invoke(" invoke-str ");\n"
           "        if (result instanceof clojure.lang.Keyword) {\n"
           "            return ((clojure.lang.Keyword) result).getName();\n"
           "        }\n"
           "        return (String) result;\n")

      ;; Map return type - convert Clojure keyword keys to string keys
      returns-map?
      (str "        @SuppressWarnings(\"unchecked\")\n"
           "        Map<Object, Object> result = (Map<Object, Object>) " java-var-name "Fn.invoke(" invoke-str ");\n"
           "        return result == null ? null : convertClojureMap(result);\n")

      ;; List of maps - convert Clojure seq with keyword keys to List of string-keyed maps
      returns-list-of-maps?
      (str "        Object result = " java-var-name "Fn.invoke(" invoke-str ");\n"
           "        return convertClojureSeqToMapList(result);\n")

      ;; Set of strings - may contain keywords, convert to strings
      returns-set-of-strings?
      (str "        @SuppressWarnings(\"unchecked\")\n"
           "        Set<Object> result = (Set<Object>) " java-var-name "Fn.invoke(" invoke-str ");\n"
           "        return convertKeywordSetToStrings(result);\n")

      :else
      (str "        return (" return-type ") " java-var-name "Fn.invoke(" invoke-str ");\n"))))

(defn- generate-close-method-body
  "Generate method body for close! that blocks on async cleanup."
  [{:keys [op-key]}]
  (let [java-var-name (op-key->java-var op-key)]
    (str "        try {\n"
         "            Object channel = " java-var-name "Fn.invoke(clojureIndex);\n"
         "            // Block until cleanup completes\n"
         "            IFn takeFn = Clojure.var(\"clojure.core.async\", \"<!!\");\n"
         "            takeFn.invoke(channel);\n"
         "        } catch (Exception e) {\n"
         "            throw new RuntimeException(\"Close failed\", e);\n"
         "        }\n")))

(defn- generate-method-body
  "Generate method body that delegates to Clojure function."
  [{:keys [async? close-special?] :as method-info}]
  (cond
    close-special? (generate-close-method-body method-info)
    async? (generate-async-method-body method-info)
    :else (generate-sync-method-body method-info)))

(defn- generate-instance-method
  "Generate a complete instance method."
  [{:keys [method-name return-type param-str doc] :as method-info}]
  (let [;; Methods that return ProximumVectorStore mutate state, so need synchronization
        needs-sync? (= return-type "ProximumVectorStore")
        modifier (if needs-sync? "public synchronized " "public ")]
    (str (when doc (str (format-javadoc doc) "\n"))
         "    " modifier return-type " " method-name "(" param-str ") {\n"
         "        ensureInitialized();\n"
         (generate-method-body method-info)
         "    }\n")))

(defn- generate-static-method
  "Generate a complete static method."
  [{:keys [method-name return-type param-str doc] :as method-info}]
  (str (when doc (str (format-javadoc doc) "\n"))
       "    public static " return-type " " method-name "(" param-str ") {\n"
       "        ensureInitialized();\n"
       (generate-method-body method-info)
       "    }\n"))

;; =============================================================================
;; Builder Generation from Config Schema
;; =============================================================================

(defn- schema-type-name
  "Get the name of a schema type, handling functions, symbols, keywords."
  [schema]
  (cond
    (keyword? schema) (name schema)
    (symbol? schema) (name schema)
    (fn? schema) (-> schema class .getName
                     (str/replace #".*\$" "")  ;; remove package prefix
                     (str/replace #"_QMARK_" "?")
                     (str/replace #"_" "-"))
    :else nil))

(defn- config-field->java-type
  "Map config field schema to Java type."
  [schema]
  (let [type-name (schema-type-name schema)]
    (cond
      (= type-name "pos-int?") "int"
      (= type-name "nat-int?") "Integer"  ;; nullable for optional
      (= type-name "boolean") "boolean"
      (= type-name "string") "String"
      (= type-name "keyword") "String"
      (= type-name "map") "Map<String, Object>"
      (= type-name "uuid") "UUID"
      (= type-name "DistanceMetric") "DistanceMetric"
      (and (vector? schema) (= :maybe (first schema)))
      (let [inner (config-field->java-type (second schema))]
        ;; Make primitive types nullable
        (case inner
          "int" "Integer"
          "boolean" "Boolean"
          inner))
      (and (vector? schema) (= := (first schema))) nil ;; fixed value, skip
      (and (vector? schema) (= :enum (first schema))) "DistanceMetric"
      :else "Object")))

(defn- config-field->java-name
  "Convert config field keyword to Java field name."
  [field-kw]
  (let [s (name field-kw)
        ;; Remove trailing ?
        clean (str/replace s #"\?$" "")
        parts (str/split clean #"-")]
    (apply str (first parts)
           (map str/capitalize (rest parts)))))

(defn- default->java-literal
  "Convert Clojure default value to Java literal."
  [value java-type]
  (cond
    (nil? value) nil
    (= java-type "boolean") (str value)
    (= java-type "int") (str value)
    (= java-type "long") (str value "L")
    (= java-type "String") (if (keyword? value)
                             (str "\"" (name value) "\"")  ;; :main -> "main"
                             (str "\"" value "\""))
    (and (keyword? value) (= java-type "DistanceMetric"))
    (str "DistanceMetric." (str/upper-case (name value)))
    :else (str value)))

(defn- extract-config-fields
  "Extract field info from HnswConfig schema.
   Returns seq of {:name :java-name :java-type :optional? :default :doc}."
  []
  (let [schema-form spec/HnswConfig
        ;; Schema is [:map {...} & field-entries]
        fields (drop 2 schema-form)] ;; skip :map and props
    (for [field fields
          :let [[field-kw props-or-schema schema] (if (map? (second field))
                                                    field
                                                    [field nil (second field)])
                props (if (map? props-or-schema) props-or-schema {})
                actual-schema (or schema props-or-schema)
                java-type (config-field->java-type actual-schema)]
          :when java-type] ;; skip fixed values like :type
      {:name field-kw
       :java-name (config-field->java-name field-kw)
       :java-type java-type
       :optional? (:optional props)
       :default (:default props)
       :doc (:doc props)})))

(defn- generate-builder-fields
  "Generate builder field declarations."
  []
  (str/join "\n"
            (for [{:keys [java-name java-type default doc]} (extract-config-fields)
                  :let [default-str (default->java-literal default java-type)
                        init (if default-str (str " = " default-str) "")]]
              (str "        private " java-type " " java-name init ";"))))

(defn- format-builder-javadoc
  "Format a field doc as single-line Javadoc."
  [doc-str]
  (when doc-str
    (str "        /** " doc-str " */")))

(defn- generate-builder-setters
  "Generate builder fluent setter methods."
  []
  (str/join "\n\n"
            (for [{:keys [java-name java-type doc]} (extract-config-fields)]
              (str (when doc (str (format-builder-javadoc doc) "\n"))
                   "        public Builder " java-name "(" java-type " " java-name ") {\n"
                   "            this." java-name " = " java-name ";\n"
                   "            return this;\n"
                   "        }"))))

(defn- generate-builder-config-map
  "Generate the config map construction in build()."
  []
  (let [fields (extract-config-fields)
        entries (for [{:keys [name java-name java-type]} fields
                      :let [kw-str (str "Keyword.intern(\"" (clojure.core/name name) "\")")
                            value-expr (cond
                                         (= java-type "DistanceMetric")
                                         (str "Keyword.intern(" java-name ".name().toLowerCase())")

                                         (= java-type "String")
                                         (if (= name :branch)
                                           (str "Keyword.intern(" java-name ")")
                                           java-name)

                                         :else java-name)]]
                  (str "                " kw-str ", " value-expr))]
    (str/join ",\n" entries)))

(defn- generate-builder-class
  "Generate the inner Builder class."
  []
  (str "
    // ==========================================================================
    // Builder (generated from HnswConfig schema)
    // ==========================================================================

    /**
     * Create a new builder for constructing a ProximumVectorStore.
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for creating ProximumVectorStore instances.
     */
    public static class Builder {
" (generate-builder-fields) "

        private Builder() {}

" (generate-builder-setters) "

        /** Alias for dim() - set vector dimensions. */
        public Builder dimensions(int dimensions) {
            return dim(dimensions);
        }

        /** Alias for M() - set max neighbors per node. */
        public Builder m(int m) {
            return M(m);
        }

        /**
         * Convenience method to set storage path with default file backend.
         * @param path directory path for storage
         * @return this builder
         */
        public Builder storagePath(String path) {
            this.storeConfig = Map.of(
                \"backend\", \":file\",
                \"path\", path,
                \"id\", UUID.randomUUID()
            );
            if (this.mmapDir == null) {
                this.mmapDir = path;
            }
            return this;
        }

        /**
         * Build the ProximumVectorStore.
         * @return the configured store
         * @throws IllegalArgumentException if required fields are missing
         */
        public ProximumVectorStore build() {
            if (dim <= 0) {
                throw new IllegalArgumentException(\"dimensions (dim) must be set and positive\");
            }
            ensureInitialized();

            // Build config map
            Map<String, Object> configMap = new HashMap<>();
            configMap.put(\":type\", \":hnsw\");
            configMap.put(\":dim\", dim);
            configMap.put(\":M\", M);
            configMap.put(\":ef-construction\", efConstruction);
            configMap.put(\":ef-search\", efSearch);
            configMap.put(\":capacity\", capacity);
            configMap.put(\":distance\", \":\" + distance.name().toLowerCase());
            configMap.put(\":branch\", \":\" + branch);
            configMap.put(\":crypto-hash?\", cryptoHash);
            configMap.put(\":chunk-size\", chunkSize);
            configMap.put(\":cache-size\", cacheSize);
            if (storeConfig != null) {
                configMap.put(\":store-config\", storeConfig);
            }
            if (mmapDir != null) {
                configMap.put(\":mmap-dir\", mmapDir);
            }
            if (mmapPath != null) {
                configMap.put(\":mmap-path\", mmapPath);
            }
            if (maxLevels != null) {
                configMap.put(\":max-levels\", maxLevels);
            }

            Object result = createIndexFn.invoke(toClojureMap(configMap));
            return new ProximumVectorStore(result, null);
        }
    }

    // Helper to convert Java map to Clojure map
    private static Object toClojureMap(Map<String, Object> map) {
        IFn hashMap = Clojure.var(\"clojure.core\", \"hash-map\");
        IFn keyword = Clojure.var(\"clojure.core\", \"keyword\");
        Object[] args = new Object[map.size() * 2];
        int i = 0;
        for (Map.Entry<String, Object> e : map.entrySet()) {
            String k = e.getKey();
            Object v = e.getValue();
            // Convert :keyword strings to actual keywords
            if (k.startsWith(\":\")) {
                args[i++] = keyword.invoke(k.substring(1));
            } else {
                args[i++] = keyword.invoke(k);
            }
            // Convert :value strings to keywords
            if (v instanceof String && ((String) v).startsWith(\":\")) {
                args[i++] = keyword.invoke(((String) v).substring(1));
            } else if (v instanceof Map) {
                args[i++] = toClojureMap((Map<String, Object>) v);
            } else {
                args[i++] = v;
            }
        }
        return hashMap.applyTo(clojure.lang.ArraySeq.create(args));
    }

    // Helper to convert Clojure search results to Java SearchResult list
    @SuppressWarnings(\"unchecked\")
    private static List<SearchResult> toSearchResults(Iterable<Object> results) {
        IFn get = Clojure.var(\"clojure.core\", \"get\");
        IFn keyword = Clojure.var(\"clojure.core\", \"keyword\");
        Object idKw = keyword.invoke(\"id\");
        Object distKw = keyword.invoke(\"distance\");
        Object metaKw = keyword.invoke(\"metadata\");

        List<SearchResult> list = new ArrayList<>();
        for (Object r : results) {
            Object id = get.invoke(r, idKw);
            Number dist = (Number) get.invoke(r, distKw);
            Object meta = get.invoke(r, metaKw);
            if (meta != null) {
                // Convert Clojure map with keyword keys to Java map with string keys
                Map<String, Object> metaMap = convertClojureMap((Map<Object, Object>) meta);
                list.add(new SearchResult(id, dist.doubleValue(), metaMap));
            } else {
                list.add(new SearchResult(id, dist.doubleValue()));
            }
        }
        return list;
    }
"))

;; =============================================================================
;; Full Class Generation
;; =============================================================================

(defn- generate-fn-declarations
  "Generate static IFn declarations for all operations."
  []
  (let [ops (java/java-operations)]
    (str/join "\n"
              (for [[op-key _] ops]
                (str "    private static IFn " (op-key->java-var op-key) "Fn;")))))

(defn- generate-fn-initializations
  "Generate function initializations in ensureInitialized()."
  []
  (let [ops (java/java-operations)]
    (str/join "\n"
              (for [[op-key _] ops]
                (str "            " (op-key->java-var op-key) "Fn = Clojure.var(\"proximum.core\", \""
                     (name op-key) "\");")))))

(defn- generate-optional-overload
  "Generate an overloaded method that includes optional params."
  [{:keys [method-name return-type params doc] :as method-info}]
  (let [all-param-str (str/join ", "
                                (map #(str (:type %) " " (:name %)) params))
        needs-sync? (= return-type "ProximumVectorStore")
        modifier (if needs-sync? "public synchronized " "public ")
        overload-info (assoc method-info
                             :param-str all-param-str
                             :include-optional? true)]
    (str (when doc (str (format-javadoc doc) "\n"))
         "    " modifier return-type " " method-name "(" all-param-str ") {\n"
         "        ensureInitialized();\n"
         (generate-method-body overload-info)
         "    }\n")))

(defn- generate-all-methods
  "Generate all API methods from specification."
  []
  (let [ops (java/java-operations)
        method-infos (map (fn [[k v]] (generate-method-signature k v)) ops)
        static-methods (filter :static? method-infos)
        instance-methods (remove :static? method-infos)
        ;; Generate overloaded methods for operations with optional params
        overloaded-methods (filter (fn [{:keys [params]}]
                                     (some :optional? params))
                                   instance-methods)]
    (str "    // ==========================================================================\n"
         "    // Static Methods (from specification)\n"
         "    // ==========================================================================\n\n"
         (str/join "\n" (map generate-static-method static-methods))
         "\n"
         "    // ==========================================================================\n"
         "    // Instance Methods (from specification)\n"
         "    // ==========================================================================\n\n"
         (str/join "\n" (map generate-instance-method instance-methods))
         (when (seq overloaded-methods)
           (str "\n"
                "    // ==========================================================================\n"
                "    // Overloaded Methods (with optional parameters)\n"
                "    // ==========================================================================\n\n"
                (str/join "\n" (map generate-optional-overload overloaded-methods)))))))

(defn generate-java-class
  "Generate the complete ProximumVectorStore.java source code."
  []
  (str
   "package org.replikativ.proximum;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.IPersistentMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * ProximumVectorStore - Persistent Vector Store with versioning support.
 *
 * <p>This class is generated from proximum.specification and provides a 1-to-1
 * mapping with the Clojure API. All operations follow persistent/immutable
 * semantics - mutating operations return new store instances.</p>
 *
 * <h2>Basic Usage:</h2>
 * <pre>{@code
 * // Create a store
 * ProximumVectorStore store = ProximumVectorStore.createIndex(config);
 *
 * // Add vectors (returns new store)
 * store = store.add(embedding);
 * store = store.add(embedding, metadata);
 *
 * // Search
 * List<SearchResult> results = store.search(queryVector, 10);
 *
 * // Persist to storage
 * store.sync();
 * }</pre>
 *
 * <h2>Thread Safety:</h2>
 * <p>All operations are thread-safe. Mutating operations return new immutable
 * store instances, so the original store can still be used concurrently.</p>
 *
 * @see SearchResult
 * @see IndexConfig
 */
public class ProximumVectorStore implements AutoCloseable {

    // Clojure runtime initialization
    private static final Object LOCK = new Object();
    private static volatile boolean initialized = false;

    // Function references (loaded lazily)
" (generate-fn-declarations) "

    // Instance state
    private volatile Object clojureIndex;
    private final IndexConfig config;

    /**
     * Private constructor - use static factory methods.
     */
    private ProximumVectorStore(Object clojureIndex, IndexConfig config) {
        this.clojureIndex = clojureIndex;
        this.config = config;
    }

    /**
     * Initialize Clojure runtime and load required namespaces.
     */
    private static void ensureInitialized() {
        if (!initialized) {
            synchronized (LOCK) {
                if (!initialized) {
                    IFn require = Clojure.var(\"clojure.core\", \"require\");
                    require.invoke(Clojure.read(\"proximum.core\"));

                    // Load all function references
" (generate-fn-initializations) "

                    initialized = true;
                }
            }
        }
    }

    /**
     * Get the underlying Clojure index object.
     * For advanced interop only.
     */
    public Object getClojureIndex() {
        return clojureIndex;
    }

" (generate-builder-class) "

" (generate-all-methods) "

    // ==========================================================================
    // External ID API (public interface - hides internal IDs)
    // ==========================================================================

    private static IFn keywordFn;

    private static Object kw(String name) {
        if (keywordFn == null) {
            keywordFn = Clojure.var(\"clojure.core\", \"keyword\");
        }
        return keywordFn.invoke(name);
    }

    /**
     * Add a vector with auto-generated UUID as ID.
     * <p>Convenience method that auto-generates a UUID and stores the vector.
     * Returns the new store instance (immutable pattern).</p>
     *
     * @param vector the embedding vector
     * @return new store with the vector added
     */
    public ProximumVectorStore addWithGeneratedId(float[] vector) {
        return addWithId(vector, UUID.randomUUID());
    }

    /**
     * Add a vector with the specified external ID.
     * <p>The ID is stored in metadata and can be any serializable type
     * (Long for Datahike, String, UUID, etc.).</p>
     *
     * @param vector the embedding vector
     * @param id the external ID to associate with this vector
     * @return new store with the vector added
     */
    public ProximumVectorStore addWithId(float[] vector, Object id) {
        ensureInitialized();
        Object result = insertFn.invoke(clojureIndex, vector, id);
        return new ProximumVectorStore(result, this.config);
    }

    /**
     * Add a vector with ID and additional metadata.
     * <p>The ID is stored in metadata under :external-id key.</p>
     *
     * @param vector the embedding vector
     * @param id the external ID to associate with this vector
     * @param metadata additional metadata to store
     * @return new store with the vector added
     */
    public ProximumVectorStore addWithId(float[] vector, Object id, Map<String, Object> metadata) {
        ensureInitialized();
        Object result = insertFn.invoke(clojureIndex, vector, id, toClojureMap(metadata));
        return new ProximumVectorStore(result, this.config);
    }

    /**
     * Search and return results with external IDs.
     * <p>Translates internal IDs to external IDs in results.
     * Vectors without external IDs will have null ID in results.</p>
     *
     * @param vector the query vector
     * @param k number of results to return
     * @return list of search results with external IDs
     */
    @SuppressWarnings(\"unchecked\")
    public List<SearchResult> searchWithIds(float[] vector, int k) {
        ensureInitialized();
        Object results = searchFn.invoke(clojureIndex, vector, k);
        return translateResults((Iterable<Object>) results);
    }

    /**
     * Search with options and return results with external IDs.
     *
     * @param vector the query vector
     * @param k number of results to return
     * @param options search options (ef, min-similarity, etc.)
     * @return list of search results with external IDs
     */
    @SuppressWarnings(\"unchecked\")
    public List<SearchResult> searchWithIds(float[] vector, int k, Map<String, Object> options) {
        ensureInitialized();
        Object results = searchFn.invoke(clojureIndex, vector, k, toClojureMap(options));
        return translateResults((Iterable<Object>) results);
    }

    /**
     * Look up the internal ID for an external ID.
     * <p>Returns null if the external ID is not found.</p>
     *
     * @param id the external ID
     * @return the internal ID, or null if not found
     */
    public Integer lookupId(Object id) {
        ensureInitialized();
        Object result = lookupInternalIdFn.invoke(clojureIndex, id);
        return result == null ? null : ((Number) result).intValue();
    }

    /**
     * Get vector by external ID.
     *
     * @param id the external ID
     * @return the vector, or null if not found
     */
    public float[] getVectorById(Object id) {
        ensureInitialized();
        return (float[]) getVectorFn.invoke(clojureIndex, id);
    }

    /**
     * Get metadata by external ID.
     *
     * @param id the external ID
     * @return the metadata map, or null if not found
     */
    @SuppressWarnings(\"unchecked\")
    public Map<String, Object> getMetadataById(Object id) {
        ensureInitialized();
        return (Map<String, Object>) getMetadataFn.invoke(clojureIndex, id);
    }

    /**
     * Delete vector by external ID.
     *
     * @param id the external ID
     * @return new store with the vector deleted, or same store if ID not found
     */
    public ProximumVectorStore deleteById(Object id) {
        ensureInitialized();
        Object result = deleteFn.invoke(clojureIndex, id);
        return new ProximumVectorStore(result, this.config);
    }

    @SuppressWarnings(\"unchecked\")
    private List<SearchResult> translateResults(Iterable<Object> results) {
        List<SearchResult> translated = new ArrayList<>();
        for (Object r : results) {
            Map<Object, Object> m = (Map<Object, Object>) r;
            int internalId = ((Number) m.get(kw(\"id\"))).intValue();
            double distance = ((Number) m.get(kw(\"distance\"))).doubleValue();

            // Get external ID from metadata
            Object meta = getMetadataFn.invoke(clojureIndex, internalId);
            Object externalId = null;
            if (meta != null) {
                Map<Object, Object> metaMap = (Map<Object, Object>) meta;
                externalId = metaMap.get(kw(\"external-id\"));
            }

            // Include metadata in result if present
            Map<String, Object> metadata = null;
            Object metaObj = m.get(kw(\"metadata\"));
            if (metaObj != null) {
                metadata = convertClojureMap((Map<Object, Object>) metaObj);
            }

            translated.add(new SearchResult(externalId, distance, metadata));
        }
        return translated;
    }

    @SuppressWarnings(\"unchecked\")
    private static Map<String, Object> convertClojureMap(Map<Object, Object> clojureMap) {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<Object, Object> e : clojureMap.entrySet()) {
            String key = e.getKey().toString();
            // Remove leading colon from keyword string representation
            if (key.startsWith(\":\")) {
                key = key.substring(1);
            }
            Object value = e.getValue();
            // Recursively convert nested maps
            if (value instanceof Map) {
                value = convertClojureMap((Map<Object, Object>) value);
            }
            result.put(key, value);
        }
        return result;
    }

    @SuppressWarnings(\"unchecked\")
    private static List<Map<String, Object>> convertClojureSeqToMapList(Object seq) {
        if (seq == null) {
            return null;
        }
        List<Map<String, Object>> result = new ArrayList<>();
        // Handle both Clojure seqs and Java iterables
        if (seq instanceof Iterable) {
            for (Object item : (Iterable<?>) seq) {
                if (item instanceof Map) {
                    result.add(convertClojureMap((Map<Object, Object>) item));
                }
            }
        }
        return result;
    }

    // Convert Java Set to Clojure persistent hash set
    private static Object toClojureSet(java.util.Set<?> javaSet) {
        if (javaSet == null) {
            return null;
        }
        IFn hashSet = Clojure.var(\"clojure.core\", \"hash-set\");
        return hashSet.applyTo(clojure.lang.ArraySeq.create(javaSet.toArray()));
    }

    // Convert filter argument at runtime - handles Java Set, null, or pass-through
    private static Object convertFilterArg(Object arg) {
        if (arg == null) {
            return null;
        }
        if (arg instanceof java.util.Set) {
            return toClojureSet((java.util.Set<?>) arg);
        }
        // Already a Clojure set, fn, or ArrayBitSet - pass through
        return arg;
    }

    // Convert Clojure set of keywords to Java Set of Strings
    private static Set<String> convertKeywordSetToStrings(Set<Object> clojureSet) {
        if (clojureSet == null) {
            return null;
        }
        Set<String> result = new java.util.HashSet<>();
        for (Object item : clojureSet) {
            if (item instanceof clojure.lang.Keyword) {
                result.add(((clojure.lang.Keyword) item).getName());
            } else {
                result.add(String.valueOf(item));
            }
        }
        return result;
    }

    /**
     * Convert core.async channel to CompletableFuture.
     * Blocks on a background thread to avoid blocking the caller.
     *
     * @param channel the core.async channel
     * @param converter function to convert the channel result to the desired Java type
     * @return CompletableFuture that completes when the channel delivers a value
     */
    @SuppressWarnings(\"unchecked\")
    private static <T> CompletableFuture<T> channelToCompletableFuture(
            Object channel,
            Function<Object, T> converter) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Block on background thread, not caller thread
                IFn takeFn = Clojure.var(\"clojure.core.async\", \"<!!\");
                Object result = takeFn.invoke(channel);
                return converter.apply(result);
            } catch (Exception e) {
                throw new RuntimeException(\"Async operation failed\", e);
            }
        });
    }

    // ==========================================================================
    // Mutable Convenience Methods
    // ==========================================================================

    /**
     * Add a vector with auto-generated ID and return the ID (mutable convenience method).
     * <p>Thread-safe: synchronized on this instance.</p>
     *
     * @param vector the embedding vector
     * @return the generated UUID
     */
    public synchronized UUID addAndGetId(float[] vector) {
        UUID id = UUID.randomUUID();
        addAndGetId(vector, id);
        return id;
    }

    /**
     * Add a vector with specified ID (mutable convenience method).
     * <p>Mutates internal state. Thread-safe: synchronized on this instance.</p>
     *
     * @param vector the embedding vector
     * @param id the external ID
     * @return the ID (same as passed in)
     */
    public synchronized Object addAndGetId(float[] vector, Object id) {
        ensureInitialized();
        Object newIdx = insertFn.invoke(clojureIndex, vector, id);
        this.clojureIndex = newIdx;
        return id;
    }

    /**
     * Add a vector with ID and metadata (mutable convenience method).
     * <p>Thread-safe: synchronized on this instance.</p>
     *
     * @param vector the embedding vector
     * @param id the external ID
     * @param metadata additional metadata
     * @return the ID (same as passed in)
     */
    public synchronized Object addAndGetId(float[] vector, Object id, Map<String, Object> metadata) {
        ensureInitialized();
        Object newIdx = insertFn.invoke(clojureIndex, vector, id, toClojureMap(metadata));
        this.clojureIndex = newIdx;
        return id;
    }
}
"))

(defn generate-java-source
  "Generate Java source files to the specified output directory.
   Creates the directory structure if needed."
  [output-dir]
  (let [package-dir (io/file output-dir "org" "replikativ" "proximum")
        store-file (io/file package-dir "ProximumVectorStore.java")]
    ;; Create directory structure
    (.mkdirs package-dir)
    ;; Write the generated source
    (spit store-file (generate-java-class))
    (println "Generated:" (.getPath store-file))
    {:generated-files [(.getPath store-file)]}))

;; =============================================================================
;; Build Integration
;; =============================================================================

(defn -main
  "CLI entry point for Java source generation.
   Usage: clj -M -m proximum.codegen.java-source [output-dir]"
  [& args]
  (let [output-dir (or (first args) "src-java-generated")]
    (println "Generating Java source from specification...")
    (generate-java-source output-dir)
    (println "Done.")))

(comment
  ;; Generate to default location
  (generate-java-source "src-java-generated")

  ;; Preview the generated class
  (println (generate-java-class))

  ;; Check method signatures
  (doseq [[k v] (java/java-operations)]
    (println (generate-method-signature k v))))
