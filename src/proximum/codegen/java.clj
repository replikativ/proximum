(ns proximum.codegen.java
  "Java API code generation utilities from specification.

   This namespace provides:
   - Java method name derivation from Clojure operation names
   - Validation of Java API coverage
   - Javadoc generation from spec docstrings

   Java names are DERIVED from operation names using conventions,
   not explicitly specified in the spec (Datahike pattern)."
  (:require [proximum.specification :as spec]
            [clojure.string :as str]
            [clojure.set :as set]))

;; =============================================================================
;; Java Name Derivation
;; =============================================================================

;; Special cases where the Java name differs from automatic derivation
;; These are for Java collection idioms, not for ! or ? handling (which is automatic)
(def ^:private java-name-overrides
  "Operations where the Java method name differs from automatic derivation.
   Keep this minimal - only for Java idioms (e.g., add vs insert)."
  {'insert "add"              ;; Java Collection convention
   'insert-batch "addBatch"   ;; Java Collection convention
   'count-vectors "count"     ;; Java Collection convention
   'branches "listBranches"   ;; Java getter convention
   'get-branch "getCurrentBranch"
   'index-metrics "getMetrics"
   'history "getHistory"
   'load "connect"            ;; Java naming convention
   'load-commit "connectCommit"}) ;; Java naming convention

(defn java-method-name
  "Get the Java method name for an operation.
   Uses spec/->java-method with overrides for special cases."
  [op-key]
  (or (get java-name-overrides op-key)
      (spec/->java-method op-key)))

;; =============================================================================
;; Operation Classification for Java
;; =============================================================================

;; Operations that should be static methods in Java
(def ^:private static-operations
  "Operations that become static methods in Java."
  #{'create-index 'load 'load-commit 'verify-from-cold})

;; Operations that are NOT exposed in Java API (internal or Clojure-only)
(def ^:private java-excluded-operations
  "Operations excluded from Java API (internal or Clojure-only)."
  #{'restore-index 'flush! 'recommended-ef-construction 'recommended-ef-search
    'make-id-filter 'abort-online-compaction! 'hash-index-commit})

(defn static-operation?
  "Check if operation should be a static Java method."
  [op-key]
  (contains? static-operations op-key))

(defn java-exposed?
  "Check if operation should be exposed in Java API."
  [op-key]
  (not (contains? java-excluded-operations op-key)))

(defn java-operations
  "Get all operations that should be exposed in Java API."
  []
  (->> spec/api-specification
       (filter (fn [[k _]] (java-exposed? k)))
       (into {})))

;; =============================================================================
;; Java API Validation
;; =============================================================================

(defn validate-java-coverage
  "Validate that Java API covers all intended operations.
   Returns map of {:exposed [...] :excluded [...] :static [...]}."
  []
  (let [all-ops (keys spec/api-specification)
        exposed (filter java-exposed? all-ops)
        excluded (filter (complement java-exposed?) all-ops)
        static-ops (filter static-operation? exposed)]
    {:exposed (vec exposed)
     :excluded (vec excluded)
     :static (vec static-ops)
     :instance (vec (remove static-operation? exposed))}))

(defn print-java-summary
  "Print a summary of Java API coverage."
  []
  (let [{:keys [exposed excluded static instance]} (validate-java-coverage)]
    (println "Java API Summary")
    (println "================")
    (println)
    (println "Total operations in spec:" (count spec/api-specification))
    (println "Exposed in Java:" (count exposed))
    (println "Excluded from Java:" (count excluded))
    (println)
    (println "Static methods:" (count static))
    (doseq [op static]
      (println "  -" (java-method-name op) "<-" op))
    (println)
    (println "Instance methods:" (count instance))
    (doseq [op instance]
      (println "  -" (java-method-name op) "<-" op))
    (when (seq excluded)
      (println)
      (println "Excluded operations:")
      (doseq [op excluded]
        (println "  -" op)))))

;; =============================================================================
;; Javadoc Generation
;; =============================================================================

(defn doc->javadoc
  "Convert a Clojure docstring to Javadoc format."
  [doc-str]
  (when doc-str
    (let [lines (str/split-lines doc-str)
          formatted (map #(str " * " %) lines)]
      (str "/**\n" (str/join "\n" formatted) "\n */"))))

(defn generate-javadoc
  "Generate Javadoc comments for all Java operations."
  []
  (for [[op-key entry] (java-operations)
        :let [method (java-method-name op-key)
              doc (:doc entry)
              javadoc (doc->javadoc doc)]]
    {:method method
     :op op-key
     :javadoc javadoc
     :static? (static-operation? op-key)}))

(defn print-javadocs
  "Print Javadoc comments for all Java methods."
  []
  (doseq [{:keys [method javadoc static?]} (generate-javadoc)]
    (when javadoc
      (println)
      (println "// Method:" method (if static? "(static)" ""))
      (println javadoc))))

;; =============================================================================
;; Method Signature Generation (for reference)
;; =============================================================================

(defn generate-method-signatures
  "Generate Java method signatures from spec.
   Returns list of {:method :signature :static? :doc}."
  []
  (for [[op-key entry] (java-operations)
        :let [method (java-method-name op-key)
              doc (:doc entry)
              static? (static-operation? op-key)
              ;; For now, just indicate return type hints from semantic info
              returns-index? (and (:referentially-transparent? entry)
                                  (= 'VectorIndex (-> entry :ret)))]]
    {:op op-key
     :method method
     :static? static?
     :returns-index? returns-index?
     :doc (first (str/split-lines (or doc "")))}))

(defn print-method-signatures
  "Print method signatures for Java implementation reference."
  []
  (println "Java Method Signatures (for reference)")
  (println "======================================")
  (println)
  (doseq [{:keys [method static? returns-index? doc]} (generate-method-signatures)]
    (let [prefix (if static? "static " "")
          return-hint (if returns-index? "// returns index" "")]
      (println (str "  " prefix method "(...) " return-hint))
      (println (str "    // " doc))
      (println))))

;; =============================================================================
;; Sync Check - Compare Spec vs Existing Java File
;; =============================================================================

(defn check-java-sync
  "Check if the existing Java file is in sync with the specification.
   Parses the Java file to find method names and compares with derived names.
   Returns {:in-sync [...] :missing-in-java [...] :extra-in-java [...]}."
  [java-file-path]
  (let [java-content (slurp java-file-path)
        ;; Simple regex to find method declarations
        method-pattern #"public\s+(?:static\s+)?[\w<>\[\],\s]+\s+(\w+)\s*\("
        java-methods (->> (re-seq method-pattern java-content)
                          (map second)
                          (into #{}))
        spec-methods (->> (java-operations)
                          (map (fn [[k _]] (java-method-name k)))
                          (into #{}))]
    {:in-sync (set/intersection java-methods spec-methods)
     :missing-in-java (set/difference spec-methods java-methods)
     :extra-in-java (set/difference java-methods spec-methods)}))

(defn print-sync-status
  "Print the sync status between spec and Java file."
  [java-file-path]
  (let [{:keys [in-sync missing-in-java extra-in-java]} (check-java-sync java-file-path)]
    (println "Java API Sync Status")
    (println "====================")
    (println)
    (println "Methods in sync:" (count in-sync))
    (when (seq missing-in-java)
      (println)
      (println "MISSING in Java (derived from spec):")
      (doseq [m (sort missing-in-java)]
        (println "  -" m)))
    (when (seq extra-in-java)
      (println)
      (println "EXTRA in Java (not derived from spec):")
      (println "(These may be Builder methods, utilities, or need spec entries)")
      (doseq [m (sort extra-in-java)]
        (println "  -" m)))))
