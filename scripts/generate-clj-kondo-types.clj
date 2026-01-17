#!/usr/bin/env bb
(ns generate-clj-kondo-types
  "Generate clj-kondo type definitions from proximum.specification.

   This script reads the Malli schemas from api-specification and generates:
   1. Type definitions for clj-kondo's :type-mismatch linter
   2. Hook implementation that expands the emit-api macro

   Usage:
     clj -M scripts/generate-clj-kondo-types.clj

   Outputs:
     .clj-kondo/config.edn - Updated with type definitions
     .clj-kondo/hooks/proximum/codegen.clj - Updated hook
     resources/clj-kondo.exports/org.replikativ/proximum/config.edn - Export config"
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :as pp]))

;; =============================================================================
;; Specification Loading
;; =============================================================================

(defn load-specification
  "Load api-specification by requiring the namespace.

   Note: This must be run with Clojure (not Babashka) to load the full spec."
  []
  (try
    (require 'proximum.specification)
    (let [spec-var (resolve 'proximum.specification/api-specification)]
      (when spec-var
        @spec-var))
    (catch Exception e
      (println "Error loading specification:" (.getMessage e))
      (println "Make sure to run with: clj -M scripts/generate-clj-kondo-types.clj")
      (System/exit 1))))

;; =============================================================================
;; Malli Schema to clj-kondo Type Conversion
;; =============================================================================

(defn malli->clj-kondo-type
  "Convert a Malli type to a clj-kondo type keyword.

   Examples:
     :int -> :int
     :string -> :string
     IndexConfig -> :map
     VectorIndex -> :any (custom types become :any)
     [:vector :int] -> :vector"
  [schema]
  (cond
    ;; Primitive types that clj-kondo understands
    (#{:int :integer :double :string :boolean :keyword :symbol :nil :any} schema)
    schema

    ;; Collection types
    (and (vector? schema) (= :vector (first schema)))
    :vector

    (and (vector? schema) (= :map (first schema)))
    :map

    (and (vector? schema) (= :set (first schema)))
    :set

    ;; Custom types (like VectorIndex, IndexConfig, etc.) -> :any
    (symbol? schema)
    :any

    ;; Default
    :else
    :any))

(defn extract-arity-info
  "Extract arity information from a Malli function schema.

   Input: [:=> [:cat Type1 Type2] RetType]
   Output: {:args [:type1 :type2] :ret :rettype}

   Input: [:=> [:alt [:cat Type1] [:cat Type1 Type2]] RetType]
   Output: [{:args [:type1] :ret :rettype}
            {:args [:type1 :type2] :ret :rettype}]"
  [schema]
  (when (and (vector? schema) (= :=> (first schema)))
    (let [[_ input-schema ret-schema] schema]
      (cond
        ;; Single arity: [:cat Type1 Type2]
        (and (vector? input-schema) (= :cat (first input-schema)))
        [{:args (mapv malli->clj-kondo-type (rest input-schema))
          :ret (malli->clj-kondo-type ret-schema)}]

        ;; Multi-arity: [:alt [:cat ...] [:cat ...]]
        (and (vector? input-schema) (= :alt (first input-schema)))
        (for [alt (rest input-schema)]
          (when (and (vector? alt) (= :cat (first alt)))
            {:args (mapv malli->clj-kondo-type (rest alt))
             :ret (malli->clj-kondo-type ret-schema)}))

        ;; Fallback
        :else
        nil))))

(defn generate-type-definitions
  "Generate clj-kondo type definitions from api-specification.

   Returns a map suitable for :linters :type-mismatch :namespaces."
  [api-spec]
  (reduce-kv
    (fn [acc op-name op-spec]
      (if-let [arities (extract-arity-info (:args op-spec))]
        (let [;; Convert arities to clj-kondo format
              arities-map (into {}
                                (map-indexed
                                  (fn [idx {:keys [args ret]}]
                                    [(count args) {:args args :ret ret}])
                                  arities))]
          (assoc acc op-name {:arities arities-map}))
        acc))
    {}
    api-spec))

;; =============================================================================
;; Config Generation
;; =============================================================================

(defn generate-config
  "Generate complete clj-kondo config.edn with type definitions."
  [api-spec]
  {:hooks {:analyze-call {'proximum.codegen.clojure/emit-api
                          'hooks.proximum.codegen/emit-api
                          'proximum.codegen.clojure/def-api-fn
                          'hooks.proximum.codegen/def-api-fn}}
   :linters {:type-mismatch
             {:namespaces
              {'proximum.core (generate-type-definitions api-spec)}}}})

(defn generate-hook
  "Generate hook implementation that reads operations from specification."
  [api-spec]
  (let [op-names (sort (keys api-spec))]
    (str
      "(ns hooks.proximum.codegen\n"
      "  \"clj-kondo hooks for proximum.codegen.clojure macros.\n"
      "\n"
      "   These hooks teach clj-kondo about the emit-api and def-api-fn macros\n"
      "   that generate the public API from proximum.specification.\n"
      "   \n"
      "   This file is AUTO-GENERATED by scripts/generate-clj-kondo-types.clj\n"
      "   Do not edit directly - regenerate from specification.\"\n"
      "  (:require [clj-kondo.hooks-api :as api]))\n"
      "\n"
      ";; List of all API operations from proximum.specification\n"
      ";; Generated at: " (java.util.Date.) "\n"
      "(def api-operations\n"
      "  '[" (str/join " " op-names) "])\n"
      "\n"
      "(defn def-api-fn\n"
      "  \"Hook for def-api-fn macro.\n"
      "\n"
      "   Transforms (def-api-fn :insert) into (def insert nil)\n"
      "   so clj-kondo understands the generated function definition.\"\n"
      "  [{:keys [node]}]\n"
      "  (let [children (:children node)\n"
      "        [_def-api-fn-sym op-key] children]\n"
      "    (when-not op-key\n"
      "      (throw (ex-info \"def-api-fn requires an operation key\" {})))\n"
      "\n"
      "    ;; Extract the operation name from the keyword\n"
      "    (let [op-name (-> op-key\n"
      "                      api/sexpr\n"
      "                      name\n"
      "                      symbol\n"
      "                      api/token-node)]\n"
      "      ;; Transform to: (def op-name nil)\n"
      "      ;; Using nil as placeholder since clj-kondo only cares about the var existence\n"
      "      {:node (api/list-node\n"
      "              [(api/token-node 'def)\n"
      "               op-name\n"
      "               (api/token-node 'nil)])})))\n"
      "\n"
      "(defn emit-api\n"
      "  \"Hook for emit-api macro.\n"
      "\n"
      "   Transforms (emit-api proximum.specification/api-specification)\n"
      "   into a series of (def function-name nil) forms\n"
      "   so clj-kondo understands all generated API functions.\n"
      "   \n"
      "   Type information is handled separately in config.edn's :type-mismatch linter.\"\n"
      "  [{:keys [node]}]\n"
      "  (let [;; Create a (do (def fn1 nil) (def fn2 nil) ...) form\n"
      "        ;; Using nil as placeholder since clj-kondo only cares about the var existence\n"
      "        def-forms (map (fn [op-name]\n"
      "                        (api/list-node\n"
      "                         [(api/token-node 'def)\n"
      "                          (api/token-node op-name)\n"
      "                          (api/token-node 'nil)]))\n"
      "                      api-operations)]\n"
      "    ;; Wrap all defs in a do block\n"
      "    {:node (api/list-node\n"
      "            (cons (api/token-node 'do) def-forms))}))\n")))

;; =============================================================================
;; File Writing
;; =============================================================================

(defn write-config!
  "Write config.edn file with pretty formatting."
  [path config]
  (io/make-parents path)
  (spit path
        (with-out-str
          (println ";; clj-kondo configuration for proximum")
          (println ";; AUTO-GENERATED by scripts/generate-clj-kondo-types.clj")
          (println ";; Do not edit directly - regenerate from specification")
          (println)
          (pp/pprint config))))

(defn write-hook!
  "Write hook implementation file."
  [path hook-code]
  (io/make-parents path)
  (spit path hook-code))

;; =============================================================================
;; Main
;; =============================================================================

(defn -main [& args]
  (println "Generating clj-kondo type definitions from proximum.specification...")

  ;; Load specification
  (println "Loading api-specification...")
  (let [api-spec (load-specification)
        _ (println (str "Found " (count api-spec) " operations"))

        ;; Generate config
        config (generate-config api-spec)
        hook (generate-hook api-spec)]

    ;; Write local config
    (println "Writing .clj-kondo/config.edn...")
    (write-config! ".clj-kondo/config.edn" config)

    ;; Write local hook
    (println "Writing .clj-kondo/hooks/proximum/codegen.clj...")
    (write-hook! ".clj-kondo/hooks/proximum/codegen.clj" hook)

    ;; Write export config
    (println "Writing resources/clj-kondo.exports/org.replikativ/proximum/config.edn...")
    (write-config! "resources/clj-kondo.exports/org.replikativ/proximum/config.edn" config)

    ;; Write export hook
    (println "Writing resources/clj-kondo.exports/org.replikativ/proximum/hooks/proximum/codegen.clj...")
    (write-hook! "resources/clj-kondo.exports/org.replikativ/proximum/hooks/proximum/codegen.clj" hook)

    (println)
    (println "✓ Generated clj-kondo configuration with type definitions")
    (println (str "✓ " (count api-spec) " operations configured with Malli types"))
    (println)
    (println "Test with: clj-kondo --lint src/proximum/core.clj")))

;; Run when executed as script
(apply -main *command-line-args*)
