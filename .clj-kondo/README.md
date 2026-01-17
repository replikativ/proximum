# clj-kondo Configuration for Proximum

This directory contains clj-kondo configuration to handle the macro-generated API in Proximum.

## Problem

The Proximum public API in `src/proximum/core.clj` is generated via the `emit-api` macro from `proximum.codegen.clojure`. This macro reads the API specification from `proximum.specification/api-specification` and generates `def` forms for all API functions.

Because the functions are macro-generated, clj-kondo doesn't know they exist and highlights them as unresolved symbols when used in other namespaces.

## Solutions

We provide two approaches to solve this problem:

### Solution 1: Hooks (Recommended)

**Location**: `.clj-kondo/config.edn` and `.clj-kondo/hooks/proximum/codegen.clj`

This approach uses clj-kondo's hooks system to teach it about the macro expansion. The hook transforms:

```clojure
(emit-api proximum.specification/api-specification)
```

Into:

```clojure
(do
  (def insert implementation-fn)
  (def search implementation-fn)
  (def delete implementation-fn)
  ;; ... all other API functions
  )
```

**Advantages**:
- clj-kondo fully understands all generated functions
- Provides proper linting for unused bindings, wrong arities, etc.
- More maintainable - teaches clj-kondo the actual structure

**Disadvantages**:
- Requires maintaining the list of API functions in the hook
- More complex configuration

**Files**:
- `.clj-kondo/config.edn` - Registers the hook
- `.clj-kondo/hooks/proximum/codegen.clj` - Hook implementation

### Solution 2: Simple Exclusions (Quick Fix)

**Location**: `.clj-kondo/config-alternative-simple.edn`

This approach uses `:lint-as` to treat the macros like built-in forms and excludes specific symbols from unresolved symbol warnings.

**Advantages**:
- Simple to set up
- No hook code to maintain

**Disadvantages**:
- Won't catch real errors in generated function usage
- Less precise linting
- Requires manually listing each function to exclude

**To use this approach instead**:
```bash
mv .clj-kondo/config.edn .clj-kondo/config-with-hooks.edn
mv .clj-kondo/config-alternative-simple.edn .clj-kondo/config.edn
```

## How the Macro Works

### Code Generation Flow

1. **Specification** (`src/proximum/specification.cljc`):
   - Defines `api-specification` - a map of operation names to specs
   - Each operation has `:args`, `:ret`, `:doc`, `:impl` (implementation symbol)
   - Example entry:
     ```clojure
     insert
     {:args [:=> [:cat VectorIndex Vector ExternalId [:? Metadata]] VectorIndex]
      :ret  VectorIndex
      :doc  "Insert a vector with an ID and optional metadata..."
      :impl proximum.api-impl/insert
      :referentially-transparent? true
      :supports-remote? true}
     ```

2. **Codegen** (`src/proximum/codegen/clojure.clj`):
   - Provides `emit-api` macro that reads the specification
   - For each operation, generates:
     ```clojure
     (do
       (def operation-name implementation-fn)
       (alter-meta! (var operation-name) assoc :arglists ...))
     ```

3. **Core API** (`src/proximum/core.clj`):
   - Simply calls `(codegen/emit-api proximum.specification/api-specification)`
   - All ~50 API functions are generated at compile time

### Current API Functions

The following functions are generated (as of the latest specification):

**Index Lifecycle**: `create-index`, `restore-index`, `load`, `load-commit`, `close!`

**Core Operations**: `insert`, `insert-batch`, `search`, `search-filtered`, `search-with-metadata`, `delete`, `fork`

**Accessors**: `count-vectors`, `get-vector`, `get-metadata`, `lookup-internal-id`, `with-metadata`, `capacity`, `remaining-capacity`, `index-type`, `index-config`

**Persistence**: `sync!`, `flush!`, `branch!`, `branches`, `get-branch`, `get-commit-id`

**Compaction**: `compact`, `start-online-compaction`, `finish-online-compaction!`, `abort-online-compaction!`, `compaction-progress`

**Maintenance**: `gc!`, `history`, `parents`, `ancestors`, `ancestor?`, `common-ancestor`, `commit-info`, `commit-graph`, `delete-branch!`, `reset!`

**Metrics**: `index-metrics`, `needs-compaction?`

**Crypto**: `get-commit-hash`, `crypto-hash?`, `hash-index-commit`, `verify-from-cold`

**HNSW-specific**: `recommended-ef-construction`, `recommended-ef-search`

**Utilities**: `make-id-filter`

## Maintaining the Hook

When adding new API operations to `proximum.specification/api-specification`, you must also update the hook:

1. Add the new operation name to the `api-operations` vector in `.clj-kondo/hooks/proximum/codegen.clj`
2. Or regenerate the list by running:
   ```bash
   clj -M -e "(require '[proximum.specification :as spec]) (println (clojure.string/join \"\n\" (sort (map name (keys spec/api-specification)))))"
   ```

### Future Improvement: Auto-generation

To avoid manual maintenance, you could:

1. **Export the list at build time**:
   Create a build step that writes the operation list to a file that the hook can read.

2. **Use clj-kondo's namespace analysis**:
   When clj-kondo lints `proximum.specification`, it could export the available operations.

3. **Create a gen-hook script**:
   ```bash
   #!/bin/bash
   # Generate hook code from specification
   clj -M -e "(require '[proximum.specification :as spec])
              (spit \".clj-kondo/hooks/proximum/codegen.clj\"
                    (generate-hook-code spec/api-specification))"
   ```

## Testing the Configuration

To verify clj-kondo recognizes the generated functions:

```bash
# Lint the core namespace
clj-kondo --lint src/proximum/core.clj

# Lint a test that uses the API
clj-kondo --lint test/proximum/core_test.clj

# Lint the whole project
clj-kondo --lint src:test
```

You should see no warnings about unresolved symbols for the generated API functions.

## References

- [clj-kondo Hooks Documentation](https://github.com/clj-kondo/clj-kondo/blob/master/doc/hooks.md)
- [clj-kondo Configuration Guide](https://github.com/clj-kondo/clj-kondo/blob/master/doc/config.md)
- [Michiel Borkent's Blog on clj-kondo Hooks](https://blog.michielborkent.nl/clj-kondo-hooks.html)
- [clj-kondo Latest Documentation (2025.12.23)](https://cljdoc.org/d/clj-kondo/clj-kondo/2025.12.23/doc/hooks)

## Exporting Configuration for Library Users

If Proximum is distributed as a library, you can export this clj-kondo configuration so users automatically get proper linting:

1. Create `resources/clj-kondo.exports/org.replikativ/proximum/config.edn`:
   ```clojure
   {:hooks {:analyze-call {proximum.codegen.clojure/emit-api hooks.proximum.codegen/emit-api}}}
   ```

2. Include the hook file at `resources/clj-kondo.exports/org.replikativ/proximum/hooks/proximum/codegen.clj`

3. When users run `clj-kondo --copy-configs --dependencies`, the configuration will be automatically imported.

See: [clj-kondo Config Export Documentation](https://github.com/clj-kondo/clj-kondo/blob/master/doc/config.md#exporting-config)
