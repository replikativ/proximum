(ns build
  "Build script for proximum.

   Usage:
     clojure -T:build jar                    - Build core JAR
     clojure -T:build-spring-ai jar-spring-ai - Build Spring AI adapter
     clojure -T:build-langchain4j jar-langchain4j - Build LangChain4j adapter
     clojure -T:build install                - Install core to local Maven repo
     clojure -T:build deploy                 - Deploy core to Clojars
     clojure -T:build clean                  - Clean build artifacts

   Integration tests (run individually):
     clojure -T:build run-langchain4j-tests      - Run LangChain4j adapter tests only
     clojure -T:build run-spring-ai-tests        - Run Spring AI adapter tests only
     clojure -T:build run-java-api-tests         - Run core Java API JUnit tests only
     clojure -T:build run-adapter-compliance-tests - Run both adapter tests
     clojure -T:build run-java-integration-tests  - Run all Java integration tests"
  (:require [clojure.tools.build.api :as b]
            [clojure.java.io :as io]))

;; Core library
(def lib 'org.replikativ/proximum)
(def version (format "0.1.%s" (b/git-count-revs nil)))
(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))
(def jar-file (format "target/%s-%s.jar" (name lib) version))
(def uber-file (format "target/%s-%s-standalone.jar" (name lib) version))

(def src-dirs ["src" "src-java" "resources"])
(def java-src-dirs ["src-java"])

;; Spring AI adapter
(def spring-ai-lib 'org.replikativ/proximum-spring-ai)
(def spring-ai-class-dir "target/classes-spring-ai")
(def spring-ai-jar-file (format "target/%s-%s.jar" (name spring-ai-lib) version))

;; LangChain4j adapter
(def langchain4j-lib 'org.replikativ/proximum-langchain4j)
(def langchain4j-class-dir "target/classes-langchain4j")
(def langchain4j-jar-file (format "target/%s-%s.jar" (name langchain4j-lib) version))

;; Java integration tests
(def java-test-class-dir "target/test-classes")

(defn clean [_]
  (b/delete {:path "target"})
  (b/delete {:path "classes"}))

(defn generate-java-api
  "Generate ProximumVectorStore.java from specification."
  [_]
  (println "Generating Java API from specification...")
  (let [result (b/process {:command-args ["clojure" "-M:dev" "-e"
                                          "(require '[proximum.codegen.java-source :as js]) (js/generate-java-source \"src-java\")"]})]
    (when (not= 0 (:exit result))
      (throw (ex-info "Java API generation failed" {:exit (:exit result)})))))

(defn compile-java
  "Compile Java source files (generates API first)."
  [_]
  (generate-java-api nil)
  (println "Compiling Java sources...")
  (b/javac {:src-dirs java-src-dirs
            :class-dir class-dir
            :basis basis
            :javac-opts ["--release" "22"
                         "--add-modules" "jdk.incubator.vector"
                         "-Xlint:unchecked" "-Xlint:deprecation"]}))

(defn jar
  "Build the JAR file."
  [_]
  (clean nil)
  (compile-java nil)
  (println "Writing pom.xml...")
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis basis
                :src-dirs src-dirs
                :scm {:url "https://github.com/replikativ/proximum"
                      :connection "scm:git:git://github.com/replikativ/proximum.git"
                      :developerConnection "scm:git:ssh://git@github.com/replikativ/proximum.git"
                      :tag (str "v" version)}
                :pom-data [[:description "Persistent vector database with version control for the JVM"]
                           [:url "https://github.com/replikativ/proximum"]
                           [:licenses
                            [:license
                             [:name "Apache License, Version 2.0"]
                             [:url "https://www.apache.org/licenses/LICENSE-2.0"]]]
                           [:developers
                            [:developer
                             [:id "whilo"]
                             [:name "Christian Weilbach"]
                             [:email "ch_weil@topiq.es"]]]]})
  (println "Copying sources...")
  (b/copy-dir {:src-dirs src-dirs
               :target-dir class-dir})
  (println (format "Building JAR: %s" jar-file))
  (b/jar {:class-dir class-dir
          :jar-file jar-file}))

(defn install
  "Install to local Maven repository."
  [_]
  (jar nil)
  (println "Installing to local Maven repo...")
  (b/install {:basis basis
              :lib lib
              :version version
              :jar-file jar-file
              :class-dir class-dir}))

(defn deploy
  "Deploy to Clojars.

   Requires environment variables:
     CLOJARS_USERNAME - your Clojars username
     CLOJARS_PASSWORD - your Clojars deploy token (not password)

   Get a deploy token at: https://clojars.org/tokens"
  [_]
  (jar nil)
  (let [dd (try (requiring-resolve 'deps-deploy.deps-deploy/deploy)
                (catch Exception _
                  (throw (ex-info "deps-deploy not available. Add deps-deploy to :build alias." {}))))]
    (println "Deploying to Clojars...")
    (dd {:installer :remote
         :artifact jar-file
         :pom-file (b/pom-path {:lib lib :class-dir class-dir})})
    (println (format "Deployed %s version %s to Clojars" lib version))))

(defn uber
  "Build an uberjar (standalone JAR with all dependencies)."
  [_]
  (clean nil)
  (compile-java nil)
  (b/copy-dir {:src-dirs src-dirs
               :target-dir class-dir})
  (b/compile-clj {:basis basis
                  :ns-compile '[proximum.core]
                  :class-dir class-dir})
  (b/uber {:class-dir class-dir
           :uber-file uber-file
           :basis basis}))

;; =============================================================================
;; Spring AI Adapter
;; =============================================================================

(defn jar-spring-ai
  "Build the Spring AI adapter JAR.

   Usage: clojure -T:build-spring-ai jar-spring-ai"
  [_]
  ;; First compile core classes to target/classes
  (compile-java nil)
  (b/delete {:path spring-ai-class-dir})

  ;; Get classpath using clojure -Spath (includes Spring AI deps)
  (let [cp-result (b/process {:command-args ["clojure" "-A:build-spring-ai" "-Spath"]
                              :out :capture})
        deps-cp (clojure.string/trim (:out cp-result))
        full-cp (str class-dir ":" deps-cp)]

    ;; Compile the Spring AI adapter
    (println "Compiling Spring AI adapter...")
    (let [result (b/process {:command-args ["javac" "--release" "22"
                                            "--add-modules" "jdk.incubator.vector"
                                            "-d" spring-ai-class-dir
                                            "-cp" full-cp
                                            "src-java-optional/org/replikativ/proximum/spring/ProximumVectorStore.java"]})]
      (when (not= 0 (:exit result))
        (throw (ex-info "Java compilation failed" {:exit (:exit result)})))))

  ;; Write pom.xml and build JAR
  (let [basis (b/create-basis {:project "deps.edn" :aliases [:build-spring-ai]})]
    (println "Writing pom.xml...")
    (b/write-pom {:class-dir spring-ai-class-dir
                  :lib spring-ai-lib
                  :version version
                  :basis basis
                  :src-dirs ["src-java-optional"]
                  :scm {:url "https://github.com/replikativ/proximum"
                        :connection "scm:git:git://github.com/replikativ/proximum.git"
                        :developerConnection "scm:git:ssh://git@github.com/replikativ/proximum.git"
                        :tag (str "v" version)}
                  :pom-data [[:description "Spring AI VectorStore adapter for proximum"]
                             [:url "https://github.com/replikativ/proximum"]
                             [:licenses
                              [:license
                               [:name "Apache License, Version 2.0"]
                               [:url "https://www.apache.org/licenses/LICENSE-2.0"]]]]}))

  ;; Copy source and build JAR
  (b/copy-dir {:src-dirs ["src-java-optional"]
               :target-dir spring-ai-class-dir
               :include "org/replikativ/proximum/spring/**"})
  (println (format "Building JAR: %s" spring-ai-jar-file))
  (b/jar {:class-dir spring-ai-class-dir
          :jar-file spring-ai-jar-file}))

(defn deploy-spring-ai
  "Deploy Spring AI adapter to Clojars.

   Usage: clojure -T:build-spring-ai deploy-spring-ai"
  [_]
  (jar-spring-ai nil)
  (let [dd (requiring-resolve 'deps-deploy.deps-deploy/deploy)]
    (println "Deploying Spring AI adapter to Clojars...")
    (dd {:installer :remote
         :artifact spring-ai-jar-file
         :pom-file (b/pom-path {:lib spring-ai-lib :class-dir spring-ai-class-dir})})
    (println (format "Deployed %s version %s to Clojars" spring-ai-lib version))))

;; =============================================================================
;; LangChain4j Adapter
;; =============================================================================

(defn jar-langchain4j
  "Build the LangChain4j adapter JAR.

   Usage: clojure -T:build-langchain4j jar-langchain4j"
  [_]
  ;; First compile core classes to target/classes
  (compile-java nil)
  (b/delete {:path langchain4j-class-dir})

  ;; Get classpath using clojure -Spath (includes LangChain4j deps)
  (let [cp-result (b/process {:command-args ["clojure" "-A:build-langchain4j" "-Spath"]
                              :out :capture})
        deps-cp (clojure.string/trim (:out cp-result))
        full-cp (str class-dir ":" deps-cp)]

    ;; Compile the LangChain4j adapter
    (println "Compiling LangChain4j adapter...")
    (let [result (b/process {:command-args ["javac" "--release" "22"
                                            "--add-modules" "jdk.incubator.vector"
                                            "-d" langchain4j-class-dir
                                            "-cp" full-cp
                                            "src-java-optional/org/replikativ/proximum/langchain4j/ProximumEmbeddingStore.java"]})]
      (when (not= 0 (:exit result))
        (throw (ex-info "Java compilation failed" {:exit (:exit result)})))))

  ;; Write pom.xml and build JAR
  (let [basis (b/create-basis {:project "deps.edn" :aliases [:build-langchain4j]})]
    (println "Writing pom.xml...")
    (b/write-pom {:class-dir langchain4j-class-dir
                  :lib langchain4j-lib
                  :version version
                  :basis basis
                  :src-dirs ["src-java-optional"]
                  :scm {:url "https://github.com/replikativ/proximum"
                        :connection "scm:git:git://github.com/replikativ/proximum.git"
                        :developerConnection "scm:git:ssh://git@github.com/replikativ/proximum.git"
                        :tag (str "v" version)}
                  :pom-data [[:description "LangChain4j EmbeddingStore adapter for proximum"]
                             [:url "https://github.com/replikativ/proximum"]
                             [:licenses
                              [:license
                               [:name "Apache License, Version 2.0"]
                               [:url "https://www.apache.org/licenses/LICENSE-2.0"]]]]}))

  ;; Copy source and build JAR
  (b/copy-dir {:src-dirs ["src-java-optional"]
               :target-dir langchain4j-class-dir
               :include "io/replikativ/proximum/langchain4j/**"})
  (println (format "Building JAR: %s" langchain4j-jar-file))
  (b/jar {:class-dir langchain4j-class-dir
          :jar-file langchain4j-jar-file}))

(defn deploy-langchain4j
  "Deploy LangChain4j adapter to Clojars.

   Usage: clojure -T:build-langchain4j deploy-langchain4j"
  [_]
  (jar-langchain4j nil)
  (let [dd (requiring-resolve 'deps-deploy.deps-deploy/deploy)]
    (println "Deploying LangChain4j adapter to Clojars...")
    (dd {:installer :remote
         :artifact langchain4j-jar-file
         :pom-file (b/pom-path {:lib langchain4j-lib :class-dir langchain4j-class-dir})})
    (println (format "Deployed %s version %s to Clojars" langchain4j-lib version))))

;; =============================================================================
;; Deploy All
;; =============================================================================

(defn deploy-all
  "Deploy all artifacts to Clojars (core + adapters).

   Usage: clojure -T:build deploy-all

   Note: Run with all required aliases for adapter builds."
  [_]
  (deploy nil)
  (println "\nTo deploy adapters, run:")
  (println "  clojure -T:build-spring-ai deploy-spring-ai")
  (println "  clojure -T:build-langchain4j deploy-langchain4j"))

;; =============================================================================
;; Java Integration / Compliance Tests
;; =============================================================================

(defn run-langchain4j-tests
  "Compile and run LangChain4j adapter integration tests.

   Usage: clojure -T:build run-langchain4j-tests"
  [_]
  (compile-java nil)
  (jar-langchain4j nil)
  (b/delete {:path java-test-class-dir})

  (let [cp-result (b/process {:command-args ["clojure" "-A:java-integration" "-Spath"]
                              :out :capture})
        deps-cp (clojure.string/trim (:out cp-result))
        full-cp (str java-test-class-dir ":" class-dir ":" langchain4j-class-dir ":" deps-cp)]

    (println "Compiling LangChain4j integration test...")
    (let [result (b/process {:command-args ["javac" "--release" "22"
                                           "--add-modules" "jdk.incubator.vector"
                                           "-d" java-test-class-dir
                                           "-cp" full-cp
                                           "test/java/org/replikativ/proximum/langchain4j/LangChain4jIntegrationTest.java"]})]
      (when (not= 0 (:exit result))
        (throw (ex-info "LangChain4j test compilation failed" {:exit (:exit result)}))))

    (println "Running LangChain4j integration test...")
    (let [result (b/process {:command-args ["java"
                                           "--add-modules" "jdk.incubator.vector"
                                           "--enable-native-access=ALL-UNNAMED"
                                           "-cp" full-cp
                                           "org.replikativ.proximum.langchain4j.LangChain4jIntegrationTest"]})]
      (when (not= 0 (:exit result))
        (throw (ex-info "LangChain4j integration test failed" {:exit (:exit result)}))))))

(defn run-spring-ai-tests
  "Compile and run Spring AI adapter integration tests.

   Usage: clojure -T:build run-spring-ai-tests"
  [_]
  (compile-java nil)
  (jar-spring-ai nil)
  (b/delete {:path java-test-class-dir})

  (let [cp-result (b/process {:command-args ["clojure" "-A:java-integration" "-Spath"]
                              :out :capture})
        deps-cp (clojure.string/trim (:out cp-result))
        full-cp (str java-test-class-dir ":" class-dir ":" spring-ai-class-dir ":" deps-cp)]

    (println "Compiling Spring AI integration test...")
    (let [result (b/process {:command-args ["javac" "--release" "22"
                                           "--add-modules" "jdk.incubator.vector"
                                           "-d" java-test-class-dir
                                           "-cp" full-cp
                                           "test/java/org/replikativ/proximum/spring/SpringAiIntegrationTest.java"]})]
      (when (not= 0 (:exit result))
        (throw (ex-info "Spring AI test compilation failed" {:exit (:exit result)}))))

    (println "Running Spring AI integration test...")
    (let [result (b/process {:command-args ["java"
                                           "--add-modules" "jdk.incubator.vector"
                                           "--enable-native-access=ALL-UNNAMED"
                                           "-cp" full-cp
                                           "org.replikativ.proximum.spring.SpringAiIntegrationTest"]})]
      (when (not= 0 (:exit result))
        (throw (ex-info "Spring AI integration test failed" {:exit (:exit result)}))))))

(defn run-java-api-tests
  "Compile and run the core Java API binding JUnit tests.

   Usage: clojure -T:build run-java-api-tests"
  [_]
  (compile-java nil)
  (b/delete {:path java-test-class-dir})

  (let [cp-result (b/process {:command-args ["clojure" "-A:java-integration:java-test" "-Spath"]
                              :out :capture})
        deps-cp (clojure.string/trim (:out cp-result))
        full-cp (str java-test-class-dir ":" class-dir ":" deps-cp)]

    (println "Compiling Java API binding tests...")
    (let [result (b/process {:command-args ["javac" "--release" "22"
                                           "--add-modules" "jdk.incubator.vector"
                                           "-d" java-test-class-dir
                                           "-cp" full-cp
                                           "test/java/org/replikativ/proximum/JavaApiBindingTest.java"]})]
      (when (not= 0 (:exit result))
        (throw (ex-info "Java API test compilation failed" {:exit (:exit result)}))))

    (println "Running JUnit 5 tests...")
    (let [result (b/process {:command-args ["java"
                                           "--add-modules" "jdk.incubator.vector"
                                           "--enable-native-access=ALL-UNNAMED"
                                           "-cp" full-cp
                                           "org.junit.platform.console.ConsoleLauncher"
                                           "--select-class" "org.replikativ.proximum.JavaApiBindingTest"
                                           "--details" "tree"]})]
      (when (not= 0 (:exit result))
        (throw (ex-info "JUnit 5 tests failed" {:exit (:exit result)}))))))

(defn run-adapter-compliance-tests
  "Compile and run Java adapter compliance tests (Spring AI + LangChain4j).

   Usage: clojure -T:build run-adapter-compliance-tests"
  [_]
  (run-langchain4j-tests nil)
  (run-spring-ai-tests nil))

(defn run-java-integration-tests
  "Compile and run all Java integration tests (adapters + core API binding).

   Usage: clojure -T:build run-java-integration-tests"
  [_]
  (run-langchain4j-tests nil)
  (run-spring-ai-tests nil)
  (run-java-api-tests nil))
