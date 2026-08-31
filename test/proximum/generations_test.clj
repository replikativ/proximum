(ns proximum.generations-test
  (:require [clojure.core.async :as a]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [konserve.gc-guard :as guard]
            [konserve.protocols :as kp]
            [proximum.core :as core]
            [proximum.generations :as generations]
            [proximum.protocols :as p]
            [proximum.vectors :as vectors]
            [proximum.writing :as writing]))

(defn- temp-dir []
  (str (java.nio.file.Files/createTempDirectory
        "proximum-generations-"
        (make-array java.nio.file.attribute.FileAttribute 0))))

(defn- delete-tree! [path]
  (doseq [f (reverse (file-seq (io/file path)))]
    (io/delete-file f true)))

(defn- source-index [mmap-dir]
  (a/<!!
   (p/sync!
    (core/insert
     (core/create-index {:type :hnsw
                         :dim 2
                         :capacity 32
                         :crypto-hash? true
                         :store-config {:backend :memory :id (random-uuid)}
                         :mmap-dir mmap-dir})
     (float-array [0.0 0.0]) :base))))

(deftest rootless-bootstrap-creates-connects-and-validates-store
  (let [dir (temp-dir)
        store-id (random-uuid)
        store-config {:backend :memory :id store-id}
        config {:type :hnsw
                :dim 2
                :capacity 32
                :M 8
                :crypto-hash? true
                :store-config store-config
                :mmap-dir dir}
        first-builder (atom nil)
        second-builder (atom nil)]
    (try
      (is (false? (k/store-exists? store-config {:sync? true})))
      (reset! first-builder (generations/begin-generation-from-config config))
      (let [raw-store (p/raw-storage
                       (generations/builder-index @first-builder))
            stored-config (k/get raw-store :index/config nil {:sync? true})]
        (is (true? (k/store-exists? store-config {:sync? true})))
        (is (= 2 (:dim stored-config)))
        (is (= 8 (:M stored-config)))
        (is (nil? (k/get raw-store :branches nil {:sync? true})))
        (a/<!! (generations/discard! @first-builder))
        (reset! first-builder nil)

        (testing "an existing store is connected, not recreated"
          (reset! second-builder
                  (generations/begin-generation-from-config config))
          (is (= stored-config
                 (k/get raw-store :index/config nil {:sync? true})))
          (is (nil? (k/get raw-store :branches nil {:sync? true})))
          (a/<!! (generations/discard! @second-builder))
          (reset! second-builder nil))

        (testing "an incompatible retry is rejected without changing config"
          (let [failure (try
                          (generations/begin-generation-from-config
                           (assoc config :dim 3))
                          nil
                          (catch clojure.lang.ExceptionInfo e e))]
            (is (= :generation-config-conflict
                   (:reason (ex-data failure))))
            (is (= stored-config
                   (k/get raw-store :index/config nil {:sync? true})))
            (is (not (guard/in-flight? store-id)))))

        (testing "the underlying index initializer also refuses config drift"
          (let [failure (try
                          (core/create-index
                           {:type :hnsw
                            :dim 3
                            :capacity 32
                            :M 8
                            :crypto-hash? true
                            :store raw-store
                            :mmap-dir dir})
                          nil
                          (catch clojure.lang.ExceptionInfo e e))]
            (is (= :index-config-conflict
                   (:reason (ex-data failure))))
            (is (= stored-config
                   (k/get raw-store :index/config nil {:sync? true}))))))
      (finally
        (when @first-builder
          (a/<!! (generations/discard! @first-builder)))
        (when @second-builder
          (a/<!! (generations/discard! @second-builder)))
        (delete-tree! dir)))))

(deftest generation-config-refuses-the-embedding-owners-live-store
  (let [dir (temp-dir)
        configured-id (random-uuid)
        raw-store (k/create-store {:backend :memory :id configured-id}
                                  {:sync? true})
        live-id (kp/store-id raw-store)
        config {:type :hnsw
                :dim 2
                :capacity 32
                :store raw-store
                ;; The prohibition is checked against `kp/store-id` on this
                ;; connected store, not against an independently supplied
                ;; `:store-config :id` alias.
                :forbidden-store-id live-id
                :mmap-dir dir}]
    (try
      (let [failure (try
                      (generations/begin-generation-from-config config)
                      nil
                      (catch clojure.lang.ExceptionInfo e e))]
        (is (= :proximum/generation-store-forbidden
               (:type (ex-data failure))))
        (is (= live-id (:store-id (ex-data failure))))
        (is (nil? (k/get raw-store :index/config nil {:sync? true})))
        (is (not (guard/in-flight? live-id))))
      (finally
        (delete-tree! dir)))))

(deftest empty-rootless-generation-roundtrip
  (let [dir (temp-dir)
        raw-store (k/create-store {:backend :memory :id (random-uuid)}
                                  {:sync? true})
        store-id (kp/store-id raw-store)
        config {:type :hnsw
                :dim 2
                :capacity 32
                :crypto-hash? true
                :store raw-store
                :mmap-dir dir}
        builder (generations/begin-generation-from-config config)
        workspace-id (:workspace-id builder)]
    (try
      (testing "construction is guarded but publishes no Proximum ref"
        (is (guard/in-flight? store-id))
        (is (zero? (p/vector-count-total
                    (generations/builder-index builder))))
        (is (nil? (k/get raw-store :main nil {:sync? true})))
        (is (nil? (k/get raw-store :branches nil {:sync? true})))
        (is (nil? (k/get raw-store workspace-id nil {:sync? true})))
        (is (some? (k/get raw-store :index/config nil {:sync? true}))))

      (let [sealed (generations/seal! builder)
            generation-id (generations/generation-id sealed)
            snapshot (k/get raw-store generation-id nil {:sync? true})
            roots (generations/reachable-keys config generation-id)
            opened (generations/open-generation config generation-id)]
        (try
          (testing "an empty generation is an immutable root with no native branch"
            (is (= #{} (:parents snapshot)))
            (is (some? snapshot))
            (is (zero? (p/vector-count-total
                        (generations/generation-index sealed))))
            (is (nil? (k/get raw-store :main nil {:sync? true})))
            (is (nil? (k/get raw-store :branches nil {:sync? true})))
            (is (nil? (k/get raw-store workspace-id nil {:sync? true}))))

          (testing "config-only reachability and exact restore are sufficient"
            (is (contains? roots generation-id))
            (is (contains? roots :index/config))
            (is (every? #(not= ::missing
                               (k/get raw-store % ::missing {:sync? true}))
                        roots))
            (is (= generation-id (:generation-id opened)))
            (is (zero? (p/vector-count-total (:index opened)))))

          (generations/rooted! sealed)
          (is (identical? sealed (generations/rooted! sealed))
              "a second owner may acknowledge the same immutable generation")
          (is (not (guard/in-flight? store-id)))
          (finally
            (when (= :sealed-unrooted @(:status sealed))
              (generations/rooted! sealed))
            (a/<!! (generations/close-view! opened))
            (a/<!! (generations/close-view! sealed)))))
      (finally
        (when-not (#{:rooted :discarded} @(:status builder))
          (a/<!! (generations/discard! builder)))
        (delete-tree! dir)))))

(deftest rootless-construction-failure-releases-guard
  (let [dir (temp-dir)
        raw-store (k/create-store {:backend :memory :id (random-uuid)}
                                  {:sync? true})
        store-id (kp/store-id raw-store)
        failure (ex-info "injected rootless construction failure"
                         {:reason :injected-failure})]
    (try
      (is (identical?
           failure
           (try
             (with-redefs [p/create-index (fn [_] (throw failure))]
               (generations/begin-generation-from-config
                {:type :hnsw
                 :dim 2
                 :capacity 32
                 :store raw-store
                 :mmap-dir dir}))
             nil
             (catch Throwable e e))))
      (is (not (guard/in-flight? store-id)))
      (is (nil? (k/get raw-store :main nil {:sync? true})))
      (is (nil? (k/get raw-store :branches nil {:sync? true})))
      (finally
        (delete-tree! dir)))))

(deftest branch-free-generation-roundtrip
  (let [dir (temp-dir)
        source (source-index dir)
        store (p/raw-storage source)
        store-id (kp/store-id store)
        original-head (k/get store :main nil {:sync? true})
        original-branches (k/get store :branches nil {:sync? true})
        builder (generations/begin-generation source)]
    (try
      (generations/put! builder :new (float-array [1.0 0.0]) {:kind :new})
      (testing "the private builder does not mutate its source's logical state"
        (is (nil? (core/get-vector source :new)))
        (is (not (identical? (p/vector-storage source)
                             (p/vector-storage (generations/builder-index builder)))))
        (is (= 1 (vectors/count-vectors (p/vector-storage source))))
        (is (= 2 (vectors/count-vectors
                  (p/vector-storage (generations/builder-index builder))))))

      (let [sealed (generations/seal! builder)
            generation-id (generations/generation-id sealed)
            roots (generations/reachable-keys source generation-id)
            opened (generations/open-generation source generation-id)]
        (try
          (testing "seal writes only the immutable generation"
            (is (= original-head (k/get store :main nil {:sync? true})))
            (is (= original-branches (k/get store :branches nil {:sync? true})))
            (is (some? (k/get store generation-id nil {:sync? true})))
            (is (= [1.0 0.0]
                   (vec (core/get-vector (generations/generation-index sealed) :new))))
            (is (nil? (core/get-vector source :new))))

          (testing "an exact-id restore has an independent cache"
            (is (= generation-id (:generation-id opened)))
            (is (= [1.0 0.0] (vec (core/get-vector (:index opened) :new))))
            (is (not= (:mmap-path sealed) (:mmap-path opened))))

          (testing "reachability is exact and sufficient at the storage-key boundary"
            (is (contains? roots generation-id))
            (is (contains? roots :index/config))
            (is (every? #(not= ::missing
                               (k/get store % ::missing {:sync? true}))
                        roots)))

          ;; Model the embedding owner durably recording generation-id.
          (is (guard/in-flight? store-id))
          (generations/rooted! sealed)
          (is (not (guard/in-flight? store-id)))
          (finally
            (when (= :sealed-unrooted @(:status sealed))
              (generations/rooted! sealed))
            (a/<!! (generations/close-view! opened))
            (a/<!! (generations/close-view! sealed)))))
      (finally
        (a/<!! (p/close! source))
        (delete-tree! dir)))))

(deftest linear-generation-derivation-is-zero-copy-and-exclusive
  (let [dir (temp-dir)
        source (source-index dir)
        builder* (atom nil)]
    (try
      (let [builder (generations/begin-generation source)
            _ (reset! builder* builder)
            source-vectors (p/vector-storage source)
            child-vectors (p/vector-storage (generations/builder-index builder))]
        (is (not (identical? source-vectors child-vectors))
            "logical writer state is never shared")
        (is (identical? (:mmap-resource source-vectors)
                        (:mmap-resource child-vectors))
            "one linear descendant shares the contiguous local cache")
        (is (= (:mmap-path source-vectors) (:mmap-path child-vectors)))
        (generations/put! builder :child (float-array [1.0 0.0]))
        (is (nil? (core/get-vector source :child)))
        (is (= :linear-generation-already-derived
               (try
                 (generations/begin-generation source)
                 nil
                 (catch clojure.lang.ExceptionInfo failure
                   (:reason (ex-data failure))))))
        (a/<!! (generations/discard! builder))
        (reset! builder* nil)

        (testing "discard releases the source reservation"
          (let [replacement (generations/begin-generation source)]
            (reset! builder* replacement)
            (generations/put! replacement :replacement
                              (float-array [2.0 0.0]))
            (is (= [2.0 0.0]
                   (vec (core/get-vector
                         (generations/builder-index replacement)
                         :replacement)))))))
      (finally
        (when @builder*
          (a/<!! (generations/discard! @builder*)))
        (a/<!! (p/close! source))
        (delete-tree! dir)))))

(deftest linear-generation-chain-retains-cache-until-final-descendant-closes
  (let [dir (temp-dir)
        source (source-index dir)
        first* (atom nil)
        second* (atom nil)]
    (try
      (let [first-builder (generations/begin-generation source)
            _ (generations/put! first-builder :first (float-array [1.0 0.0]))
            first (generations/seal! first-builder)
            _ (reset! first* first)
            _ (generations/rooted! first)
            second-builder (generations/begin-generation
                            (generations/generation-index first))
            _ (generations/put! second-builder :second (float-array [2.0 0.0]))
            second (generations/seal! second-builder)
            _ (reset! second* second)
            _ (generations/rooted! second)
            resource (:mmap-resource
                      (p/vector-storage (generations/generation-index second)))]
        (is (= 3 @(:refs resource)))
        (is (nil? (core/get-vector source :first)))
        (is (nil? (core/get-vector
                   (generations/generation-index first) :second)))

        ;; Closing an intermediate ancestor cannot invalidate the final
        ;; descendant or make an older ancestor eligible to fork into the same
        ;; suffix. Each logical generation contributes one resource ref.
        (a/<!! (generations/close-view! first))
        (reset! first* nil)
        (is (= 2 @(:refs resource)))
        (is (= :linear-generation-already-derived
               (try
                 (generations/begin-generation source)
                 nil
                 (catch clojure.lang.ExceptionInfo failure
                   (:reason (ex-data failure))))))
        (is (= [1.0 0.0]
               (vec (core/get-vector
                     (generations/generation-index second) :first))))
        (is (= [2.0 0.0]
               (vec (core/get-vector
                     (generations/generation-index second) :second))))

        (a/<!! (generations/close-view! second))
        (reset! second* nil)
        (is (= 1 @(:refs resource)))
        (testing "closing the final descendant rolls the tip back past closed ancestors"
          (let [replacement (generations/begin-generation source)]
            (a/<!! (generations/discard! replacement)))))
      (finally
        (when @second*
          (a/<!! (generations/close-view! @second*)))
        (when @first*
          (a/<!! (generations/close-view! @first*)))
        (a/<!! (p/close! source))
        (delete-tree! dir)))))

(deftest reused-linear-cache-does-not-change-durable-history
  (let [dir (temp-dir)
        source (source-index dir)
        first* (atom nil)
        replacement* (atom nil)
        opened-first* (atom nil)
        opened-replacement* (atom nil)]
    (try
      (let [first-builder (generations/begin-generation source)
            _ (generations/put! first-builder :first (float-array [1.0 0.0]))
            first (generations/seal! first-builder)
            _ (reset! first* first)
            _ (generations/rooted! first)
            first-id (generations/generation-id first)]
        ;; Release the tip, then derive an alternative child from the source.
        ;; It deliberately reuses and overwrites the same physical vector slot.
        (a/<!! (generations/close-view! first))
        (reset! first* nil)
        (let [replacement-builder (generations/begin-generation source)
              _ (generations/put! replacement-builder :replacement
                                  (float-array [2.0 0.0]))
              replacement (generations/seal! replacement-builder)
              _ (reset! replacement* replacement)
              _ (generations/rooted! replacement)
              replacement-id (generations/generation-id replacement)
              opened-first (generations/open-generation source first-id)
              _ (reset! opened-first* opened-first)
              opened-replacement
              (generations/open-generation source replacement-id)
              _ (reset! opened-replacement* opened-replacement)]
          (is (= [1.0 0.0]
                 (vec (core/get-vector
                       (generations/generation-index opened-first) :first))))
          (is (nil? (core/get-vector
                     (generations/generation-index opened-first) :replacement)))
          (is (= [2.0 0.0]
                 (vec (core/get-vector
                       (generations/generation-index opened-replacement)
                       :replacement))))
          (is (nil? (core/get-vector
                     (generations/generation-index opened-replacement)
                     :first)))))
      (finally
        (doseq [generation [@opened-replacement* @opened-first* @replacement*
                            @first*]]
          (when generation
            (a/<!! (generations/close-view! generation))))
        (a/<!! (p/close! source))
        (delete-tree! dir)))))

(deftest retained-generation-views-have-independent-lifetimes
  (let [dir (temp-dir)
        source (source-index dir)
        builder (generations/begin-generation source)
        sealed* (atom nil)
        view* (atom nil)
        retained* (atom nil)]
    (try
      (generations/put! builder :new (float-array [1.0 0.0]))
      (let [sealed (generations/seal! builder)
            _ (reset! sealed* sealed)
            generation-id (generations/generation-id sealed)
            view (generations/open-generation source generation-id)
            _ (reset! view* view)
            retained (generations/retain-generation-view view)
            _ (reset! retained* retained)
            mmap-path (:mmap-path view)]
        (generations/rooted! sealed)
        (is (identical? (:index view) (:index retained)))
        (is (= mmap-path (:mmap-path retained)))
        (a/<!! (generations/close-view! view))
        (a/<!! (generations/close-view! view))
        (is (.exists (io/file mmap-path))
            "one logical close cannot delete another DB value's mmap")
        (is (= [1.0 0.0]
               (vec (core/get-vector (generations/generation-index retained)
                                     :new))))
        (a/<!! (generations/close-view! retained))
        (is (not (.exists (io/file mmap-path)))
            "the last lease closes the native handle and removes its cache"))
      (finally
        (when (and @retained* (not @(:closed? @retained*)))
          (a/<!! (generations/close-view! @retained*)))
        (when (and @view* (not @(:closed? @view*)))
          (a/<!! (generations/close-view! @view*)))
        (when @sealed*
          (a/<!! (generations/close-view! @sealed*)))
        (a/<!! (p/close! source))
        (delete-tree! dir)))))

(deftest sealed-generation-can-transfer-its-live-query-view
  (let [dir (temp-dir)
        source (source-index dir)
        builder (generations/begin-generation source)
        sealed* (atom nil)
        view* (atom nil)]
    (try
      (generations/put! builder :new (float-array [1.0 0.0]))
      (let [sealed (generations/seal! builder)
            _ (reset! sealed* sealed)
            view (generations/take-generation-view! sealed)
            _ (reset! view* view)
            mmap-path (:mmap-path sealed)]
        (is (identical? (generations/generation-index sealed)
                        (generations/generation-index view)))
        (is (= mmap-path (:mmap-path view)))
        (is (= :generation-view-already-transferred
               (:reason
                (ex-data
                 (try
                   (generations/take-generation-view! sealed)
                   nil
                   (catch clojure.lang.ExceptionInfo failure failure))))))

        ;; The sealed value now owns only guard acknowledgement. Closing it
        ;; after publication cannot invalidate the transferred query handle.
        (generations/rooted! sealed)
        (a/<!! (generations/close-view! sealed))
        (is (= [1.0 0.0]
               (vec (core/get-vector (generations/generation-index view)
                                     :new))))
        (is (.exists (io/file mmap-path)))

        (a/<!! (generations/close-view! view))
        (is (.exists (io/file mmap-path))
            "the native source branch owns the shared reusable cache")
        (is (= 1 @(:refs (:mmap-resource
                          (p/vector-storage source))))))
      (finally
        (when (and @sealed* (= :sealed-unrooted @(:status @sealed*)))
          (generations/discard! @sealed*))
        (when (and @view* (not @(:closed? @view*)))
          (a/<!! (generations/close-view! @view*)))
        (a/<!! (p/close! source))
        (delete-tree! dir)))))

(deftest aborted-transferred-generation-separates-guard-and-view-cleanup
  (let [dir (temp-dir)
        source (source-index dir)
        store-id (kp/store-id (p/raw-storage source))
        builder (generations/begin-generation source)
        sealed* (atom nil)
        view* (atom nil)]
    (try
      (generations/put! builder :new (float-array [1.0 0.0]))
      (let [sealed (generations/seal! builder)
            _ (reset! sealed* sealed)
            view (generations/take-generation-view! sealed)
            _ (reset! view* view)
            mmap-path (:mmap-path view)]
        (is (guard/in-flight? store-id))
        (a/<!! (generations/discard! sealed))
        (is (not (guard/in-flight? store-id)))
        (is (.exists (io/file mmap-path))
            "discard releases the guard but not transferred ownership")
        (is (= [1.0 0.0]
               (vec (core/get-vector (generations/generation-index view)
                                     :new))))
        (a/<!! (generations/close-view! view))
        (is (.exists (io/file mmap-path))
            "the native source branch owns the shared reusable cache")
        (is (= 1 @(:refs (:mmap-resource
                          (p/vector-storage source))))))
      (finally
        (when (and @sealed* (= :sealed-unrooted @(:status @sealed*)))
          (generations/discard! @sealed*))
        (when (and @view* (not @(:closed? @view*)))
          (a/<!! (generations/close-view! @view*)))
        (a/<!! (p/close! source))
        (delete-tree! dir)))))

(deftest publication-hold-does-not-retain-the-native-generation
  (let [dir (temp-dir)
        source (source-index dir)
        store-id (kp/store-id (p/raw-storage source))
        builder (generations/begin-generation source)
        view* (atom nil)
        hold* (atom nil)]
    (try
      (generations/put! builder :new (float-array [1.0 0.0]))
      (let [sealed (generations/seal! builder)
            view (generations/take-generation-view! sealed)
            _ (reset! view* view)
            hold (generations/take-publication-hold! sealed)
            _ (reset! hold* hold)]
        (is (= (generations/generation-id sealed) (:generation-id hold)))
        (is (nil? (:index hold)))
        (is (nil? (:mmap-path hold)))
        (is (guard/in-flight? store-id))
        (is (= :generation-publication-hold-transferred
               (:reason
                (ex-data
                 (try
                   (generations/take-publication-hold! sealed)
                   nil
                   (catch clojure.lang.ExceptionInfo failure failure))))))
        (a/<!! (generations/close-view! sealed))
        (is (guard/in-flight? store-id)
            "ordinary sealed-handle cleanup does not complete its transferred hold")
        (let [original-done guard/done!
              attempts (atom 0)
              failure (ex-info "injected durable completion failure"
                               {:reason :injected-completion-failure})]
          (is (identical?
               failure
               (try
                 (with-redefs [guard/done!
                               (fn [sid token]
                                 (if (= 1 (swap! attempts inc))
                                   (throw failure)
                                   (original-done sid token)))]
                   (generations/root-publication! hold))
                 nil
                 (catch Throwable thrown thrown))))
          (is (false? @(:completed? hold)))
          (is (guard/in-flight? store-id))
          (with-redefs [guard/done! original-done]
            (generations/root-publication! hold)))
        (generations/root-publication! hold)
        (is (not (guard/in-flight? store-id)))
        (is (= [1.0 0.0]
               (vec (core/get-vector (generations/generation-index view)
                                     :new)))))
      (finally
        (when (and @hold* (not @(:completed? @hold*)))
          (generations/abort-publication! @hold*))
        (when (and @view* (not @(:closed? @view*)))
          (a/<!! (generations/close-view! @view*)))
        (a/<!! (p/close! source))
        (delete-tree! dir)))))

(deftest seal-failure-never-publishes-or-mutates-source
  (let [dir (temp-dir)
        source (source-index dir)
        store (p/raw-storage source)
        store-id (kp/store-id store)
        original-head (k/get store :main nil {:sync? true})
        builder (generations/begin-generation source)]
    (try
      (generations/put! builder :never-visible (float-array [2.0 0.0]))
      (let [failure (ex-info "injected immutable-generation write failure"
                             {:reason :injected-failure})]
        (is (identical?
             failure
             (try
               (with-redefs [writing/write-generation! (fn [& _] (throw failure))]
                 (generations/seal! builder))
               nil
               (catch Throwable e e)))))
      (is (= :failed @(:status builder)))
      (is (guard/in-flight? store-id))
      (is (= original-head (k/get store :main nil {:sync? true})))
      (is (nil? (core/get-vector source :never-visible)))
      (finally
        (a/<!! (generations/discard! builder))
        (is (not (guard/in-flight? store-id)))
        (a/<!! (p/close! source))
        (delete-tree! dir)))))

(deftest generation-builder-batch-insertion-is-atomic
  (let [dir (temp-dir)
        source (source-index dir)
        builder (generations/begin-generation source)]
    (try
      (generations/put-batch!
       builder
       [(float-array [1.0 0.0]) (float-array [2.0 0.0])]
       [:one :two]
       {:parallelism 2})
      (is (= [1.0 0.0]
             (vec (core/get-vector (generations/builder-index builder) :one))))
      (is (= [2.0 0.0]
             (vec (core/get-vector (generations/builder-index builder) :two))))
      (let [before (core/count-vectors (generations/builder-index builder))]
        (is (= :dimension-mismatch
               (try
                 (generations/put-batch!
                  builder
                  [(float-array [3.0 0.0]) (float-array [4.0])]
                  [:three :invalid])
                 nil
                 (catch clojure.lang.ExceptionInfo failure
                   (:reason (ex-data failure))))))
        (is (= before
               (core/count-vectors (generations/builder-index builder)))
            "a malformed later vector prevents the entire batch from appending"))
      (finally
        (a/<!! (generations/discard! builder))
        (a/<!! (p/close! source))
        (delete-tree! dir)))))

(deftest contaminated-source-handle-is-rejected
  (let [dir (temp-dir)
        source (source-index dir)
        ;; Current Proximum descendants share the source VectorStore.  This is
        ;; the condition the generation boundary must detect, not bless.
        descendant (core/insert source (float-array [3.0 0.0]) :descendant)]
    (try
      (is (= 1 (p/vector-count-total source)))
      (is (= 2 (vectors/count-vectors (p/vector-storage source))))
      (is (= :generation-source-storage-mutated
             (try
               (generations/begin-generation source)
               nil
               (catch clojure.lang.ExceptionInfo e
                 (:reason (ex-data e))))))
      (finally
        ;; One close only: these two legacy values share the same VectorStore.
        (a/<!! (p/close! descendant))
        (delete-tree! dir)))))
