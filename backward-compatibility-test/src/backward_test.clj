(ns backward-test
  "Cross-release persistence fixture; loaded only by the compatibility script."
  (:require [clojure.core.async :as async]
            [proximum.core :as proximum]))

(def ^:private store-id
  #uuid "17e4f444-3d70-4a41-a6c8-d3a12a63e6d0")

(defn- store-config []
  {:backend :file
   :path (str (System/getenv "BACK_COMPAT_ROOT") "/store")
   :id store-id})

(defn- mmap-dir []
  (str (System/getenv "BACK_COMPAT_ROOT") "/"
       (System/getenv "BACK_COMPAT_MMAP")))

(defn- f32s [x y z w]
  (float-array [(float x) (float y) (float z) (float w)]))

(def ^:private base-vectors
  [["north" (f32s 1 0 0 0)]
   ["east" (f32s 0 1 0 0)]])

(defn- add-vectors [index entries]
  (reduce (fn [current [id vector]]
            (proximum/insert current vector id {:source "compat"}))
          index
          entries))

(defn write [_]
  (let [created (proximum/create-index {:type :hnsw
                                        :dim 4
                                        :capacity 64
                                        :store-config (store-config)
                                        :mmap-dir (mmap-dir)})
        main (async/<!! (proximum/sync! (add-vectors created base-vectors)))
        feature-base (proximum/branch! main :feature)
        feature (async/<!! (proximum/sync!
                            (proximum/insert feature-base
                                             (f32s 0 0 1 0)
                                             "feature"
                                             {:source "release"})))]
    (proximum/close! feature)
    (proximum/close! main)))

(defn verify [_]
  (let [main (proximum/load (store-config) :branch :main :mmap-dir (mmap-dir))
        feature (proximum/load (store-config) :branch :feature :mmap-dir (mmap-dir))]
    (try
      (assert (= 2 (proximum/count-vectors main)))
      (assert (= 3 (proximum/count-vectors feature)))
      (assert (some? (proximum/get-vector main "north")))
      (assert (nil? (proximum/get-vector main "feature")))
      (assert (some? (proximum/get-vector feature "feature")))
      (let [updated (proximum/insert main
                                     (f32s 0 0 0 1)
                                     "current"
                                     {:source "current"})
            synced (async/<!! (proximum/sync! updated))]
        (proximum/close! synced))
      (finally
        (proximum/close! feature)
        (proximum/close! main))))
  (let [main (proximum/load (store-config) :branch :main :mmap-dir (mmap-dir))
        feature (proximum/load (store-config) :branch :feature :mmap-dir (mmap-dir))]
    (try
      (assert (= 3 (proximum/count-vectors main)))
      (assert (some? (proximum/get-vector main "current")))
      (assert (= 3 (proximum/count-vectors feature)))
      (assert (nil? (proximum/get-vector feature "current")))
      (finally
        (proximum/close! feature)
        (proximum/close! main)))))
