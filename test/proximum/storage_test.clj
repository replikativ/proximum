(ns proximum.storage-test
  "Tests for PSS-based address map functions in proximum.storage."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [proximum.storage :as storage]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [clojure.core.async :as a]))

(def ^:dynamic *store* nil)
(def ^:dynamic *storage* nil)

(defn with-store-fixture [f]
  (let [store (a/<!! (new-mem-store))]
    (binding [*store* store
              *storage* (storage/create-storage store {})]
      (f))))

(use-fixtures :each with-store-fixture)

;; -----------------------------------------------------------------------------
;; Address PSS Creation and Basic Operations

(deftest test-create-address-pss-empty
  (testing "creates empty PSS without storage"
    (let [pss (storage/create-address-pss nil)]
      (is (some? pss))
      (is (zero? (count pss)))))

  (testing "creates empty PSS with storage"
    (let [pss (storage/create-address-pss *storage*)]
      (is (some? pss))
      (is (zero? (count pss))))))

(deftest test-address-pss-assoc-and-get
  (testing "assoc and get single entry"
    (let [pss (storage/create-address-pss nil)
          uuid (java.util.UUID/randomUUID)
          pss' (storage/address-pss-assoc pss 42 uuid)]
      (is (= 1 (count pss')))
      (is (= uuid (storage/address-pss-get pss' 42)))))

  (testing "assoc multiple entries"
    (let [pss (storage/create-address-pss nil)
          uuid1 (java.util.UUID/randomUUID)
          uuid2 (java.util.UUID/randomUUID)
          uuid3 (java.util.UUID/randomUUID)
          pss' (-> pss
                   (storage/address-pss-assoc 10 uuid1)
                   (storage/address-pss-assoc 20 uuid2)
                   (storage/address-pss-assoc 30 uuid3))]
      (is (= 3 (count pss')))
      (is (= uuid1 (storage/address-pss-get pss' 10)))
      (is (= uuid2 (storage/address-pss-get pss' 20)))
      (is (= uuid3 (storage/address-pss-get pss' 30)))))

  (testing "get returns nil for missing position"
    (let [pss (storage/create-address-pss nil)
          uuid (java.util.UUID/randomUUID)
          pss' (storage/address-pss-assoc pss 42 uuid)]
      (is (nil? (storage/address-pss-get pss' 99)))))

  (testing "assoc overwrites existing entry"
    (let [pss (storage/create-address-pss nil)
          uuid1 (java.util.UUID/randomUUID)
          uuid2 (java.util.UUID/randomUUID)
          pss' (-> pss
                   (storage/address-pss-assoc 42 uuid1)
                   (storage/address-pss-assoc 42 uuid2))]
      (is (= 1 (count pss')))
      (is (= uuid2 (storage/address-pss-get pss' 42))))))

;; -----------------------------------------------------------------------------
;; Map Conversion

(deftest test-address-pss-to-map
  (testing "converts empty PSS to empty map"
    (let [pss (storage/create-address-pss nil)
          m (storage/address-pss-to-map pss)]
      (is (= {} m))))

  (testing "converts populated PSS to map"
    (let [pss (storage/create-address-pss nil)
          uuid1 (java.util.UUID/randomUUID)
          uuid2 (java.util.UUID/randomUUID)
          pss' (-> pss
                   (storage/address-pss-assoc 10 uuid1)
                   (storage/address-pss-assoc 20 uuid2))
          m (storage/address-pss-to-map pss')]
      (is (= {10 uuid1 20 uuid2} m))))

  (testing "returns nil for nil PSS"
    (is (nil? (storage/address-pss-to-map nil)))))

(deftest test-map-to-address-pss
  (testing "converts empty map to empty PSS"
    (let [pss (storage/map-to-address-pss {} nil)]
      (is (some? pss))
      (is (zero? (count pss)))))

  (testing "converts populated map to PSS"
    (let [uuid1 (java.util.UUID/randomUUID)
          uuid2 (java.util.UUID/randomUUID)
          m {10 uuid1 20 uuid2}
          pss (storage/map-to-address-pss m nil)]
      (is (= 2 (count pss)))
      (is (= uuid1 (storage/address-pss-get pss 10)))
      (is (= uuid2 (storage/address-pss-get pss 20))))))

(deftest test-map-pss-roundtrip
  (testing "map -> PSS -> map roundtrip preserves data"
    (let [uuid1 (java.util.UUID/randomUUID)
          uuid2 (java.util.UUID/randomUUID)
          uuid3 (java.util.UUID/randomUUID)
          original {10 uuid1 20 uuid2 30 uuid3}
          pss (storage/map-to-address-pss original nil)
          result (storage/address-pss-to-map pss)]
      (is (= original result)))))

;; -----------------------------------------------------------------------------
;; Persistence

(deftest test-store-and-restore-address-pss
  (testing "stores and restores empty PSS"
    (let [pss (storage/create-address-pss *storage*)
          root (storage/store-address-pss! pss *storage*)
          restored (storage/restore-address-pss root *storage*)]
      (is (some? root))
      (is (zero? (count restored)))))

  (testing "stores and restores populated PSS"
    (let [uuid1 (java.util.UUID/randomUUID)
          uuid2 (java.util.UUID/randomUUID)
          pss (-> (storage/create-address-pss *storage*)
                  (storage/address-pss-assoc 10 uuid1)
                  (storage/address-pss-assoc 20 uuid2))
          root (storage/store-address-pss! pss *storage*)
          restored (storage/restore-address-pss root *storage*)]
      (is (some? root))
      (is (= 2 (count restored)))
      (is (= uuid1 (storage/address-pss-get restored 10)))
      (is (= uuid2 (storage/address-pss-get restored 20)))))

  (testing "store returns nil for nil PSS"
    (is (nil? (storage/store-address-pss! nil *storage*))))

  (testing "restore returns nil for nil root"
    (is (nil? (storage/restore-address-pss nil *storage*)))))

(deftest test-full-persistence-roundtrip
  (testing "map -> PSS -> store -> restore -> map roundtrip"
    (let [uuid1 (java.util.UUID/randomUUID)
          uuid2 (java.util.UUID/randomUUID)
          uuid3 (java.util.UUID/randomUUID)
          original {100 uuid1 200 uuid2 300 uuid3}
          pss (storage/map-to-address-pss original *storage*)
          root (storage/store-address-pss! pss *storage*)
          restored (storage/restore-address-pss root *storage*)
          result (storage/address-pss-to-map restored)]
      (is (= original result)))))

;; -----------------------------------------------------------------------------
;; Edge Cases

(deftest test-large-address-map
  (testing "handles address map with many entries"
    (let [;; Create map with 1000 entries
          entries (into {} (for [i (range 1000)]
                             [i (java.util.UUID/randomUUID)]))
          pss (storage/map-to-address-pss entries *storage*)
          root (storage/store-address-pss! pss *storage*)
          restored (storage/restore-address-pss root *storage*)
          result (storage/address-pss-to-map restored)]
      (is (= 1000 (count result)))
      (is (= entries result)))))

(deftest test-address-pss-with-long-positions
  (testing "handles Long positions (encoded HNSW positions)"
    (let [;; Encoded positions like (layer << 32) | chunkIdx
          pos1 (bit-or (bit-shift-left 0 32) 0)    ; layer 0, chunk 0
          pos2 (bit-or (bit-shift-left 0 32) 100)  ; layer 0, chunk 100
          pos3 (bit-or (bit-shift-left 1 32) 50)   ; layer 1, chunk 50
          uuid1 (java.util.UUID/randomUUID)
          uuid2 (java.util.UUID/randomUUID)
          uuid3 (java.util.UUID/randomUUID)
          m {pos1 uuid1 pos2 uuid2 pos3 uuid3}
          pss (storage/map-to-address-pss m *storage*)
          root (storage/store-address-pss! pss *storage*)
          restored (storage/restore-address-pss root *storage*)
          result (storage/address-pss-to-map restored)]
      (is (= m result)))))
