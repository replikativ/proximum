(ns proximum.metadata-test
  "Tests for metadata and external-id management.

   Tests cover:
   - Metadata storage and retrieval
   - External ID index operations
   - Uniqueness enforcement
   - Type handling (Long, String, UUID, keywords)
   - Edge cases (nil, blank strings, number normalization)"
  (:require [clojure.test :refer [deftest is testing]]
            [proximum.metadata :as meta]
            [org.replikativ.persistent-sorted-set :as pss])
  (:import [java.util UUID]))

;; -----------------------------------------------------------------------------
;; Metadata Operations Tests

(deftest test-set-and-lookup-metadata
  (testing "Set and lookup metadata"
    (let [pss (meta/create-metadata-pss nil)
          pss' (meta/set-metadata pss 1 {:foo "bar" :baz 42})]

      (is (nil? (meta/lookup-metadata pss 1)) "No metadata before set")
      (is (= {:foo "bar" :baz 42} (meta/lookup-metadata pss' 1)) "Metadata retrieved correctly"))))

(deftest test-update-metadata
  (testing "Update existing metadata"
    (let [pss (meta/create-metadata-pss nil)
          pss1 (meta/set-metadata pss 1 {:version 1})
          pss2 (meta/set-metadata pss1 1 {:version 2})]

      (is (= {:version 1} (meta/lookup-metadata pss1 1)))
      (is (= {:version 2} (meta/lookup-metadata pss2 1)) "Metadata updated"))))

(deftest test-multiple-nodes-metadata
  (testing "Multiple nodes with different metadata"
    (let [pss (meta/create-metadata-pss nil)
          pss' (-> pss
                   (meta/set-metadata 1 {:name "alice"})
                   (meta/set-metadata 2 {:name "bob"})
                   (meta/set-metadata 3 {:name "charlie"}))]

      (is (= {:name "alice"} (meta/lookup-metadata pss' 1)))
      (is (= {:name "bob"} (meta/lookup-metadata pss' 2)))
      (is (= {:name "charlie"} (meta/lookup-metadata pss' 3)))
      (is (nil? (meta/lookup-metadata pss' 999)) "Non-existent node returns nil"))))

(deftest test-set-metadata-with-nil
  (testing "Set metadata with nil is a no-op"
    (let [pss (meta/create-metadata-pss nil)
          pss1 (meta/set-metadata pss 1 {:data "exists"})
          pss2 (meta/set-metadata pss1 1 nil)]

      (is (= {:data "exists"} (meta/lookup-metadata pss1 1)))
      (is (= {:data "exists"} (meta/lookup-metadata pss2 1)) "nil doesn't clear existing metadata"))))

;; -----------------------------------------------------------------------------
;; External ID Extraction Tests

(deftest test-external-id-from-meta
  (testing "Extract external id from metadata map"
    (is (= :foo (meta/external-id-from-meta {:external-id :foo})))
    (is (= "bar" (meta/external-id-from-meta {:external-id "bar"})))
    (is (= 123 (meta/external-id-from-meta {:external-id 123})))

    ;; Edge cases
    (is (nil? (meta/external-id-from-meta nil)) "nil map returns nil")
    (is (nil? (meta/external-id-from-meta {})) "Missing key returns nil")
    (is (nil? (meta/external-id-from-meta {:external-id nil})) "nil value returns nil")
    (is (nil? (meta/external-id-from-meta {:external-id ""})) "Empty string returns nil")
    (is (nil? (meta/external-id-from-meta {:external-id "  "})) "Blank string returns nil")
    (is (nil? (meta/external-id-from-meta {:other-key :value})) "Other keys return nil")))

;; -----------------------------------------------------------------------------
;; External ID Index Tests

(deftest test-set-and-lookup-external-id
  (testing "Set and lookup external id"
    (let [pss (meta/create-external-id-pss nil)
          pss' (meta/set-external-id pss :alice 1)]

      (is (nil? (meta/lookup-external-id pss :alice)) "No mapping before set")
      (is (= 1 (meta/lookup-external-id pss' :alice)) "External id maps to node id"))))

(deftest test-external-id-types
  (testing "External IDs support multiple types"
    (let [pss (meta/create-external-id-pss nil)
          uuid (UUID/randomUUID)
          pss' (-> pss
                   (meta/set-external-id :keyword-id 1)
                   (meta/set-external-id "string-id" 2)
                   (meta/set-external-id 123 3)
                   (meta/set-external-id uuid 4))]

      (is (= 1 (meta/lookup-external-id pss' :keyword-id)))
      (is (= 2 (meta/lookup-external-id pss' "string-id")))
      (is (= 3 (meta/lookup-external-id pss' 123)))
      (is (= 4 (meta/lookup-external-id pss' uuid))))))

(deftest test-external-id-number-normalization
  (testing "Number external IDs normalized to Long"
    (let [pss (meta/create-external-id-pss nil)
          pss' (meta/set-external-id pss 42 100)]

      ;; Both Integer and Long lookups should work
      (is (= 100 (meta/lookup-external-id pss' (int 42))))
      (is (= 100 (meta/lookup-external-id pss' (long 42))))
      (is (= 100 (meta/lookup-external-id pss' 42))))))

(deftest test-external-id-uniqueness
  (testing "External id must be unique"
    (let [pss (meta/create-external-id-pss nil)
          pss' (meta/set-external-id pss :alice 1)]

      ;; Same external-id, same node-id: allowed (idempotent)
      (is (some? (meta/set-external-id pss' :alice 1)) "Re-setting same mapping is ok")

      ;; Same external-id, different node-id: throws
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"External id already exists"
           (meta/set-external-id pss' :alice 2))
          "Cannot reuse external-id for different node"))))

(deftest test-remove-external-id
  (testing "Remove external id mapping"
    (let [pss (meta/create-external-id-pss nil)
          pss1 (meta/set-external-id pss :alice 1)
          pss2 (meta/remove-external-id pss1 :alice)]

      (is (= 1 (meta/lookup-external-id pss1 :alice)) "Mapping exists")
      (is (nil? (meta/lookup-external-id pss2 :alice)) "Mapping removed"))))

(deftest test-remove-nonexistent-external-id
  (testing "Remove nonexistent external id is a no-op"
    (let [pss (meta/create-external-id-pss nil)
          pss' (meta/remove-external-id pss :nonexistent)]

      (is (= pss pss') "PSS unchanged"))))

(deftest test-external-id-nil-handling
  (testing "External ID operations handle nil gracefully"
    (let [pss (meta/create-external-id-pss nil)]

      (is (= pss (meta/set-external-id pss nil 1)) "set with nil id is no-op")
      (is (nil? (meta/lookup-external-id pss nil)) "lookup nil returns nil")
      (is (= pss (meta/remove-external-id pss nil)) "remove nil is no-op"))))

;; -----------------------------------------------------------------------------
;; Comparator Tests

(deftest test-metadata-comparator
  (testing "Metadata entries sorted by node-id"
    (let [cmp meta/metadata-comparator
          e1 {:node-id 1 :data {:foo 1}}
          e2 {:node-id 2 :data {:foo 2}}
          e3 {:node-id 3 :data {:foo 3}}]

      (is (neg? (cmp e1 e2)))
      (is (pos? (cmp e3 e2)))
      (is (zero? (cmp e1 e1)))

      ;; Can compare entries to bare node-ids
      (is (zero? (cmp e1 1)))
      (is (zero? (cmp 2 e2))))))

(deftest test-external-id-comparator
  (testing "External ID entries sorted by external-id"
    (let [cmp meta/external-id-comparator
          e1 {:external-id :a :node-id 1}
          e2 {:external-id :b :node-id 2}
          e3 {:external-id :c :node-id 3}]

      (is (neg? (cmp e1 e2)))
      (is (pos? (cmp e3 e2)))
      (is (zero? (cmp e1 e1)))

      ;; Can compare entries to bare external-ids
      (is (zero? (cmp e1 :a)))
      (is (zero? (cmp :b e2))))))

(deftest test-external-id-comparator-mixed-types
  (testing "External ID comparator handles mixed types"
    (let [cmp meta/external-id-comparator
          e-keyword {:external-id :foo :node-id 1}
          e-string {:external-id "foo" :node-id 2}
          e-number {:external-id 42 :node-id 3}]

      ;; Different types: compared by class name
      (is (not (zero? (cmp e-keyword e-string))))
      (is (not (zero? (cmp e-string e-number))))

      ;; Same types: compared by value
      (is (neg? (cmp {:external-id :a} {:external-id :z})))
      (is (neg? (cmp {:external-id "aaa"} {:external-id "zzz"})))
      (is (neg? (cmp {:external-id 10} {:external-id 99}))))))

;; -----------------------------------------------------------------------------
;; Integration Tests

(deftest test-external-id-workflow
  (testing "Complete workflow: insert, lookup, update, remove"
    (let [pss (meta/create-external-id-pss nil)]

      ;; Insert several mappings
      (let [pss1 (-> pss
                     (meta/set-external-id :user-1 100)
                     (meta/set-external-id :user-2 101)
                     (meta/set-external-id :user-3 102))]

        (is (= 100 (meta/lookup-external-id pss1 :user-1)))
        (is (= 101 (meta/lookup-external-id pss1 :user-2)))
        (is (= 102 (meta/lookup-external-id pss1 :user-3)))

        ;; Remove one
        (let [pss2 (meta/remove-external-id pss1 :user-2)]
          (is (= 100 (meta/lookup-external-id pss2 :user-1)))
          (is (nil? (meta/lookup-external-id pss2 :user-2)) "user-2 removed")
          (is (= 102 (meta/lookup-external-id pss2 :user-3)))

          ;; Can reuse external-id after removal
          (let [pss3 (meta/set-external-id pss2 :user-2 200)]
            (is (= 200 (meta/lookup-external-id pss3 :user-2)) "Reused external-id with new node")))))))

(deftest test-metadata-and-external-id-together
  (testing "Metadata and external-id work independently"
    (let [meta-pss (meta/create-metadata-pss nil)
          ext-pss (meta/create-external-id-pss nil)

          ;; Set metadata for node 42
          meta-pss' (meta/set-metadata meta-pss 42 {:name "Alice" :age 30})

          ;; Set external id for node 42
          ext-pss' (meta/set-external-id ext-pss :alice 42)]

      (is (= {:name "Alice" :age 30} (meta/lookup-metadata meta-pss' 42)))
      (is (= 42 (meta/lookup-external-id ext-pss' :alice)))

      ;; They are independent structures
      (is (nil? (meta/lookup-metadata meta-pss' 999)))
      (is (nil? (meta/lookup-external-id ext-pss' :nonexistent))))))
