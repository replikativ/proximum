(ns proximum.logging
  "Structured logging for proximum using trove facade.

   This namespace provides structured logging that integrates with SLF4J,
   allowing both Java and Clojure code to use the same logging backend.

   Users can configure logging by:
   1. Using slf4j-simple (default) - logs to stderr
   2. Excluding slf4j-simple and adding logback-classic for production
   3. Using any SLF4J-compatible backend (log4j2, etc.)

   Log levels: :trace :debug :info :warn :error

   Usage:
     (require '[proximum.logging :as log])

     ;; Simple message
     (log/info :proximum/insert \"Vector inserted\")

     ;; Structured data
     (log/info :proximum/insert {:node-id 42 :level 3})

     ;; Message with data
     (log/info :proximum/insert \"Insert complete\" {:node-id 42 :duration-ms 5})"
  (:require [taoensso.trove :as trove]))

;; Configure trove to use SLF4J backend
;; This ensures Clojure logs go to same destination as Java SLF4J logs
(require 'taoensso.trove.slf4j)

;; -----------------------------------------------------------------------------
;; Log IDs - namespaced keywords for filtering and analysis
;;
;; Convention: :proximum/<component>.<operation>
;;   :proximum/insert       - vector insertion
;;   :proximum/search       - search operations
;;   :proximum/delete       - deletion operations
;;   :proximum/compaction   - compaction operations
;;   :proximum/sync         - persistence sync
;;   :proximum/branch       - branching operations
;;   :proximum/connect      - index connection/loading
;;   :proximum/graph        - HNSW graph operations
;;   :proximum/cache        - cache hits/misses/evictions

;; -----------------------------------------------------------------------------
;; Convenience macros for structured logging

(defmacro trace
  "Log at TRACE level. For very detailed debugging.
   Usage: (trace :proximum/graph \"Traversing edge\" {:from 1 :to 2})"
  ([id msg-or-data]
   `(trove/log! {:level :trace :id ~id :msg ~msg-or-data}))
  ([id msg data]
   `(trove/log! {:level :trace :id ~id :msg ~msg :data ~data})))

(defmacro debug
  "Log at DEBUG level. For debugging information.
   Usage: (debug :proximum/insert {:node-id 42 :level 3})"
  ([id msg-or-data]
   `(trove/log! {:level :debug :id ~id :msg ~msg-or-data}))
  ([id msg data]
   `(trove/log! {:level :debug :id ~id :msg ~msg :data ~data})))

(defmacro info
  "Log at INFO level. For normal operational messages.
   Usage: (info :proximum/compaction \"Compaction complete\" {:vectors 1000})"
  ([id msg-or-data]
   `(trove/log! {:level :info :id ~id :msg ~msg-or-data}))
  ([id msg data]
   `(trove/log! {:level :info :id ~id :msg ~msg :data ~data})))

(defmacro warn
  "Log at WARN level. For warning conditions.
   Usage: (warn :proximum/sync \"Slow sync detected\" {:duration-ms 5000})"
  ([id msg-or-data]
   `(trove/log! {:level :warn :id ~id :msg ~msg-or-data}))
  ([id msg data]
   `(trove/log! {:level :warn :id ~id :msg ~msg :data ~data})))

(defmacro error
  "Log at ERROR level. For error conditions.
   Usage: (error :proximum/insert \"Insert failed\" {:error e :node-id 42})"
  ([id msg-or-data]
   `(trove/log! {:level :error :id ~id :msg ~msg-or-data}))
  ([id msg data]
   `(trove/log! {:level :error :id ~id :msg ~msg :data ~data})))

;; -----------------------------------------------------------------------------
;; Timing utilities for performance logging

(defmacro with-timing
  "Execute body and log duration at specified level.
   Returns the result of body.

   Usage:
     (with-timing :info :proximum/search \"Search completed\"
       (search idx query k))"
  [level id msg & body]
  `(let [start# (System/nanoTime)
         result# (do ~@body)
         duration-ms# (/ (- (System/nanoTime) start#) 1e6)]
     (trove/log! {:level ~level
                  :id ~id
                  :msg ~msg
                  :data {:duration-ms duration-ms#}})
     result#))

(defmacro debug-timing
  "Execute body and log duration at DEBUG level."
  [id msg & body]
  `(with-timing :debug ~id ~msg ~@body))

(defmacro info-timing
  "Execute body and log duration at INFO level."
  [id msg & body]
  `(with-timing :info ~id ~msg ~@body))
