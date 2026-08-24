(ns proximum.warm
  "Warm a restored index's lazy trees — the metadata PSS and the external-id
   PSS — in concurrent waves, sharing one budget.

   ## Why this warms only the trees

   Proximum's restore is mostly EAGER by design: an HNSW search descends
   through arbitrary graph nodes, so there is no useful lazy subset of edges,
   and `restore-index` loads every edge chunk and every vector chunk up front
   (in parallel — see `proximum.fetch`; `:fetch-width` on the restore opts is
   the knob for that phase). What restore leaves LAZY are the two
   persistent-sorted-set trees:

     - the external-id index — every id-keyed lookup, insert and delete
       descends it, so a cold tree taxes exactly the operations a fresh
       function instance serves first;
     - the metadata tree — filtered searches and metadata reads.

   Those restore address-rooted, one blocking storage read per node touched.
   This warms them the way datahike and stratum warm theirs: breadth-first,
   each level fetched concurrently, `:budget` as a hard ceiling shared
   round-robin so neither tree can starve the other
   (`org.replikativ.persistent-sorted-set.warm`, PSS >= 0.5.142).

   ## Use

     (def idx (prox/load-index store {:mmap-dir dir}))
     (warm/warm! idx)                       ; both trees, :depth :interior
     (warm/warm! idx {:depth :with-leaves}) ; everything, budget-bounded

   Report: {:fetched :by-level :rounds :height :by-index :budget-left
   :budget-exhausted? :budget-clamped? :ms}, `:by-index` keyed by
   `:external-id-index` / `:metadata`. A warm changes no results — skipping
   it, or running out of budget, costs round trips and never correctness."
  (:require [org.replikativ.persistent-sorted-set.warm :as pss-warm]
            [proximum.protocols :as p]))

(defn warm!
  "Warm the index's external-id and metadata trees, sharing one budget.
   Options and report: `org.replikativ.persistent-sorted-set.warm` (`:depth`
   `:budget` `:width` `:cache-size`)."
  ([idx] (warm! idx {}))
  ([idx opts]
   (pss-warm/warm-trees!
    (for [[k tree] [[:external-id-index (p/external-id-index idx)]
                    [:metadata (p/metadata-index idx)]]
          :when tree]
      {:key k :set tree})
    opts)))
