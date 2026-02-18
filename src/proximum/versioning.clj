(ns proximum.versioning
  "Git-like versioning API for proximum.

   This namespace provides version control operations:

   Branch Operations:
   - branch!: Create a new branch from current state
   - branches: List all branches
   - delete-branch!: Delete a branch
   - get-branch: Get current branch name

   Commit Operations:
   - get-commit-id: Get current commit ID
   - parents: Get parent commit IDs
   - commit-info: Get metadata for a commit

   History & Graph Operations:
   - history: Get commit history for current branch
   - ancestors: Get all ancestor commits
   - ancestor?: Check ancestry relationship
   - common-ancestor: Find common ancestor of two commits
   - commit-graph: Get full commit DAG for visualization

   Advanced Operations:
   - merge!: Merge vectors from source branch
   - reset!: Reset branch to a different commit

   Note: This namespace shadows clojure.core/parents, ancestors, and reset!
   to provide git-like versioning semantics."
  (:refer-clojure :exclude [parents ancestors reset!])
  (:require [proximum.protocols :as p]
            [proximum.vectors :as vectors]
            [proximum.logging :as log]
            [konserve.core :as k]
            [clojure.core.async :as a]
            [clojure.java.io :as io]))

;; -----------------------------------------------------------------------------
;; Branch Operations

(defn branches
  "List all branches in the store. Like `git branch --list`.

   Args:
     idx - VectorIndex with storage

   Returns:
     Set of branch keywords, e.g., #{:main :feature-x}"
  [idx]
  (when-let [edge-store (p/raw-storage idx)]
    (k/get edge-store :branches nil {:sync? true})))

(defn get-branch
  "Get current branch name. Like `git branch --show-current`.

   Returns keyword."
  [idx]
  (p/current-branch idx))

(defn branch!
  "Create a new branch from current state. Like `git branch <name>`.

   Forks the current branch to a new branch with the given name.
   The new branch gets its own copy of the mmap file (using reflink if supported).
   After forking, the returned index is connected to the new branch.

   The current index must be synced before branching (no uncommitted changes).

   Args:
     idx         - VectorIndex to branch from
     new-branch  - Keyword name for the new branch (e.g., :feature-x)

   Returns:
     New VectorIndex connected to the new branch"
  [idx new-branch]
  (when-not (p/raw-storage idx)
    (throw (ex-info "Cannot branch an in-memory index. Use a durable Konserve store."
                    {:hint "Create with :store-config (including :id) and provide :mmap-dir for branch mmaps."})))

  (let [edge-store (p/raw-storage idx)
        current-branch (p/current-branch idx)

        ;; Check branch doesn't already exist
        existing-branches (k/get edge-store :branches nil {:sync? true})
        _ (when (contains? existing-branches new-branch)
            (throw (ex-info "Branch already exists"
                            {:branch new-branch
                             :existing-branches existing-branches})))

        ;; Get current branch snapshot
        current-snapshot (k/get edge-store current-branch nil {:sync? true})
        _ (when-not current-snapshot
            (throw (ex-info "Current branch has no commits. Call sync! before branching."
                            {:branch current-branch})))

        ;; Use Forkable protocol for clean forking
        forked-vectors (p/fork-vector-storage idx new-branch)
        forked-graph (p/fork-graph-storage idx)

        ;; Create snapshot for new branch
        new-commit-id (java.util.UUID/randomUUID)
        now (java.util.Date.)
        new-snapshot (-> current-snapshot
                         (dissoc :mmap-path)  ;; Remove if present from old snapshot
                         (assoc :commit-id new-commit-id
                                :parents #{(:commit-id current-snapshot)}
                                :created-at now
                                :updated-at now
                                :branch new-branch))]

    ;; Write new branch snapshot under both commit-id and branch name
    (k/assoc edge-store new-commit-id new-snapshot {:sync? true})
    (k/assoc edge-store new-branch new-snapshot {:sync? true})
    (k/update edge-store :branches #(conj (or % #{}) new-branch) {:sync? true})

    ;; Assemble forked index using Forkable protocol
    (p/assemble-forked-index idx forked-vectors forked-graph new-branch new-commit-id)))

(defn delete-branch!
  "Delete a branch. Like `git branch -d <name>`.

   Removes branch from :branches set and deletes the branch's mmap file.
   Commits remain until GC. Cannot delete current branch or :main.

   Args:
     idx    - Index (uses its store)
     branch - Branch keyword to delete

   Returns:
     Updated idx (still on original branch)"
  [idx branch]
  (when (= branch (p/current-branch idx))
    (throw (ex-info "Cannot delete current branch" {:branch branch})))
  (when (= branch :main)
    (throw (ex-info "Cannot delete main branch" {})))
  (let [edge-store (p/raw-storage idx)
        mmap-dir (p/mmap-dir idx)]
    ;; Remove from branches set
    (k/update edge-store :branches #(disj % branch) {:sync? true})
    ;; Delete mmap file for branch
    (when mmap-dir
      (let [mmap-path (vectors/branch-mmap-path mmap-dir branch)]
        (when (.exists (io/file mmap-path))
          (io/delete-file mmap-path))))
    idx))

;; -----------------------------------------------------------------------------
;; Commit Operations

(defn get-commit-id
  "Get current commit ID. Like `git rev-parse HEAD`.

   Returns UUID, or nil if no commits yet."
  [idx]
  (p/current-commit idx))

(defn parents
  "Get parent commit IDs. Like `git rev-parse HEAD^@`.

   Returns set of parent commit IDs (UUIDs).
   First commit has #{:main} or similar branch keyword as parent."
  [idx]
  (when-let [edge-store (p/raw-storage idx)]
    (let [branch (p/current-branch idx)
          snapshot (k/get edge-store branch nil {:sync? true})]
      (:parents snapshot #{}))))

(defn commit-info
  "Get metadata for a commit. Like `git show --stat <commit>`.

   Args:
     idx       - Index (uses its store)
     commit-id - UUID of commit

   Returns:
     {:commit-id    #uuid \"...\"
      :parents      #{...}
      :created-at   #inst \"...\"
      :branch       :main
      :vector-count 1000
      :deleted-count 50}"
  [idx commit-id]
  (when-let [edge-store (p/raw-storage idx)]
    (when-let [snapshot (k/get edge-store commit-id nil {:sync? true})]
      {:commit-id (:commit-id snapshot)
       :parents (:parents snapshot)
       :created-at (:created-at snapshot)
       :updated-at (:updated-at snapshot)
       :branch (:branch snapshot)
       :vector-count (:branch-vector-count snapshot)
       :deleted-count (:branch-deleted-count snapshot)})))

;; -----------------------------------------------------------------------------
;; History & Graph Operations

(defn history
  "Get commit history for current branch. Like `git log`.

   Walks parent chain from current commit to root.
   Returns seq of commit snapshots, most recent first.

   Example:
     (history idx)
     ; => [{:commit-id #uuid \"...\" :parents #{...} ...}
     ;     {:commit-id #uuid \"...\" :parents #{...} ...}]"
  [idx]
  (when-let [edge-store (p/raw-storage idx)]
    (let [branch (p/current-branch idx)]
      (loop [ref branch
             commits []]
        ;; Commits are stored in edge-store
        (if-let [snapshot (k/get edge-store ref nil {:sync? true})]
          (let [parents (:parents snapshot #{})
                parent-commit (first parents)]
            (if parent-commit
              (recur parent-commit (conj commits snapshot))
              ;; No more parents (empty set), this is the root commit
              (conj commits snapshot)))
          ;; Ref not found
          commits)))))

(defn ancestors
  "Get all ancestor commits. Like `git rev-list HEAD`.

   Returns seq of commit IDs (UUIDs), most recent first.
   Optionally from a specific commit.

   Args:
     idx            - Index
     commit-id      - Optional starting commit (default: current)"
  ([idx] (ancestors idx (get-commit-id idx)))
  ([idx commit-id]
   (when-let [edge-store (p/raw-storage idx)]
     (loop [queue [commit-id]
            visited #{}
            result []]
       (if (empty? queue)
         result
         (let [current (first queue)]
           (if (or (nil? current) (visited current))
             (recur (rest queue) visited result)
             (if-let [snapshot (k/get edge-store current nil {:sync? true})]
               (let [parent-commits (:parents snapshot #{})]
                 (recur (concat (rest queue) parent-commits)
                        (conj visited current)
                        (conj result current)))
               (recur (rest queue) (conj visited current) result)))))))))

(defn ancestor?
  "Check if commit A is ancestor of commit B. Like `git merge-base --is-ancestor`.

   Returns true if ancestor-id is reachable from descendant-id."
  [idx ancestor-id descendant-id]
  (let [descendant-ancestors (set (ancestors idx descendant-id))]
    (contains? descendant-ancestors ancestor-id)))

(defn common-ancestor
  "Find common ancestor of two commits. Like `git merge-base`.

   Returns commit ID of most recent common ancestor, or nil if none."
  [idx commit-a commit-b]
  (let [ancestors-a (set (ancestors idx commit-a))]
    (first (filter ancestors-a (ancestors idx commit-b)))))

(defn commit-graph
  "Get full commit DAG for visualization.

   Returns:
     {:nodes    {commit-id {:parents #{...} :created-at ... :branch ...}}
      :branches {:main commit-id-1 :feature commit-id-2}
      :roots    #{first-commit-id}}"
  [idx]
  (when-let [edge-store (p/raw-storage idx)]
    (let [all-branches (k/get edge-store :branches #{} {:sync? true})]
      (loop [queue (vec all-branches)
             visited #{}
             nodes {}]
        (if (empty? queue)
          {:nodes nodes
           :branches (into {}
                           (for [b all-branches
                                 :let [snap (k/get edge-store b nil {:sync? true})]
                                 :when snap]
                             [b (:commit-id snap)]))
           :roots (set (for [[id {:keys [parents]}] nodes
                             :when (empty? parents)]
                         id))}
          (let [ref (first queue)]
            (if (or (nil? ref) (visited ref))
              (recur (rest queue) visited nodes)
              (if-let [snapshot (k/get edge-store ref nil {:sync? true})]
                (let [commit-id (:commit-id snapshot)
                      parent-set (:parents snapshot #{})]
                  (recur (into (rest queue) parent-set)
                         (conj visited ref)
                         (if commit-id
                           (assoc nodes commit-id
                                  {:parents parent-set
                                   :created-at (:created-at snapshot)
                                   :branch (:branch snapshot)
                                   :vector-count (:branch-vector-count snapshot)})
                           nodes)))
                (recur (rest queue) (conj visited ref) nodes)))))))))

;; -----------------------------------------------------------------------------
;; Advanced Operations

(defn reset!
  "Reset current branch to a different commit. Like `git reset --hard`.

   Moves branch head to point at specified commit.
   Returns new index connected to that state.

   WARNING: This is a dangerous operation. Commits after the target
   become unreachable (will be GC'd eventually).

   Args:
     idx       - Index
     commit-id - Target commit UUID

   Example:
     ;; Reset to previous commit
     (def old-idx (reset! idx previous-commit-id))"
  [idx commit-id]
  (let [edge-store (p/raw-storage idx)
        branch (p/current-branch idx)
        snapshot (k/get edge-store commit-id nil {:sync? true})]
    (when-not snapshot
      (throw (ex-info "Commit not found" {:commit-id commit-id})))
    ;; Update branch head to point to this commit
    (k/assoc edge-store branch snapshot {:sync? true})
    ;; Return index connected to this state
    (p/restore-index snapshot edge-store {:mmap-dir (p/mmap-dir idx)})))

(defn merge!
  "Merge vectors from source branch. Like `git merge`.

   Copies specified vectors from source branch into current index
   and creates a merge commit with multiple parents.

   Unlike git, vector merge doesn't auto-resolve - user specifies what to copy.
   No compaction performed; use `compact` separately if needed.

   Args:
     idx           - Target index (must have been synced at least once)
     source-branch - Branch keyword to merge from
     opts          - Options map:
                     :ids     - Coll of external-ids to copy from source
                                Use :all to copy all vectors
                     :parents - Optional explicit parent set (advanced)
                     :message - Optional commit message string

   Vectors already present on the target (by external-id) are skipped.
   Creates a merge commit with both branches as parents.

   Examples:
     ;; Merge all vectors from feature branch
     (merge! idx :feature {:ids :all})

     ;; Merge specific vectors
     (merge! idx :feature {:ids [\"doc-1\" \"doc-2\" \"doc-3\"]})

     ;; Merge with explicit parents (advanced)
     (merge! idx :feature {:ids :all
                           :parents #{commit-a commit-b commit-c}})

   Returns:
     New synced index with merged vectors and merge commit"
  [idx source-branch {:keys [ids parents message]}]
  (when-not ids
    (throw (ex-info "merge! requires :ids option" {:hint "Use :all or a collection of external-ids"})))
  (let [edge-store (p/raw-storage idx)
        ;; Load source branch snapshot
        source-snapshot (k/get edge-store source-branch nil {:sync? true})
        _ (when-not source-snapshot
            (throw (ex-info "Source branch not found" {:branch source-branch})))
        ;; Restore source index
        source-idx (p/restore-index source-snapshot edge-store
                                    {:mmap-dir (p/mmap-dir idx)})
        ;; Determine which IDs to copy
        ids-to-copy (if (= ids :all)
                      (keep :external-id (seq (p/external-id-index source-idx)))
                      ids)
        ;; Copy vectors from source to target, skipping duplicates
        merged-idx (reduce (fn [acc ext-id]
                             ;; Skip if already exists in target
                             (if (when-let [entry (first (filter #(= (:external-id %) ext-id)
                                                                 (seq (p/external-id-index acc))))]
                                   (:node-id entry))
                               acc
                               (if-let [internal-id (when-let [entry (first (filter #(= (:external-id %) ext-id)
                                                                                    (seq (p/external-id-index source-idx))))]
                                                      (:node-id entry))]
                                 (let [vec (p/get-vector source-idx internal-id)
                                       meta (p/get-metadata source-idx internal-id)]
                                   (if vec
                                     (p/insert acc vec (assoc meta :external-id ext-id))
                                     acc))
                                 acc)))
                           idx
                           ids-to-copy)
        ;; Determine parents for merge commit
        merge-parents (or parents
                          #{(p/current-commit idx)
                            (:commit-id source-snapshot)})
        ;; Sync with merge parents to create the merge commit - returns channel
        sync-chan (p/sync! merged-idx {:parents merge-parents
                                       :message message})]
    ;; Close source index and return channel
    (p/close! source-idx)
    ;; Return channel that delivers synced index
    (a/go
      (a/<! sync-chan))))
