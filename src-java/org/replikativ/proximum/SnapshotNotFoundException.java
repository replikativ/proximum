package org.replikativ.proximum;

import java.util.UUID;

/**
 * Thrown when a requested snapshot or historical version cannot be found.
 */
public class SnapshotNotFoundException extends ProximumException {

    private final UUID commitId;

    /**
     * Create a new snapshot not found exception.
     *
     * @param commitId the commit-id that was not found
     */
    public SnapshotNotFoundException(UUID commitId) {
        super("Snapshot not found: " + commitId);
        this.commitId = commitId;
    }

    /**
     * Get the commit-id that was not found.
     *
     * @return commit-id
     */
    public UUID getCommitId() {
        return commitId;
    }
}
