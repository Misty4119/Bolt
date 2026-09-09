package org.popcraft.bolt.data;

public final class StaleWriteException extends RuntimeException {
    public StaleWriteException(final String aggregateType, final String aggregateId, final long expectedVersion) {
        super("Stale " + aggregateType + " write for " + aggregateId + "; expected version " + expectedVersion);
    }
}
