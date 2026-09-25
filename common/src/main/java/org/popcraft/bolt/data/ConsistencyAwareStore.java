package org.popcraft.bolt.data;

public interface ConsistencyAwareStore {
    ConsistencyHealth.Snapshot healthSnapshot();

    boolean allows(SensitiveOperation operation);
}
