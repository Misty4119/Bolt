package org.popcraft.bolt.data;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface WorldRegistryStore {
    CompletableFuture<WorldIdentity> registerWorld(WorldIdentity identity);

    CompletableFuture<WorldIdentity> loadWorld(UUID worldUuid);
}
