package org.popcraft.bolt.listeners.adapter;

import io.papermc.paper.event.entity.ItemTransportingEntityValidateTargetEvent;
import org.bukkit.block.Block;
import org.bukkit.entity.Entity;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.popcraft.bolt.BoltPlugin;
import org.popcraft.bolt.data.SensitiveOperation;

import java.util.function.Consumer;

public record ItemTransportingEntityValidateTargetEventListener(BoltPlugin plugin, Handler handler) implements Listener {
    public static boolean canUse() {
        try {
            Class.forName("io.papermc.paper.event.entity.ItemTransportingEntityValidateTargetEvent");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    @EventHandler
    public void onItemTransportingEntityValidateTarget(final ItemTransportingEntityValidateTargetEvent e) {
        if (!plugin.allowsSensitiveOperation(SensitiveOperation.HOPPER)) {
            e.setAllowed(false);
            return;
        }
        handler.accept(e.getEntity(), e.getBlock(), e::setAllowed);
    }

    @FunctionalInterface
    public interface Handler {
        void accept(Entity entity, Block block, Consumer<Boolean> setAllowed);
    }
}
