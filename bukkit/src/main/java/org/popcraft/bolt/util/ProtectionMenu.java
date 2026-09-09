package org.popcraft.bolt.util;

import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import net.kyori.adventure.text.format.TextDecoration;
import org.bukkit.Bukkit;
import org.bukkit.Material;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.inventory.InventoryClickEvent;
import org.bukkit.event.inventory.InventoryCloseEvent;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.InventoryHolder;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.ItemMeta;
import org.popcraft.bolt.BoltPlugin;
import org.popcraft.bolt.protection.BlockProtection;
import org.popcraft.bolt.protection.Protection;

import java.util.Map;
import java.util.UUID;

/**
 * The first GUI surface for protection management. It is intentionally read-mostly:
 * mutations continue through the existing permission and event pipeline until the
 * ACL editor has a complete player-selection flow.
 */
public final class ProtectionMenu implements Listener {
    private static final int SIZE = 27;
    private static final int CLOSE_SLOT = 22;
    private final BoltPlugin plugin;

    public ProtectionMenu(final BoltPlugin plugin) {
        this.plugin = plugin;
    }

    public void open(final Player player, final Protection protection) {
        if (!(protection instanceof BlockProtection blockProtection) || !player.getUniqueId().equals(protection.getOwner())) {
            return;
        }
        final MenuHolder holder = new MenuHolder();
        final Inventory inventory = Bukkit.createInventory(holder, SIZE, Component.text("Bolt Protection", NamedTextColor.DARK_AQUA));
        holder.inventory = inventory;
        inventory.setItem(4, item(Material.CHEST, Component.text("Protected block", NamedTextColor.AQUA),
                Component.text(blockProtection.getBlock() + " @ " + blockProtection.getWorld(), NamedTextColor.GRAY),
                Component.text("Version " + blockProtection.getVersion(), NamedTextColor.DARK_GRAY)));
        inventory.setItem(11, item(Material.PLAYER_HEAD, Component.text("Owner", NamedTextColor.GREEN),
                Component.text(player.getName(), NamedTextColor.GRAY)));
        inventory.setItem(13, item(Material.WRITABLE_BOOK, Component.text("Access entries", NamedTextColor.YELLOW),
                Component.text(Integer.toString(protection.getAccess().size()), NamedTextColor.GRAY),
                Component.text("Use /bolt edit to change friends", NamedTextColor.DARK_GRAY)));
        inventory.setItem(15, item(Material.HOPPER, Component.text("Automation permissions", NamedTextColor.GOLD),
                Component.text("Hopper insert/extract are evaluated separately", NamedTextColor.GRAY),
                Component.text("Detailed filters are managed in the next GUI page", NamedTextColor.DARK_GRAY)));
        inventory.setItem(CLOSE_SLOT, item(Material.BARRIER, Component.text("Close", NamedTextColor.RED)));
        player.openInventory(inventory);
    }

    @EventHandler
    public void onInventoryClick(final InventoryClickEvent event) {
        if (!(event.getWhoClicked() instanceof Player player) || !(event.getView().getTopInventory().getHolder(false) instanceof MenuHolder)) {
            return;
        }
        event.setCancelled(true);
        if (event.getRawSlot() == CLOSE_SLOT) {
            player.closeInventory();
        }
    }

    @EventHandler
    public void onInventoryClose(final InventoryCloseEvent event) {
        if (event.getInventory().getHolder(false) instanceof MenuHolder) {
            event.getInventory().clear();
        }
    }

    private static ItemStack item(final Material material, final Component name, final Component... lore) {
        final ItemStack item = new ItemStack(material);
        final ItemMeta meta = item.getItemMeta();
        meta.displayName(name.decoration(TextDecoration.ITALIC, false));
        meta.lore(java.util.List.of(lore));
        item.setItemMeta(meta);
        return item;
    }

    private static final class MenuHolder implements InventoryHolder {
        private Inventory inventory;

        @Override
        public Inventory getInventory() {
            return inventory;
        }
    }
}
