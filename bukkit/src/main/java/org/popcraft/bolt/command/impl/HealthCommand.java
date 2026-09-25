package org.popcraft.bolt.command.impl;

import net.kyori.adventure.text.Component;
import org.bukkit.command.CommandSender;
import org.popcraft.bolt.BoltPlugin;
import org.popcraft.bolt.command.Arguments;
import org.popcraft.bolt.command.BoltCommand;

import java.util.List;

public final class HealthCommand extends BoltCommand {
    public HealthCommand(final BoltPlugin plugin) {
        super(plugin);
    }

    @Override
    public void execute(final CommandSender sender, final Arguments arguments) {
        final var health = plugin.consistencyHealth();
        final String error = health.lastError() == null ? "none" : health.lastError();
        sender.sendMessage(Component.text("Bolt health: state=%s, lastError=%s, lastSuccessfulProbeMillis=%d"
                .formatted(health.state(), error, health.lastSuccessfulProbeMillis())));
    }

    @Override
    public List<String> suggestions(final CommandSender sender, final Arguments arguments) {
        return List.of();
    }

    @Override
    public void shortHelp(final CommandSender sender, final Arguments arguments) {
        sender.sendMessage(Component.text("/bolt health - show SQL/Redis consistency health"));
    }

    @Override
    public void longHelp(final CommandSender sender, final Arguments arguments) {
        sender.sendMessage(Component.text("Shows the fail-closed consistency state used by shared authorization, portal, hopper, and mutation checks."));
    }
}
