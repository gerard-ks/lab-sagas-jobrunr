package com.poc.modules.inventory.internal;

import com.poc.platform.messaging.OutboxRelay;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class InventoryOutboxScheduler {

    private final OutboxRelay outboxRelay;

    public InventoryOutboxScheduler(OutboxRelay outboxRelay) {
        this.outboxRelay = outboxRelay;
    }

    // Le module Inventory utilise sa propre configuration YAML (ou 2000ms par défaut)
    @Scheduled(fixedDelayString = "${inventory.outbox.delay:2000}")
    public void processOutbox() {
        // Le module Inventory délègue le travail à la Platform, mais lui demande
        // de lire UNIQUEMENT la table Outbox de l'inventory_schema.
        outboxRelay.relayMessages("inventory_schema");
    }
}
