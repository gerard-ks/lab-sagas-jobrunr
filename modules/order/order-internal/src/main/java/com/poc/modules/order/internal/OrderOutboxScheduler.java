package com.poc.modules.order.internal;

import com.poc.platform.messaging.OutboxRelay;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OrderOutboxScheduler {

    private final OutboxRelay outboxRelay;

    public OrderOutboxScheduler(OutboxRelay outboxRelay) {
        this.outboxRelay = outboxRelay;
    }

    // Le module Order utilise sa propre configuration YAML (ou 2000ms par défaut)
    @Scheduled(fixedDelayString = "${order.outbox.delay:2000}")
    public void processOutbox() {
        // Le module Order délègue le travail à la Platform, mais lui demande
        // de lire UNIQUEMENT la table Outbox de l'order_schema.
        outboxRelay.relayMessages("order_schema");
    }
}
