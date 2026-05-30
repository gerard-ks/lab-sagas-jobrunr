package com.poc.modules.inventory.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class InventoryInfraMaintenanceJob {

    private static final Logger log = LoggerFactory.getLogger(InventoryInfraMaintenanceJob.class);
    private final JdbcClient jdbcClient;
    private final InventoryProperties properties;

    public InventoryInfraMaintenanceJob(JdbcClient jdbcClient, InventoryProperties properties) {
        this.jdbcClient = jdbcClient;
        this.properties = properties;
    }


    // 1. PURGE DE L'INBOX
    // Protège l'idempotence des commandes entrantes (ReserveStockCommand)
    @Scheduled(fixedDelay = 3600000)
    public void purgeOldInbox() {
        try {
            // Sécurité Multi-instances
            boolean acquired = Boolean.TRUE.equals(jdbcClient.sql("SELECT pg_try_advisory_xact_lock(10001)")
                    .query(Boolean.class).single());
            if (!acquired) return;

            // Lecture dynamique depuis le YAML
            long secondsToKeep = properties.inbox().purgeDelay().getSeconds();

            int deleted = jdbcClient.sql(
                            "DELETE FROM inventory_schema.inbox WHERE processed_at < NOW() - (? * INTERVAL '1 second')"
                    )
                    .param(secondsToKeep)
                    .update();

            if (deleted > 0) log.info("[Inventory Maintenance] {} verrous Inbox supprimés.", deleted);
        } catch (Exception e) {
            log.error("Erreur lors de la purge de l'Inbox Inventory", e);
        }
    }

    // 2. PURGE DE L'OUTBOX
    // Nettoie les réponses (StockReservedEvent) qui ont déjà été envoyées à NATS
    @Scheduled(fixedDelayString = "3600000") // Ex: toutes les heures
    public void purgeOldOutbox() {
        try {
            // Sécurité Multi-instances
            boolean acquired = Boolean.TRUE.equals(jdbcClient.sql("SELECT pg_try_advisory_xact_lock(10002)")
                    .query(Boolean.class).single());
            if (!acquired) return;

            // Lecture dynamique depuis le YAML
            long secondsToKeep = properties.outbox().retentionDelay().getSeconds();

            int deleted = jdbcClient.sql(
                            "DELETE FROM inventory_schema.outbox WHERE status = 'SENT' AND sent_at < NOW() - (? * INTERVAL '1 second')"
                    )
                    .param(secondsToKeep)
                    .update();

            if (deleted > 0) log.info("[Inventory Maintenance] {} vieux messages Outbox supprimés.", deleted);
        } catch (Exception e) {
            log.error("Erreur lors de la purge de l'Outbox Inventory", e);
        }
    }
}
