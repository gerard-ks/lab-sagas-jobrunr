package com.poc.modules.notification.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class NotificationInboxPurgeJob {

    private static final Logger log = LoggerFactory.getLogger(NotificationInboxPurgeJob.class);
    private final JdbcClient jdbcClient;

    public NotificationInboxPurgeJob(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    // Tourne toutes les heures
    @Scheduled(fixedDelayString = "3600000")
    public void purgeOldInboxMessages() {
        log.info("[Notification] Purge des vieux messages Inbox...");

        try {
            // Sécurité Multi-instances
            boolean acquired = Boolean.TRUE.equals(jdbcClient.sql("SELECT pg_try_advisory_xact_lock(20001)")
                    .query(Boolean.class).single());
            if (!acquired) return;


            // Utilisation d'un intervalle SQL natif statique (Sûr car pas de concaténation de variable)
            int deletedRows = jdbcClient.sql(
                    "DELETE FROM notification_schema.inbox WHERE processed_at < NOW() - INTERVAL '24 hours'"
            ).update();

            if (deletedRows > 0) {
                log.info("[Notification] Purge terminée. {} messages supprimés.", deletedRows);
            }
        } catch (Exception e) {
            log.error("Erreur lors de la purge de l'Inbox Notification", e);
        }
    }
}
