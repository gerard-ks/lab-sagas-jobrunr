package com.poc.modules.order.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OrderInfraMaintenanceJob {

    private static final Logger log = LoggerFactory.getLogger(OrderInfraMaintenanceJob.class);

    private final JdbcClient jdbcClient;
    private final OrderProperties properties;

    public OrderInfraMaintenanceJob(JdbcClient jdbcClient, OrderProperties properties) {
        this.jdbcClient = jdbcClient;
        this.properties = properties;
    }

    @Scheduled(fixedDelay = 3600000)
    @Transactional
    public void purgeFinishedSagas() {
        try {
            // Sécurité Multi-instances
            boolean acquired = Boolean.TRUE.equals(jdbcClient.sql("SELECT pg_try_advisory_xact_lock(30001)")
                    .query(Boolean.class).single());
            if (!acquired) return;


            long seconds = properties.saga().retentionDelay().getSeconds();
            int deleted = jdbcClient.sql(
                            "DELETE FROM order_schema.saga_instances " +
                                    "WHERE status IN ('COMPLETED', 'FAILED', 'TIMED_OUT') " +
                                    "AND updated_at < NOW() - (? * INTERVAL '1 second')"
                    )
                    .param(seconds)
                    .update();
            if (deleted > 0) log.info("[Order Maintenance] {} vieilles sagas supprimées.", deleted);
        } catch (Exception e) {
            log.error("Erreur lors de la purge des Sagas", e);
        }
    }

    @Scheduled(fixedDelay = 3600000)
    @Transactional
    public void purgeOldInbox() {
        try {
            // Sécurité Multi-instances
            boolean acquired = Boolean.TRUE.equals(jdbcClient.sql("SELECT pg_try_advisory_xact_lock(30002)")
                    .query(Boolean.class).single());
            if (!acquired) return;


            long seconds = properties.inbox().purgeDelay().getSeconds();
            int deleted = jdbcClient.sql(
                            "DELETE FROM order_schema.inbox WHERE processed_at < NOW() - (? * INTERVAL '1 second')"
                    )
                    .param(seconds)
                    .update();
            if (deleted > 0) log.info("[Order Maintenance] {} verrous Inbox supprimés.", deleted);
        } catch (Exception e) {
            log.error("Erreur lors de la purge de l'Inbox", e);
        }
    }

    @Scheduled(fixedDelay = 3600000)
    @Transactional
    public void purgeOldOutbox() {
        try {
            // Sécurité Multi-instances
            boolean acquired = Boolean.TRUE.equals(jdbcClient.sql("SELECT pg_try_advisory_xact_lock(30003)")
                    .query(Boolean.class).single());
            if (!acquired) return;

            long seconds = properties.outbox().retentionDelay().getSeconds();
            int deleted = jdbcClient.sql(
                            "DELETE FROM order_schema.outbox WHERE status = 'SENT' AND sent_at < NOW() - (? * INTERVAL '1 second')"
                    )
                    .param(seconds)
                    .update();
            if (deleted > 0) log.info("[Order Maintenance] {} vieux messages Outbox supprimés.", deleted);
        } catch (Exception e) {
            log.error("Erreur lors de la purge de l'Outbox", e);
        }
    }
}