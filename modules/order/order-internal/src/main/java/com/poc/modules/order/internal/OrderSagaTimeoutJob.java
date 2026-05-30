package com.poc.modules.order.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.modules.order.contract.events.OrderCancelledEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.UUID;

@Component
public class OrderSagaTimeoutJob {
    private static final Logger log = LoggerFactory.getLogger(OrderSagaTimeoutJob.class);

    private final JdbcClient jdbcClient;
    private final TransactionTemplate txTemplate;
    private final ObjectMapper objectMapper;

    public OrderSagaTimeoutJob(JdbcClient jdbcClient, TransactionTemplate txTemplate, ObjectMapper objectMapper) {
        this.jdbcClient = jdbcClient;
        this.txTemplate = txTemplate;
        this.objectMapper = objectMapper;
    }

    // Le Job tourne toutes les minutes
    @Scheduled(fixedDelay = 60000)
    public void cancelStuckSagas() {
        log.debug("[Saga Timeout] Analyse des sagas expirées...");

        int processedCount = 0;
        boolean hasMore = true;

        // Utilisation d'une boucle paginée pour éviter de surcharger la mémoire
        while (hasMore && processedCount < 500) { // Sécurité max : 500 sagas par itération du cron
            List<UUID> expiredSagaIds = fetchExpiredSagasChunk(100); // Paquets de 100

            if (expiredSagaIds.isEmpty()) {
                hasMore = false;
                continue;
            }

            for (UUID sagaId : expiredSagaIds) {
                boolean success = cancelSingleSaga(sagaId);
                if (success) processedCount++;
            }
        }

        if (processedCount > 0) {
            log.info("[Saga Timeout] Fin du traitement. Total de sagas annulées : {}", processedCount);
        }
    }

    private List<UUID> fetchExpiredSagasChunk(int limit) {
        // Simple SELECT non bloquant pour obtenir un lot de candidats potentiels
        return jdbcClient.sql(
                        "SELECT saga_id FROM order_schema.saga_instances " +
                                "WHERE status = 'RUNNING' AND timeout_at < NOW() " +
                                "LIMIT ?")
                .param(limit)
                .query(UUID.class)
                .list();
    }

    private boolean cancelSingleSaga(UUID sagaId) {
        try {
            // Chaque saga est traitée de manière isolée et commitée immédiatement
            return Boolean.TRUE.equals(txTemplate.execute(status -> {

                // 1. FOR UPDATE SKIP LOCKED + Extraction directe de l'OrderId
                // Si une autre instance traite déjà cette saga, l'enregistrement est sauté (retourne vide)
                var sagaData = jdbcClient.sql(
                                "SELECT status, (context->>'orderId')::uuid as order_id " +
                                        "FROM order_schema.saga_instances " +
                                        "WHERE saga_id = ? AND status = 'RUNNING' FOR UPDATE SKIP LOCKED")
                        .param(sagaId)
                        .query((rs, rowNum) -> new SagaRow(
                                rs.getString("status"),
                                UUID.fromString(rs.getString("order_id"))
                        ))
                        .optional();

                // Si vide, c'est que la saga est déjà verrouillée ailleurs ou n'est plus RUNNING
                if (sagaData.isEmpty()) {
                    return false;
                }

                UUID orderId = sagaData.get().orderId();
                log.error("[Saga Timeout] Verrou acquis. Annulation forcée de la Saga {} (Commande {})", sagaId, orderId);

                // 2. Mise à jour de l'état de la Saga
                jdbcClient.sql(
                                "UPDATE order_schema.saga_instances " +
                                        "SET status = 'TIMED_OUT', last_step = 'TimeoutReached', updated_at = NOW() " +
                                        "WHERE saga_id = ?")
                        .param(sagaId)
                        .update();

                // 3. Annulation de l'Entité Métier
                jdbcClient.sql("UPDATE order_schema.orders SET status = 'CANCELLED', updated_at = NOW() WHERE id = ?")
                        .param(orderId)
                        .update();

                // 4. Publication de l'échec dans l'Outbox transactionnelle
                OrderCancelledEvent cancelEvent = new OrderCancelledEvent(
                        UUID.randomUUID(), orderId, 1, sagaId, null, "SAGA_TIMEOUT"
                );

                publishToOutbox(cancelEvent, orderId, sagaId);

                return true;
            }));
        } catch (Exception e) {
            log.error("[Saga Timeout] Échec de la compensation pour la saga {}", sagaId, e);
            return false;
        }
    }

    private void publishToOutbox(OrderCancelledEvent event, UUID orderId, UUID sagaId) {
        try {
            jdbcClient.sql(
                            "INSERT INTO order_schema.outbox (id, event_type, aggregate_type, aggregate_id, version, correlation_id, payload) " +
                                    "VALUES (?, 'OrderCancelledEvent', 'Order', ?, 1, ?, ?::jsonb)")
                    .param(event.eventId())
                    .param(orderId)
                    .param(sagaId)
                    .param(objectMapper.writeValueAsString(event))
                    .update();
        } catch (Exception ex) {
            throw new RuntimeException("Erreur d'écriture Outbox lors du timeout", ex);
        }
    }

    // DTO local pour mapper le résultat optimisé
    private record SagaRow(String status, UUID orderId) {}
}
