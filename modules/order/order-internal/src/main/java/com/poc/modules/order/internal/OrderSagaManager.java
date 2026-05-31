package com.poc.modules.order.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.poc.modules.inventory.contract.events.StockFailedEvent;
import com.poc.modules.inventory.contract.events.StockReservedEvent;
import com.poc.modules.order.contract.events.OrderCancelledEvent;
import com.poc.modules.order.contract.events.OrderValidatedEvent;
import com.poc.modules.order.contract.events.ReserveStockCommand;
import com.poc.platform.messaging.DuplicateMessageException;
import com.poc.platform.messaging.InboxFilter;
import com.poc.shared.event.DomainEvent;
import io.nats.client.*;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Consumer;

@Service
public class OrderSagaManager {
    private static final Logger log = LoggerFactory.getLogger(OrderSagaManager.class);

    private final JdbcClient jdbcClient;
    private final TransactionTemplate txTemplate;
    private final Connection natsConnection;
    private final InboxFilter inboxFilter;
    private final ObjectMapper objectMapper;
    private final OrderProperties properties;

    public OrderSagaManager(JdbcClient jdbcClient, TransactionTemplate txTemplate,
                            Connection natsConnection, InboxFilter inboxFilter, ObjectMapper objectMapper, OrderProperties properties) {
        this.jdbcClient = jdbcClient;
        this.txTemplate = txTemplate;
        this.natsConnection = natsConnection;
        this.inboxFilter = inboxFilter;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    // ===================================================================================
    // ÉTAPE 1 : DÉMARRAGE DE LA SAGA (Sécurisé dans une transaction atomique)
    // ===================================================================================
    public void startSaga(UUID orderId, String productId, UUID correlationId) {
        log.info("[Saga] Démarrage de la Saga pour la commande {} (Correlation: {})", orderId, correlationId);

        // L'ensemble du démarrage doit être ACID
        txTemplate.executeWithoutResult(status -> {
            ObjectNode context = objectMapper.createObjectNode();
            context.put("orderId", orderId.toString());
            context.put("productId", productId);

            // 1. Persistance de l'état initial
            jdbcClient.sql(
                            "INSERT INTO order_schema.saga_instances " +
                                    "(saga_id, saga_type, status, timeout_at, last_step, context) " +
                                    "VALUES (?, 'OrderCreationSaga', 'RUNNING', NOW() + INTERVAL '5 minutes', 'Started', ?::jsonb)")
                    .param(correlationId)
                    .param(context.toString())
                    .update();

            // 2. Publication Outbox atomique
            ReserveStockCommand cmd = new ReserveStockCommand(
                    UUID.randomUUID(), orderId, 1, correlationId, null, productId
            );
            publishToOutbox(cmd, "ReserveStockCommand", correlationId, null);
        });
    }

    // ===================================================================================
    // ÉTAPE 2 : ÉCOUTE DES RÉPONSES
    // ===================================================================================
    @PostConstruct
    public void subscribeToInventoryResponses() {
        try {
            Dispatcher dispatcher = natsConnection.createDispatcher();
            JetStream js = natsConnection.jetStream();

            // Consumer 1 : Succès
            PushSubscribeOptions optionsReserved = PushSubscribeOptions.builder()
                    .durable("order-saga-reserved-consumer")
                    .configuration(io.nats.client.api.ConsumerConfiguration.builder().maxDeliver(3).build())
                    .build();
            js.subscribe("events.StockReservedEvent", dispatcher, this::handleStockReserved, false, optionsReserved); // Le 'false' signifie autoAck = false

            // Consumer 2 : Échec
            PushSubscribeOptions optionsFailed = PushSubscribeOptions.builder()
                    .durable("order-saga-failed-consumer")
                    .configuration(io.nats.client.api.ConsumerConfiguration.builder().maxDeliver(3).build())
                    .build();
            js.subscribe("events.StockFailedEvent", dispatcher, this::handleStockFailed, false, optionsFailed); // Le 'false' signifie autoAck = false
        } catch (Exception e) {
            log.error("[Saga] Erreur d'abonnement NATS", e);
        }
    }

    // --- LE HAPPY PATH (SUCCÈS CORRIGÉ) ---
    private void handleStockReserved(Message msg) {
        processSagaResponse(msg, StockReservedEvent.class, (event) -> {
            log.info("[Saga] Succès du stock reçu pour Saga {}", event.correlationId());

            // 1. Verrouiller la saga en PREMIER et valider son état
            String currentStatus = lockAndGetStatus(event.correlationId());
            if (!"RUNNING".equals(currentStatus)) {
                log.warn("[Saga] Saga {} déjà traitée (Status actuel: {}).", event.correlationId(), currentStatus);
                return;
            }

            // 2. Mise à jour des entités métier (Toujours exécuter les updates dans le même ordre d'entités pour éviter les deadlocks)
            jdbcClient.sql(
                            "UPDATE order_schema.orders SET status = 'VALIDATED', updated_at = NOW() WHERE id = " +
                                    "(SELECT (context->>'orderId')::uuid FROM order_schema.saga_instances WHERE saga_id = ?)")
                    .param(event.correlationId())
                    .update();

            // 3. Clôture de la Saga
            updateSagaStatus(event.correlationId(), "COMPLETED", "StockReserved");

            // 4. Notification sortante via Outbox
            OrderValidatedEvent finalEvent = new OrderValidatedEvent(
                    UUID.randomUUID(), UUID.randomUUID(), 1, event.correlationId(), event.eventId()
            );
            publishToOutbox(finalEvent, "OrderValidatedEvent", event.correlationId(), event.causationId());
        });
    }

    // --- LA COMPENSATION (ÉCHEC CORRIGÉ) ---
    private void handleStockFailed(Message msg) {
        processSagaResponse(msg, StockFailedEvent.class, (event) -> {
            log.error("[Saga] Échec du stock pour Saga {} ! Annulation de la commande.", event.correlationId());

            String currentStatus = lockAndGetStatus(event.correlationId());
            if (!"RUNNING".equals(currentStatus)) return;

            // 1. Rollback métier
            jdbcClient.sql(
                            "UPDATE order_schema.orders SET status = 'CANCELLED', updated_at = NOW() WHERE id = " +
                                    "(SELECT (context->>'orderId')::uuid FROM order_schema.saga_instances WHERE saga_id = ?)")
                    .param(event.correlationId())
                    .update();

            // 2. Passage de la saga en échec
            updateSagaStatus(event.correlationId(), "FAILED", "StockFailed");

            // 3. Notification d'échec
            OrderCancelledEvent finalEvent = new OrderCancelledEvent(
                    UUID.randomUUID(), UUID.randomUUID(), 1, event.correlationId(), event.eventId(), event.reason()
            );
            publishToOutbox(finalEvent, "OrderCancelledEvent", event.correlationId(), event.causationId());
        });
    }

    // ===================================================================================
    // MÉTHODES UTILITAIRES (Sécurisées)
    // ===================================================================================
    private <T extends DomainEvent> void processSagaResponse(Message msg, Class<T> eventClass, Consumer<T> logic) {
        T event = null;
        try {
            // 1. Désérialisation de l'événement
            event = objectMapper.readValue(new String(msg.getData()), eventClass);
            final T finalEvent = event;

            // 2. Exécution du filtre d'idempotence et de la logique métier
            // L'inbox filter et la transaction globale entourent tout le traitement
            inboxFilter.processSafely("order_schema", finalEvent.eventId(), "OrderSagaManager", () -> {
                txTemplate.executeWithoutResult(status -> logic.accept(finalEvent));
            });

            // 3. Succès nominal : On acquitte le message auprès de NATS
            msg.ack();
        }  catch (DuplicateMessageException e) {
            // CAS DU DOUBLON : L'événement a déjà été traité avec succès par le passé.
            // On effectue un ACK pour dire à NATS de ne plus nous renvoyer ce message.
            UUID eventId = (event != null) ? event.eventId() : null;
            log.info("[Saga] Doublon détecté pour l'événement {} dans l'Inbox. Message acquitté (ACK) pour stopper la boucle.", eventId);
            msg.ack();
        } catch (Exception e) {
            // CAS D'ÉCHEC RÉEL : Erreur technique, crash base de données, deadlock, etc.
            // La transaction a été annulée. On envoie un NAK pour rejouer le message plus tard.
            log.error("[Saga] Vraie erreur de traitement lors de la réponse de la Saga. Envoi d'un NAK avec délai.", e);

            Duration nakDuration = properties.consumer() != null ? properties.consumer().nakDelay() : Duration.ofSeconds(2);
            long delayMillis = nakDuration.toMillis();

            msg.nakWithDelay(delayMillis);
        }
    }

    private String lockAndGetStatus(UUID sagaId) {
        // Le FOR UPDATE force l'isolation stricte sur la ligne de cette saga spécifique
        return jdbcClient.sql("SELECT status FROM order_schema.saga_instances WHERE saga_id = ? FOR UPDATE")
                .param(sagaId).query(String.class).single();
    }

    private void updateSagaStatus(UUID sagaId, String newStatus, String step) {
        jdbcClient.sql("UPDATE order_schema.saga_instances SET status = ?, last_step = ?, updated_at = NOW() WHERE saga_id = ?")
                .param(newStatus).param(step).param(sagaId).update();
    }

    private void publishToOutbox(Object eventObj, String type, UUID correlationId, UUID causationId) {
        try {
            jdbcClient.sql(
                            "INSERT INTO order_schema.outbox " +
                                    "(id, event_type, aggregate_type, aggregate_id, version, correlation_id, causation_id, payload) " +
                                    "VALUES (?, ?, 'OrderSaga', ?, 1, ?, ?, ?::jsonb)") // 6 points d'interrogation
                    .param(UUID.randomUUID())
                    .param(type)
                    .param(correlationId) // aggregate_id
                    .param(correlationId) // correlation_id
                    .param(causationId)   // causation_id (peut être null)
                    .param(objectMapper.writeValueAsString(eventObj)) // payload
                    .update();
        } catch (Exception e) {
            throw new RuntimeException("Échec de l'écriture dans l'Outbox locale", e);
        }
    }
}
