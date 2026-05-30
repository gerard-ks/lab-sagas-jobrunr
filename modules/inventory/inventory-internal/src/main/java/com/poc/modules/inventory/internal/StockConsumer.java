package com.poc.modules.inventory.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.modules.inventory.contract.events.StockFailedEvent;
import com.poc.modules.inventory.contract.events.StockReservedEvent;
import com.poc.modules.order.contract.events.ReserveStockCommand;
import com.poc.platform.messaging.DuplicateMessageException;
import com.poc.platform.messaging.InboxFilter;
import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.UUID;

@Component
public class StockConsumer {
    private static final Logger log = LoggerFactory.getLogger(StockConsumer.class);

    private final Connection natsConnection;
    private final InboxFilter inboxFilter;
    private final JdbcClient jdbcClient;
    private final TransactionTemplate txTemplate;
    private final ObjectMapper objectMapper;
    private final InventoryProperties properties;

    public StockConsumer(Connection natsConnection, InboxFilter inboxFilter, JdbcClient jdbcClient,
                         TransactionTemplate txTemplate, ObjectMapper objectMapper, InventoryProperties properties) {
        this.natsConnection = natsConnection;
        this.inboxFilter = inboxFilter;
        this.jdbcClient = jdbcClient;
        this.txTemplate = txTemplate;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    @PostConstruct
    public void startListening() {
        try {
            JetStream js = natsConnection.jetStream();
            Dispatcher dispatcher = natsConnection.createDispatcher();

            PushSubscribeOptions options = PushSubscribeOptions.builder()
                    .durable("inventory-stock-consumer")
                    .configuration(ConsumerConfiguration.builder()
                            .maxDeliver(3) // Si le message plante 3 fois, NATS arrête de l'envoyer !!
                            .build())
                    .build();

            js.subscribe("events.ReserveStockCommand", dispatcher, this::handleReserveCommand, false, options);
            log.info("[Inventory] En écoute sur NATS (events.ReserveStockCommand)...");
        } catch (Exception e) {
            log.error("[Inventory] Erreur d'abonnement NATS", e);
        }
    }

    private void handleReserveCommand(Message msg) {
        // Variable d'aiguillage pour le comportement NATS en sortie de transaction
        boolean isDuplicate = false;

        try {
            ReserveStockCommand command = objectMapper.readValue(new String(msg.getData()), ReserveStockCommand.class);
            log.info("[Inventory] Commande de réservation reçue pour le produit {} (SagaID: {})", command.productId(), command.correlationId());

            try {
                // Exécution de la transaction globale
                txTemplate.executeWithoutResult(status -> {
                    try {
                        inboxFilter.processSafely("inventory_schema", command.eventId(), "StockConsumer", () -> {

                            // 1. Déduction du stock avec verrou pessimiste (Row Lock)
                            int rowsUpdated = jdbcClient.sql(
                                            "UPDATE inventory_schema.stock " +
                                                    "SET quantity = quantity - 1 " +
                                                    "WHERE product_id = ? AND quantity >= 1")
                                    .param(command.productId())
                                    .update();

                            // 2. Traitement des cas métier (Garantie de réponse Outbox)
                            if (rowsUpdated > 0) {
                                log.info("[Inventory] Stock déduit avec succès ! Envoi de StockReservedEvent.");
                                StockReservedEvent successEvent = new StockReservedEvent(
                                        UUID.randomUUID(), command.aggregateId(), 1, command.correlationId(), command.eventId()
                                );
                                publishToOutbox(successEvent, "StockReservedEvent", command.correlationId(), command.eventId(), command.productId());
                            } else {
                                log.warn("[Inventory] RUPTURE DE STOCK ! Envoi de StockFailedEvent.");
                                StockFailedEvent failedEvent = new StockFailedEvent(
                                        UUID.randomUUID(), command.aggregateId(), 1, command.correlationId(), command.eventId(), "OUT_OF_STOCK"
                                );
                                publishToOutbox(failedEvent, "StockFailedEvent", command.correlationId(), command.eventId(), command.productId());
                            }
                        });
                    } catch (DuplicateMessageException e) {
                        // On attrape le doublon À L'INTÉRIEUR du bloc transactionnel.
                        // On force le rollback pour ne rien modifier en DB, mais on lève un drapeau
                        // pour éviter de déclencher le bloc de crash technique général.
                        status.setRollbackOnly();
                        throw e;
                    }
                });
            } catch (DuplicateMessageException e) {
                // L'exception a été proprement propagée après le marquage en rollback.
                // On bascule le flag à vrai pour acquitter le message auprès de NATS.
                isDuplicate = true;
            }

            if (isDuplicate) {
                log.info("[Inventory] Doublon traité avec succès (Ignoré). Envoi du ACK à NATS.");
            } else {
                log.info("[Inventory] Transaction métier validée et commitée. Envoi du ACK à NATS.");
            }

            // Dans les deux cas (Succès nominal OU Doublon sécurisé), on supprime le message de NATS JetStream
            msg.ack();

        } catch (Exception e) {
            // Vrai crash technique (ex: Postgres déconnecté, JSON corrompu, etc.)
            log.error("[Inventory] Erreur technique majeure détectée. Demande de Retry NATS.", e);
            // 1. On récupère la Duration ou une valeur par défaut de 2 secondes
            Duration nakDuration = properties.consumer() != null ? properties.consumer().nakDelay() : Duration.ofSeconds(2);

            // 2. On extrait les millisecondes sous forme de primitif long pour l'API NATS
            long delayMillis = nakDuration.toMillis();

            // 3. Envoi du NAK à NATS avec le long attendu
            msg.nakWithDelay(delayMillis);
        }
    }

    private void publishToOutbox(Object eventObj, String type, UUID correlationId, UUID causationId, String productId) {
        try {
            // On crée un UUID "propre" à partir de la String productId pour rester constant
            UUID aggregateId = UUID.nameUUIDFromBytes(productId.getBytes());

            jdbcClient.sql(
                            "INSERT INTO inventory_schema.outbox " +
                                    "(id, event_type, aggregate_type, aggregate_id, version, correlation_id, causation_id, payload) " +
                                    "VALUES (?, ?, 'Inventory', ?, 1, ?, ?, ?::jsonb)")
                    .param(UUID.randomUUID())
                    .param(type)
                    .param(aggregateId)
                    .param(correlationId)
                    .param(causationId)
                    .param(objectMapper.writeValueAsString(eventObj))
                    .update();
        } catch (Exception e) {
            throw new RuntimeException("Erreur d'écriture dans l'Outbox du Participant", e);
        }
    }
}
