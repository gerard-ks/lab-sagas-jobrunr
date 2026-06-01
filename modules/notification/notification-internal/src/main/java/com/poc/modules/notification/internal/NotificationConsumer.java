package com.poc.modules.notification.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.modules.order.contract.events.OrderCancelledEvent;
import com.poc.modules.order.contract.events.OrderValidatedEvent;
import com.poc.platform.messaging.DuplicateMessageException;
import com.poc.platform.messaging.InboxFilter;
import com.poc.shared.event.DomainEvent;
import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import jakarta.annotation.PostConstruct;
import org.jobrunr.scheduling.BackgroundJobRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Consumer;

@Component
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    private final Connection natsConnection;
    private final InboxFilter inboxFilter;
    private final TransactionTemplate txTemplate;
    private final ObjectMapper objectMapper;

    public NotificationConsumer(Connection natsConnection, InboxFilter inboxFilter, TransactionTemplate txTemplate, ObjectMapper objectMapper) {
        this.natsConnection = natsConnection;
        this.inboxFilter = inboxFilter;
        this.txTemplate = txTemplate;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void startListening() throws Exception {
        JetStream js = natsConnection.jetStream();
        Dispatcher dispatcher = natsConnection.createDispatcher();

        // Consumer 1 : Pour les Succès
        PushSubscribeOptions optionsValidated = PushSubscribeOptions.builder()
                .durable("notification-validated-consumer") // Nom unique
                .configuration(ConsumerConfiguration.builder().maxDeliver(3).build())
                .build();
        js.subscribe("events.OrderValidatedEvent", dispatcher, this::handleValidated, false, optionsValidated);

        // Consumer 2 : Pour les Annulations
        PushSubscribeOptions optionsCancelled = PushSubscribeOptions.builder()
                .durable("notification-cancelled-consumer") // Nom unique
                .configuration(ConsumerConfiguration.builder().maxDeliver(3).build())
                .build();
        js.subscribe("events.OrderCancelledEvent", dispatcher, this::handleCancelled, false, optionsCancelled);
    }

    private void handleValidated(Message msg) {
        processSafelyAndSchedule(msg, OrderValidatedEvent.class, (event) -> {
            log.info("[Notification] Préparation de l'email de SUCCÈS pour l'Order {}", event.aggregateId());
            // On délègue l'appel SMTP lent et risqué à JobRunr !
            BackgroundJobRequest.enqueue(new SendEmailJobRequest(event.aggregateId(), "Votre commande est confirmée !"));
        });
    }

    private void handleCancelled(Message msg) {
        processSafelyAndSchedule(msg, OrderCancelledEvent.class, (event) -> {
            log.warn("[Notification] Préparation de l'email d'EXCUSE pour l'Order {} (Raison: {})", event.aggregateId(), event.reason());
            BackgroundJobRequest.enqueue(new SendEmailJobRequest(event.aggregateId(), "Désolé, annulation suite à : " + event.reason()));
        });
    }

    // Le Wrapper Transactionnel (La clé de l'architecture)
    private <T extends DomainEvent> void processSafelyAndSchedule(Message msg, Class<T> eventClass, Consumer<T> logic) {
        T event = null;
        try {
            event = objectMapper.readValue(new String(msg.getData()), eventClass);
            final T finalEvent = event;

            inboxFilter.processSafely("notification_schema", finalEvent.eventId(), "NotificationConsumer", () -> {
                txTemplate.executeWithoutResult(status -> logic.accept(finalEvent));
            });

            msg.ack();

        } catch (DuplicateMessageException e) {
            // C'est un doublon de notification. L'e-mail a déjà été planifié par le passé.
            // On envoie un ACK pour sortir le message de la boucle NATS.
            UUID eventId = (event != null) ? event.eventId() : null;
            log.info("[Notification] Doublon détecté pour l'événement {}. Message acquitté (ACK).", eventId);
            msg.ack();

        } catch (Exception e) {
            log.error("Erreur technique de notification (Crash DB, JSON corrompu...). Envoi d'un NAK.", e);
            // Correction de l'API NATS Java (nakWithDelay attend des millisecondes en type long)
            msg.nakWithDelay(Duration.ofSeconds(2).toMillis());
        }
    }
}