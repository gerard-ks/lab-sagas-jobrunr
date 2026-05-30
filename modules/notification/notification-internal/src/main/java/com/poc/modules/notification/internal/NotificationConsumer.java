package com.poc.modules.notification.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.modules.order.contract.events.OrderCancelledEvent;
import com.poc.modules.order.contract.events.OrderValidatedEvent;
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
        try {
            T event = objectMapper.readValue(new String(msg.getData()), eventClass);

            // 1. L'Inbox protège des doublons NATS
            inboxFilter.processSafely("notification_schema", event.eventId(), "NotificationConsumer", () -> {

                // 2. Le @Transactional englobe la création du JobRunr
                txTemplate.executeWithoutResult(status -> logic.accept(event));
            });
            msg.ack();
        } catch (Exception e) {
            log.error("Erreur de notification", e);
            msg.nakWithDelay(Duration.ofSeconds(2));
        }
    }
}