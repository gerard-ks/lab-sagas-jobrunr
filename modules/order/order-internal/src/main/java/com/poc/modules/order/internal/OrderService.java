package com.poc.modules.order.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final JdbcClient jdbcClient;
    private final ApplicationEventPublisher eventPublisher;

    public OrderService(JdbcClient jdbcClient, ApplicationEventPublisher eventPublisher) {
        this.jdbcClient = jdbcClient;
        this.eventPublisher = eventPublisher;
    }

    @Transactional
    public void initiateOrderSaga(String itemName, double amount) {
        UUID orderId = UUID.randomUUID();
        UUID sagaId = UUID.randomUUID();

        log.info("[OrderService] Persistance de l'intention d'achat pour {} (SagaID: {})", itemName, sagaId);

        // 1. Insertion de la commande métier uniquement
        jdbcClient.sql("INSERT INTO order_schema.orders (id, item_name, amount, status, created_at) VALUES (?, ?, ?, 'PENDING_VALIDATION', NOW())")
                .param(orderId)
                .param(itemName)
                .param(amount)
                .update();

        // 2. Publication d'un événement interne Spring (Ne déclenche rien immédiatement)
        eventPublisher.publishEvent(new OrderCreatedInternalEvent(orderId, itemName, sagaId));

        log.info("[OrderService] Commande persistée en DB. En attente du commit...");
    }

}
