package com.poc.modules.order.contract.events;

import com.poc.shared.event.DomainEvent;
import java.util.UUID;

public record ReserveStockCommand(
        UUID eventId,
        UUID aggregateId,       // L'ID de la Commande
        int version,
        UUID correlationId,     // Le SAGA_ID qui nous intéresse !
        UUID causationId,
        String productId        // La donnée utile pour Inventory
) implements DomainEvent {

    @Override
    public String eventType() {
        return "ReserveStockCommand";
    }

    @Override
    public String aggregateType() {
        return "OrderSaga";
    }
}