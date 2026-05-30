package com.poc.modules.order.contract.events;

import com.poc.shared.event.DomainEvent;
import java.util.UUID;

public record OrderCancelledEvent(
        UUID eventId,
        UUID aggregateId,
        int version,
        UUID correlationId,
        UUID causationId,
        String reason // Pour pouvoir dire au client pourquoi on a annulé
) implements DomainEvent {

    @Override
    public String eventType() {
        return "OrderCancelledEvent";
    }

    @Override
    public String aggregateType() {
        return "Order";
    }
}
