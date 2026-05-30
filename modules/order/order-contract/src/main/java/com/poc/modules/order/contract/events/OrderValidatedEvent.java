package com.poc.modules.order.contract.events;

import com.poc.shared.event.DomainEvent;
import java.util.UUID;

public record OrderValidatedEvent(
        UUID eventId,
        UUID aggregateId,
        int version,
        UUID correlationId,
        UUID causationId
) implements DomainEvent {

    @Override
    public String eventType() {
        return "OrderValidatedEvent";
    }


    @Override
    public String aggregateType() {
        return "Order";
    }
}
