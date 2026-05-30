package com.poc.modules.inventory.contract.events;

import com.poc.shared.event.DomainEvent;
import java.util.UUID;

public record StockFailedEvent(
        UUID eventId,
        UUID aggregateId,
        int version,
        UUID correlationId,
        UUID causationId,
        String reason // ex: "Out of Stock"
) implements DomainEvent {

    @Override
    public String eventType() {
        return "StockFailedEvent";
    }

    @Override
    public String aggregateType() {
        return "Inventory";
    }
}
