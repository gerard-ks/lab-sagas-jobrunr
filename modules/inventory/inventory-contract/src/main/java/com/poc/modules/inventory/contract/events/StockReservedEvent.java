package com.poc.modules.inventory.contract.events;

import com.poc.shared.event.DomainEvent;
import java.util.UUID;

public record StockReservedEvent(
        UUID eventId,
        UUID aggregateId,       // L'ID du Produit
        int version,
        UUID correlationId,     // On lui renvoie le SAGA_ID qu'il nous a envoyé !
        UUID causationId        // L'eventId de la ReserveStockCommand
) implements DomainEvent {

    @Override
    public String eventType() { return "StockReservedEvent"; }

    @Override
    public String aggregateType() { return "Inventory"; }
}