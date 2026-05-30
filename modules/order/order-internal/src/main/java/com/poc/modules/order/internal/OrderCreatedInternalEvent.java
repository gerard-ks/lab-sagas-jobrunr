package com.poc.modules.order.internal;

import java.util.UUID;

public record OrderCreatedInternalEvent(
        UUID orderId,
        String itemName,
        UUID sagaId) {
}
