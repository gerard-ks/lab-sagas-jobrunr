package com.poc.modules.order.internal;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "order")
public record OrderProperties(
        OutboxProperties outbox,
        InboxProperties inbox,
        SagaProperties saga,
        ConsumerProperties consumer
) {
    public record OutboxProperties(long delay, Duration retentionDelay) {}
    public record InboxProperties(Duration purgeDelay) {}
    public record SagaProperties(Duration retentionDelay) {}
    public record ConsumerProperties(Duration nakDelay) {}
}