package com.poc.modules.notification.internal;

import org.jobrunr.jobs.lambdas.JobRequestHandler;
import org.jobrunr.jobs.lambdas.JobRequest;

import java.util.UUID;


public class SendEmailJobRequest implements JobRequest {
    private UUID orderId;
    private String message;

    // Constructeur par défaut requis par Jackson
    public SendEmailJobRequest() {}

    public SendEmailJobRequest(UUID orderId, String message) {
        this.orderId = orderId;
        this.message = message;
    }

    public UUID getOrderId() { return orderId; }
    public String getMessage() { return message; }

    @Override
    public Class<? extends JobRequestHandler> getJobRequestHandler() {
        return SendEmailJobHandler.class;
    }
}
