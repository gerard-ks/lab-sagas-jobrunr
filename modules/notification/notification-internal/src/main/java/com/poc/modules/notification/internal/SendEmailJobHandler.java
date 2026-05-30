package com.poc.modules.notification.internal;

import org.jobrunr.jobs.lambdas.JobRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;


@Component
public class SendEmailJobHandler implements JobRequestHandler<SendEmailJobRequest> {
    private static final Logger log = LoggerFactory.getLogger(SendEmailJobHandler.class);

    @Override
    public void run(SendEmailJobRequest job) throws Exception {
        log.info("[JobRunr - Background Worker] Démarrage de l'envoi SMTP...");

        // Simulation d'une API distante (ex: SendGrid) qui prend du temps
        Thread.sleep(2000);

        // Simulation d'un échec réseau aléatoire
        if (Math.random() > 0.8) {
            log.error("[JobRunr] Le serveur SMTP a crashé ! JobRunr va réessayer plus tard.");
            throw new RuntimeException("Erreur SMTP");
        }

        log.info("[JobRunr - Background Worker] EMAIL ENVOYÉ ! (Order: {}, Message: '{}')", job.getOrderId(), job.getMessage());
    }
}
