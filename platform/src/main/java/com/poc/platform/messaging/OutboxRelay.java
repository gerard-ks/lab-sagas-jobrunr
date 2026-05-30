package com.poc.platform.messaging;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.regex.Pattern;

@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final Pattern SCHEMA_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+$");

    private final JdbcClient jdbcClient;
    private final Connection natsConnection;
    private final TransactionTemplate transactionTemplate;
    private final MessagingProperties properties;

    public OutboxRelay(JdbcClient jdbcClient, Connection natsConnection,
                       TransactionTemplate transactionTemplate, MessagingProperties properties) {
        this.jdbcClient = jdbcClient;
        this.natsConnection = natsConnection;
        this.transactionTemplate = transactionTemplate;
        this.properties = properties;
    }

    public void relayMessages(String schemaName) {
        if (schemaName == null || !SCHEMA_PATTERN.matcher(schemaName).matches()) {
            return;
        }

        var outboxProps = properties.outbox();
        var natsProps = properties.nats();

        // Requête de sélection avec verrouillage (Le verrou tiendra jusqu'à la fin de la transaction Spring)
        String selectSql = String.format(
                "SELECT id, event_type as eventType, payload, retry_count as retryCount " +
                        "FROM %s.outbox " +
                        "WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT %d FOR UPDATE SKIP LOCKED",
                schemaName, outboxProps.batchSize()
        );

        String updateSentSql = String.format("UPDATE %s.outbox SET status = 'SENT', sent_at = NOW() WHERE id = ?", schemaName);

        String updateFailedSql = String.format(
                "UPDATE %s.outbox SET status = CASE WHEN retry_count + 1 >= ? THEN 'FAILED' ELSE 'PENDING' END, " +
                        "retry_count = retry_count + 1, last_error = ? WHERE id = ?", schemaName
        );

        try {
            JetStreamOptions jsOptions = new JetStreamOptions.Builder().requestTimeout(natsProps.requestTimeout()).build();
            JetStream js = natsConnection.jetStream(jsOptions);

            boolean hasMoreMessages = true;

            while (hasMoreMessages) {
                // TOUT le lot est traité dans une seule transaction SQL.
                // Si le serveur plante au milieu, tout fait ROLLBACK. Zéro doublon.
                hasMoreMessages = Boolean.TRUE.equals(transactionTemplate.execute(status -> {

                    List<OutboxMessage> messages = jdbcClient.sql(selectSql)
                            .query(OutboxMessage.class)
                            .list();

                    if (messages.isEmpty()) {
                        return false;
                    }

                    for (OutboxMessage msg : messages) {
                        try {
                            String subject = "events." + msg.eventType();

                            // L'appel réseau (rapide)
                            js.publish(subject, msg.payload().getBytes());

                            // Succès : Modification en DB
                            jdbcClient.sql(updateSentSql).param(msg.id()).update();

                        } catch (Exception e) {
                            log.error("[Outbox] Échec d'envoi NATS pour le message {}.", msg.id(), e);

                            // Échec : Incrémentation du Retry
                            jdbcClient.sql(updateFailedSql)
                                    .param(outboxProps.maxRetries())
                                    .param(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName())
                                    .param(msg.id())
                                    .update();
                        }
                    }
                    return true;
                }));
            }
        } catch (Exception e) {
            log.error("[Outbox] Erreur critique lors du relais", e);
        }
    }
}