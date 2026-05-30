package com.poc.platform.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class InboxFilter {
    private static final Logger log = LoggerFactory.getLogger(InboxFilter.class);
    private final JdbcClient jdbcClient;

    public InboxFilter(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    // Pas de TransactionTemplate interne ici ! On réutilise la transaction obligatoire du Consumer appelant.
    public void processSafely(String schemaName, UUID messageId, String handlerName, Runnable businessLogic) {
        String insertSql = String.format("INSERT INTO %s.inbox (message_id, handler_name, processed_at) VALUES (?, ?, NOW())", schemaName);

        try {
            // 1. Tente d'insérer le jeton d'idempotence
            jdbcClient.sql(insertSql)
                    .param(messageId)
                    .param(handlerName)
                    .update();

        } catch (DuplicateKeyException e) {
            // C'est un doublon strict. On lève une exception dédiée pour stopper l'exécution métier proprement
            // sans marquer la transaction globale comme corrompue.
            log.info("[Inbox] Doublon détecté pour le message {} sur {}. Interruption silencieuse.", messageId, handlerName);
            throw new DuplicateMessageException("Message déjà traité", e);
        }

        // 2. Si l'insertion a réussi, on déroule la logique métier dans la même transaction
        businessLogic.run();
    }
}
