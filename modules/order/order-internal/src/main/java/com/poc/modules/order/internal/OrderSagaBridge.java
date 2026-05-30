package com.poc.modules.order.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
public class OrderSagaBridge {
    private static final Logger log = LoggerFactory.getLogger(OrderSagaBridge.class);
    private final OrderSagaManager sagaManager;

    public OrderSagaBridge(OrderSagaManager sagaManager) {
        this.sagaManager = sagaManager;
    }

    // Cette méthode s'exécute STRICTEMENT après le commit de la commande en base
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onOrderCreated(OrderCreatedInternalEvent event) {
        log.info("[Saga Bridge] Transaction validée pour la commande {}. Démarrage sécurisé de la saga.", event.orderId());

        // Exécution de startSaga dans sa propre transaction isolée autonome
        sagaManager.startSaga(event.orderId(), event.itemName(), event.sagaId());
    }
}
