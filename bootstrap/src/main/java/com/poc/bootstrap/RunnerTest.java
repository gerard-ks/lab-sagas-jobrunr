package com.poc.bootstrap;

import com.poc.modules.order.internal.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;


@Component
public class RunnerTest implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(RunnerTest.class);

    private final OrderService orderService;

    // Feature Flag : On peut forcer un produit à tomber en rupture de stock !
    @Value("${chaos.monkey.force-stock-fail:false}")
    private boolean forceStockFail;

    public RunnerTest(OrderService orderService) {
        this.orderService = orderService;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("==================================================");
        log.info("DÉMARRAGE DU TEST LAB 2 (SAGAS & JOBRUNR)...");
        log.info("==================================================");

        try {
            // Laisse le temps à l'application de démarrer et à NATS de créer ses Streams
            Thread.sleep(5000);

            // Simulation d'une session utilisateur
            String itemName = forceStockFail ? "PRODUIT_EPUISE" : "PC_GAMER";
            double amount = 1500.00;

            if (forceStockFail) {
                log.warn("CHAOS MONKEY ACTIF : On commande un produit qui n'est pas en stock !");
            } else {
                log.info("[User] Clique sur 'Acheter un {}'...", itemName);
            }

            // Déclenchement de la Saga !
            orderService.initiateOrderSaga(itemName, amount);

        } catch (InterruptedException e) {
            log.error("[Test] Le thread a été interrompu", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("[Test] Une erreur inattendue est survenue", e);
        }
    }
}

