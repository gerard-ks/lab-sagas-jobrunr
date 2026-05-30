# 🧪 Architecture Lab 2 : Orchestration Sagas & Job Queue

Bienvenue dans le **Lab 2**. Ce projet est une Preuve de Concept (PoC) avancée démontrant la maîtrise des transactions distribuées dans un **Monolithe Modulaire** ou un système **Microservices**.

L'objectif de ce laboratoire est de prouver qu'il est possible de gérer des flux métiers complexes traversant plusieurs schémas de base de données isolés, **sans jamais utiliser de transaction SQL globale (2PC)**, et en isolant les effets de bord techniques des événements de domaine.

---

## 🎯 Problèmes Résolus
L'absence de jointure SQL ou de Commit/Rollback global entre deux modules (ou microservices) rend les erreurs asynchrones extrêmement dangereuses. Si le "Paiement" réussit mais que le "Stock" échoue 3 secondes plus tard, le système doit savoir faire marche arrière.

Ce laboratoire résout ces défis via l'implémentation de 4 patterns industriels :

1. **Saga Pattern (Orchestration)** : Un composant central (Machine à États) coordonne les actions asynchrones, mémorise le contexte, et déclenche des *Transactions de Compensation* (Rollback logique) en cas de refus d'un participant.
2. **Anti-Race Condition (Transaction Boundary Bridge)** : Utilisation de hooks (`TransactionPhase.AFTER_COMMIT`) pour empêcher la publication prématurée d'événements réseau avant que l'état initial de la Saga ne soit fermement écrit en base.
3. **Deadlock Prevention (Pessimistic Locking)** : Application stricte d'un ordonnancement de verrous SQL (`FOR UPDATE`) pour protéger l'état de la Saga contre les doublons réseaux ou les Timeout Cron exécutés à la même milliseconde.
4. **Day-2 Operations (Technical Job Queue)** : Séparation claire entre le Bus de Domaine (NATS) et le Bus Technique (JobRunr) pour gérer les pannes d'API externes (SMTP, Stripe) via un Backoff exponentiel local, sans polluer le réseau de l'entreprise.

---

## ⚙️ Stack Technologique
* **Langage & Framework :** Java 21, Spring Boot 3.5.x
* **Base de Données :** PostgreSQL 18+ (Schémas multiples isolés)
* **Accès aux données :** Spring Data JDBC (`JdbcClient`)
* **Message Broker :** NATS JetStream (Transport des Domain Events)
* **Job Queue :** JobRunr (Effets de bord ACIDisés via Postgres)
* **Gouvernance :** Maven Multi-Modules strict (Contrats vs Internals)

---

## 🏗️ Architecture du Projet

```text
lab2-sagas-jobrunr/
 ├── bootstrap/            # Lanceur. Contient les configs et exécute Flyway.
 ├── shared/               # Interfaces primitives (DomainEvent).
 ├── platform/             # Moteurs agnostiques (OutboxRelay, InboxFilter, NatsConfig).
 └── modules/
      ├── order/           # 📦 ORCHESTRATEUR SAGA
      │    ├── contract/   # Expose (ReserveStockCommand, OrderValidated, OrderCancelled)
      │    └── internal/   # Machine à états (SagaManager), Chasseur de timeouts.
      ├── inventory/       # 📦 PARTICIPANT
      │    ├── contract/   # Expose (StockReserved, StockFailed)
      │    └── internal/   # Vérifie et déduit (Row Lock), répond à l'Orchestrateur.
      └── notification/    # 📦 TECHNICIEN (Effets de Bord)
           └── internal/   # Écoute la fin de la Saga, enqueue le mail dans JobRunr.
```

---

## 🚀 Démarrage Rapide

### 1. Démarrer l'infrastructure
Assurez-vous d'avoir Docker démarré. Les disques virtuels sont montés pour prouver la résilience aux pannes matérielles.
```bash
docker-compose up -d
```

### 2. Compiler et Lancer
```bash
mvn clean compile
```
Exécutez `com.poc.bootstrap.LabApplication`. Flyway construira les schémas isolés et JobRunr créera son infrastructure interne (`infra_schema`).

---

## 🧪 Scénarios de Test Inclus

L'application inclut un `RunnerTest.java` simulant une intention d'achat. Observez la console pour suivre la danse distribuée.

### Scénario 1 : Le Happy Path (Succès)
1. `[Order]` Enregistre la commande + Démarre la Saga (`AWAITING_STOCK`).
2. `[Inventory]` Reçoit l'ordre, déduit le stock, et publie sa réussite.
3. `[Order]` Reçoit le succès, passe la Saga à `COMPLETED`, et valide la commande.
4. `[Notification]` Intercepte le succès final et planifie un Job (JobRunr).
5. `[JobRunr]` Exécute l'envoi de l'email en arrière-plan.

### 💥 Scénario 2 : Le Rollback Asynchrone (La Force de la Saga)
* **Configuration :** Mettez `chaos.monkey.force-stock-fail: true` dans `application.yml`.
* **Résultat :** L'Inventory ne crashe pas, mais publie une "Rupture de Stock". L'Orchestrateur l'écoute, verrouille la machine à états, passe en `FAILED`, et exécute un **Rollback Logique** de la commande (passage en "Annulée"). La notification envoie l'email d'excuse.

### 💥 Scénario 3 : L'Isolement des Pannes Tiers
Le module `Notification` a un bug artificiel : l'envoi SMTP échoue de manière aléatoire.
Observez vos logs (ou le Dashboard JobRunr sur `http://localhost:8000`). Vous verrez que NATS et le module `Order` ne sont jamais informés de cette panne. JobRunr retentera silencieusement la tâche 30 secondes plus tard jusqu'à la victoire. L'architecture est étanche.

---
*Ce lab est la démonstration technique que l'absence de base de données partagée (JOIN) n'est pas une faiblesse, mais une opportunité pour concevoir des systèmes massivement scalables et résilients.*
