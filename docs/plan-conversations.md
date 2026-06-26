# Plan de travail collaboratif — Conversations Claude Code

> Ce document découpe le projet en **conversations Claude Code distinctes** (A → F). Chaque
> conversation = un contexte propre, focalisé sur une partie du projet. Objectif : un travail
> **collaboratif et pédagogique** où **j'écris le code et les explications, et toi tu exécutes,
> tu prends des screenshots, et tu apprends au passage**.

## Comment ça marche

1. Tu ouvres une **nouvelle conversation Claude Code** dans ce dossier.
2. Tu **copies-colles le « Prompt de démarrage »** de la conversation visée (encadré ci-dessous).
3. Je te guide pas à pas. **Pour chaque étape concrète :**
   - 🧑‍💻 *Je prépare* le code / la commande + je t'explique **ce que ça fait et pourquoi**.
   - ▶️ *Tu exécutes* la commande de ton côté.
   - 📸 *Tu prends un screenshot* du résultat (à ranger dans `screenshots/`).
   - 🔁 Tu me colles le résultat / le screenshot ; je vérifie, j'explique, on continue.
4. En fin de conversation, **on met à jour l'avancement** dans le `README.md` et **on commit**.

> 💡 Convention screenshots : `screenshots/<conversation>-<sujet>.png`
> (ex. `screenshots/C-redpanda-topic-cree.png`).

---

## Vue d'ensemble

| Conv. | Titre | Exercice | Livrable(s) visé(s) |
|------|-------|----------|---------------------|
| ~~**A**~~ | ~~Cadrage & mise en place de l'environnement~~ ✅ *validée (Docker + Python OK)* | — | — |
| **B** | Modélisation de l'architecture hybride | Ex. 1 | Schéma (PDF/PNG) + doc d'évaluation de compatibilité |
| **C** | Redpanda + producteur de tickets | Ex. 2 — Étape 1 | Cluster Redpanda, topic `client_tickets`, producteur Python |
| **D** | Traitement PySpark | Ex. 2 — Étape 2 | Script PySpark (lecture, transformations, agrégations) |
| **E** | Export + Conteneurisation | Ex. 2 — Étapes 3 & 4 | Export JSON/Parquet, Dockerfiles + docker-compose |
| **F** | Documentation, Mermaid & vidéo + packaging | Ex. 2 — Étape 5 | README + diagramme Mermaid + vidéo + zip des livrables |

> Ordre conseillé : **B → C → D → E → F** (la Conversation A est déjà validée). B (modélisation)
> peut être faite en parallèle de C/D si tu préfères, car elle est indépendante du code.

---

## ✅ Conversation A — Cadrage & mise en place de l'environnement *(déjà validée)*

Étape de cadrage **considérée comme faite** : l'environnement est confirmé fonctionnel
(**Docker 29 + Compose v5**, **Python 3.12**) et le dépôt est opérationnel. Pas besoin d'y revenir —
on démarre directement à la **Conversation B**.

> *Note : Java n'est pas installé sur l'hôte ; ce n'est pas bloquant car PySpark tournera dans un
> conteneur Docker (qui embarque Java), conformément à l'étape de conteneurisation.*

---

## ⬜ Conversation B — Modélisation de l'architecture hybride (Exercice 1)

**Objectif** : produire les **2 livrables de l'Exercice 1** : le schéma d'architecture hybride et le
document d'évaluation de compatibilité.

**Ce que tu vas apprendre** : mapping on-premise → **services Redpanda** (Redpanda Cloud,
connecteurs/Connect…), notions de sécurité (chiffrement, SASL/RBAC, intégration Active Directory,
réseau privé), interopérabilité et raisonnement coûts.

**Au programme**
- Analyser le SI on-premise d'InduTechData (SQL Server 40 To, SAN 10 To, Active Directory, ERP/CRM).
- Choisir et **justifier** les composants **Redpanda** (ingestion IoT temps réel, connecteurs,
  traitement/streaming, sécurité & identité, réseau hybride…).
- Construire le **schéma** on-premise ↔ cloud (outil au choix → export **PDF/PNG**).
- Rédiger le **document d'évaluation de compatibilité** (sécurité, interopérabilité, coûts).
- 📸 Screenshots : le schéma final, étapes de construction.

**Livrables** : `docs/exercice1-modelisation/` → schéma (PDF/PNG) + doc d'évaluation.
**Definition of done** : les 2 livrables existent, nommés selon la convention.

> **Prompt de démarrage à copier :**
> ```
> Projet OpenClassrooms "Modélisez une infrastructure dans le cloud". Lis consigne.md (section
> Exercice 1). On fait la Conversation B : modélisation de l'architecture hybride on-premise ↔ cloud
> en utilisant les services Redpanda (comme demandé dans la consigne). Aide-moi à choisir/justifier
> les composants Redpanda, à produire le schéma (PDF/PNG) et le document d'évaluation de
> compatibilité (sécurité, interopérabilité, coûts). Explique-moi les choix, je prends des
> screenshots. Mets à jour le README à la fin.
> ```

---

## ⬜ Conversation C — Redpanda + producteur de tickets (Ex. 2 — Étape 1)

**Objectif** : un cluster Redpanda opérationnel, le topic `client_tickets`, et un producteur Python
qui y envoie des tickets aléatoires en continu.

**Ce que tu vas apprendre** : le streaming type Kafka, notion de topic/partition, produire des
messages depuis Python (`kafka-python` ou `confluent-kafka`), structurer un message JSON.

**Au programme**
- Lancer Redpanda via Docker + sa console web.
- Créer le topic **`client_tickets`** (via `rpk` ou la console).
- Écrire `src/producer/` : générateur de tickets (ID ticket, ID client, date/heure, demande, type,
  priorité) → envoi dans le topic.
- 📸 Screenshots : topic créé, messages qui arrivent dans la console Redpanda.

**Livrable** : `src/producer/` fonctionnel.
**Definition of done** : tu vois les tickets affluer dans la console Redpanda.

> **Prompt de démarrage à copier :**
> ```
> Projet OpenClassrooms "Modélisez une infrastructure dans le cloud". Lis consigne.md (Exercice 2,
> Étape 1). On fait la Conversation C : démarrer Redpanda (Docker), créer le topic client_tickets,
> et écrire un producteur Python de tickets aléatoires. Guide-moi pas à pas, je veux exécuter et
> screenshoter chaque étape, et comprendre ce qui se passe. Mets à jour le README à la fin.
> ```

---

## ⬜ Conversation D — Traitement PySpark (Ex. 2 — Étape 2)

**Objectif** : un consommateur **PySpark** qui lit `client_tickets`, applique des transformations et
des agrégations, et génère des insights.

**Ce que tu vas apprendre** : Spark Structured Streaming, connecteur Spark-Kafka, transformations
(colonnes dérivées) et agrégations, réglages de **performance** (mémoire, partitions) et de
**résilience** (gestion d'erreurs / reprise).

**Au programme**
- Configurer PySpark + le connecteur Kafka (packages Spark).
- Lire le flux `client_tickets`, parser le JSON.
- Transformations (ex. assigner une équipe support selon le type) + agrégations (ex. nb de tickets
  par type / priorité).
- 📸 Screenshots : sortie console Spark (tableaux d'agrégations), logs du job.

**Livrable** : `src/spark/` fonctionnel.
**Definition of done** : Spark consomme le flux et affiche des insights en continu.

> **Prompt de démarrage à copier :**
> ```
> Projet OpenClassrooms "Modélisez une infrastructure dans le cloud". Lis consigne.md (Exercice 2,
> Étape 2). On fait la Conversation D : écrire le traitement PySpark qui lit le topic
> client_tickets, applique transformations + agrégations, en gérant performance et résilience.
> Guide-moi pas à pas, j'exécute et je screenshote. Mets à jour le README à la fin.
> ```

---

## ⬜ Conversation E — Export + Conteneurisation (Ex. 2 — Étapes 3 & 4)

**Objectif** : exporter les résultats (JSON/Parquet) et **tout conteneuriser** avec un seul
`docker compose up`.

**Ce que tu vas apprendre** : écrire des sinks Spark (Parquet/JSON), écrire des **Dockerfiles**,
orchestrer plusieurs services avec **Docker Compose**, gérer **volumes** et **réseau** entre
conteneurs.

**Au programme**
- Ajouter l'export des résultats d'analyse en **JSON / Parquet**.
- **Dockerfile** pour : Redpanda, producteur de tickets, traitement PySpark.
- **docker-compose.yml** : orchestration des conteneurs + volumes.
- 📸 Screenshots : `docker compose up` (tous les services UP), fichiers d'export générés.

**Livrable** : `docker/` (Dockerfiles + docker-compose.yml), exports dans `data/`.
**Definition of done** : `docker compose up --build` démarre tout le pipeline de bout en bout.

> **Prompt de démarrage à copier :**
> ```
> Projet OpenClassrooms "Modélisez une infrastructure dans le cloud". Lis consigne.md (Exercice 2,
> Étapes 3 & 4). On fait la Conversation E : export des résultats (JSON/Parquet) + conteneurisation
> complète (un Dockerfile par composant + docker-compose). Guide-moi pas à pas, j'exécute et je
> screenshote chaque service qui démarre. Mets à jour le README à la fin.
> ```

---

## ⬜ Conversation F — Documentation, Mermaid, vidéo & packaging (Ex. 2 — Étape 5)

**Objectif** : finaliser la **documentation** (README + diagramme **Mermaid** + **vidéo**) et
**préparer les livrables zippés** pour le dépôt OpenClassrooms.

**Ce que tu vas apprendre** : écrire une doc claire, modéliser un flux en Mermaid, scénariser une
démo vidéo, packager proprement un rendu.

**Au programme**
- Finaliser le README (prérequis, lancement, architecture, insights).
- Diagramme **Mermaid** du pipeline ETL intégré au README.
- Script + tournage d'une **vidéo de démo** courte (YouTube/Loom) → lien dans le README.
- Renommer + **zipper** tous les livrables selon la convention (`Nom_Prenom_n°_..._mmaaaa`).
- Compléter la **fiche d'autoévaluation** pour la session bilan.
- 📸 Screenshots : README rendu, diagramme Mermaid, dossier de livrables prêt.

**Livrables** : README final, vidéo, `livrables/*.zip`.
**Definition of done** : tout est documenté, la vidéo est en ligne, les zips sont prêts à déposer.

> **Prompt de démarrage à copier :**
> ```
> Projet OpenClassrooms "Modélisez une infrastructure dans le cloud". Lis consigne.md (Exercice 2,
> Étape 5) et le README. On fait la Conversation F : finaliser la doc (README + diagramme Mermaid
> du pipeline), préparer la vidéo de démo, et packager/nommer/zipper les livrables selon la
> convention OpenClassrooms. Guide-moi pas à pas, j'exécute et je screenshote. Mets à jour le
> README à la fin.
> ```

---

## Suivi inter-conversations

À la fin de **chaque** conversation, on :
1. met à jour la section **Avancement** du `README.md` (⬜ → 🟦 → ✅) ;
2. range les screenshots dans `screenshots/` ;
3. fait un **commit git** clair (ex. `feat(ex2): producteur de tickets Redpanda`) ;
4. note les **questions ouvertes** à poser au mentor.
