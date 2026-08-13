# Plan de travail multi-agents — Conversations Claude Code

> Ce document découpe le projet en **conversations Claude Code indépendantes** (B → G). Chaque
> conversation = **un agent, un contexte propre, une branche git**. Tu copies-colles le
> « **Prompt de démarrage** » de la conversation visée dans une nouvelle session, l'agent exécute.
> Une **conversation finale G** regroupe, fusionne et pousse l'ensemble.

---

## 1. Vue d'ensemble & parallélisation

| Conv. | Titre | Périmètre | Branche git | Dépend de | **Parallélisable avec** |
|---|---|---|---|---|---|
| ~~A~~ | ~~Cadrage & environnement~~ ✅ | — | *(fusionnée)* | — | — |
| **B** | Mise en conformité Exercice 1 | Ex. 1 — Étapes 1/2/3 | `conv-b-ex1-modelisation` | rien | ✅ **C**, D, E, F |
| **C** | Redpanda + producteur de tickets | Ex. 2 — Étape 1 | `conv-c-redpanda-producteur` | rien | ✅ **B** · ⚠️ D (sous condition) |
| **D** | Traitement PySpark | Ex. 2 — Étape 2 | `conv-d-pyspark` | **contrat ticket** (issu de C) | ✅ B · ⚠️ C (sous condition) |
| **E** | Export + conteneurisation | Ex. 2 — Étapes 3 & 4 | `conv-e-export-docker` | C (obligatoire), D (partiel) | ✅ B · ⚠️ D (partiel) |
| **F** | Documentation, Mermaid & vidéo | Ex. 2 — Étape 5 | `conv-f-documentation` | **E** (pipeline qui tourne) | ❌ aucune |
| **G** | 🏁 Regroupement, merge & push | Toutes | `main` | **B, C, D, E, F** | ❌ aucune |

### Détail des dépendances

| Paire | Parallélisable ? | Pourquoi / condition |
|---|---|---|
| **B ∥ C** | ✅ **Oui, totalement** | Zéro recouvrement de fichiers : B ne touche que `docs/exercice1-modelisation/`, C que `src/producer/`. **C'est le vrai gain de temps : lance-les ensemble.** |
| **C ∥ D** | ⚠️ **Oui, sous condition** | D a besoin du **schéma JSON du ticket**. Condition : **C fige le contrat** dans `docs/contrat-ticket.md` et le pousse **en premier commit**. Dès que ce fichier est sur `origin`, D peut démarrer sans attendre le reste de C. Sans ce contrat → **séquentiel obligatoire**. |
| **D ∥ E** | ⚠️ **Partiellement** | E peut écrire les Dockerfiles **Redpanda** + **producteur** et le squelette `docker-compose.yml` dès la fin de C. Mais le **Dockerfile PySpark** et le câblage final exigent le script de D → E ne peut pas *finir* avant D. |
| **E → F** | ❌ **Non** | La **vidéo de démo** exige un `docker compose up --build` fonctionnel de bout en bout. |
| **F → G** | ❌ **Non** | G empaquette les livrables finaux, dont la vidéo et le README de F. |

### Ordonnancement recommandé (3 vagues)

```
Vague 1   ┌── B (Exercice 1)          ← indépendant, peut tourner du début à la fin
          └── C (Redpanda + producteur)
                 │  (dès que docs/contrat-ticket.md est poussé)
Vague 2          ├── D (PySpark)
                 └── E (Docker, partie Redpanda/producteur)  →  E finalise après D
Vague 3               └── F (doc + Mermaid + vidéo)
                              └── G (merge + packaging + push)
```

---

## 2. Règles communes à TOUS les agents

Ces règles sont rappelées dans chaque prompt de démarrage. Elles sont **non négociables**.

### 2.1 Une branche par agent
- Chaque agent travaille **exclusivement sur sa branche** (colonne « Branche git » ci-dessus).
- Il crée la branche **depuis `origin/main` à jour**, jamais depuis une autre branche de travail.
- Il **pousse sa branche** sur `origin` (`git push -u origin <branche>`) dès le premier commit,
  puis à chaque étape importante — c'est ce qui rend son avancée **visible aux autres agents**.
- Il **ne merge jamais** dans `main`. Seule la **Conversation G** touche `main`.

### 2.2 État des lieux obligatoire — au début ET pendant
Avant de commencer **et à chaque reprise de travail** :

```bash
git fetch --all --prune && git branch -a && git log --oneline --graph --all -20 && git status
```

L'agent lit ensuite `consigne.md`, ce plan, `README.md` et **tous** les `docs/journal/*.md` pour
savoir ce que les autres agents ont produit. Si `origin/main` a bougé depuis la création de sa
branche → `git rebase origin/main` **avant** de continuer.

### 2.3 Journal d'avancement (canal de notification entre agents)
- Chaque agent tient **son propre fichier** `docs/journal/<conv>.md` (ex. `docs/journal/C.md`) :
  ce qui est fait, ce qui est décidé, ce qui reste, questions ouvertes.
- **Aucun agent ne modifie la section « Avancement » du `README.md`** — c'est la source n°1 de
  conflits de merge. **Seule la Conversation G** consolide le README à partir des journaux.
- Les **contrats partagés** (schéma du ticket, noms de topics, chemins d'export, ports) vont dans
  `docs/contrat-ticket.md` — à figer tôt, à ne modifier qu'en prévenant dans le journal.

### 2.4 Poser des questions
> **En cas d'incertitude, l'agent s'arrête et pose la question à l'utilisateur. Il ne devine pas,
> il n'invente pas de valeur par défaut sur un point structurant.**

Exemples de points qui **doivent** déclencher une question : choix d'un service cloud payant, format
de livrable ambigu, date de démarrage pour le nommage, arbitrage de périmètre, conflit de merge
non trivial, dépendance manquante dans l'environnement.

### 2.5 Commits
- Commits **atomiques** et messages clairs en français : `feat(ex2): producteur de tickets Redpanda`.
- Screenshots rangés dans `screenshots/<conv>-<sujet>.png` (ex. `screenshots/C-topic-cree.png`).

---

## 3. Les conversations

### ⬜ Conversation B — Mise en conformité de l'Exercice 1

**Contexte** : le schéma et l'évaluation existent déjà mais **ne respectent pas** les Étapes 1/2/3
détaillées de la consigne. Il s'agit d'une **mise en conformité**, pas d'un travail à zéro.

**À corriger (écarts identifiés)**
- ❌ **Entrepôt de données cloud absent** → en choisir un (ex. Amazon Redshift) et **expliquer la
  synchronisation depuis SQL Server on-premise**.
- ❌ **Service d'extension de l'AD non nommé** → proposer explicitement un service (ex. AWS Directory
  Service / AWS Managed Microsoft AD) et expliquer la gestion unifiée des identités.
- ❌ **Aucune estimation chiffrée des coûts** → chiffrage **initial + récurrent, par composant**.
- ❌ **Aucune recommandation de surveillance des coûts** → CloudWatch, AWS Budgets, Cost Explorer.
- ⚠️ **Format du doc** : 1476 mots aujourd'hui → cible **400–1200 mots**, structuré en
  **avantages / limitations / points d'attention**.
- ⚠️ **Schéma** : ajouter entrepôt + service AD, **annoter les protocoles de transfert
  (batch / temps réel)**, puis **exporter en PDF/PNG**.
- ⚠️ **Positionnement** : l'archi est **AWS + Redpanda**, pas « Redpanda seul ». Supprimer la
  fausse question ouverte AWS-vs-Redpanda.

**Livrables** : `docs/exercice1-modelisation/` → schéma **PDF/PNG** + évaluation conforme.
**Definition of done** : tous les items « Exercice 1 » de la checklist `consigne.md` §8 cochés.

> **Prompt de démarrage à copier :**
> ```
> Projet OpenClassrooms "Modélisez une infrastructure dans le cloud". Tu es l'agent de la
> Conversation B. Applique les règles communes de docs/plan-conversations.md §2 :
> 1) Fais l'état des lieux git (fetch, branches, log, status) et lis consigne.md,
>    docs/plan-conversations.md, README.md et tous les docs/journal/*.md.
> 2) Travaille EXCLUSIVEMENT sur la branche conv-b-ex1-modelisation, créée depuis origin/main à
>    jour, et pousse-la sur origin. Ne merge jamais dans main.
> 3) Ne touche PAS la section Avancement du README. Tiens docs/journal/B.md à jour.
> 4) Si tu as le moindre doute, ARRÊTE-TOI et pose-moi la question. Ne devine pas.
>
> Mission : mettre l'Exercice 1 en conformité avec les Étapes 1, 2 et 3 détaillées de consigne.md
> §4. Corrige les écarts listés dans la Conversation B du plan : ajoute l'entrepôt de données cloud
> (+ synchronisation SQL Server), le service d'extension de l'Active Directory, l'estimation
> chiffrée des coûts par composant (initiaux + récurrents), les recommandations de surveillance des
> coûts. Refonds l'évaluation en 400–1200 mots structurés en avantages / limitations / points
> d'attention. Mets à jour le schéma (nouveaux composants + annotation des protocoles batch/temps
> réel) et exporte-le en PDF/PNG. Explique-moi tes choix au fur et à mesure.
> ```

---

### ⬜ Conversation C — Redpanda + producteur de tickets (Ex. 2 — Étape 1)

**Objectif** : cluster Redpanda opérationnel, topic `client_tickets`, producteur Python de tickets
aléatoires.

**⚠️ Priorité n°1 — débloquer D** : le **tout premier commit** doit créer et pousser
`docs/contrat-ticket.md` figeant le **schéma JSON du ticket** (6 champs imposés + types + exemple),
le **nom du topic**, le **nombre de partitions** et les **ports/adresses du broker**. C'est ce
fichier qui permet à la Conversation D de démarrer en parallèle.

**Au programme** : Redpanda via Docker + console web · topic `client_tickets` (`rpk` ou console) ·
`src/producer/` (ID ticket, ID client, date/heure, demande, type, priorité) · screenshots.

**Livrable** : `src/producer/` fonctionnel.
**Definition of done** : les tickets affluent visiblement dans la console Redpanda.

> **Prompt de démarrage à copier :**
> ```
> Projet OpenClassrooms "Modélisez une infrastructure dans le cloud". Tu es l'agent de la
> Conversation C. Applique les règles communes de docs/plan-conversations.md §2 :
> 1) Fais l'état des lieux git (fetch, branches, log, status) et lis consigne.md,
>    docs/plan-conversations.md, README.md et tous les docs/journal/*.md.
> 2) Travaille EXCLUSIVEMENT sur la branche conv-c-redpanda-producteur, créée depuis origin/main à
>    jour, et pousse-la sur origin. Ne merge jamais dans main.
> 3) Ne touche PAS la section Avancement du README. Tiens docs/journal/C.md à jour.
> 4) Si tu as le moindre doute, ARRÊTE-TOI et pose-moi la question. Ne devine pas.
>
> Mission : consigne.md §5, Exercice 2 Étape 1. PREMIÈRE ACTION PRIORITAIRE : crée et pousse
> docs/contrat-ticket.md figeant le schéma JSON du ticket (6 champs imposés, types, exemple), le
> nom du topic client_tickets, le nombre de partitions et les ports du broker — c'est ce qui
> débloque la Conversation D en parallèle. Ensuite : démarre Redpanda via Docker, crée le topic
> client_tickets, écris le producteur Python de tickets aléatoires dans src/producer/. Guide-moi
> pas à pas, je veux exécuter et screenshoter chaque étape.
> ```

---

### ⬜ Conversation D — Traitement PySpark (Ex. 2 — Étape 2)

**Prérequis bloquant** : `docs/contrat-ticket.md` présent sur `origin` (produit par C).

**Objectif** : consommateur **PySpark** qui lit `client_tickets`, transforme, agrège, produit des
insights.

**Au programme** : PySpark + connecteur Kafka · lecture du flux + parsing JSON · transformation
(ex. équipe support selon le type) + agrégation (ex. nb de tickets par type/priorité) · réglages
**performance** (mémoire, partitions) et **résilience** (gestion d'erreurs, reprise).

**Livrable** : `src/spark/` fonctionnel.
**Definition of done** : Spark consomme le flux et affiche des insights en continu.

> **Prompt de démarrage à copier :**
> ```
> Projet OpenClassrooms "Modélisez une infrastructure dans le cloud". Tu es l'agent de la
> Conversation D. Applique les règles communes de docs/plan-conversations.md §2 :
> 1) Fais l'état des lieux git (fetch, branches, log, status) et lis consigne.md,
>    docs/plan-conversations.md, README.md et tous les docs/journal/*.md.
> 2) VÉRIFIE d'abord que docs/contrat-ticket.md existe sur origin (produit par la Conversation C).
>    S'il n'existe pas, préviens-moi et attends — ne devine pas le schéma du ticket.
> 3) Travaille EXCLUSIVEMENT sur la branche conv-d-pyspark, créée depuis origin/main à jour, et
>    pousse-la sur origin. Ne merge jamais dans main.
> 4) Ne touche PAS la section Avancement du README. Tiens docs/journal/D.md à jour.
> 5) Si tu as le moindre doute, ARRÊTE-TOI et pose-moi la question. Ne devine pas.
>
> Mission : consigne.md §5, Exercice 2 Étape 2. Écris dans src/spark/ le traitement PySpark qui lit
> le topic client_tickets, applique au moins une transformation et une agrégation, en traitant
> explicitement la performance (mémoire, nombre de partitions) et la résilience (gestion d'erreurs,
> reprise après déconnexion). Guide-moi pas à pas, j'exécute et je screenshote.
> ```

---

### ⬜ Conversation E — Export + conteneurisation (Ex. 2 — Étapes 3 & 4)

**Objectif** : exporter les résultats (JSON/Parquet) et tout conteneuriser en un
`docker compose up --build`.

**Stratégie de parallélisation** : commencer par les Dockerfiles **Redpanda** et **producteur** +
le squelette `docker-compose.yml` (possible dès la fin de C), **finaliser** le Dockerfile PySpark et
le câblage une fois D poussée.

**Livrable** : `docker/` (Dockerfiles + `docker-compose.yml`), exports dans `data/`.
**Definition of done** : `docker compose up --build` démarre tout le pipeline de bout en bout.

> **Prompt de démarrage à copier :**
> ```
> Projet OpenClassrooms "Modélisez une infrastructure dans le cloud". Tu es l'agent de la
> Conversation E. Applique les règles communes de docs/plan-conversations.md §2 :
> 1) Fais l'état des lieux git (fetch, branches, log, status) et lis consigne.md,
>    docs/plan-conversations.md, README.md et tous les docs/journal/*.md.
> 2) Regarde où en sont les branches conv-c-redpanda-producteur et conv-d-pyspark. Tu peux démarrer
>    les Dockerfiles Redpanda et producteur dès que C est poussée ; le Dockerfile PySpark et le
>    câblage final nécessitent D. Dis-moi clairement ce que tu peux faire et ce qui est bloqué.
> 3) Travaille EXCLUSIVEMENT sur la branche conv-e-export-docker, créée depuis origin/main à jour,
>    et pousse-la sur origin. Ne merge jamais dans main.
> 4) Ne touche PAS la section Avancement du README. Tiens docs/journal/E.md à jour.
> 5) Si tu as le moindre doute, ARRÊTE-TOI et pose-moi la question. Ne devine pas.
>
> Mission : consigne.md §5, Exercice 2 Étapes 3 et 4. Ajoute l'export des résultats d'analyse en
> JSON/Parquet, puis un Dockerfile par composant (Redpanda, producteur de tickets, traitement
> PySpark) et un docker-compose.yml orchestrant conteneurs et volumes. Guide-moi pas à pas,
> j'exécute et je screenshote chaque service qui démarre.
> ```

---

### ⬜ Conversation F — Documentation, Mermaid & vidéo (Ex. 2 — Étape 5)

**Prérequis bloquant** : le pipeline démarre de bout en bout (branche E poussée et fonctionnelle).

**Objectif** : README final + diagramme **Mermaid** du pipeline ETL + **vidéo de démonstration**.

**Au programme** : README (prérequis, lancement, architecture, insights) · diagramme Mermaid ·
script + tournage de la vidéo (YouTube/Loom) → lien dans le README.

> ⚠️ La **vidéo** doit être tournée par toi (Mathieu) — l'agent prépare le script et le déroulé,
> il ne peut pas filmer. C'est le seul livrable non automatisable.

**Livrables** : README final (hors section Avancement) + vidéo en ligne + lien intégré.
**Definition of done** : un lecteur externe peut lancer et comprendre le POC avec le seul README.

> **Prompt de démarrage à copier :**
> ```
> Projet OpenClassrooms "Modélisez une infrastructure dans le cloud". Tu es l'agent de la
> Conversation F. Applique les règles communes de docs/plan-conversations.md §2 :
> 1) Fais l'état des lieux git (fetch, branches, log, status) et lis consigne.md,
>    docs/plan-conversations.md, README.md et tous les docs/journal/*.md.
> 2) Vérifie que le pipeline démarre bien de bout en bout (branche conv-e-export-docker). Si ce
>    n'est pas le cas, préviens-moi avant d'écrire la doc.
> 3) Travaille EXCLUSIVEMENT sur la branche conv-f-documentation, créée depuis origin/main à jour,
>    et pousse-la sur origin. Ne merge jamais dans main.
> 4) Ne touche PAS la section Avancement du README (elle est réservée à la Conversation G).
>    Tiens docs/journal/F.md à jour.
> 5) Si tu as le moindre doute, ARRÊTE-TOI et pose-moi la question. Ne devine pas.
>
> Mission : consigne.md §5, Exercice 2 Étape 5. Finalise le README (prérequis, lancement,
> architecture, insights), intègre un diagramme Mermaid du pipeline ETL, et écris-moi le script et
> le déroulé précis de la vidéo de démonstration que je vais tourner moi-même. Guide-moi pas à pas.
> ```

---

### ⬜ 🏁 Conversation G — Regroupement, merge & push final

**C'est la seule conversation autorisée à toucher `main`.**

**Objectif** : fusionner toutes les branches dans l'ordre, résoudre les conflits, consolider la
documentation, packager les livrables et pousser.

**Ordre de merge imposé** (du plus indépendant au plus dépendant, pour minimiser les conflits) :

```
main ← conv-b-ex1-modelisation
     ← conv-c-redpanda-producteur
     ← conv-d-pyspark
     ← conv-e-export-docker
     ← conv-f-documentation
```

**Au programme**
- Merger les branches **dans l'ordre ci-dessus**, en relançant les tests/le `docker compose` après
  chaque merge pour identifier immédiatement quelle branche casse quoi.
- **Consolider la section « Avancement » du README** à partir de tous les `docs/journal/*.md`.
- Vérifier **chaque item** des checklists `consigne.md` §8 (Exercice 1, Exercice 2, Finalisation).
- **Renommer et zipper** les livrables selon la convention
  `Nom_Prenom_n°_nom_du_livrable_mmaaaa` → `livrables/`.
- Compléter la **fiche d'autoévaluation**.
- **Push final** sur `origin/main` + suppression des branches de travail fusionnées.

**Definition of done** : `main` contient tout, la checklist est intégralement cochée, les zips sont
prêts à déposer sur OpenClassrooms.

> **Prompt de démarrage à copier :**
> ```
> Projet OpenClassrooms "Modélisez une infrastructure dans le cloud". Tu es l'agent de la
> Conversation G (finale). Tu es le SEUL autorisé à toucher main.
> 1) Fais l'état des lieux git complet (fetch --all --prune, branches locales et distantes, log
>    --graph --all) et lis consigne.md, docs/plan-conversations.md, README.md et TOUS les
>    docs/journal/*.md. Fais-moi un point d'avancement avant de merger quoi que ce soit.
> 2) Merge les branches dans cet ordre exact : conv-b-ex1-modelisation, conv-c-redpanda-producteur,
>    conv-d-pyspark, conv-e-export-docker, conv-f-documentation. Après CHAQUE merge, revérifie que
>    le projet tient debout (docker compose, scripts) avant de passer au suivant.
> 3) En cas de conflit non trivial, ARRÊTE-TOI et demande-moi l'arbitrage. Ne tranche pas seul.
> 4) Consolide la section Avancement du README à partir des journaux, puis vérifie un par un tous
>    les items des checklists de consigne.md §8 et dis-moi ce qui manque encore.
> 5) Renomme et zippe les livrables dans livrables/ selon la convention
>    Nom_Prenom_n°_nom_du_livrable_mmaaaa (demande-moi la date de démarrage du projet si elle n'est
>    pas déjà tranchée dans le repo).
> 6) Montre-moi ce que tu vas pousser AVANT de pousser sur origin/main, et attends ma validation.
> ```

---

## 4. Questions ouvertes (à trancher avec Mathieu ou le mentor)

| # | Question | Impact | Statut |
|---|---|---|---|
| 1 | **Date de démarrage du projet** pour le nommage (`mmaaaa`) — `062026` ? | Nommage de tous les livrables (Conv. G) | ⬜ à trancher |
| 2 | Entrepôt de données : **Redshift** (cohérent AWS) ou **Snowflake** ? | Conv. B — justification + chiffrage | ⬜ à trancher |
| 3 | Granularité du **CDC SQL Server** : tables ERP/CRM ciblées ou base entière ? | Conv. B — schéma + coûts | ⬜ mentor |
| 4 | **Durée de rétention** des topics avant déchargement Tiered Storage ? | Conv. B — chiffrage des coûts | ⬜ mentor |
| 5 | Support de la **vidéo** : YouTube (non répertoriée) ou Loom ? | Conv. F | ⬜ à trancher |
