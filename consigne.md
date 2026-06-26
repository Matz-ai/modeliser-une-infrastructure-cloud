# Consignes du projet — Modélisez une infrastructure dans le cloud

> Document de référence reprenant **l'intégralité des consignes officielles** du projet
> OpenClassrooms, recopiées et organisées à partir de la plateforme.
> Parcours : **Data Engineer**.
> Source : <https://openclassrooms.com/fr/paths/1039/projects/1837/assignment>
> Capturé le : 2026-06-26 · Page projet mise à jour le : mardi 5 mai 2026 · Durée estimée : **60 heures**.

---

## 1. Vue d'ensemble

### Ce que vous allez apprendre dans ce projet

Dans ce projet, vous allez apprendre à concevoir et implémenter une **infrastructure de
données hybride** qui combine des technologies de **traitement distribué (Spark)** et de
**streaming de données (Redpanda)**. Vous acquerrez des compétences clés dans l'architecture
de **pipelines ETL en temps réel**, depuis l'extraction de données issues de multiples
sources jusqu'à leur transformation et stockage dans le cloud.

Vous allez **découvrir et pratiquer** :

- la modélisation d'architectures hybrides et la sélection de services de stockage et de
  traitement dans le cloud ;
- la configuration et l'utilisation de **Spark** pour l'ETL de données volumineuses issues de
  diverses sources ;
- la mise en œuvre d'un **pipeline de données en temps réel** via un moteur de streaming comme
  **Redpanda**.

Ce projet renforce les compétences en **manipulation de données massives** et en **architecture
cloud** : compatibilité des environnements on-premise et cloud, pratiques de sécurisation des
données, et interopérabilité des systèmes.

### Pourquoi ces compétences sont importantes

Compétences cruciales et recherchées en **data engineering**, **architecture de données** et
**cloud computing**. Concevoir des infrastructures hybrides et orchestrer des flux de données en
temps réel à grande échelle est essentiel pour les entreprises modernes (scalabilité, analyse en
temps réel, intégration du cloud).

---

## 2. Compétences évaluées

Le projet évalue les compétences suivantes (telles que listées sur la page d'évaluation) :

1. **Représenter visuellement** une infrastructure de gestion des données.
2. **Transformer** des données afin de les adapter à leur utilisation finale.
3. **Charger** des données afin de les stocker dans un emplacement adapté.
4. **Évaluer la compatibilité** des composants avec l'environnement SI de l'organisation.
5. **Extraire** des données issues de toutes sources confondues pour les traiter ou les déplacer.
6. **Documenter son travail.**
7. **Identifier et sélectionner** les composants nécessaires à une infrastructure de données.

> ⚠️ La grille d'évaluation détaillée (page « Évaluation ») n'est débloquée qu'une fois les
> projets précédents validés : *« Votre mentor vous assignera ce projet dès que vous aurez validé
> les précédents. »* Les 7 compétences ci-dessus constituent donc la base d'évaluation connue.

---

## 3. Structure du projet (5 activités)

Le projet est découpé en **5 activités : 3 cours et 2 exercices**.

| # | Type | Intitulé |
|---|------|----------|
| 1 | Cours | **Adoptez les approches modernes des bases de données** — comprendre les enjeux des datalakes et lakehouses |
| 2 | Cours | **Analysez les flux de données en temps réel avec Redpanda** — traitement et analyse en temps réel |
| 3 | Exercice 1 | **Modélisez une infrastructure hybride dans le cloud** + fiche d'autoévaluation |
| 4 | Cours | **Réalisez des calculs distribués avec Spark** — traitement et analyse de données massives |
| 5 | Exercice 2 | **Gérez des tickets clients avec Redpanda et PySpark** + fiche d'autoévaluation |

À l'issue du projet : **session de bilan avec le mentor** pour discuter du projet et des
compétences.

---

## 4. Exercice 1 — Modélisez une infrastructure hybride dans le cloud

### Objectif

Modéliser une **infrastructure hybride dans le cloud** capable d'interagir avec l'environnement
**système d'information (SI) existant** d'une organisation fictive. L'objectif est d'intégrer des
services cloud pour **moderniser l'infrastructure de gestion des données** tout en assurant la
**compatibilité avec le SI on-premise existant**.

> **Rappel — On-premise** : infrastructure informatique installée et hébergée localement, dans les
> locaux de l'entreprise (serveurs, matériel, logiciels physiquement présents, sous la
> responsabilité directe de l'organisation), par opposition au cloud.

### Contexte — *InduTechData*

**InduTechData** est une entreprise de taille moyenne, fondée il y a 15 ans, spécialisée dans
l'analyse de données pour le **secteur industriel**.

- Avec l'introduction récente de solutions **IoT**, elle enregistre une augmentation mensuelle de
  **50 Go de données en temps réel** (flux continus de capteurs nécessitant un traitement rapide
  et fiable).
- L'**infrastructure datacenter actuelle atteint ses limites de capacité**. L'entreprise souhaite
  moderniser sa gestion des données pour améliorer **scalabilité et performance**.
- Elle envisage d'exploiter la flexibilité du cloud, **en particulier les services AWS**, pour
  intégrer les nouvelles solutions tout en assurant la compatibilité avec son SI existant.
- Objectif : tirer parti du cloud pour une gestion plus évolutive des données tout en maintenant
  une **interopérabilité fluide** avec l'infrastructure on-premise — notamment pour la **gestion
  des identités** et la **sécurisation des flux de données**.

#### Description du SI on-premise de InduTechData

- Un **cluster de serveurs SQL Server** hébergeant **40 To de données critiques** : données des
  applications métiers (ERP, CRM), avec sauvegardes régulières et réplication pour la résilience.
- Une **baie de stockage SAN** (Storage Area Network) pour les **données non structurées (10 To)** :
  journaux système, fichiers utilisateurs et capteurs IoT.
- Un serveur **Active Directory (AD)** pour l'authentification, l'autorisation et la gestion des
  utilisateurs.
- Des serveurs dédiés à un **ERP** et un **CRM** (comptabilité, ressources humaines, relation
  client).

### Votre mission

1. **Identifier les composants cloud adaptés** et **modéliser l'architecture hybride**.
2. **Créer un schéma** montrant les interactions entre le datacenter **on-premise** et le **cloud**.
3. **Évaluer la compatibilité** de l'infrastructure hybride proposée avec le SI existant, en tenant
   compte des contraintes de **sécurité**, d'**interopérabilité** et de **coûts**.

> *« Cet exercice est entièrement guidé. »*

> ⚠️ **Ambiguïté repérée dans l'énoncé.** Le point 1 de la mission est rédigé sur le site comme
> « Identifier les composants cloud adaptés (en utilisant les services Redpanda) ». Or tout le
> contexte parle de **services AWS**. Il s'agit très probablement d'une coquille : on retient
> **AWS** comme fournisseur cloud cible pour la modélisation. À confirmer avec le mentor.

### Livrables de l'Exercice 1

- **Schéma de l'infrastructure hybride** : un diagramme illustrant la structure de l'infrastructure
  hybride — **format PDF / PNG**.
- **Évaluation de compatibilité avec l'environnement SI** : un document détaillant l'évaluation des
  choix faits pour assurer une **intégration fonctionnelle** entre l'infrastructure cloud et le SI
  on-premise.

> *Aucun outil de schéma n'est imposé par la consigne.* Le diagramme doit simplement être livré en
> PDF/PNG (draw.io, Excalidraw, la bibliothèque Python `diagrams`, etc. sont tous acceptables).

---

## 5. Exercice 2 — Gérez des tickets clients avec Redpanda et PySpark

### Contexte

Votre manager chez **InduTech** vous demande de réaliser un **POC (Proof Of Concept)** sur un
**système de gestion de tickets clients**. Les tickets sont **générés en temps réel** et contiennent
des informations sur les demandes des clients :

- L'**ID du ticket**
- L'**ID du client**
- La **date et l'heure de création**
- La **demande**
- Le **type de demande**
- La **priorité**

Votre tâche : mettre en place un **pipeline de données** pour **ingérer, traiter et analyser** ces
tickets en temps réel.

> *« L'entreprise venant de migrer chez AWS et Redpanda, on vous demande de **simuler** l'utilisation
> de Redpanda avec votre POC. »* → le POC tourne **en local** (Docker), il simule l'usage cloud.

### Votre mission

- **Configurer un cluster Redpanda** pour ingérer les données de tickets en temps réel ;
- **Utiliser PySpark** pour lire les données de Redpanda, les traiter et les analyser ;
- **Générer des rapports et des insights** basés sur les données de tickets.

> *« Cet exercice est entièrement guidé. »*

### Étapes détaillées

#### Étape 1 — Configurez Redpanda

**Instructions**
- *Installation de Redpanda* : téléchargez et installez Redpanda sur votre machine **ou utilisez une
  image Docker**. Lancez Redpanda et assurez-vous qu'il est opérationnel. *(N'hésitez pas à valider
  l'opérationnalité de Redpanda avec votre mentor si besoin.)*
- *Création d'un topic* : créez un topic nommé **`client_tickets`** dans Redpanda pour stocker les
  données de tickets.
- Écrivez un **script Python** pour **produire des données de tickets (aléatoires)** dans le topic
  `client_tickets`. A minima, les tickets doivent contenir : l'ID du ticket, l'ID du client, la date
  et l'heure de création, la demande, le type de demande, et la priorité.

**Résultat attendu** — Votre code Python.
**Outils** — MySQL ou équivalent · Python 3 + Redpanda.
**Ressources** — Cours « Analysez les flux de données en temps réel avec Redpanda » · Tutoriel
Redpanda + Python (simulation d'un chat) · Cours Docker (conteneurisation).

#### Étape 2 — Traitez les données avec PySpark

**Instructions**
- *Lecture des données de Redpanda* : assurez-vous que **PySpark et les dépendances nécessaires pour
  Kafka** sont installées. Écrivez un script **PySpark** pour **lire** les données du topic
  `client_tickets` et les **traiter**.
- *Transformation et analyse des données* : ajoutez des **transformations et des agrégations** pour
  générer des insights. *Ex. : ajouter automatiquement le nom d'une équipe de support assignée en
  fonction du type de demande, ou calculer le nombre de tickets par type.*

**Résultat attendu** — Votre code Python (PySpark).
**Points de vigilance**
- *Performance du cluster Spark* : configurer adéquatement la **mémoire** et le **nombre de
  partitions** pour tirer le meilleur parti du cluster.
- *Résilience* : gérer les **erreurs** pour éviter des interruptions dans le pipeline (ex. reprise
  après une déconnexion du SQL).

**Outils** — MySQL ou équivalent · Python 3 + PySpark.
**Ressources** — Cours « Réalisez des calculs distribués avec Spark ».

#### Étape 3 — Exportez les données

**Instructions**
- Exportez les **résultats des analyses** dans un fichier au **format adapté (JSON, Parquet ou
  autre)** pour une visualisation ultérieure.

#### Étape 4 — Organisez la conteneurisation

**Instructions**
- Créez un fichier **Dockerfile** pour **chaque élément** de votre projet :
  - L'image Docker de **Redpanda**
  - Le **script générateur de tickets**
  - Le **script de traitement PySpark**
- Utilisez **Docker Compose** pour orchestrer, construire et lancer les **conteneurs et volumes** de
  votre projet.

**Résultat attendu** — Un répertoire zippé contenant l'ensemble de votre code (**Redpanda + PySpark
+ Docker**).

#### Étape 5 — Construisez votre documentation

**Instructions**
- Rédigez un **README**.
- Utilisez **Mermaid** afin d'intégrer un **diagramme de votre pipeline** dans votre README.
- Réalisez une **vidéo** pour expliquer comment **utiliser votre POC** et intégrez-la dans le README.
  - Support libre : **YouTube, Loom ou autre**.
  - Vidéo **courte et efficiente**, **aucune durée maximale imposée**.

**Résultats attendus**
- *Schéma de flux de données* : diagramme (intégré dans le README via **Mermaid**) montrant le flux
  de données depuis les différentes sources et illustrant l'architecture du **pipeline ETL**.
- *Démonstration* : courte **vidéo** présentant votre pipeline ETL et prouvant sa fonctionnalité.

#### Vérifiez votre travail et faites le point avec votre mentor

Pour vérifier que vous n'avez rien oublié, **téléchargez et complétez la fiche d'autoévaluation**.
Parlez-en avec votre mentor durant votre dernière session de mentorat.

---

## 6. Récapitulatif des livrables à déposer

| Activité | Livrables |
|----------|-----------|
| **Exercice 1 — Modélisez une infrastructure hybride** | **Schéma** de l'infrastructure hybride (PDF/PNG) · **Document d'évaluation de compatibilité** avec le SI on-premise |
| **Exercice 2 — Gérez des tickets clients** | **Code** (répertoire zippé : Redpanda + PySpark + Docker) · **Schéma de flux** (Mermaid, dans le README) · **Vidéo de démonstration** |

### Convention de nommage du dépôt

Déposez sur la plateforme, dans un dossier **zip** nommé `Titre_du_projet_nom_prenom`, tous les
livrables nommés ainsi : `Nom_Prenom_n°_du_livrable_nom_du_livrable_date_de_démarrage_du_projet`
(date au format `mmaaaa`).

Exemples (date de démarrage = juin 2026 → `062026`) :

- `Nom_Prenom_1_schema_mmaaaa` → ex. `Zinzen_Mathieu_1_schema_062026`
- `Nom_Prenom_2_evaluation_mmaaaa` → ex. `Zinzen_Mathieu_2_evaluation_062026`
- etc.

> Exemple officiel donné par OpenClassrooms : `Janek_Meriem_1_schema_012025`.

---

## 7. Session de bilan avec le mentor

Pour finaliser le projet, réservez votre **dernière session de mentorat** pour faire un bilan de vos
compétences. Pendant la session, suivez ces **4 étapes** :

1. **Discutez de votre fiche d'autoévaluation** et des commentaires laissés dans la colonne « Notes ».
2. **Expliquez les difficultés rencontrées** et ce qui a été plus difficile (pour mieux les aborder
   à l'avenir).
3. **Présentez vos points forts**, ce que vous avez apprécié accomplir et pourquoi ces tâches vous
   ont paru plus faciles.
4. **Identifiez les actions à mener ensuite** : cours à revoir, éléments à approfondir, points de
   vigilance.

---

## 8. Ressources officielles

- Cours : **Adoptez les approches modernes des bases de données** (datalakes, lakehouses, Delta Lake,
  formats open table, Databricks, gouvernance).
- Cours : **Analysez les flux de données en temps réel avec Redpanda**.
- Cours : **Réalisez des calculs distribués avec Spark**.
- Cours : **Optimisez votre déploiement en créant des conteneurs avec Docker** (Docker Compose,
  Docker Hub, Docker Swarm).
- PDF OpenClassrooms : *« Entraînez votre mémoire »* (révisions).
- **Fiche d'autoévaluation** (à télécharger sur la plateforme — à compléter avant la session bilan).

---

## 9. Synthèse des exigences techniques (checklist)

- [ ] Topic Redpanda nommé exactement **`client_tickets`**.
- [ ] Script Python **producteur** de tickets aléatoires (6 champs imposés).
- [ ] Script **PySpark consommateur** lisant `client_tickets` (dépendances Kafka pour Spark).
- [ ] Au moins **une transformation + une agrégation** générant un insight.
- [ ] Prise en compte **performance** (mémoire, partitions) et **résilience** (gestion d'erreurs).
- [ ] **Export** des résultats en JSON / Parquet / autre format adapté.
- [ ] Un **Dockerfile par composant** (Redpanda, producteur, PySpark) + **docker-compose**.
- [ ] **README** documenté avec **diagramme Mermaid** du pipeline ETL.
- [ ] **Vidéo de démonstration** du POC, liée dans le README.
- [ ] **Schéma d'architecture hybride** on-premise ↔ cloud (PDF/PNG).
- [ ] **Document d'évaluation de compatibilité** (sécurité, interopérabilité, coûts).
- [ ] Livrables **nommés et zippés** selon la convention.
- [ ] **Fiche d'autoévaluation** complétée pour la session bilan.
