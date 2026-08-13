# Consignes du projet — Modélisez une infrastructure dans le cloud

> Document de référence reprenant **l'intégralité des consignes officielles** du projet
> OpenClassrooms, y compris le **détail des étapes** (accordéons dépliés sur la plateforme).
> Parcours : **Data Engineer**.
> Sources :
> - <https://openclassrooms.com/fr/paths/1039/projects/1837/assignment>
> - <https://openclassrooms.com/fr/paths/1039/projects/1837/2804-exercice-1---modelisez-une-infrastructure-hybride-dans-le-cloud>
> - <https://openclassrooms.com/fr/paths/1039/projects/1837/2806-exercice-2---gerez-des-tickets-clients-avec-redpanda-et-pyspark>
> - <https://openclassrooms.com/fr/paths/1039/projects/1837/2807-livrables-et-bilan>
>
> **Capturé le : 2026-08-14** · Page projet **mise à jour le lundi 27 juillet 2026** · Durée estimée : **60 heures**.

> ### ⚠️ Historique de cette page
> Une première capture (2026-06-26, basée sur la version du 5 mai 2026) ne contenait que le
> *chapeau* des exercices. Les **Étapes 1/2/3 détaillées de l'Exercice 1** étaient absentes et
> imposent des exigences supplémentaires (**entrepôt de données**, **service d'extension d'AD**,
> **chiffrage des coûts par composant**, **format 400–1200 mots**). Cette version les intègre.

---

## 1. Vue d'ensemble

### Ce que vous allez apprendre dans ce projet

Dans ce projet, vous allez apprendre à concevoir et implémenter une **infrastructure de
données hybride** qui combine des technologies de **traitement distribué (Spark)** et de
**streaming de données (Redpanda)**. Vous acquerrez des compétences clés dans l'architecture
de **pipelines ETL en temps réel**, depuis l'extraction de données issues de multiples
sources jusqu'à leur transformation et stockage dans le cloud.

### Livrables annoncés dans la vue d'ensemble

> *« Vous allez produire les livrables suivants : »*
> - Une **sélection de composants cloud** pour une architecture hybride
> - Un **schéma d'architecture** illustrant le flux de données entre on-premise et cloud
> - Un **pipeline ETL opérationnel** avec Spark et Redpanda

### Vous allez découvrir et pratiquer

- la modélisation d'architectures hybrides et la sélection de services de stockage et de
  traitement dans le cloud ;
- la configuration et l'utilisation de **Spark** pour l'ETL de données volumineuses issues de
  diverses sources ;
- la mise en œuvre d'un **pipeline de données en temps réel** via un moteur de streaming comme
  **Redpanda**.

Ce projet renforce les compétences en **manipulation de données massives** et en **architecture
cloud** : compatibilité des environnements on-premise et cloud, pratiques de sécurisation des
données, et interopérabilité des systèmes.

---

## 2. Compétences évaluées

1. **Représenter visuellement** une infrastructure de gestion des données.
2. **Transformer** des données afin de les adapter à leur utilisation finale.
3. **Charger** des données afin de les stocker dans un emplacement adapté.
4. **Évaluer la compatibilité** des composants avec l'environnement SI de l'organisation.
5. **Extraire** des données issues de toutes sources confondues pour les traiter ou les déplacer.
6. **Documenter son travail.**
7. **Identifier et sélectionner** les composants nécessaires à une infrastructure de données.

---

## 3. Structure du projet (5 activités)

| # | Type | Intitulé |
|---|------|----------|
| 1 | Cours | **Adoptez les approches modernes des bases de données** — datalakes et lakehouses |
| 2 | Cours | **Analysez les flux de données en temps réel avec Redpanda** |
| 3 | Exercice 1 | **Modélisez une infrastructure hybride dans le cloud** + fiche d'autoévaluation |
| 4 | Cours | **Réalisez des calculs distribués avec Spark** |
| 5 | Exercice 2 | **Gérez des tickets clients avec Redpanda et PySpark** + fiche d'autoévaluation |

À l'issue du projet : **session de bilan avec le mentor**.

---

## 4. Exercice 1 — Modélisez une infrastructure hybride dans le cloud

### Objectif

Modéliser une **infrastructure hybride dans le cloud** capable d'interagir avec l'environnement
**système d'information (SI) existant** d'une organisation fictive. L'objectif est d'intégrer des
services cloud pour **moderniser l'infrastructure de gestion des données** tout en assurant la
**compatibilité avec le SI on-premise existant**.

> **Rappel — On-premise** : infrastructure informatique installée et hébergée localement, dans les
> locaux de l'entreprise, sous la responsabilité directe de l'organisation, par opposition au cloud.

### Contexte — *InduTechData*

**InduTechData** est une entreprise de taille moyenne, fondée il y a 15 ans, spécialisée dans
l'analyse de données pour le **secteur industriel**.

- Avec l'introduction récente de solutions **IoT**, elle enregistre une augmentation mensuelle de
  **50 Go de données en temps réel** (flux continus de capteurs nécessitant un traitement rapide
  et fiable).
- L'**infrastructure datacenter actuelle atteint ses limites de capacité**. L'entreprise souhaite
  moderniser sa gestion des données pour améliorer **scalabilité et performance**.
- Elle envisage d'exploiter la flexibilité du cloud, **en particulier avec les services AWS**, pour
  intégrer les nouvelles solutions tout en assurant la compatibilité avec son SI existant.
- Objectif : gestion plus évolutive des données tout en maintenant une **interopérabilité fluide**
  avec l'infrastructure on-premise — notamment pour la **gestion des identités** et la
  **sécurisation des flux de données**.

#### Description du SI on-premise de InduTechData

- Un **cluster de serveurs SQL Server** hébergeant **40 To de données critiques** : données des
  applications métiers (ERP, CRM), avec sauvegardes régulières et réplication pour la résilience.
- Une **baie de stockage SAN** pour les **données non structurées (10 To)** : journaux système,
  fichiers utilisateurs et capteurs IoT.
- Un serveur **Active Directory (AD)** pour l'authentification, l'autorisation et la gestion des
  utilisateurs.
- Des serveurs dédiés à un **ERP** et un **CRM** (comptabilité, ressources humaines, relation
  client).

### Votre mission

1. **Identifier les composants cloud adaptés** (*en utilisant les services Redpanda*) et
   **modéliser l'architecture hybride**.
2. **Créer un schéma** montrant les interactions entre le datacenter **on-premise** et le **cloud**.
3. **Évaluer la compatibilité** de l'infrastructure hybride proposée avec le SI existant, en tenant
   compte des contraintes de **sécurité**, d'**interopérabilité** et de **coûts**.

> *« Cet exercice est entièrement guidé. »*

> ### 🎯 Arbitrage AWS vs Redpanda — **tranché : c'est les deux**
> L'Étape 1 ci-dessous demande explicitement de sélectionner un **service de stockage d'objets**,
> un **entrepôt de données cloud** et un **service d'extension de l'Active Directory** — ce sont
> des services **AWS** — **et** d'adopter **Redpanda** comme plateforme de streaming.
> L'architecture attendue est donc **AWS + Redpanda**, pas l'un ou l'autre.

---

### Étape 1 — Identifiez et sélectionnez les composants cloud

**Prérequis**
- Avoir lu le cours « Adoptez les approches modernes des bases de données ».
- Avoir lu le cours « Analysez les flux de données en temps réel avec Redpanda ».
- Avoir compris l'objectif du responsable d'infrastructure cloud d'InduTechData.

**Résultat attendu** — *Identification des composants cloud.*

**Instructions** — À partir des besoins mentionnés, et **en utilisant Redpanda**, sélectionnez les
services cloud les plus adaptés :

1. **Stockage de données non structurées** (logs, données brutes IoT, fichiers utilisateurs) :
   - Utilisez un **service de stockage d'objets** dans le cloud pour héberger les données non
     structurées.
   - **Justifiez votre choix** en termes de **scalabilité**, **sécurité**, et **interopérabilité
     avec Redpanda**.
2. **Entrepôt de données** :
   - Sélectionnez un **entrepôt de données cloud**. Il permettra de centraliser les données
     analytiques et de supporter des **requêtes SQL complexes**.
   - **Expliquez comment synchroniser** les bases **SQL Server on-premise** avec la solution cloud
     choisie.
3. **Traitement des données en temps réel (streaming)** :
   - Adoptez **Redpanda** comme plateforme de streaming — compatible avec l'écosystème **Kafka**,
     performances optimales pour les flux IoT et les logs.
   - **Justifiez** par sa **simplicité d'installation**, sa **faible consommation de ressources** et
     ses **fonctionnalités intégrées** pour orchestrer les flux de données.
4. **Sécurisation et gestion des accès** :
   - Proposez un **service pour étendre l'Active Directory on-premise au cloud**.
   - **Expliquez comment** il garantit une **gestion unifiée des identités et des permissions** sur
     l'ensemble de l'infrastructure.

**Points de vigilance**
- Ne pas oublier de **représenter les flux de données critiques** (en temps réel et/ou batch).
- S'assurer que les **accès utilisateurs et les transferts de données sont sécurisés**.
- Ne pas **sous-estimer l'impact des coûts** d'utilisation des services cloud.

**Ressources** — Cours « Adoptez les approches modernes des bases de données » · Cours « Analysez
les flux de données en temps réel avec Redpanda ».

---

### Étape 2 — Représentez visuellement l'infrastructure hybride

**Instructions**
- Utilisez un **outil de modélisation** (Lucidchart, Draw.io, etc.) pour créer un schéma clair de
  l'infrastructure hybride, **incluant** :
  - les **composants cloud et leurs connexions** ;
  - les **flux de données critiques (IoT, logs)** traités en **temps réel via Redpanda** ;
  - les **points de synchronisation** entre le SI on-premise (**SQL Server**, **Active Directory**)
    et les services cloud ;
  - les **flux de données, avec des indications sur les protocoles de transfert (batch ou temps
    réel)**.
- **Exportez** votre schéma au format **PDF/PNG**.

**Résultat attendu** — Un **schéma visuel** de l'infrastructure hybride (**PDF/PNG**).

---

### Étape 3 — Évaluez la compatibilité avec l'environnement SI existant

En analysant les choix de composants et la modélisation de l'architecture, rédigez une évaluation de
compatibilité de cette infrastructure hybride avec l'environnement SI on-premise d'InduTechData.
Vous aborderez les éléments suivants :

1. **Sécurité et conformité**
   - Vérifiez si Redpanda et les solutions cloud garantissent une **protection des flux de données
     sensibles pendant leur transfert (via SSL/TLS)**.
   - Évaluez si la **gestion des identités est homogène** entre **AD** et la solution cloud choisie.
2. **Interopérabilité**
   - Analysez comment **Redpanda peut s'intégrer avec SQL Server** et d'autres systèmes on-premise
     pour traiter et transmettre les données.
   - Assurez-vous que les **flux de données peuvent être automatisés**.
3. **Scalabilité et gestion des coûts**
   - Expliquez comment cette architecture **répond aux besoins futurs de croissance** (IoT, logs).
   - Faites des **recommandations pour surveiller les coûts** d'utilisation des services cloud
     (ex. : **CloudWatch**, **budget AWS**).
   - Faites une **première estimation des coûts initiaux et récurrents du projet, par composant**.
   - N'hésitez pas à consulter un calculateur comme l'**AWS Pricing Calculator**.

**Résultat attendu** — Un **document Word (ou équivalent)** comportant :
- une **justification documentée pour chaque composant cloud sélectionné** ;
- une **analyse de la compatibilité** de l'architecture avec le SI on-premise.

> **Format imposé** : document structuré d'**environ 400 à 1200 mots** présentant :
> - les **avantages** ;
> - les **limitations** ;
> - les **points d'attention** pour l'intégration de l'infrastructure hybride.

---

### Livrables de l'Exercice 1

- **Schéma de l'infrastructure hybride** : diagramme illustrant la structure de l'infrastructure
  hybride — **format PDF / PNG**.
- **Évaluation de compatibilité avec l'environnement SI** : document détaillant l'évaluation des
  choix faits pour assurer une **intégration fonctionnelle** entre l'infrastructure cloud et le SI
  on-premise.

---

## 5. Exercice 2 — Gérez des tickets clients avec Redpanda et PySpark

### Contexte

Votre manager chez **InduTech** vous demande de réaliser un **POC (Proof Of Concept)** sur un
**système de gestion de tickets clients**. Les tickets sont **générés en temps réel** et contiennent :

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

### Étape 1 — Configurez Redpanda

**Instructions**
- *Installation de Redpanda* : téléchargez et installez Redpanda sur votre machine **ou utilisez une
  image Docker**. Lancez Redpanda et assurez-vous qu'il est opérationnel. *(N'hésitez pas à valider
  l'opérationnalité de Redpanda avec votre mentor si besoin.)*
- *Création d'un topic* : créez un topic nommé **`client_tickets`** dans Redpanda pour stocker les
  données de tickets.
- Écrivez un **script Python** pour **produire des données de tickets (aléatoires)** dans le topic
  `client_tickets`. A minima : ID du ticket, ID du client, date/heure de création, demande, type de
  demande, priorité.

**Résultat attendu** — Votre code Python.
**Outils** — MySQL ou équivalent · Python 3 + Redpanda.
**Ressources** — Cours Redpanda · Tutoriel Redpanda + Python (simulation d'un chat) · Cours Docker.

### Étape 2 — Traitez les données avec PySpark

**Instructions**
- *Lecture des données de Redpanda* : assurez-vous que **PySpark et les dépendances nécessaires pour
  Kafka** sont installées. Écrivez un script **PySpark** pour **lire** les données du topic
  `client_tickets` et les **traiter**.
- *Transformation et analyse* : ajoutez des **transformations et des agrégations** pour générer des
  insights. *Ex. : ajouter automatiquement le nom d'une équipe de support assignée en fonction du
  type de demande, ou calculer le nombre de tickets par type.*

**Résultat attendu** — Votre code Python (PySpark).
**Points de vigilance**
- *Performance du cluster Spark* : configurer adéquatement la **mémoire** et le **nombre de
  partitions**.
- *Résilience* : **gérer les erreurs** pour éviter des interruptions dans le pipeline (ex. reprise
  après une déconnexion du SQL).

**Outils** — MySQL ou équivalent · Python 3 + PySpark.
**Ressources** — Cours « Réalisez des calculs distribués avec Spark ».

### Étape 3 — Exportez les données

- Exportez les **résultats des analyses** dans un fichier au **format adapté (JSON, Parquet ou
  autre)** pour une visualisation ultérieure.

### Étape 4 — Organisez la conteneurisation

- Créez un fichier **Dockerfile** pour **chaque élément** du projet :
  - l'image Docker de **Redpanda** ;
  - le **script générateur de tickets** ;
  - le **script de traitement PySpark**.
- Utilisez **Docker Compose** pour orchestrer, construire et lancer les **conteneurs et volumes**.

**Résultat attendu** — Un répertoire zippé contenant l'ensemble de votre code (**Redpanda + PySpark
+ Docker**).
**Outils** — Docker.

### Étape 5 — Construisez votre documentation

- Rédigez un **README**.
- Utilisez **Mermaid** afin d'intégrer un **diagramme de votre pipeline** dans votre README.
- Réalisez une **vidéo** pour expliquer comment **utiliser votre POC** et intégrez-la dans le README.
  - Support libre : **YouTube, Loom ou autre**.
  - Vidéo **courte et efficiente**, **aucune durée maximale imposée**.

**Résultats attendus**
- *Schéma de flux de données* : diagramme (intégré au README via **Mermaid**) montrant le flux de
  données depuis les différentes sources et illustrant l'architecture du **pipeline ETL**.
- *Démonstration* : courte **vidéo** présentant votre pipeline ETL et prouvant sa fonctionnalité.

---

## 6. Récapitulatif des livrables à déposer

| Activité | Livrables |
|----------|-----------|
| **Exercice 1 — Modélisez une infrastructure hybride** | **Schéma** de l'infrastructure hybride (PDF/PNG) · **Document d'évaluation de compatibilité** (400–1200 mots) |
| **Exercice 2 — Gérez des tickets clients** | **Code** (répertoire zippé : Redpanda + PySpark + Docker) · **Schéma de flux** (Mermaid, dans le README) · **Vidéo de démonstration** |

### Convention de nommage du dépôt

Déposez sur la plateforme, dans un dossier **zip** nommé `Titre_du_projet_nom_prenom`, tous les
livrables nommés ainsi : `Nom_Prenom_n°_du_livrable_nom_du_livrable_date_de_démarrage_du_projet`
(date au format `mmaaaa`).

**Date de démarrage retenue : `082026` (août 2026).**

- `Zinzen_Mathieu_1_schema_082026`
- `Zinzen_Mathieu_2_evaluation_082026`
- `Zinzen_Mathieu_3_code_082026`
- `Zinzen_Mathieu_4_video_082026`

> Exemple officiel donné par OpenClassrooms : `Janek_Meriem_1_schema_012025`.

---

## 7. Session de bilan avec le mentor

1. **Discutez de votre fiche d'autoévaluation** et des commentaires laissés en colonne « Notes ».
2. **Expliquez les difficultés rencontrées**.
3. **Présentez vos points forts**.
4. **Identifiez les actions à mener ensuite**.

---

## 8. Synthèse des exigences techniques (checklist)

### Exercice 1
- [ ] **Service de stockage d'objets** sélectionné + justifié (scalabilité, sécurité, interop Redpanda).
- [ ] **Entrepôt de données cloud** sélectionné + **méthode de synchronisation SQL Server → cloud** expliquée.
- [ ] **Redpanda** justifié (simplicité d'installation, faible conso ressources, fonctionnalités intégrées).
- [ ] **Service d'extension de l'Active Directory au cloud** proposé + gestion unifiée des identités expliquée.
- [ ] **Schéma PDF/PNG** : composants cloud + connexions, flux critiques temps réel, points de
      synchronisation on-prem ↔ cloud, **protocoles de transfert annotés (batch / temps réel)**.
- [ ] **Évaluation** : SSL/TLS, homogénéité des identités AD ↔ cloud, intégration Redpanda ↔ SQL Server,
      **automatisation des flux**, scalabilité future.
- [ ] **Surveillance des coûts** recommandée (CloudWatch, AWS Budgets).
- [ ] **Estimation chiffrée des coûts initiaux et récurrents, par composant** (AWS Pricing Calculator).
- [ ] **Format** : document Word ou équivalent, **400–1200 mots**, structuré en
      **avantages / limitations / points d'attention**.

### Exercice 2
- [ ] Topic Redpanda nommé exactement **`client_tickets`**.
- [ ] Script Python **producteur** de tickets aléatoires (6 champs imposés).
- [ ] Script **PySpark consommateur** lisant `client_tickets` (dépendances Kafka pour Spark).
- [ ] Au moins **une transformation + une agrégation** générant un insight.
- [ ] Prise en compte **performance** (mémoire, partitions) et **résilience** (gestion d'erreurs).
- [ ] **Export** des résultats en JSON / Parquet / autre format adapté.
- [ ] Un **Dockerfile par composant** (Redpanda, producteur, PySpark) + **docker-compose**.
- [ ] **README** documenté avec **diagramme Mermaid** du pipeline ETL.
- [ ] **Vidéo de démonstration** du POC, liée dans le README.

### Finalisation
- [ ] Livrables **nommés et zippés** selon la convention.
- [ ] **Fiche d'autoévaluation** complétée pour la session bilan.
