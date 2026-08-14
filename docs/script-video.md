# Script et déroulé de la vidéo de démonstration

> **Livrable** : Exercice 2, Étape 5 — « Réalisez une vidéo pour expliquer comment utiliser votre
> POC et intégrez-la dans le README ».
> **Support** : **Loom** (décision §4.5 du plan). **Durée cible : 5 à 6 minutes** — la consigne
> demande une vidéo « courte et efficiente », sans durée maximale imposée.
> **À tourner par Mathieu.** L'agent prépare le déroulé, il ne peut pas filmer.

---

## 1. Préparation — à faire AVANT de lancer l'enregistrement

Ces cinq minutes de préparation évitent de tourner trois fois.

### 1.1 Repartir d'un état vierge

```bash
docker compose -f docker/docker-compose.yml down -v
```

```bash
rm -rf data/
```

> Sans cela, le compteur d'insights démarre à plusieurs milliers de tickets et l'on ne **voit** pas
> le pipeline se remplir — c'est pourtant tout l'intérêt de la démo.

### 1.2 Pré-construire les images

```bash
docker compose -f docker/docker-compose.yml build
```

> **Indispensable.** Le premier `up --build` prend 2 à 4 minutes de construction : filmer une barre
> de progression Docker n'apporte rien. Une fois les images en cache, le `up --build` de la démo
> démarre en une dizaine de secondes — et reste parfaitement honnête, c'est bien la même commande.

### 1.3 Préparer l'écran

| À faire | Pourquoi |
|---|---|
| Terminal en **police 16-18 px minimum**, thème sombre | Loom compresse : du texte à 11 px est illisible chez le lecteur |
| Fenêtre du navigateur **déjà ouverte** sur deux onglets : `localhost:8080` et `localhost:4040` | Éviter de taper des URL à l'écran |
| Explorateur de fichiers ouvert sur la **racine du dépôt** | Pour montrer `data/` apparaître |
| **Fermer** Slack, mail, notifications | — |
| Micro testé | Loom n'a pas de rattrapage audio |

### 1.4 Ce qu'il ne faut PAS faire

- Ne pas lire ce script mot à mot — les phrases ci-dessous sont des **repères**, pas un texte.
- Ne pas s'excuser d'un temps de chargement : le commenter suffit.
- Ne pas montrer le code ligne à ligne. **La vidéo prouve que ça marche**, le README explique
  comment. Deux livrables, deux rôles.

---

## 2. Le déroulé, séquence par séquence

### 🎬 Séquence 1 — Intro · *~20 s* · écran : README en haut de page

> « Bonjour, je suis Mathieu Zinzen. Je vous présente le POC de l'exercice 2 du projet
> *Modélisez une infrastructure dans le cloud* : un **pipeline ETL temps réel** de gestion de
> tickets clients, avec **Redpanda** pour l'ingestion, **PySpark** pour le traitement, et
> **Docker** pour tout faire tourner d'une seule commande. »

> « En trois minutes vous allez voir des tickets être générés, ingérés, transformés, agrégés et
> exportés — en direct. »

---

### 🎬 Séquence 2 — L'architecture · *~50 s* · écran : le diagramme Mermaid du README

**Faire défiler doucement jusqu'au schéma de flux et le laisser à l'écran.**

> « Le pipeline se lit de gauche à droite, en trois temps. »

> « **Extract** : un producteur Python génère des tickets clients aléatoires — six champs imposés
> par la consigne — et les publie dans un topic Redpanda, `client_tickets`, réparti sur
> **3 partitions**. La clé de partitionnement est l'identifiant client, ce qui garantit que les
> tickets d'un même client restent ordonnés. »

> « **Transform** : PySpark lit ce flux en continu. Chaque message est d'abord **validé** contre le
> contrat de données ; ceux qui ne le respectent pas partent en quarantaine sans interrompre le
> pipeline. Les autres sont enrichis — on leur assigne automatiquement une **équipe support** selon
> le type de demande, et un **délai d'engagement** selon la priorité — puis agrégés. »

> « **Load** : trois sorties. Le détail en **Parquet**, les agrégations en **JSON**, et la
> quarantaine. Plus les insights affichés en direct dans la console. »

> « Un point important : les **checkpoints**. C'est ce qui permet au traitement de reprendre
> exactement où il s'était arrêté après un incident — je vous le montrerai à la fin. »

---

### 🎬 Séquence 3 — Le lancement · *~50 s* · écran : terminal

> « Prérequis : **Docker, et rien d'autre**. Ni Python, ni Java, ni Spark sur la machine — tout est
> dans les conteneurs. »

**Taper et lancer :**

```bash
docker compose -f docker/docker-compose.yml up --build
```

> « Une seule commande. Elle construit les trois images, démarre le broker, **crée le topic**,
> lance le producteur puis le traitement Spark — dans le bon ordre, avec les bonnes dépendances. »

**Laisser défiler. Pointer `redpanda-topic-init` quand il sort :**

> « Ce conteneur-là, `topic-init`, crée le topic puis s'arrête : c'est normal qu'il apparaisse en
> "Exited". C'est lui qui rend le pipeline démarrable d'une seule commande. »

---

### 🎬 Séquence 4 — Les tickets arrivent · *~55 s* · écran : navigateur, `localhost:8080`

> « Voici la console web de Redpanda. »

**Aller dans Topics → `client_tickets`.**

> « Le topic `client_tickets`, avec ses **3 partitions**. »

**Onglet Messages, laisser défiler quelques secondes.**

> « Et les tickets qui arrivent en direct. Chacun contient les six champs demandés : identifiant du
> ticket, identifiant du client, date de création, la demande, son type et sa priorité. »

**Cliquer sur un message pour déplier le JSON.**

> « Les textes de demande sont cohérents avec le type — un ticket "technique" parle d'un capteur en
> panne, pas d'une facture. C'est ce qui rendra les agrégations lisibles. »

---

### 🎬 Séquence 5 — Le traitement et les insights · *~80 s* · écran : terminal

**Revenir au terminal (ou `docker compose -f docker/docker-compose.yml logs -f spark`).**

> « Et voilà le cœur du sujet : Spark consomme le flux et **recalcule ses insights toutes les
> 10 secondes**. »

**Laisser passer un cycle complet, puis commenter les trois tableaux :**

> « Premier tableau : le nombre de tickets **par type de demande**, avec l'**équipe support assignée
> automatiquement**. C'est la transformation demandée par la consigne — la facturation part au
> Service Facturation, le technique au Support Technique N2, et ainsi de suite. »

> « Deuxième tableau : la répartition **par priorité**. On retrouve bien la pyramide attendue —
> beaucoup de demandes basses, peu de critiques. »

> « Troisième tableau, le plus utile : la **charge par équipe**, avec la **part de tickets
> critiques**. Parce qu'à volume égal, deux équipes ne sont pas sous la même pression. C'est le seul
> chiffre sur lequel un responsable support peut réellement agir. »

**Attendre le cycle suivant.**

> « Et le compteur monte : c'est bien du **temps réel**, pas un traitement par lot. »

**Basculer sur `localhost:4040` → onglet Structured Streaming.**

> « L'interface Spark confirme les **trois requêtes actives** en parallèle : le flux détaillé, les
> agrégations, et la quarantaine. Chacune a son propre checkpoint. »

---

### 🎬 Séquence 6 — Les exports · *~55 s* · écran : explorateur + terminal

**Montrer le dossier `data/` apparu à la racine.**

> « Troisième étape de la consigne : l'export. Tout atterrit sous `data/`. »

**Ouvrir `data/exports/tickets_enrichis/`.**

> « Le flux détaillé en **Parquet**, **partitionné par type de demande** — un dossier par type.
> Parquet parce que c'est colonnaire, compressé et typé : ça se relit directement dans pandas,
> DuckDB ou Power BI. C'est la "visualisation ultérieure" que demande l'énoncé. »

**Ouvrir `data/exports/agregats/par_equipe/` et afficher le JSON.**

> « Et les agrégations en **JSON**, réécrites à chaque micro-batch. Petites, lisibles telles quelles
> — c'est le tableau de bord. »

**Optionnel, si l'environnement Python le permet :**

```bash
python -c "import pandas as pd; print(pd.read_parquet('data/exports/tickets_enrichis').head())"
```

> « Relu depuis pandas, sans rien installer d'autre : on voit l'équipe assignée et l'échéance
> calculée pour chaque ticket. »

---

### 🎬 Séquence 7 — La résilience · *~50 s* · écran : terminal

> « Dernier point, et c'est un point de vigilance explicite de la consigne : la **résilience**. »

**Noter à voix haute le numéro du dernier micro-batch affiché.**

```bash
docker compose -f docker/docker-compose.yml restart spark
```

> « Je coupe brutalement le traitement Spark, en pleine consommation. »

**Attendre le redémarrage et le premier tableau d'insights.**

> « Il redémarre… et il **reprend au micro-batch suivant**, pas à zéro. Le cumul continue là où il
> s'était arrêté. Les offsets Kafka **et** l'état des agrégations ont été restaurés depuis le
> checkpoint. Aucun ticket n'est reperdu ni recompté. »

> « Même chose pour les messages malformés : un JSON illisible ou incomplet part en quarantaine avec
> son motif et sa position exacte dans le topic — il n'interrompt jamais le pipeline. »

---

### 🎬 Séquence 8 — Conclusion · *~20 s*

```bash
docker compose -f docker/docker-compose.yml down
```

> « Voilà pour le POC : ingestion temps réel avec Redpanda, traitement et agrégations avec PySpark,
> exports Parquet et JSON, le tout conteneurisé et démarrable d'une seule commande. »

> « Tout est documenté dans le README du dépôt, avec les prérequis, le schéma de flux et le détail
> des insights produits. Merci de votre attention. »

---

## 3. Après le tournage

1. Dans Loom : **Share → Copy link**.
2. Remplacer `https://www.loom.com/share/LIEN-A-REMPLACER` dans le [`README.md`](../README.md),
   section « 🎬 Vidéo de démonstration » (**deux occurrences** : le lien et la note au-dessus).
3. Vérifier que le partage est réglé sur **« Anyone with the link »** — un lien privé bloquerait
   l'évaluateur.
4. Commiter le README mis à jour.

## 4. Aide-mémoire des commandes de la démo

```bash
docker compose -f docker/docker-compose.yml down -v
```

```bash
docker compose -f docker/docker-compose.yml build
```

```bash
docker compose -f docker/docker-compose.yml up --build
```

```bash
docker compose -f docker/docker-compose.yml logs -f spark
```

```bash
docker compose -f docker/docker-compose.yml restart spark
```

```bash
docker compose -f docker/docker-compose.yml down
```

| Onglet | URL |
|---|---|
| Console Redpanda | <http://localhost:8080> |
| Interface Spark | <http://localhost:4040> |
