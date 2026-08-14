# Script de la vidéo de démonstration — à lire à voix haute

> **Livrable** : Exercice 2, Étape 5. **Support** : Loom (décision §4.5 du plan).
>
> **Usage prévu** : ce document sur l'**écran 2**, tu le lis. Le terminal et Chrome sur l'**écran 1**.
> Les blocs `🗣️` se lisent **mot à mot**. Les blocs `🎬` sont les gestes à faire, **pas à lire**.

## ⏱️ Durée — lis ceci avant tout le reste

Le texte à lire fait **593 mots**, compté. À un débit de lecture confortable :

| | Parole seule | **Vidéo finale** |
|---|---|---|
| Lecture lente (130 mots/min) | 4 min 33 | **4 min 53** |
| Lecture posée (140 mots/min) | 4 min 14 | **4 min 34** |
| Lecture normale (150 mots/min) | 3 min 57 | **4 min 17** |

> ### 🔴 Les deux pauses Loom sont OBLIGATOIRES, pas optionnelles
>
> Ces chiffres supposent que tu **mets Loom en pause** pendant les deux temps morts : le démarrage
> du pipeline (séquence ⑤, ~25 s) et le redémarrage de Spark (séquence ⑪, ~17 s).
>
> **Sans ces pauses, la vidéo dure 5 min 16 à 5 min 35 — et le plan gratuit Loom coupe à
> 5 minutes.** Les deux emplacements sont signalés en rouge dans le script.
>
> Tu avais raison de te méfier : la première version de ce script dépassait effectivement.
> Il a été raccourci de 120 mots pour tenir.

Si ça déborde quand même, voir **§5** — il y a un repli simple qui supprime complètement la
contrainte.

---

## 1. Préparation — 5 minutes, à faire AVANT d'enregistrer

### 1.1 Le bon dossier

⚠️ **Tout se passe dans `worktrees\demo`**, et nulle part ailleurs. C'est le seul endroit où le
pipeline complet existe aujourd'hui : `docker-compose.yml` vit sur la branche de la Conversation E,
le traitement Spark sur celle de D, et ce dossier est le seul où les deux sont réunies. Le dossier
`modeliser-une-infrastructure-cloud` est sur la branche B et **n'a pas de `docker/`** — c'est ce qui
a provoqué l'erreur « The system cannot find the file specified ».

*(Après la Conversation G, tout sera à la racine du dépôt et cette précaution disparaîtra.)*

```powershell
cd "C:\Users\Utilisateur\Documents\openclassrooms\modérlisez une infra dans le cloud\worktrees\demo"
```

### 1.2 Repartir d'un état vierge

```powershell
docker compose -f docker/docker-compose.yml down -v
```

```powershell
Remove-Item -Recurse -Force data
```

> Sans ça, le compteur d'insights démarre à plusieurs milliers de tickets et on **ne voit pas** le
> pipeline se remplir — c'est pourtant ce que la vidéo doit prouver.

### 1.3 Pré-construire les images

```powershell
docker compose -f docker/docker-compose.yml build
```

> **Indispensable.** Le tout premier build prend 2 à 4 minutes ; filmer une barre de progression
> Docker ne prouve rien. Une fois en cache, c'est **3 secondes** — et la commande montrée dans la
> vidéo reste exactement la même, donc la démo reste honnête.

### 1.4 Régler le débit sur 50 tickets/s

```powershell
$env:PRODUCER_RATE=50
```

> ⚠️ **À taper dans le terminal qui servira à la démo**, et dans celui-là seulement — la variable
> ne vit que dans cette fenêtre.
>
> Au débit par défaut de 5 tickets/s, les tableaux affichent une trentaine de tickets après une
> minute : illisible et peu convaincant. À 50/s, on dépasse **1700 tickets en 40 secondes** et les
> proportions se stabilisent visiblement. Mesuré, pas estimé.
>
> ⚠️ La syntaxe bash `PRODUCER_RATE=50 docker compose …` **ne fonctionne pas sous PowerShell**.

### 1.5 L'écran 1

| À préparer | Pourquoi |
|---|---|
| Terminal en **police 16-18 px minimum**, thème sombre, fenêtre **maximisée** | Loom compresse : du texte à 11 px est illisible chez l'évaluateur |
| Chrome avec **3 onglets déjà ouverts** : le README (GitHub ou local), `localhost:8080`, `localhost:4040` | Ne jamais taper une URL à l'écran |
| L'**explorateur de fichiers** ouvert sur `worktrees\demo` | Pour montrer `data\` apparaître |
| **Notifications coupées**, Slack et mail fermés | — |
| Micro testé | Loom n'a aucun rattrapage audio |

### 1.6 Dans Loom

- Mode **Écran + caméra** (ou écran seul, au choix) · **écran 1 uniquement**.
- **Repérer le bouton Pause avant de commencer** : il sert deux fois, aux séquences **⑤** et **⑪**,
  et c'est ce qui fait tenir la vidéo sous les 5 minutes. Les deux emplacements sont signalés en
  rouge dans le script.

---

## 2. Le script

> **Rappel de lecture** : `🗣️` = tu lis. `🎬` = tu fais, tu ne lis pas.
> Les `…` sont des respirations, pas des hésitations.

---

### ① `0:00` · Écran : **le README, en haut de page**

> 🗣️ **Bonjour, je suis Mathieu Zinzen.**
>
> **Voici le POC de l'exercice deux : un pipeline ETL temps réel de gestion de tickets clients.**
>
> **Redpanda pour l'ingestion, PySpark pour le traitement, Docker pour tout orchestrer.**
>
> **En quatre minutes, vous allez voir des tickets naître, être traités, et exportés. En direct.**

---

### ② `0:20` · 🎬 Faire défiler jusqu'au **schéma Mermaid** et le laisser plein écran

> 🗣️ **Le pipeline se lit de gauche à droite, en trois temps.**
>
> **À gauche, l'extraction. Un producteur Python génère des tickets clients aléatoires — les six
> champs imposés par la consigne — et les publie dans un topic Redpanda réparti sur trois
> partitions.**
>
> **Au centre, la transformation. PySpark lit ce flux en continu. Chaque message est validé : ceux
> qui ne respectent pas le contrat partent en quarantaine, sans jamais interrompre le pipeline. Les
> autres sont enrichis, puis agrégés.**
>
> **À droite, le chargement. Trois sorties : le détail en Parquet, les agrégations en JSON, et les
> insights affichés en direct dans la console.**

---

### ③ `1:00` · 🎬 Basculer sur le **terminal**

> 🗣️ **Prérequis : Docker, et rien d'autre. Ni Python, ni Java, ni Spark sur la machine. Tout est
> dans les conteneurs.**
>
> **Une seule commande.**

---

### ④ `1:12` · 🎬 Taper la commande, **sans encore valider**

```powershell
docker compose -f docker/docker-compose.yml up --build
```

> 🗣️ **Elle construit les images, démarre le broker, crée le topic, puis lance le producteur et le
> traitement Spark — dans le bon ordre, avec les bonnes dépendances.**

---

### ⑤ `1:22` · 🎬 **Valider.** Laisser défiler.

> 🗣️ **Ce conteneur-là, `topic-init`, crée le topic puis s'arrête : c'est normal qu'il apparaisse
> en "Exited". C'est lui qui rend le pipeline démarrable d'une seule commande.**

### 🔴 PAUSE LOOM N°1 — obligatoire

🎬 **Mettre Loom en pause** dès la phrase finie. Attendre le **premier tableau d'insights**
(~25 s de plus). **Reprendre l'enregistrement** juste avant la séquence ⑥.

> Sans cette pause, la vidéo dépasse les 5 minutes du plan gratuit Loom.

---

### ⑥ `1:35` · 🎬 Chrome, onglet **`localhost:8080`** · Topics → `client_tickets`

> 🗣️ **Voici la console web de Redpanda. Le topic `client_tickets`, avec ses trois partitions.**

🎬 **Onglet Messages.** Laisser défiler 3 secondes, puis **cliquer sur un message** pour déplier le
JSON.

> 🗣️ **Et les tickets qui arrivent en direct. Chacun contient les six champs demandés :
> identifiant du ticket, identifiant du client, date de création, la demande, son type et sa
> priorité.**
>
> **Les textes sont cohérents avec le type : un ticket "technique" parle d'un capteur en panne, pas
> d'une facture. C'est ce qui rend les agrégations lisibles.**

---

### ⑦ `2:05` · 🎬 Retour au **terminal**. Attendre qu'un cycle d'insights s'affiche entièrement.

> 🗣️ **Et voilà le cœur du sujet. Spark recalcule ses insights toutes les dix secondes.**

🎬 Pointer le **premier** tableau.

> 🗣️ **Premier tableau : le nombre de tickets par type de demande, avec l'équipe support assignée
> automatiquement. C'est la transformation demandée par la consigne.**

🎬 Pointer le **deuxième**.

> 🗣️ **Deuxième tableau : la répartition par priorité. On retrouve la pyramide attendue —
> beaucoup de demandes basses, peu de critiques.**

🎬 Pointer le **troisième**.

> 🗣️ **Troisième tableau, le plus utile : la charge par équipe, avec la part de tickets critiques.
> Parce qu'à volume égal, deux équipes ne sont pas sous la même pression.**

---

### ⑧ `2:45` · 🎬 Attendre le **cycle suivant**, montrer que le compteur a monté

> 🗣️ **Et le compteur monte à chaque cycle. C'est bien du temps réel, pas un traitement par lot.**

🎬 Chrome, onglet **`localhost:4040`** → **Structured Streaming**.

> 🗣️ **L'interface Spark confirme les trois requêtes actives en parallèle, chacune avec son propre
> point de reprise.**

---

### ⑨ `3:00` · 🎬 Explorateur de fichiers → `data\exports\tickets_enrichis\`

> 🗣️ **Troisième étape de la consigne : l'export. Tout atterrit sous `data`.**
>
> **Le détail en Parquet, partitionné par type de demande. Parquet parce que c'est colonnaire et
> typé : ça se relit directement dans pandas ou Power BI. C'est la "visualisation ultérieure" que
> demande l'énoncé.**

🎬 Aller dans `data\exports\agregats\par_equipe\` et **ouvrir le fichier `.json`**.

> 🗣️ **Et les agrégations en JSON, réécrites à chaque micro-batch. Petites, lisibles telles
> quelles. C'est le tableau de bord.**

---

### ⑩ `3:30` · 🎬 Retour au **terminal**

> 🗣️ **Dernier point, et c'est un point de vigilance explicite de la consigne : la résilience.**

🎬 **Lire à voix haute le numéro du dernier micro-batch affiché** (par exemple « micro-batch sept »).

> 🗣️ **Je note le numéro du dernier micro-batch… et je coupe brutalement le traitement Spark.**

---

### ⑪ `3:45` · 🎬 Ouvrir un **second onglet de terminal** et lancer :

```powershell
docker compose -f docker/docker-compose.yml restart spark
```

> 🗣️ **Il redémarre.**

### 🔴 PAUSE LOOM N°2 — obligatoire

🎬 **Mettre Loom en pause.** Attendre le premier **nouveau** tableau d'insights (~17 s).
**Reprendre l'enregistrement.**

---

### ⑫ `3:55` · 🎬 Montrer le nouveau numéro de micro-batch

> 🗣️ **Et il reprend au micro-batch suivant. Pas à zéro. Le cumul continue là où il s'était
> arrêté.**
>
> **Les offsets Kafka et l'état des agrégations ont été restaurés depuis le point de reprise. Aucun
> ticket n'est reperdu, aucun n'est recompté.**

---

### ⑬ `4:20` · 🎬 Taper, sans forcément attendre la fin :

```powershell
docker compose -f docker/docker-compose.yml down
```

> 🗣️ **Voilà pour ce POC : ingestion temps réel avec Redpanda, traitement et agrégations avec
> PySpark, exports Parquet et JSON, le tout démarrable d'une seule commande.**
>
> **Tout est documenté dans le README du dépôt. Merci de votre attention.**

**⏹️ Arrêter l'enregistrement.** Durée attendue : **4 min 17 à 4 min 53** selon ton débit de
lecture, les deux pauses ayant été respectées.

---

## 3. Aide-mémoire des commandes

| Moment | Commande |
|---|---|
| Avant (§1.2) | `docker compose -f docker/docker-compose.yml down -v` |
| Avant (§1.2) | `Remove-Item -Recurse -Force data` |
| Avant (§1.3) | `docker compose -f docker/docker-compose.yml build` |
| Avant (§1.4) | `$env:PRODUCER_RATE=50` |
| Séquence ④ | `docker compose -f docker/docker-compose.yml up --build` |
| Séquence ⑪ | `docker compose -f docker/docker-compose.yml restart spark` |
| Séquence ⑬ | `docker compose -f docker/docker-compose.yml down` |

| Onglet Chrome | URL |
|---|---|
| Console Redpanda | <http://localhost:8080> |
| Interface Spark | <http://localhost:4040> |

### Repères de temps mesurés

| Étape | Durée réelle |
|---|---|
| `build` images en cache | **3 s** |
| `up --build` → 5 services démarrés | **10 s** |
| `up --build` → premier tableau d'insights | **27 s** |
| `restart spark` → reprise visible | **17 s** |

---

## 4. Après le tournage

1. Dans Loom : **Share → Copy link**.
2. Régler le partage sur **« Anyone with the link »** — un lien privé bloquerait l'évaluateur.
3. Remplacer `https://www.loom.com/share/LIEN-A-REMPLACER` dans le [`README.md`](../README.md),
   section « 🎬 Vidéo de démonstration » — **deux occurrences** : la note d'avertissement et le lien.
4. Commiter le README mis à jour.

---

## 5. Si la vidéo dépasse quand même 5 minutes

**D'abord, vérifier l'évident** : les **deux pauses** ont-elles bien été faites ? Elles valent à
elles seules 42 secondes. C'est la cause n°1 d'un dépassement.

**Ensuite, dans l'ordre du moins coûteux au plus coûteux :**

1. **Couper la séquence ⑧** (interface Spark `:4040`) — le seul passage vraiment optionnel : il ne
   prouve rien que les tableaux d'insights ne prouvent déjà. **~10 s.**
2. **Raccourcir la séquence ②** : annoncer les trois blocs Extract / Transform / Load sans
   détailler la validation, qui est de toute façon reprise en ⑫. **~15 s.**
3. **Fusionner ⑬ dans ⑫** : lancer le `down` en parlant, plutôt qu'après. **~5 s.**

### Le vrai repli : changer de support

La limite de 5 minutes vient du **plan gratuit de Loom**, **pas du projet**. La consigne dit
explicitement « **YouTube, Loom ou autre** » et précise « **aucune durée maximale imposée** ».

Une vidéo **YouTube en "non répertoriée"** supprime entièrement la contrainte, sans rien changer au
livrable ni à ce script — et le lien s'intègre dans le README exactement de la même façon. **À
privilégier** plutôt que de sacrifier une séquence de démonstration pour rentrer dans un quota
commercial. Le choix de Loom était une préférence, pas une exigence.
