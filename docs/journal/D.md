# Journal — Conversation D (Traitement PySpark)

**Périmètre** : Exercice 2, Étape 2 · **Branche** : `conv-d-pyspark`
**Dossier de travail** : `worktrees/conv-d/` (un worktree par agent — plan §2.1)

---

## 2026-08-14 — État des lieux au démarrage

Le prérequis bloquant du plan (`docs/contrat-ticket.md` présent sur `origin`) était **satisfait** :
la Conversation C avait figé le contrat et validé son pipeline de bout en bout.

**Deux écarts par rapport au plan, constatés et assumés :**

1. **La Conversation E était déjà terminée** au moment du démarrage de D, alors que le plan la
   plaçait après. E a inversé la dépendance à dessein (voir `docs/journal/E.md`) : la machine n'a
   ni Java ni PySpark, donc **D ne peut rien exécuter tant que E n'a pas livré l'image Spark**.
   Bon choix — D a démarré avec un runtime prêt et un second contrat (`docs/contrat-export.md`)
   fixant chemins, formats et checkpoints. Il ne restait qu'un trou à combler : `src/spark/`.
2. **Trois sessions Claude Code tournaient simultanément** sur le même dépôt avec la même consigne.
   Signalé à Mathieu, qui a arrêté les autres. D a travaillé dans son propre worktree, et a testé
   **sans jamais toucher au worktree de E** (conteneurs jetables, données dans un dossier temporaire).

### Ce que D n'a PAS eu à décider

Tout ce qui suit était déjà figé et a été **repris tel quel**, sans réinterprétation :
schéma JSON du ticket et énumérations (contrat ticket §2), adresses du broker (§3), image Spark et
version du connecteur Kafka (contrat export §1), chemins et formats d'export (§3-4), emplacement des
checkpoints (§5), variables d'environnement (§6).

---

## 2026-08-14 — Le traitement (`src/spark/streaming_tickets.py`)

Un seul script, **trois requêtes streaming** partageant la même lecture du topic, chacune avec son
propre checkpoint (deux requêtes ne peuvent jamais partager un checkpoint) :

| Requête | Sortie | Mode | Rôle |
|---|---|---|---|
| `tickets_enrichis` | **Parquet**, partitionné par `request_type` | `append` | Le flux détaillé, ticket par ticket, après enrichissement. |
| `agregats` | **JSON** réécrit à chaque micro-batch + **insights en console** | `complete` | Le tableau de bord vivant. |
| `rejets` | **JSON** | `append` | Quarantaine des messages hors contrat. |

### Les deux transformations

- **Équipe support d'après le type de demande** — c'est l'exemple explicitement cité par la consigne.
  L'énumération `request_type` étant **fermée** (contrat §2.1), la table est exhaustive ; le repli
  `ÉQUIPE_INCONNUE` n'est là qu'au titre de la résilience, pour un producteur hors contrat.
  **Un type inattendu n'est pas rejeté** : le ticket reste exploitable, simplement non routé.
- **Engagement de délai (SLA) d'après la priorité** — 2 h / 8 h / 24 h / 72 h, et l'échéance
  correspondante calculée par ticket. Sans elle, les agrégats comptent des tickets sans dire
  lesquels risquent de déraper.

### Les agrégations

Une **seule** requête d'agrégation (`request_type` × `equipe_support` × `priority`), dont trois vues
métier sont dérivées dans `foreachBatch` : par type, par priorité, par équipe. C'est volontaire —
chaque requête streaming supplémentaire ouvrirait **son propre consommateur Kafka** et relirait le
topic entier pour le même résultat.

La vue « charge par équipe » ajoute la **part de tickets critiques** : à volume égal, deux équipes
ne sont pas sous la même pression. C'est le seul agrégat réellement actionnable des trois.

---

## Les pièges rencontrés (et ce qu'ils ont coûté)

### 1. `from_json` ne renvoie PAS une structure nulle sur un JSON illisible

Le piège le plus coûteux, **trouvé uniquement parce que la quarantaine a été testée pour de vrai**.

En mode permissif (le défaut), `from_json` sur une chaîne illisible renvoie une structure
**non nulle dont tous les champs sont nuls** — et non `NULL` comme la documentation le laisse
supposer. Conséquence : `col("ticket").isNull()` ne détecte **jamais rien**.

- Le **filtrage** était juste par chance : il testait déjà les 6 champs un par un.
- Le **motif de rejet**, lui, était faux à 100 % : tout message illisible était étiqueté
  `champ_obligatoire_manquant` au lieu de `json_illisible`.

Corrigé en jugeant sur les champs et jamais sur la structure : *tous* les champs nuls ⇒ illisible,
*certains* nuls ⇒ amputé.

### 2. `countDistinct` est interdit en streaming

Spark rejette toute opération distincte sur un flux, faute de pouvoir en borner l'état.
Remplacé par `approx_count_distinct` (HyperLogLog) : état de taille fixe, erreur de l'ordre de 2 %,
largement suffisant pour « combien de clients distincts se plaignent ? ».

### 3. `docker stop` ne déclenche pas d'arrêt gracieux — et c'est normal

Le script installe des gestionnaires SIGINT/SIGTERM, mais **sous `spark-submit` ils ne servent
jamais** : le PID 1 du conteneur est la **JVM**, qui intercepte le signal avant le driver Python.
`docker stop` termine donc en **code 143**, micro-batch en cours interrompu.

Ce n'est pas un défaut à corriger, c'est le fonctionnement de PySpark. **La garantie de reprise ne
repose pas sur l'arrêt gracieux mais sur les checkpoints** — ce qui a été vérifié explicitement
(voir le tableau ci-dessous). Le code des gestionnaires reste utile pour un lancement direct
`python streaming_tickets.py`, et la documentation du script dit désormais exactement cela plutôt
que de promettre un arrêt propre qui n'a pas lieu.

### 4. Le mode `complete` interdit les sinks fichier

Piège transmis par E, confirmé à l'exécution : `foreachBatch` est la seule parade. Il redonne un
DataFrame **statique**, sur lequel `.mode("overwrite")` redevient utilisable.

### 5. Le sink fichier écrit un fichier **vide** à chaque micro-batch

Trouvé pendant la validation de bout en bout. Le sink `json` écrit un fichier par tâche et par
micro-batch, **y compris quand il n'y a rien à écrire**. Sur un topic conforme — le cas nominal —
`exports/rejets/` se remplissait d'un fichier de 0 octet toutes les 10 secondes : des milliers de
fichiers vides dans un dossier livrable.

Passé en `foreachBatch`, qui saute les lots vides et journalise en WARNING le nombre de messages
écartés. Compromis assumé : **au-moins-une-fois** au lieu d'exactement-une-fois. Pour une
quarantaine, un doublon après reprise est sans conséquence.

### 6. Absorber une erreur « au cas où » peut fabriquer une panne totale et silencieuse

**Le défaut le plus grave du lot, et il venait de moi.** Il n'est apparu qu'en enchaînant
arrêt brutal → redémarrage, ce qu'aucun test unitaire n'aurait produit.

Enchaînement constaté :

1. `docker stop` (code 143) interrompt l'écriture du state store **en plein vol** — il reste un
   fichier `.15.delta…tmp` de **0 octet** dans `checkpoints/agregats/state/0/0/` ;
2. au redémarrage, la requête `agregats` échoue avec `File …/15.delta does not exist` ;
3. **et mon gestionnaire d'erreur l'absorbait à chaque lot**, en journalisant sereinement
   « instantané ignoré, le prochain le réécrira ».

Résultat : un pipeline qui **paraît vivant** — conteneur « Up », requêtes actives, aucune sortie en
erreur — mais qui ne publie plus un seul agrégat et n'affiche plus un seul insight. Le pire mode de
défaillance possible.

La justification d'origine ne valait que pour une erreur **passagère** (un verrou de fichier sur le
volume monté). Appliquée à une panne **permanente**, elle transformait un échec bruyant en panne
silencieuse. Corrigé : compteur d'échecs **consécutifs**, remis à zéro à chaque succès ; au-delà de
3 (réglable par `SPARK_MAX_ECHECS_AGREGATS`), l'exception remonte et le job sort en erreur avec le
message indiquant la manœuvre de réparation.

> **Leçon retenue** : un `except` qui protège d'un incident passager doit **compter**. Sans borne,
> « on réessaiera au prochain tour » devient « on ne fera plus jamais rien, sans le dire ».

### Recommandation à répercuter (Conv. E ou G)

Les **checkpoints** sont aujourd'hui sur le montage lié Windows (`../data:/data`). Ils n'ont
pourtant aucune raison d'être lisibles depuis l'hôte : c'est de l'**état interne**, pas un livrable.
Les basculer sur un **volume Docker nommé** — en laissant `exports/` sur le montage lié, lui bien
destiné à être consulté — réduirait le risque d'écriture interrompue observé ci-dessus, les
montages Windows offrant des garanties de durabilité plus faibles qu'un volume natif.
**Non fait ici** : `docker-compose.yml` appartient à la Conversation E, et le modifier depuis la
branche de D créerait un conflit de merge pour G.

---

## Performance et résilience — les deux points de vigilance de l'Étape 2

### Performance

| Réglage | Valeur | Pourquoi |
|---|---|---|
| `spark.sql.shuffle.partitions` | **3** | Aligné sur les 3 partitions du topic. Le défaut de **200** ferait brasser 197 partitions vides à chaque agrégation. |
| `--driver-memory` | **2 g** (fixé par le compose de E) | En `local[*]`, tout se passe dans la JVM du driver : le défaut de 1 Go est juste. |
| `maxOffsetsPerTrigger` | **5000** | Plafonne le volume d'un micro-batch. Sans lui, un premier `earliest` sur un topic déjà rempli engloutit tout l'historique d'un coup — pic mémoire et premier affichage très retardé. |
| `cache()` dans `foreachBatch` | — | Le lot est relu 4 fois (détail + 3 vues) ; sans cache, Spark rejouerait l'agrégation à chaque écriture. |
| `coalesce(1)` sur les agrégats | — | Un fichier par instantané au lieu de 200 fragments : c'est un tableau de bord, pas un jeu de données massif. |

> **Observé** : pendant le rattrapage du retard initial, Spark signale `Current batch is falling
> behind` (11,5 s pour un déclencheur de 10 s). Le régime se stabilise une fois le topic rattrapé —
> c'est le comportement attendu, pas une saturation.

### Résilience

| Mécanisme | Ce qu'il couvre |
|---|---|
| **Checkpoints par requête** | Reprise aux mêmes offsets Kafka **et** restauration de l'état des agrégations. |
| `failOnDataLoss=false` | Le topic a 7 jours de rétention. Job arrêté plus longtemps ⇒ offsets disparus. Le défaut refuse de redémarrer ; ici on repart au plus ancien offset disponible, en le signalant. |
| **Quarantaine `rejets`** | Un message hors contrat n'interrompt pas le pipeline : il est écarté, motivé et tracé à sa position exacte dans le topic. |
| **Repli `ÉQUIPE_INCONNUE`** | Un `request_type` inattendu ne fait pas tomber le ticket. |
| **Reprise bornée** (3 tentatives, délai exponentiel) | Une coupure du broker relance les requêtes depuis le checkpoint. Bornée à dessein : au-delà, l'incident est structurel et doit rester visible. |
| **Échec d'écriture des agrégats absorbé** | L'instantané est réécrit intégralement au micro-batch suivant (mode `complete`) : en perdre un ne perd aucune donnée, alors qu'un verrou de fichier passager tuerait tout le pipeline. |

---

## Vérification de bout en bout (effectuée)

Exécutée dans l'image `poc-tickets/spark:1.0.0` livrée par E, sur le broker réel alimenté par le
producteur de C. Conteneurs jetables, données dans un dossier temporaire — **le worktree de E n'a
pas été touché**.

| Contrôle | Résultat |
|---|---|
| Démarrage du job, connecteur Kafka embarqué | **OK** — aucune résolution `--packages`, 3 requêtes actives en 2 s |
| Lecture du topic réel `client_tickets` | **OK** — **12 800+ tickets** consommés en continu |
| Transformation « équipe support » | **OK** — les 5 types routés vers les 5 équipes attendues |
| Agrégation par priorité | **OK** — 3992 / 2986 / 2042 / 976 sur 9996 tickets, soit **40 / 30 / 20 / 10 %** : exactement la pondération du producteur de C |
| Insights affichés **en continu** | **OK** — 3 tableaux réaffichés à chaque micro-batch de 10 s |
| Export **Parquet** partitionné | **OK** — `exports/tickets_enrichis/request_type=…` × 5, snappy |
| Export **JSON** des agrégats | **OK** — 4 vues (`detail_type_priorite`, `par_type`, `par_priorite`, `par_equipe`), écrasées à chaque lot |
| **Reprise après incident** (`docker stop` en code 143, puis redémarrage) | **OK** — reprise au **micro-batch 6, pas 0** ; cumul poursuivi à 12 511 au lieu de repartir de zéro. Offsets **et** état d'agrégation restaurés. |
| **Quarantaine** — JSON illisible | **OK** — motif `json_illisible` |
| **Quarantaine** — champ obligatoire manquant | **OK** — motif `champ_obligatoire_manquant` |
| **Repli** — `request_type` hors énumération | **OK** — ticket **conservé**, équipe `ÉQUIPE_INCONNUE` |
| Encodage UTF-8 des accents dans les exports | **OK** — fichiers en UTF-8 ; l'affichage abîmé sous PowerShell 5.1 vient du lecteur (ANSI par défaut), pas du fichier |

### Puis validation **du pipeline complet**, les 5 services ensemble

Faite dans un worktree jetable fusionnant `conv-d-pyspark` et `conv-e-export-docker` — **le merge
D ↔ E ne produit aucun conflit**, information utile pour la Conversation G.

| Contrôle | Résultat |
|---|---|
| `docker compose up --build` — **une seule commande** | **OK** — 5 services démarrés dans l'ordre, `topic-init` sort en code 0 |
| Les 3 images se construisent | **OK** — redpanda, producteur, spark |
| Spark démarre et consomme | **OK** — 3 requêtes actives en ~3 s, **18 000+ tickets** |
| Insights en console | **OK** — 3 tableaux toutes les 10 s |
| Répartition des priorités | **OK** — 7186 / 5386 / 3649 / 1738 sur 17 959, soit **40 / 30 / 20 / 10 %** |
| Exports Parquet + JSON sous `data/` depuis Windows | **OK** |
| **Plus aucun fichier vide** dans `exports/rejets/` | **OK** — le dossier n'est même pas créé quand il n'y a aucun rejet |
| Interface Spark `localhost:4040` | **OK** — 3 requêtes dans l'onglet Structured Streaming |
| Console Redpanda `localhost:8080` | **OK** — HTTP 200 |
| **`docker compose restart spark`** | **OK** — dernier lot **9** avant, reprise au lot **10**, cumul poursuivi 18 059 → 18 109 |

> **Nuance importante sur la reprise.** Elle a fonctionné à tous les redémarrages **sauf un** :
> celui où l'arrêt est tombé pile pendant l'écriture d'un `.delta`. Le state store était alors
> incomplet et la requête ne pouvait plus repartir — cas décrit au piège n°6, désormais **bruyant**
> au lieu d'être silencieux, avec la manœuvre de réparation dans le message d'erreur
> (supprimer `data/checkpoints/agregats/`). C'est aussi ce qui motive la recommandation de
> basculer les checkpoints sur un volume Docker nommé.

---

## Écart au contrat d'export — à valider par E ou G

Le contrat `docs/contrat-export.md` §2 décrit une arborescence à **deux** sorties. D en ajoute une
**troisième**, `exports/rejets/` (+ `checkpoints/rejets/`), pour la quarantaine.

- **Additif** : aucun chemin existant n'est déplacé ni modifié, rien ne casse côté E ni F.
- **Motivé** : c'est la preuve tangible de l'exigence de résilience de l'Étape 2 — sans elle, « le
  pipeline ne s'interrompt pas sur un message malformé » resterait une affirmation invérifiable.
- Les vues d'agrégats sont écrites en **sous-dossiers** de `exports/agregats/` plutôt qu'en un seul
  jeu de fichiers. Le contrat §7 laisse explicitement à D « le choix des insights ».

**À faire** : répercuter ces deux points dans `docs/contrat-export.md` §2 lors du merge (Conv. G).

---

## Reste à faire

- [x] **Câblage final du service `spark`** — le nom de script attendu par E
      (`src/spark/streaming_tickets.py`) et l'absence de `requirements.txt` à installer sont
      **conformes** à ce que E avait prévu : **aucune modification nécessaire de son côté**.
- [x] **Validation `docker compose up --build` complète** avec les 5 services — faite dans un
      worktree jetable fusionnant D et E (voir le tableau ci-dessus). Le merge est sans conflit.
- [ ] **Screenshots** à prendre par Mathieu (`screenshots/D-*.png`) — procédure ci-dessous.
- [ ] **Basculer les checkpoints sur un volume Docker nommé** — recommandation à arbitrer par
      Mathieu, à appliquer par E ou G (voir plus haut).

## Procédure de rejeu (pour les captures d'écran)

Une fois D et E réunies sur une même copie de travail, depuis la racine du dépôt :

1. `docker compose -f docker/docker-compose.yml up --build` → les 5 services démarrent
2. `docker compose -f docker/docker-compose.yml logs -f spark` → les 3 tableaux d'insights
   défilent toutes les 10 s
3. `ls data/exports/tickets_enrichis/` → les 5 dossiers `request_type=…`
4. `cat data/exports/agregats/par_equipe/*.json` → la charge par équipe
5. <http://localhost:4040> → interface Spark, onglet **Structured Streaming**, les 3 requêtes actives

Pour rejouer la **reprise après incident** : `docker compose restart spark`, puis vérifier dans les
logs que le numéro de micro-batch **reprend où il en était** au lieu de repartir à 0.

Pour repartir d'un état vierge : supprimer `data/checkpoints/`.

## Questions ouvertes

*Aucune pour Mathieu.* Un seul point d'arbitrage laissé à la Conversation G : répercuter dans
`docs/contrat-export.md` l'ajout de `exports/rejets/` et les sous-dossiers d'agrégats.
