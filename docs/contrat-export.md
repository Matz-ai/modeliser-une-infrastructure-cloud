# Contrat de données — export des résultats et runtime Spark

> **Version 1.0.0** — complément de [`contrat-ticket.md`](contrat-ticket.md), dont le §5 renvoie ici
> pour le format et le chemin d'export des résultats d'analyse.
>
> Source de vérité unique pour tout ce qui touche à la **sortie** du pipeline : l'image Spark,
> l'arborescence des données, les formats d'export et les chemins de checkpoint. Le traitement
> PySpark écrit à ces chemins, `docker-compose.yml` monte les volumes correspondants.

---

## 1. Runtime Spark

Le poste de développement n'a **ni Java, ni PySpark installés** : un job Spark ne peut donc pas être
exécuté directement depuis l'hôte Windows. **Le conteneur `spark` est le seul runtime Spark du
projet** — c'est un choix assumé, qui garantit aussi que le POC démarre sur une machine vierge.

| Paramètre | Valeur figée | Justification |
|---|---|---|
| **Image de base** | **`spark:3.5.6-python3`** | Image **officielle Docker** publiée par Apache. Embarque Spark 3.5.6, Python 3 et un JRE — rien à installer côté hôte. |
| **Version Spark / PySpark** | **3.5.6** | Branche 3.5 : connecteur Kafka mature et abondamment documenté. Spark 4.x introduit des ruptures inutiles pour un POC. `pip install pyspark==3.5.6` en local suffit pour l'autocomplétion de l'IDE. |
| **Scala** | **2.12** | Celle de l'image ⇒ l'artefact Kafka est le `..._2.12`. Un `_2.13` produirait un `NoSuchMethodError` à l'exécution. |
| **Connecteur Kafka** | **`org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6`** | Version **strictement identique** à celle de Spark. Les JARs sont **pré-téléchargés dans l'image au build** : le conteneur démarre sans accès réseau, et sans les 30 s de résolution Ivy de `--packages` à chaque lancement. |

> Redpanda expose une API Kafka native : c'est bien le connecteur **Kafka** standard qui est utilisé,
> aucun adaptateur spécifique à Redpanda n'existe ni n'est nécessaire.

---

## 2. Arborescence des données

Tout ce que le pipeline écrit vit sous **une seule racine**, `DATA_DIR` — un unique volume monté
côté Docker, un unique dossier à consulter côté hôte.

```
data/                          ← DATA_DIR   (hôte : ./data   |  conteneur : /data)
├── exports/
│   ├── tickets_enrichis/      ← Parquet, flux détaillé enrichi   (§3)
│   ├── agregats/              ← JSON, instantané des agrégations (§4)
│   └── rejets/                ← JSON, messages hors contrat      (§4bis)
└── checkpoints/
    ├── tickets_enrichis/      ← état de reprise Spark            (§5)
    ├── agregats/
    └── rejets/
```

`data/` est **ignoré par git** (`.gitignore`) : ce sont des données générées, pas du code.

---

## 3. Export n°1 — Flux détaillé enrichi (**Parquet**, `append`)

Le flux ticket par ticket, **après enrichissement** (équipe support et échéance SLA).

| Paramètre | Valeur |
|---|---|
| **Chemin** | `${DATA_DIR}/exports/tickets_enrichis/` |
| **Format** | **Parquet** |
| **Mode de sortie** | `append` |
| **Partitionnement** | `partitionBy("request_type")` |
| **Checkpoint** | `${DATA_DIR}/checkpoints/tickets_enrichis/` |

**Pourquoi Parquet** : format **colonnaire et compressé**, lisible directement par pandas, Power BI,
DuckDB ou Athena — c'est la « visualisation ultérieure » demandée par la consigne (Étape 3). Le
schéma et les types y sont embarqués, contrairement au CSV.

**Pourquoi `append` fonctionne ici** : il n'y a **pas d'agrégation** sur ce flux, seulement un
enrichissement ligne à ligne. C'est le seul mode que les sinks fichier de Spark acceptent (§4).

---

## 4. Export n°2 — Agrégats (**JSON**, instantané écrasé)

Les agrégations continues (nombre de tickets par type, par priorité, par équipe).

| Paramètre | Valeur |
|---|---|
| **Chemin** | `${DATA_DIR}/exports/agregats/` |
| **Format** | **JSON** |
| **Écriture** | `foreachBatch` + `.write.mode("overwrite")` |
| **Checkpoint** | `${DATA_DIR}/checkpoints/agregats/` |

**Pourquoi JSON** : les agrégats sont **petits et lus par un humain**. Un JSON s'ouvre dans
n'importe quel éditeur ; un Parquet non.

> ### Pourquoi `foreachBatch` et pas `.format("json")`
>
> Une agrégation streaming impose le mode de sortie **`complete`** (ou `update`).
> Or **les sinks fichier de Spark ne supportent que `append`** : brancher `writeStream.format("json")`
> sur un `groupBy(...).count()` échoue au démarrage avec
> `Data source json does not support Complete output mode`.
>
> La parade standard est **`foreachBatch`** : à chaque micro-batch, Spark passe un DataFrame
> **statique**, sur lequel l'API batch classique — et donc `.mode("overwrite")` — redevient utilisable.
>
> ```python
> def ecrire_agregats(df, epoch_id):
>     (df.coalesce(1)
>        .write.mode("overwrite")
>        .json(f"{EXPORT_DIR}/agregats"))
>
> (agregats.writeStream
>     .outputMode("complete")
>     .foreachBatch(ecrire_agregats)
>     .option("checkpointLocation", f"{CHECKPOINT_DIR}/agregats")
>     .trigger(processingTime="10 seconds")
>     .start())
> ```
>
> `coalesce(1)` : un seul fichier par instantané plutôt que 200 fragments — c'est un tableau de bord,
> pas un jeu de données massif.

---

## 5. Checkpoints — la reprise après incident

`${DATA_DIR}/checkpoints/<nom_de_la_requete>/`, **un sous-dossier par requête streaming**
(deux requêtes ne peuvent jamais partager un checkpoint).

C'est ce qui couvre l'exigence de **résilience** de l'Étape 2 : offsets Kafka et état des
agrégations y sont persistés, donc un job relancé **repart où il s'était arrêté** au lieu de
re-consommer le topic depuis le début. Le dossier étant sur le **volume monté**, la reprise
survit à un `docker compose down` / `up`.

Pour repartir de zéro : supprimer `data/checkpoints/`.

---

## 6. Variables d'environnement

`docker-compose.yml` surcharge ces variables. Les **défauts visent une exécution depuis l'hôte**,
comme dans [`contrat-ticket.md`](contrat-ticket.md) §4.

| Variable | Défaut (hôte) | Valeur en conteneur |
|---|---|---|
| `DATA_DIR` | `./data` | `/data` |
| `EXPORT_DIR` | `${DATA_DIR}/exports` | `/data/exports` |
| `CHECKPOINT_DIR` | `${DATA_DIR}/checkpoints` | `/data/checkpoints` |
| `SPARK_TRIGGER_SECONDS` | `10` | `10` |

Les chemins sont lus via `os.getenv("EXPORT_DIR", "./data/exports")` et jamais codés en dur : le
conteneur et l'hôte n'ont pas la même arborescence, et c'est exactement ce que ces variables règlent.

---

## 7. Ce que ce contrat ne couvre **pas**

- La **logique** de transformation et d'agrégation, le choix des insights, la table
  `request_type` → équipe support, le réglage mémoire/partitions : voir `src/spark/`.
- Le **schéma du ticket**, le topic et les ports du broker :
  [`contrat-ticket.md`](contrat-ticket.md).

---

## 8. Journal des versions

| Version | Date | Modification |
|---|---|---|
| 1.0.0 | 2026-08-14 | Version initiale — image Spark, arborescence, formats d'export, checkpoints, variables d'environnement. |
