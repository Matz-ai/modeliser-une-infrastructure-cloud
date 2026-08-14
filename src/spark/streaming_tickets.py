"""Traitement PySpark du flux de tickets clients issu du topic Redpanda ``client_tickets``.

Projet OpenClassrooms « Modélisez une infrastructure dans le cloud » — Exercice 2, Étape 2.

Lit le topic en continu, valide chaque message contre le contrat, enrichit les tickets
(équipe support, engagement de délai), calcule des agrégations vivantes et exporte le tout.

Contrats de référence — **ce script ne réinvente rien de ce qui y est figé** :
  * ``docs/contrat-ticket.md``  §2  schéma JSON du ticket, énumérations fermées
  * ``docs/contrat-ticket.md``  §3  adresses du broker
  * ``docs/contrat-export.md``  §2-5 chemins, formats d'export et checkpoints

Trois requêtes streaming tournent en parallèle, chacune avec son propre checkpoint :

  1. ``tickets_enrichis``  Parquet, ``append``, partitionné par ``request_type``
  2. ``agregats``          JSON, instantané réécrit à chaque micro-batch + insights en console
  3. ``rejets``            JSON, quarantaine des messages hors contrat

Configuration par variables d'environnement (surchargeables en ligne de commande) :

    REDPANDA_BROKERS         adresse du broker            (défaut : localhost:19092)
    TICKET_TOPIC             nom du topic                 (défaut : client_tickets)
    EXPORT_DIR               racine des exports           (défaut : ./data/exports)
    CHECKPOINT_DIR           racine des checkpoints       (défaut : ./data/checkpoints)
    SPARK_TRIGGER_SECONDS    période des micro-batchs     (défaut : 10)
    SPARK_STARTING_OFFSETS   earliest | latest            (défaut : earliest)
    SPARK_MAX_OFFSETS        messages max par micro-batch (défaut : 5000)
    SPARK_SHUFFLE_PARTITIONS partitions de shuffle        (défaut : 3)
    SPARK_MAX_REDEMARRAGES   reprises après échec         (défaut : 3)
    SPARK_MAX_ECHECS_AGREGATS echecs consécutifs tolérés  (défaut : 3)

Exemples :

    # Dans le conteneur fourni par la Conversation E — le seul runtime Spark du projet :
    docker compose -f docker/docker-compose.yml up --build spark

    # Vérifier la configuration résolue sans contacter le broker ni démarrer Spark :
    python src/spark/streaming_tickets.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import time
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

try:  # Spark >= 3.4 : les exceptions ont migré vers `pyspark.errors`.
    from pyspark.errors import StreamingQueryException
except ImportError:  # pragma: no cover — filet pour les versions antérieures.
    from pyspark.sql.utils import StreamingQueryException

# --------------------------------------------------------------------------------------
# Contrat de données — docs/contrat-ticket.md §2.4. Les 6 champs sont imposés par la
# consigne ; toute divergence ici ferait silencieusement tomber tous les tickets en
# quarantaine, puisque la validation ci-dessous exige les 6 champs non nuls.
# --------------------------------------------------------------------------------------

TICKET_SCHEMA = StructType(
    [
        StructField("ticket_id", StringType(), False),
        StructField("client_id", StringType(), False),
        StructField("created_at", StringType(), False),
        StructField("request", StringType(), False),
        StructField("request_type", StringType(), False),
        StructField("priority", StringType(), False),
    ]
)

CHAMPS_TICKET: tuple[str, ...] = tuple(champ.name for champ in TICKET_SCHEMA.fields)

#: JSON n'a pas de type date : `created_at` transite en chaîne ISO 8601 UTC et n'est converti
#: qu'ici. `XXX` accepte le suffixe « Z » comme décalage nul (contrat §2.4).
FORMAT_HORODATAGE = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"

# --------------------------------------------------------------------------------------
# Transformation n°1 — assignation de l'équipe support d'après le type de demande.
# C'est l'exemple explicitement cité par la consigne (Étape 2, « Transformation et analyse »).
#
# L'énumération `request_type` étant FERMÉE (contrat §2.1), cette table est exhaustive.
# Le repli ÉQUIPE_INCONNUE n'est donc pas censé servir : il est là au titre de la résilience,
# pour le jour où un producteur hors contrat écrirait dans le topic. Un type inattendu ne
# doit pas faire tomber le ticket en quarantaine — il reste exploitable, simplement non routé.
# --------------------------------------------------------------------------------------

EQUIPES_SUPPORT: dict[str, str] = {
    "facturation": "Service Facturation",
    "technique": "Support Technique N2",
    "commercial": "Équipe Commerciale",
    "compte": "Support Comptes & Accès",
    "livraison": "Logistique & Expéditions",
}

EQUIPE_INCONNUE = "ÉQUIPE_INCONNUE"

# --------------------------------------------------------------------------------------
# Transformation n°2 — engagement de délai (SLA) déduit de la priorité.
#
# Elle rend les agrégats actionnables : compter les tickets ne dit pas lesquels risquent de
# dépasser leur échéance. `echeance_sla` est calculée par ticket et exportée avec lui.
# --------------------------------------------------------------------------------------

SLA_HEURES: dict[str, int] = {
    "critique": 2,
    "haute": 8,
    "moyenne": 24,
    "basse": 72,
}

#: Ordre métier des priorités, pour que les tableaux d'insights se lisent du plus grave au
#: plus anodin plutôt que dans l'ordre alphabétique.
ORDRE_PRIORITES: dict[str, int] = {"critique": 1, "haute": 2, "moyenne": 3, "basse": 4}

LOGGER = logging.getLogger("spark-tickets")


# --------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------


def resoudre_config() -> argparse.Namespace:
    """Résout la configuration : valeurs par défaut < variables d'env < arguments CLI.

    Les défauts visent une exécution depuis l'hôte ; le conteneur surcharge par
    variables d'environnement (contrats ticket §4 et export §6). Aucun chemin en dur :
    l'hôte et le conteneur n'ont pas la même arborescence.
    """
    parser = argparse.ArgumentParser(
        description="Traite en continu le topic client_tickets avec PySpark."
    )
    parser.add_argument(
        "--brokers",
        default=os.getenv("REDPANDA_BROKERS", "localhost:19092"),
        help="Adresse du broker Redpanda (défaut : localhost:19092).",
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("TICKET_TOPIC", "client_tickets"),
        help="Nom du topic (défaut : client_tickets).",
    )
    parser.add_argument(
        "--export-dir",
        default=os.getenv("EXPORT_DIR", "./data/exports"),
        help="Racine des exports (défaut : ./data/exports).",
    )
    parser.add_argument(
        "--checkpoint-dir",
        default=os.getenv("CHECKPOINT_DIR", "./data/checkpoints"),
        help="Racine des checkpoints (défaut : ./data/checkpoints).",
    )
    parser.add_argument(
        "--trigger-seconds",
        type=float,
        default=float(os.getenv("SPARK_TRIGGER_SECONDS", "10")),
        help="Période des micro-batchs, en secondes (défaut : 10).",
    )
    parser.add_argument(
        "--starting-offsets",
        default=os.getenv("SPARK_STARTING_OFFSETS", "earliest"),
        choices=("earliest", "latest"),
        help=(
            "Point de départ au TOUT PREMIER lancement (défaut : earliest). "
            "Ensuite le checkpoint fait foi et cette option est ignorée."
        ),
    )
    parser.add_argument(
        "--max-offsets",
        type=int,
        default=int(os.getenv("SPARK_MAX_OFFSETS", "5000")),
        help="Plafond de messages consommés par micro-batch (défaut : 5000).",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "3")),
        help="Partitions de shuffle, alignées sur le topic (défaut : 3).",
    )
    parser.add_argument(
        "--max-echecs-agregats",
        type=int,
        default=int(os.getenv("SPARK_MAX_ECHECS_AGREGATS", "3")),
        help="Échecs consécutifs de publication des agrégats tolérés (défaut : 3).",
    )
    parser.add_argument(
        "--max-redemarrages",
        type=int,
        default=int(os.getenv("SPARK_MAX_REDEMARRAGES", "3")),
        help="Reprises automatiques après échec du flux (défaut : 3).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Affiche la configuration résolue et sort, sans démarrer Spark.",
    )
    args = parser.parse_args()

    if args.trigger_seconds <= 0:
        parser.error("--trigger-seconds doit être strictement positif.")
    if args.max_offsets <= 0:
        parser.error("--max-offsets doit être strictement positif.")
    if args.shuffle_partitions <= 0:
        parser.error("--shuffle-partitions doit être strictement positif.")
    if args.max_redemarrages < 0:
        parser.error("--max-redemarrages ne peut pas être négatif.")
    if args.max_echecs_agregats < 1:
        parser.error("--max-echecs-agregats doit valoir au moins 1.")
    return args


def creer_session(config: argparse.Namespace) -> SparkSession:
    """Crée la session Spark et applique les réglages de performance de l'Étape 2.

    Les deux points de vigilance de la consigne sont traités à deux endroits complémentaires :

    * la **mémoire** relève de `spark-submit` (`--driver-memory`), fixée par le
      `docker-compose.yml` de la Conversation E : en `local[*]` tout le travail se fait dans
      la JVM du driver, il n'y a pas d'exécuteur distinct à dimensionner ;
    * le **nombre de partitions** est fixé ici. Le défaut Spark de 200 partitions de shuffle
      est calibré pour un cluster ; sur un topic à 3 partitions il ferait brasser 197
      partitions vides à chaque agrégation, pour un surcoût de planification pur.
    """
    session = (
        SparkSession.builder.appName("poc-tickets-streaming")
        .config("spark.sql.shuffle.partitions", config.shuffle_partitions)
        # Tous les horodatages sont en UTC de bout en bout : le producteur émet en UTC, Spark
        # ne doit pas les réinterpréter dans le fuseau de la machine hôte.
        .config("spark.sql.session.timeZone", "UTC")
        # La barre de progression rend les logs du conteneur illisibles pendant la démo, et
        # les insights qu'on affiche s'y noieraient.
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    # INFO ferait défiler des centaines de lignes du moteur par micro-batch.
    session.sparkContext.setLogLevel("WARN")
    return session


# --------------------------------------------------------------------------------------
# Lecture, validation et enrichissement
# --------------------------------------------------------------------------------------


def lire_topic(spark: SparkSession, config: argparse.Namespace) -> DataFrame:
    """Ouvre le flux Kafka/Redpanda et décode le JSON contre le schéma du contrat.

    Redpanda expose une API Kafka native : c'est le connecteur Kafka standard qui est utilisé,
    il n'existe aucun adaptateur spécifique à Redpanda (contrat export §1).
    """
    brut = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.brokers)
        .option("subscribe", config.topic)
        .option("startingOffsets", config.starting_offsets)
        # Résilience : la rétention du topic est de 7 jours. Si le job reste arrêté plus
        # longtemps, les offsets du checkpoint auront disparu du broker. Avec le défaut
        # (`true`), Spark refuse alors de redémarrer ; ici il reprend au plus ancien offset
        # encore disponible en le signalant dans les logs. Perdre l'historique expiré est
        # préférable à un pipeline qui refuse de repartir.
        .option("failOnDataLoss", "false")
        # Performance : plafonne le volume d'un micro-batch. Sans cela, un premier
        # `earliest` sur un topic déjà rempli engloutirait tout l'historique en un seul
        # batch — pic mémoire sur le driver et premier affichage d'insights très retardé.
        .option("maxOffsetsPerTrigger", config.max_offsets)
        .load()
    )

    # Les métadonnées Kafka sont conservées : elles servent à tracer un ticket jusqu'à sa
    # position exacte dans le topic, et c'est ce qui rend la quarantaine exploitable.
    return brut.select(
        F.col("value").cast("string").alias("charge_utile"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("recu_a"),
        F.from_json(F.col("value").cast("string"), TICKET_SCHEMA).alias("ticket"),
    )


def _est_conforme() -> Column:
    """Prédicat de conformité au contrat : les 6 champs du ticket sont renseignés.

    ⚠️ **Le piège de `from_json`** : en mode permissif (le défaut), un JSON illisible ne donne
    **pas** une structure nulle comme on s'y attend — il donne une structure **non nulle dont
    tous les champs sont nuls**. Tester `col("ticket").isNull()` ne détecte donc rien du tout.
    Vérifié sur un message « ceci n'est pas du json » : la structure était bien non nulle.
    C'est pourquoi la conformité se juge **champ par champ**, jamais sur la structure.

    `isNotNull()` ne renvoyant jamais NULL, la négation du prédicat est fiable : un message
    tombe toujours d'un côté ou de l'autre, jamais entre les deux.
    """
    predicat = F.lit(True)
    for champ in CHAMPS_TICKET:
        predicat = predicat & F.col(f"ticket.{champ}").isNotNull()
    return predicat


def _table(correspondances: dict[str, Any], cle: Column, defaut: Column) -> Column:
    """Traduit un dictionnaire Python en table de correspondance SQL, avec valeur de repli."""
    table = F.create_map([F.lit(x) for couple in correspondances.items() for x in couple])
    return F.coalesce(table[cle], defaut)


def enrichir(tickets: DataFrame) -> DataFrame:
    """Applique les deux transformations et normalise l'horodatage.

    Le résultat est le flux détaillé exporté en Parquet : un ticket par ligne, augmenté de
    l'équipe qui doit le traiter et de l'échéance à laquelle il doit l'être.
    """
    type_demande = F.col("request_type")
    priorite = F.col("priority")

    return (
        tickets.select("ticket.*", "kafka_partition", "kafka_offset", "recu_a")
        .withColumn("created_at_ts", F.to_timestamp(F.col("created_at"), FORMAT_HORODATAGE))
        .withColumn(
            "equipe_support", _table(EQUIPES_SUPPORT, type_demande, F.lit(EQUIPE_INCONNUE))
        )
        .withColumn("sla_heures", _table(SLA_HEURES, priorite, F.lit(None).cast("int")))
        # Échéance = création + SLA. Le passage par l'epoch évite de composer un intervalle
        # à partir d'une colonne, ce que `INTERVAL` ne permet pas.
        .withColumn(
            "echeance_sla",
            (F.col("created_at_ts").cast("long") + F.col("sla_heures") * 3600).cast("timestamp"),
        )
        .withColumn("traite_a", F.current_timestamp())
    )


def qualifier_rejets(messages: DataFrame) -> DataFrame:
    """Isole les messages hors contrat et documente la raison du rejet.

    Résilience : un message malformé ne doit **pas** interrompre le pipeline. Plutôt que de
    le laisser tomber silencieusement, on le met en quarantaine avec sa position exacte dans
    le topic — de quoi le rejouer ou remonter le problème au producteur fautif.
    """
    # Distinguer les deux causes de rejet ne peut pas se faire sur la nullité de la structure
    # (voir `_est_conforme`) : c'est le fait que TOUS les champs soient nuls qui signe un
    # message que le décodeur n'a pas su lire, par opposition à un JSON valide mais amputé.
    tous_champs_nuls = F.lit(True)
    for champ in CHAMPS_TICKET:
        tous_champs_nuls = tous_champs_nuls & F.col(f"ticket.{champ}").isNull()

    return messages.select(
        F.col("charge_utile"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("recu_a"),
        F.when(tous_champs_nuls, F.lit("json_illisible"))
        .otherwise(F.lit("champ_obligatoire_manquant"))
        .alias("motif_rejet"),
        F.current_timestamp().alias("rejete_a"),
    )


# --------------------------------------------------------------------------------------
# Agrégations et insights
# --------------------------------------------------------------------------------------


def agreger(enrichis: DataFrame) -> DataFrame:
    """Agrégation vivante : le croisement type × équipe × priorité, en mode `complete`.

    Une seule requête d'agrégation est démarrée, et les vues métier en sont dérivées dans
    `publier_agregats`. C'est volontaire : chaque requête streaming supplémentaire ouvrirait
    son propre consommateur Kafka et relirait le topic en entier pour le même résultat.

    Pas de fenêtre temporelle ni de watermark ici : on veut le cumul **depuis le début du
    flux**, c'est ce que montre le tableau de bord. La cardinalité est bornée par les
    énumérations fermées du contrat (5 types × 4 priorités), l'état reste donc minuscule.
    """
    return enrichis.groupBy("request_type", "equipe_support", "priority").agg(
        F.count("*").alias("nb_tickets"),
        # `countDistinct` est INTERDIT en streaming — Spark rejette toute opération distincte
        # sur un flux, faute de pouvoir en borner l'état. `approx_count_distinct` (HyperLogLog)
        # est l'équivalent supporté : état de taille fixe, erreur de l'ordre de 2 %, largement
        # suffisant pour répondre à « combien de clients distincts se plaignent ? ».
        F.approx_count_distinct("client_id").alias("nb_clients_env"),
    )


def _ecrire_json(df: DataFrame, chemin: str) -> None:
    """Écrit un instantané JSON à `chemin`, en écrasant le précédent.

    `coalesce(1)` : un seul fichier par instantané plutôt que 200 fragments — ces agrégats
    sont un tableau de bord lu par un humain, pas un jeu de données massif.
    """
    df.coalesce(1).write.mode("overwrite").json(chemin)


def publier_rejets(config: argparse.Namespace):
    """Fabrique la fonction `foreachBatch` qui écrit la quarantaine — en sautant les lots vides.

    **Pourquoi pas un sink `json` classique ici** : il écrit un fichier par tâche et par
    micro-batch, **y compris quand il n'y a rien à écrire**. Sur un topic conforme — le cas
    nominal — `exports/rejets/` se remplissait ainsi d'un fichier de 0 octet toutes les 10 s,
    soit des milliers de fichiers vides dans un dossier livrable. Constaté à l'exécution.

    Le compromis assumé : `foreachBatch` donne du **au-moins-une-fois** là où le sink fichier
    garantissait l'exactement-une-fois. Pour une quarantaine de messages malformés, un doublon
    après reprise est sans conséquence — bien moins gênant que le bruit qu'on supprime.
    """
    chemin = f"{config.export_dir}/rejets"

    def ecrire(batch: DataFrame, epoch_id: int) -> None:
        batch.cache()
        try:
            if batch.isEmpty():
                return
            nombre = batch.count()
            batch.coalesce(1).write.mode("append").json(chemin)
            # En WARNING et non en INFO : un message hors contrat dans le topic signale un
            # producteur défaillant, c'est exactement ce qu'on veut voir passer dans les logs.
            LOGGER.warning(
                "%d message(s) hors contrat mis en quarantaine dans %s", nombre, chemin
            )
        finally:
            batch.unpersist()

    return ecrire


def publier_agregats(config: argparse.Namespace):
    """Fabrique la fonction `foreachBatch` qui exporte les agrégats et affiche les insights.

    **Pourquoi `foreachBatch` et pas un sink JSON classique** (contrat export §4) : une
    agrégation streaming impose le mode `complete`, or les sinks fichier de Spark ne savent
    écrire qu'en `append`. `foreachBatch` reçoit un DataFrame **statique** — l'API batch, et
    donc `.mode("overwrite")`, y redevient utilisable.
    """
    racine = f"{config.export_dir}/agregats"
    # Compteur d'échecs CONSÉCUTIFS. Voir la clause `except` : c'est lui qui fait la
    # différence entre « incident passager » et « le pipeline est mort mais fait semblant ».
    etat = {"echecs": 0}

    def ecrire(batch: DataFrame, epoch_id: int) -> None:
        # Le DataFrame est relu quatre fois (le détail + trois vues dérivées) : sans cache,
        # Spark rejouerait l'agrégation à chaque écriture.
        batch.cache()
        try:
            par_type = (
                batch.groupBy("request_type", "equipe_support")
                .agg(F.sum("nb_tickets").alias("nb_tickets"))
                .orderBy(F.col("nb_tickets").desc())
            )
            par_priorite = (
                batch.groupBy("priority")
                .agg(F.sum("nb_tickets").alias("nb_tickets"))
                .withColumn("rang", _table(ORDRE_PRIORITES, F.col("priority"), F.lit(99)))
                .orderBy("rang")
                .drop("rang")
            )
            # Charge par équipe, avec la part de critiques : c'est l'insight actionnable —
            # deux équipes à volume égal ne sont pas sous la même pression.
            par_equipe = (
                batch.groupBy("equipe_support")
                .agg(
                    F.sum("nb_tickets").alias("nb_tickets"),
                    F.sum(
                        F.when(F.col("priority") == "critique", F.col("nb_tickets")).otherwise(0)
                    ).alias("nb_critiques"),
                )
                .withColumn(
                    "part_critiques_pct",
                    F.round(100 * F.col("nb_critiques") / F.col("nb_tickets"), 1),
                )
                .orderBy(F.col("nb_critiques").desc())
            )

            _ecrire_json(batch, f"{racine}/detail_type_priorite")
            _ecrire_json(par_type, f"{racine}/par_type")
            _ecrire_json(par_priorite, f"{racine}/par_priorite")
            _ecrire_json(par_equipe, f"{racine}/par_equipe")

            total = batch.agg(F.sum("nb_tickets")).collect()[0][0] or 0
            print(
                f"\n{'=' * 78}\n"
                f"  INSIGHTS — micro-batch {epoch_id} — {total} ticket(s) traités depuis le début\n"
                f"{'=' * 78}",
                flush=True,
            )
            print("  Tickets par type de demande et équipe assignée", flush=True)
            par_type.show(truncate=False)
            print("  Tickets par priorité", flush=True)
            par_priorite.show(truncate=False)
            print("  Charge par équipe support", flush=True)
            par_equipe.show(truncate=False)
            etat["echecs"] = 0
        except Exception:  # noqa: BLE001 — voir le commentaire ci-dessous
            etat["echecs"] += 1
            # Un instantané est intégralement réécrit au micro-batch suivant (mode `complete`)
            # : en perdre un ne perd aucune donnée. Laisser remonter l'exception tuerait tout
            # le pipeline pour un simple verrou de fichier passager — d'où l'absorption.
            #
            # MAIS l'absorber INDÉFINIMENT est pire que tout, et ça s'est produit : après un
            # arrêt brutal ayant laissé le state store incomplet, l'écriture échouait à CHAQUE
            # lot. Le pipeline paraissait vivant, ne publiait plus rien et n'affichait plus un
            # seul insight — une panne totale, silencieuse. On ne tolère donc que des échecs
            # PASSAGERS : au-delà de quelques échecs consécutifs, la panne est structurelle et
            # doit devenir bruyante (elle remonte à la boucle de reprise, puis sort en erreur).
            LOGGER.exception(
                "Échec de publication des agrégats au micro-batch %d (%d échec(s) consécutif(s)).",
                epoch_id,
                etat["echecs"],
            )
            if etat["echecs"] >= config.max_echecs_agregats:
                LOGGER.error(
                    "%d échecs consécutifs : la panne n'est pas passagère, la requête est "
                    "interrompue plutôt que de tourner à vide. Si le message porte sur un "
                    "fichier .delta manquant, le state store est incomplet — supprimer "
                    "%s/agregats pour repartir proprement.",
                    etat["echecs"],
                    config.checkpoint_dir,
                )
                raise
        finally:
            batch.unpersist()

    return ecrire


# --------------------------------------------------------------------------------------
# Requêtes streaming
# --------------------------------------------------------------------------------------


def demarrer_requetes(spark: SparkSession, config: argparse.Namespace) -> list[Any]:
    """Démarre les trois requêtes streaming et renvoie leurs poignées.

    Chaque requête a **son propre sous-dossier de checkpoint** : deux requêtes ne peuvent
    jamais partager un checkpoint (contrat export §5). C'est là que sont persistés les
    offsets Kafka et l'état des agrégations — donc ce qui permet à un job relancé de repartir
    où il s'était arrêté au lieu de reconsommer le topic depuis le début.
    """
    messages = lire_topic(spark, config)
    conforme = _est_conforme()
    enrichis = enrichir(messages.filter(conforme))
    rejets = qualifier_rejets(messages.filter(~conforme))
    declencheur = f"{config.trigger_seconds} seconds"

    requete_detail = (
        enrichis.writeStream.queryName("tickets_enrichis")
        .format("parquet")
        .outputMode("append")
        # Partitionné par type de demande : c'est l'axe de lecture naturel en aval (« tous
        # les tickets techniques »), et il évite de rescanner l'ensemble des fichiers.
        .partitionBy("request_type")
        .option("path", f"{config.export_dir}/tickets_enrichis")
        .option("checkpointLocation", f"{config.checkpoint_dir}/tickets_enrichis")
        .trigger(processingTime=declencheur)
        .start()
    )

    requete_agregats = (
        agreger(enrichis)
        .writeStream.queryName("agregats")
        .outputMode("complete")
        .foreachBatch(publier_agregats(config))
        .option("checkpointLocation", f"{config.checkpoint_dir}/agregats")
        .trigger(processingTime=declencheur)
        .start()
    )

    requete_rejets = (
        rejets.writeStream.queryName("rejets")
        .outputMode("append")
        .foreachBatch(publier_rejets(config))
        .option("checkpointLocation", f"{config.checkpoint_dir}/rejets")
        .trigger(processingTime=declencheur)
        .start()
    )

    return [requete_detail, requete_agregats, requete_rejets]


def superviser(spark: SparkSession, requetes: list[Any], arret: dict[str, bool]) -> None:
    """Attend la fin des requêtes, ou l'arrêt demandé, puis les stoppe proprement.

    Le réveil toutes les 2 s est ce qui rend l'arrêt réactif : sans lui, on resterait bloqué
    dans `awaitAnyTermination()` jusqu'à la fin du micro-batch en cours.

    ⚠️ **Ce chemin ne sert que si le script est lancé directement** (`python
    streaming_tickets.py`, ce qui suppose une JVM sur l'hôte). Sous `spark-submit` — donc dans
    le conteneur — le PID 1 est la **JVM**, qui intercepte SIGTERM et SIGINT avant le driver
    Python : `docker stop` termine le conteneur en **code 143** sans passer par ici. Ce n'est
    pas un défaut à corriger, c'est le fonctionnement de PySpark ; la garantie de reprise ne
    repose donc pas sur cet arrêt gracieux mais sur les **checkpoints**, validés à chaque
    micro-batch (voir `demarrer_requetes`).
    """
    try:
        while not arret["demande"]:
            # Renvoie True dès qu'une requête se termine ; relève son exception s'il y a lieu.
            if spark.streams.awaitAnyTermination(timeout=2):
                break
    finally:
        for requete in requetes:
            if requete.isActive:
                # `stop()` laisse le micro-batch en cours s'achever et valide son checkpoint :
                # la reprise se fera exactement là, sans doublon ni trou.
                requete.stop()
        spark.streams.resetTerminated()


# --------------------------------------------------------------------------------------
# Point d'entrée
# --------------------------------------------------------------------------------------


def main() -> int:
    # Les tickets contiennent des accents et la console Windows n'est pas en UTF-8 par défaut.
    for flux in (sys.stdout, sys.stderr):
        if hasattr(flux, "reconfigure"):
            flux.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    config = resoudre_config()

    if config.dry_run:
        LOGGER.info("Mode --dry-run : Spark n'est pas démarré.")
        for cle, valeur in sorted(vars(config).items()):
            LOGGER.info("  %-20s %s", cle, valeur)
        return 0

    arret = {"demande": False}

    def demander_arret(*_: Any) -> None:
        if not arret["demande"]:
            LOGGER.info("Arrêt demandé — fin du micro-batch en cours puis validation ...")
        arret["demande"] = True

    signal.signal(signal.SIGINT, demander_arret)
    signal.signal(signal.SIGTERM, demander_arret)

    spark = creer_session(config)
    LOGGER.info(
        "Lecture du topic '%s' sur %s — micro-batchs de %.0f s, exports vers %s.",
        config.topic,
        config.brokers,
        config.trigger_seconds,
        config.export_dir,
    )

    # Résilience : une coupure du broker fait échouer les requêtes. Les checkpoints étant
    # persistés sur le volume, il suffit de les redémarrer pour reprendre aux mêmes offsets.
    # Le nombre de tentatives est BORNÉ à dessein : au-delà, l'incident est structurel et
    # doit rester visible dans les logs plutôt que défiler dans une boucle infinie.
    tentative = 0
    try:
        while True:
            try:
                requetes = demarrer_requetes(spark, config)
                LOGGER.info(
                    "Requêtes actives : %s. Ctrl+C pour arrêter.",
                    ", ".join(r.name for r in requetes),
                )
                superviser(spark, requetes, arret)
                LOGGER.info("Flux arrêté proprement.")
                return 0
            except StreamingQueryException as exc:
                if arret["demande"]:
                    LOGGER.info("Échec survenu pendant l'arrêt — ignoré.")
                    return 0
                tentative += 1
                if tentative > config.max_redemarrages:
                    LOGGER.error(
                        "Échec du flux après %d reprise(s) : %s", config.max_redemarrages, exc
                    )
                    return 1
                delai = min(30, 2**tentative)
                LOGGER.warning(
                    "Flux interrompu (%s). Reprise %d/%d dans %d s depuis le checkpoint.",
                    exc.__class__.__name__,
                    tentative,
                    config.max_redemarrages,
                    delai,
                )
                time.sleep(delai)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
