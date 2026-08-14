# Image du traitement PySpark — Exercice 2, Étape 4.
#
# Conteneurise le job de streaming (src/spark/).
#
# Cette image est le SEUL runtime Spark du projet : la machine de développement n'a ni Java
# ni PySpark installés (voir docs/contrat-export.md §1). C'est ici que Spark s'exécute.
#
# Construction (depuis la racine du dépôt) :
#   docker build -f docker/spark/Dockerfile -t poc-tickets/spark .

FROM spark:3.5.6-python3

# Image officielle Apache : Spark 3.5.6, Scala 2.12, Python 3 et un JRE, déjà assemblés.
# Versions figées par docs/contrat-export.md §1 — elles doivent rester alignées entre elles.
ARG SPARK_VERSION=3.5.6
ARG SCALA_VERSION=2.12
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG MAVEN_CENTRAL=https://repo1.maven.org/maven2

USER root

# ---------------------------------------------------------------------------------------
# Connecteur Kafka — embarqué au BUILD plutôt que résolu au démarrage.
#
# L'usage courant est `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_...`,
# qui déclenche une résolution Ivy à CHAQUE lancement : plusieurs dizaines de secondes, et un
# échec pur et simple si le conteneur n'a pas accès à Internet. En plaçant les JARs dans
# $SPARK_HOME/jars, le connecteur fait partie du classpath par construction : démarrage
# immédiat et hors ligne.
#
# Les quatre artefacts ci-dessous sont les dépendances `compile` réelles du connecteur,
# relevées dans son POM Maven — et non devinées. `jsr305` et `spark-tags` en font aussi
# partie mais sont déjà fournis par la distribution Spark.
# ---------------------------------------------------------------------------------------
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl; \
    rm -rf /var/lib/apt/lists/*; \
    for jar in \
      "org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
      "org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
      "org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
    ; do \
      curl -fsSL -o "${SPARK_HOME}/jars/$(basename "${jar}")" "${MAVEN_CENTRAL}/${jar}"; \
    done

WORKDIR /opt/app

# Le code du job. Le dossier est copié en entier : le script et ses éventuels
# modules voisins suivent, sans avoir à lister chaque fichier ici.
COPY src/spark/ ./src/spark/

# Racine unique des sorties (docs/contrat-export.md §2), montée en volume par le compose.
# Créée et donnée à l'utilisateur `spark` (uid 185 de l'image officielle) pour que les
# écritures Parquet/JSON et les checkpoints ne butent pas sur les permissions.
RUN mkdir -p /data/exports /data/checkpoints && chown -R spark:spark /data /opt/app

USER spark

ENV REDPANDA_BROKERS=redpanda:9092 \
    TICKET_TOPIC=client_tickets \
    DATA_DIR=/data \
    EXPORT_DIR=/data/exports \
    CHECKPOINT_DIR=/data/checkpoints \
    SPARK_TRIGGER_SECONDS=10 \
    PYTHONUNBUFFERED=1

# `local[*]` : le POC tourne sur un seul nœud, Spark utilise tous les cœurs disponibles.
# La commande réelle est fixée par le docker-compose.yml, qui y ajoute les réglages mémoire.
CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "src/spark/streaming_tickets.py"]
