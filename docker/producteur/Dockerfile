# Image du producteur de tickets — Exercice 2, Étape 4.
#
# Conteneurise src/producer/producer.py.
#
# Construction (depuis la racine du dépôt) :
#   docker build -f docker/producteur/Dockerfile -t poc-tickets/producteur .

FROM python:3.12-slim

# Python 3.12 est imposé par le choix de `confluent-kafka` :
# kafka-python n'est pas compatible 3.12, confluent-kafka l'est (roues manylinux
# précompilées — aucun compilateur C n'est donc nécessaire dans l'image).

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /opt/app

# Les dépendances sont copiées et installées AVANT le code : tant que requirements.txt ne
# bouge pas, Docker réutilise cette couche et la reconstruction après une modification du
# producteur est instantanée.
COPY src/producer/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src/producer/producer.py ./

# Exécution sans privilèges : le producteur n'écrit rien sur le disque, il n'a aucune raison
# de tourner en root.
RUN useradd --create-home --uid 1000 app && chown -R app:app /opt/app
USER app

# Valeurs adaptées au réseau Docker. Elles surchargent les défauts « hôte » du script
# (localhost:19092), conformément à docs/contrat-ticket.md §4.
ENV REDPANDA_BROKERS=redpanda:9092 \
    TICKET_TOPIC=client_tickets \
    PRODUCER_RATE=5 \
    PRODUCER_MAX_MESSAGES=0

# ENTRYPOINT + CMD vide : `docker compose run producteur --rate 20 --max-messages 100`
# transmet les arguments directement au script, sans réécrire la commande complète.
ENTRYPOINT ["python", "producer.py"]
CMD []
