# Image du broker Redpanda — Exercice 2, Étape 4.
#
# La consigne demande « un Dockerfile pour chaque élément du projet », Redpanda compris.
# L'image officielle suffirait à faire tourner le broker : ce Dockerfile n'existe donc pas
# pour réécrire Redpanda, mais pour deux raisons concrètes —
#   1. ÉPINGLER la version en un seul endroit (v26.2.1) plutôt que dans chaque `image:` ;
#   2. EMBARQUER le script d'amorçage du topic, exécuté par le service `topic-init` du
#      docker-compose.yml, qui réutilise cette même image (et donc `rpk`).
#
# Construction (depuis la racine du dépôt) :
#   docker build -f docker/redpanda/Dockerfile -t poc-tickets/redpanda .

FROM redpandadata/redpanda:v26.2.1

# L'image officielle tourne sous l'utilisateur `redpanda` : il faut repasser root le temps
# d'installer le script.
USER root

COPY docker/redpanda/creer-topic.sh /usr/local/bin/creer-topic.sh

# Le dépôt est cloné depuis Windows : un script commité avec des fins de ligne CRLF ferait
# échouer le shebang avec un « bad interpreter: /usr/bin/env bash^M ». `.gitattributes` force
# déjà le LF ; ce `sed` rend le build robuste même si la configuration git locale diffère.
RUN sed -i 's/\r$//' /usr/local/bin/creer-topic.sh \
    && chmod +x /usr/local/bin/creer-topic.sh

USER redpanda

# L'entrypoint de l'image officielle est conservé : ce conteneur démarre un broker.
# Le service `topic-init` le surcharge pour exécuter le script d'amorçage à la place.
