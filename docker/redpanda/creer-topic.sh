#!/usr/bin/env bash
#
# Amorçage du topic `client_tickets` — Exercice 2, Étape 4.
#
# Le broker tourne avec `auto_create_topics_enabled=false` :
# aucun topic ne naît tout seul. Ce script est donc le SEUL endroit où `client_tickets` est
# créé, avec les caractéristiques figées par docs/contrat-ticket.md §1.
#
# Il s'exécute dans un service `topic-init` à durée de vie courte : producteur et Spark
# attendent qu'il se termine avec succès avant de démarrer.

set -euo pipefail

BROKER="${REDPANDA_BROKERS:-redpanda:9092}"
ADMIN="${REDPANDA_ADMIN:-redpanda:9644}"
TOPIC="${TICKET_TOPIC:-client_tickets}"
PARTITIONS="${TICKET_PARTITIONS:-3}"
REPLICAS="${TICKET_REPLICAS:-1}"
RETENTION_MS="${TICKET_RETENTION_MS:-604800000}"

# Deux pièges de rpk v26, tous deux vérifiés sur le broker en fonctionnement :
#
#  1. Le drapeau `--brokers` n'existe plus : la cible se passe par `-X brokers=...`.
#  2. `-X brokers=` ne configure QUE l'API Kafka. Les commandes qui interrogent l'API d'admin
#     (`cluster health`) continuent de viser 127.0.0.1:9644 — soit, depuis ce conteneur-ci,
#     lui-même et non le broker : « connection refused » en boucle. Il leur faut
#     `-X admin.hosts=...`.
RPK_KAFKA=(rpk -X "brokers=${BROKER}")
RPK_ADMIN=(rpk -X "admin.hosts=${ADMIN}")

# La sonde de disponibilité passe par l'API KAFKA, et non par l'API d'admin : c'est
# précisément l'API que le producteur et Spark utiliseront. Un broker dont l'admin répond mais
# dont le listener Kafka n'est pas encore ouvert laisserait passer un faux « prêt ».
echo "→ Attente de l'API Kafka sur ${BROKER} ..."
pret=0
for _ in $(seq 1 60); do
  if "${RPK_KAFKA[@]}" cluster info >/dev/null 2>&1; then
    pret=1
    break
  fi
  sleep 2
done

if [ "${pret}" -ne 1 ]; then
  echo "✗ L'API Kafka de ${BROKER} n'a pas répondu dans le délai imparti." >&2
  exit 1
fi

echo "✓ API Kafka joignable."
"${RPK_ADMIN[@]}" cluster health || echo "  (état détaillé indisponible — sans conséquence)"

# Idempotence : `docker compose up` est relancé des dizaines de fois pendant un POC, et le
# volume du broker survit à un `down`. Recréer un topic existant échouerait sans cela.
if "${RPK_KAFKA[@]}" topic describe "${TOPIC}" >/dev/null 2>&1; then
  echo "✓ Le topic '${TOPIC}' existe déjà — rien à créer."
else
  echo "→ Création du topic '${TOPIC}' (${PARTITIONS} partitions, réplication ${REPLICAS}) ..."
  "${RPK_KAFKA[@]}" topic create "${TOPIC}" \
    --partitions "${PARTITIONS}" \
    --replicas "${REPLICAS}" \
    -c "retention.ms=${RETENTION_MS}"
fi

echo
"${RPK_KAFKA[@]}" topic describe "${TOPIC}"
