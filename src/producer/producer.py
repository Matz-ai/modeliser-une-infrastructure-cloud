"""Producteur de tickets clients vers le topic Redpanda ``client_tickets``.

Projet OpenClassrooms « Modélisez une infrastructure dans le cloud » — Exercice 2, Étape 1.

Génère des tickets aléatoires conformes au contrat figé dans ``docs/contrat-ticket.md``
(6 champs imposés, énumérations fermées) et les publie dans Redpanda.

Configuration par variables d'environnement (surchargeables en ligne de commande) :

    REDPANDA_BROKERS        adresse du broker      (défaut : localhost:19092)
    TICKET_TOPIC            nom du topic           (défaut : client_tickets)
    PRODUCER_RATE           tickets par seconde    (défaut : 5)
    PRODUCER_MAX_MESSAGES   0 = illimité           (défaut : 0)
    PRODUCER_SEED           graine aléatoire       (défaut : aléatoire)

Exemples :

    python producer.py --dry-run --max-messages 3    # aucun broker requis
    python producer.py                               # flux continu, Ctrl+C pour arrêter
    python producer.py --rate 20 --max-messages 500
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import KafkaException, Producer

# --------------------------------------------------------------------------------------
# Contrat de données — voir docs/contrat-ticket.md §2. Toute modification ici doit y être
# répercutée dans docs/contrat-ticket.md : le traitement Spark parse sur cette base.
# --------------------------------------------------------------------------------------

#: Format ISO 8601 UTC attendu côté Spark : yyyy-MM-dd'T'HH:mm:ss.SSSXXX
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

#: Pool de clients — 500 identifiants, pour que les agrégations par client aient du sens.
CLIENT_POOL_SIZE = 500

#: Énumération fermée des priorités, avec leur poids de tirage (pyramide réaliste :
#: peu de tickets critiques, beaucoup de demandes de faible urgence).
PRIORITIES: dict[str, int] = {
    "basse": 40,
    "moyenne": 30,
    "haute": 20,
    "critique": 10,
}

#: Énumération fermée des types de demande, associée au catalogue de demandes
#: correspondantes. Le texte de `request` est toujours cohérent avec `request_type` :
#: c'est ce qui rendra la transformation Spark (assignation d'équipe) démonstrative.
REQUEST_CATALOGUE: dict[str, tuple[str, ...]] = {
    "facturation": (
        "La facture du mois dernier comporte une ligne d'abonnement facturée en double.",
        "Le prélèvement automatique a échoué alors que le compte bancaire est approvisionné.",
        "Je demande le remboursement de la période non consommée après résiliation.",
        "Le taux de TVA appliqué sur le devis ne correspond pas à notre régime fiscal.",
        "Merci de m'envoyer les factures acquittées des six derniers mois pour l'audit.",
    ),
    "technique": (
        "Le capteur IoT de la ligne 3 ne remonte plus de mesures depuis ce matin.",
        "La passerelle d'acquisition redémarre en boucle depuis la dernière mise à jour.",
        "Les temps de réponse du tableau de bord dépassent 30 secondes aux heures de pointe.",
        "L'export automatique des relevés de production échoue avec une erreur de délai dépassé.",
        "Deux capteurs de température renvoient des valeurs incohérentes depuis hier soir.",
    ),
    "commercial": (
        "Nous souhaitons un devis pour l'ajout de 40 capteurs sur le site de Lyon.",
        "Pouvez-vous détailler les conditions de renouvellement de notre contrat annuel ?",
        "Nous envisageons de passer à l'offre supérieure, quel serait l'écart tarifaire ?",
        "Nous demandons une remise volume dans le cadre de l'extension du parc industriel.",
        "Merci de nous transmettre les conditions générales à jour avant le comité d'achat.",
    ),
    "compte": (
        "Impossible de me connecter au portail, le mot de passe est refusé après réinitialisation.",
        "Merci d'ouvrir un accès en lecture seule pour notre nouveau responsable qualité.",
        "Un collaborateur a quitté l'entreprise, son compte doit être désactivé sans délai.",
        "L'adresse de facturation enregistrée sur notre compte n'est plus la bonne.",
        "L'authentification à deux facteurs ne reconnaît plus mon téléphone professionnel.",
    ),
    "livraison": (
        "La commande annoncée pour mardi n'a toujours pas été expédiée.",
        "Le colis est arrivé avec deux boîtiers de capteurs manquants par rapport au bordereau.",
        "Un module est arrivé endommagé, l'emballage était percé à la réception.",
        "Le numéro de suivi communiqué ne correspond à aucun envoi chez le transporteur.",
        "Merci de reprogrammer la livraison, notre entrepôt sera fermé la semaine prochaine.",
    ),
}

REQUEST_TYPES: tuple[str, ...] = tuple(REQUEST_CATALOGUE)

LOGGER = logging.getLogger("producer")


# --------------------------------------------------------------------------------------
# Génération des tickets
# --------------------------------------------------------------------------------------


def build_ticket(rng: random.Random) -> dict[str, Any]:
    """Construit un ticket aléatoire conforme au contrat (exactement 6 champs)."""
    request_type = rng.choice(REQUEST_TYPES)
    priority = rng.choices(
        tuple(PRIORITIES), weights=tuple(PRIORITIES.values()), k=1
    )[0]

    # datetime.now(timezone.utc) donne des microsecondes ; le contrat impose des
    # millisecondes suivies de « Z » — d'où la troncature à 3 décimales.
    now = datetime.now(timezone.utc)
    created_at = f"{now.strftime(TIMESTAMP_FORMAT)[:-3]}Z"

    # UUID v4 tiré du générateur seedé (et non de uuid.uuid4(), qui ignore la graine) :
    # c'est ce qui rend une série entièrement rejouable avec --seed.
    ticket_id = uuid.UUID(int=rng.getrandbits(128), version=4)

    return {
        "ticket_id": str(ticket_id),
        "client_id": f"CLI-{rng.randint(1, CLIENT_POOL_SIZE):05d}",
        "created_at": created_at,
        "request": rng.choice(REQUEST_CATALOGUE[request_type]),
        "request_type": request_type,
        "priority": priority,
    }


# --------------------------------------------------------------------------------------
# Producteur Kafka / Redpanda
# --------------------------------------------------------------------------------------


class DeliveryStats:
    """Compteurs d'accusés de réception, alimentés par le callback de livraison."""

    def __init__(self) -> None:
        self.delivered = 0
        self.failed = 0

    def on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            self.failed += 1
            LOGGER.error("Échec de livraison : %s", err)
            return
        self.delivered += 1
        if self.delivered % 25 == 0:
            LOGGER.info(
                "%d tickets confirmés par le broker (dernier : partition %d, offset %d)",
                self.delivered,
                msg.partition(),
                msg.offset(),
            )


def make_producer(brokers: str) -> Producer:
    """Crée le producteur et vérifie immédiatement que le broker répond.

    Résilience : on privilégie une erreur explicite au démarrage plutôt qu'un flux
    silencieusement bloqué. `acks=all` garantit qu'un ticket n'est considéré comme produit
    qu'une fois écrit par le broker ; les retries couvrent les coupures réseau brèves.
    """
    producer = Producer(
        {
            "bootstrap.servers": brokers,
            "client.id": "ticket-producer",
            "acks": "all",
            "enable.idempotence": True,  # pas de doublon en cas de retry
            "retries": 10,
            "retry.backoff.ms": 250,
            "linger.ms": 20,  # petit batching : moins d'appels réseau
            "compression.type": "snappy",
        }
    )

    LOGGER.info("Connexion au broker %s ...", brokers)
    try:
        metadata = producer.list_topics(timeout=10)
    except KafkaException as exc:
        raise SystemExit(
            f"Broker injoignable sur '{brokers}'.\n"
            "  - Redpanda est-il démarré ?  docker ps\n"
            "  - Depuis l'hôte, l'adresse attendue est localhost:19092 ;\n"
            "    depuis un conteneur, c'est redpanda:9092 (voir docs/contrat-ticket.md §3)."
        ) from exc

    LOGGER.info("Broker joignable — %d broker(s) dans le cluster.", len(metadata.brokers))
    return producer


def resolve_config() -> argparse.Namespace:
    """Résout la configuration : valeurs par défaut < variables d'env < arguments CLI."""
    parser = argparse.ArgumentParser(
        description="Produit des tickets clients aléatoires dans un topic Redpanda."
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
        "--rate",
        type=float,
        default=float(os.getenv("PRODUCER_RATE", "5")),
        help="Nombre de tickets par seconde (défaut : 5).",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=int(os.getenv("PRODUCER_MAX_MESSAGES", "0")),
        help="Nombre de tickets à produire, 0 pour un flux illimité (défaut : 0).",
    )
    parser.add_argument(
        "--seed",
        default=os.getenv("PRODUCER_SEED") or None,
        help="Graine aléatoire, pour rejouer exactement la même série de tickets.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Affiche les tickets sur la sortie standard sans contacter le broker.",
    )
    args = parser.parse_args()

    if args.rate <= 0:
        parser.error("--rate doit être strictement positif.")
    if args.max_messages < 0:
        parser.error("--max-messages ne peut pas être négatif.")
    return args


def main() -> int:
    # Force UTF-8 en sortie : les tickets contiennent des accents et la console Windows
    # n'est pas en UTF-8 par défaut.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    args = resolve_config()
    rng = random.Random(args.seed)

    # Ctrl+C ne doit pas laisser de tickets non transmis : on lève un drapeau et on sort
    # proprement de la boucle pour vider la file d'envoi.
    stopping = False

    def request_stop(*_: Any) -> None:
        nonlocal stopping
        if not stopping:
            LOGGER.info("Arrêt demandé — vidage de la file d'envoi ...")
        stopping = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    if args.dry_run:
        LOGGER.info("Mode --dry-run : aucun broker contacté.")
        for _ in range(args.max_messages or 5):
            print(json.dumps(build_ticket(rng), ensure_ascii=False, indent=2))
        return 0

    producer = make_producer(args.brokers)
    stats = DeliveryStats()
    interval = 1.0 / args.rate
    sent = 0

    LOGGER.info(
        "Production vers le topic '%s' à %.1f ticket(s)/s (%s). Ctrl+C pour arrêter.",
        args.topic,
        args.rate,
        f"{args.max_messages} tickets" if args.max_messages else "flux illimité",
    )

    next_send = time.monotonic()
    while not stopping and (args.max_messages == 0 or sent < args.max_messages):
        ticket = build_ticket(rng)
        payload = json.dumps(ticket, ensure_ascii=False).encode("utf-8")

        try:
            producer.produce(
                topic=args.topic,
                # Clé = client_id : ordre garanti par client, répartition homogène (contrat §1).
                key=ticket["client_id"].encode("utf-8"),
                value=payload,
                on_delivery=stats.on_delivery,
            )
        except BufferError:
            # File locale saturée : le broker n'absorbe pas assez vite. On laisse le
            # producteur se vider plutôt que de perdre le ticket.
            LOGGER.warning("File d'envoi saturée, attente de désengorgement ...")
            producer.poll(1.0)
            continue
        except KafkaException as exc:
            LOGGER.error("Refus du broker : %s — nouvelle tentative dans 1 s.", exc)
            time.sleep(1.0)
            continue

        sent += 1
        producer.poll(0)  # sert les callbacks de livraison sans bloquer

        next_send += interval
        time.sleep(max(0.0, next_send - time.monotonic()))

    remaining = producer.flush(timeout=15)
    if remaining:
        LOGGER.warning("%d ticket(s) non confirmé(s) à l'arrêt.", remaining)

    LOGGER.info(
        "Terminé — %d ticket(s) envoyé(s), %d confirmé(s), %d en échec.",
        sent,
        stats.delivered,
        stats.failed,
    )
    return 1 if stats.failed else 0


if __name__ == "__main__":
    sys.exit(main())
