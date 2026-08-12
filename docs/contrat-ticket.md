# Contrat de données — ticket client, topic et broker

> **Version 1.0.1** — source de vérité unique pour tout ce qui circule entre les composants du
> pipeline : le schéma JSON du ticket, le nom et la configuration du topic, les adresses et ports
> du broker, et les variables d'environnement de configuration.
>
> Le producteur écrit des messages conformes à ce document, le traitement PySpark les parse sur
> cette base, et `docker-compose.yml` câble les services sur ces adresses. Toute modification est
> donc cassante par nature : elle se fait en incrémentant la version ci-dessus.

---

## 1. Topic Redpanda

| Paramètre | Valeur figée | Justification |
|---|---|---|
| **Nom du topic** | **`client_tickets`** | **Imposé** par la consigne (§5, Exercice 2, Étape 1) — orthographe exacte, ne pas renommer. |
| **Partitions** | **3** | Permet de **démontrer le parallélisme Spark** (point de vigilance « nombre de partitions » de l'Étape 2) tout en restant frugal sur un POC mono-broker. |
| **Facteur de réplication** | **1** | Cluster **mono-broker** en local : c'est la seule valeur possible. En production, 3. |
| **Politique de rétention** | `delete`, **7 jours** (`retention.ms = 604800000`) | 7 jours de données « chaudes » : de quoi couvrir le rejeu Spark après incident sans payer du disque inutilement. |
| **Clé de partitionnement** | **`client_id`** (UTF-8) | Garantit l'**ordre des tickets d'un même client** dans une partition, avec une répartition homogène sur 3 partitions (pool de 500 clients). |
| **Valeur du message** | **JSON UTF-8** (voir §2) | Format lisible en console Redpanda, parsable nativement par `from_json` côté Spark. |

### Commande de création (référence)

```bash
docker exec -it redpanda rpk topic create client_tickets --partitions 3 --replicas 1 \
  -c retention.ms=604800000
```

> `retention.ms` est **explicitement fixé** plutôt que laissé au défaut du broker : le défaut vaut
> bien 7 jours sur Redpanda v26.2.1, mais s'en remettre à lui rendrait la rétention dépendante de
> la version du broker au lieu du contrat.
>
> Le broker est démarré avec **`auto_create_topics_enabled=false`**. Un producteur ou un job Spark
> lancé **avant** cette commande échouera franchement, au lieu de faire naître silencieusement un
> `client_tickets` à **1 partition** — ce qui violerait le contrat sans prévenir.

---

## 2. Schéma JSON du ticket

Le message est un **objet JSON plat** contenant **exactement les 6 champs imposés** par la consigne.
**Aucun champ supplémentaire, aucun champ imbriqué, aucune valeur `null`** : les 6 clés sont
toujours présentes et non vides.

| # | Clé JSON | Type JSON | Contrainte | Champ de la consigne |
|---|---|---|---|---|
| 1 | `ticket_id` | `string` | **UUID v4**, 36 caractères, unique | ID du ticket |
| 2 | `client_id` | `string` | Motif `^CLI-[0-9]{5}$`, tiré dans `CLI-00001` → `CLI-00500` | ID du client |
| 3 | `created_at` | `string` | **ISO 8601 UTC**, format `yyyy-MM-dd'T'HH:mm:ss.SSSXXX` (suffixe `Z`) | Date et heure de création |
| 4 | `request` | `string` | Texte libre non vide, **≤ 200 caractères**, cohérent avec `request_type` | La demande |
| 5 | `request_type` | `string` | **Énumération fermée** de 5 valeurs (voir §2.1) | Le type de demande |
| 6 | `priority` | `string` | **Énumération fermée** de 4 valeurs (voir §2.2) | La priorité |

> Les deux énumérations ci-dessous sont **fermées et exhaustives** : la table de correspondance
> `request_type` → équipe support peut donc être écrite sans branche `default`. Le traitement prévoit
> malgré tout un cas de repli (`ÉQUIPE_INCONNUE`) au titre de la résilience, au cas où un message
> hors contrat arriverait dans le topic.

### 2.1 Énumération `request_type` (5 valeurs)

| Valeur | Signification |
|---|---|
| `facturation` | Facture, paiement, remboursement, litige tarifaire |
| `technique` | Panne, bug, capteur IoT hors ligne, performance dégradée |
| `commercial` | Devis, renouvellement, montée en gamme, question tarifaire |
| `compte` | Accès, mot de passe, droits utilisateur, coordonnées |
| `livraison` | Expédition, retard, matériel manquant ou endommagé |

### 2.2 Énumération `priority` (4 valeurs)

| Valeur | Niveau |
|---|---|
| `critique` | Service totalement interrompu |
| `haute` | Impact fort, contournement possible |
| `moyenne` | Impact modéré |
| `basse` | Demande d'information, sans urgence |

### 2.3 Exemple de message (valeur du topic)

```json
{
  "ticket_id": "3f2b1c8e-9d41-4a7f-b0c5-6e8a2d1f4b93",
  "client_id": "CLI-00327",
  "created_at": "2026-08-14T09:23:41.512Z",
  "request": "Le capteur IoT de la ligne 3 ne remonte plus de mesures depuis ce matin.",
  "request_type": "technique",
  "priority": "critique"
}
```

Clé du message correspondante : `CLI-00327`.

### 2.4 Schéma PySpark équivalent

```python
from pyspark.sql.types import StructType, StructField, StringType

TICKET_SCHEMA = StructType([
    StructField("ticket_id",    StringType(), False),
    StructField("client_id",    StringType(), False),
    StructField("created_at",   StringType(), False),
    StructField("request",      StringType(), False),
    StructField("request_type", StringType(), False),
    StructField("priority",     StringType(), False),
])

# created_at est transporté en string ISO 8601 puis converti côté Spark :
#   to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
```

> **Pourquoi `created_at` en `string` et non en `timestamp`** : JSON n'a pas de type date. Le
> transport en chaîne ISO 8601 évite toute ambiguïté de fuseau horaire entre le producteur et le
> consommateur ; la conversion est explicite côté Spark.

---

## 3. Broker Redpanda — adresses et ports

Redpanda expose **deux listeners** : un **interne** (résolution par nom de service sur le réseau
Docker) et un **externe** (depuis la machine hôte). C'est la configuration standard du quickstart
Redpanda ; s'en écarter casserait le câblage de `docker-compose.yml`.

| Usage | Depuis un **conteneur** (réseau Docker) | Depuis l'**hôte** (Windows) |
|---|---|---|
| **Kafka API** (producteur, Spark) | **`redpanda:9092`** | **`localhost:19092`** |
| Admin API (`rpk`, santé du cluster) | `redpanda:9644` | `localhost:9644` |
| Schema Registry | `redpanda:8081` | `localhost:18081` |
| HTTP Proxy (pandaproxy) | `redpanda:8082` | `localhost:18082` |
| **Console web Redpanda** | `console:8080` | **<http://localhost:8080>** |

- **Nom du service / conteneur du broker** : `redpanda`
- **Nom du service / conteneur de la console** : `console`
- **Volume de données du broker** : `redpanda-data` monté sur `/var/lib/redpanda/data`

> **Le piège à connaître** : un client Kafka se connecte d'abord au *bootstrap*, puis le broker lui
> renvoie l'adresse annoncée (`advertised address`) sur laquelle il doit réellement produire ou
> consommer. Un producteur **conteneurisé** doit donc viser `redpanda:9092` et un producteur lancé
> **depuis l'hôte** `localhost:19092` — utiliser la mauvaise valeur produit un timeout silencieux.
> C'est exactement ce que règle la variable `REDPANDA_BROKERS` ci-dessous.

---

## 4. Interface de configuration (variables d'environnement)

Tous les scripts Python du projet se configurent **par variables d'environnement**, avec les
**valeurs par défaut prévues pour une exécution depuis l'hôte**. `docker-compose.yml` surcharge ces
variables avec les adresses internes au réseau Docker.

| Variable | Défaut (hôte) | Valeur en conteneur | Utilisée par |
|---|---|---|---|
| `REDPANDA_BROKERS` | `localhost:19092` | `redpanda:9092` | producteur, Spark |
| `TICKET_TOPIC` | `client_tickets` | `client_tickets` | producteur, Spark |
| `PRODUCER_RATE` | `5` (tickets/seconde) | idem | producteur |
| `PRODUCER_MAX_MESSAGES` | `0` (= illimité) | idem | producteur |
| `PRODUCER_SEED` | *(vide = aléatoire)* | idem | producteur |

---

## 5. Ce que ce contrat ne couvre **pas**

Ces points sont figés dans [`contrat-export.md`](contrat-export.md) ou dans le code, et ne doivent
pas être présupposés à partir d'ici :

- la table de correspondance `request_type` → équipe support et les agrégations ;
- le format et le chemin d'export des résultats d'analyse ;
- le contenu des Dockerfiles et du `docker-compose.yml`.

---

## 6. Journal des versions

| Version | Date | Modification |
|---|---|---|
| 1.0.0 | 2026-08-14 | Version initiale — schéma du ticket, topic, broker, variables d'environnement. |
| 1.0.1 | 2026-08-14 | **Non cassant** — précisions issues de la mise en service réelle du cluster : rétention fixée explicitement à la création du topic, création automatique de topics désactivée sur le broker. Schéma JSON, nom du topic, nombre de partitions et ports inchangés. |
