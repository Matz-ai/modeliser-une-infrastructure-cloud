# Évaluation de compatibilité de l'infrastructure hybride — InduTechData

> **Livrable 2 de l'Exercice 1.** Ce document évalue la compatibilité de l'architecture hybride
> proposée (voir le schéma [`architecture-hybride.svg`](architecture-hybride.svg)) avec le système
> d'information (SI) on-premise existant d'InduTechData, selon trois axes : **sécurité**,
> **interopérabilité** et **coûts**. Il justifie également la **sélection des composants Redpanda**.

---

## 1. Rappel du contexte et du SI existant

**InduTechData** (analyse de données pour l'industrie) doit absorber **+50 Go/mois de données IoT
en temps réel** alors que son **datacenter sature**. Objectif : moderniser la gestion des données
via le cloud **sans casser** le SI on-premise et en garantissant une **interopérabilité fluide**
(identités, sécurisation des flux).

| Composant on-premise | Volume / rôle |
|---|---|
| Cluster **SQL Server** | **40 To** — données critiques ERP/CRM (sauvegardes + réplication) |
| Baie **SAN** | **10 To** — non structuré : journaux, fichiers utilisateurs, archives capteurs |
| **Active Directory** | Authentification, autorisation, gestion des utilisateurs |
| Serveurs **ERP / CRM** | Comptabilité, RH, relation client |
| **Capteurs IoT** (récent) | Flux continus, +50 Go/mois, traitement rapide et fiable attendu |

---

## 2. Composants retenus et justification

Conformément à la consigne d'action (« *en utilisant les services Redpanda* »), la couche cloud est
modélisée autour de la plateforme de **streaming temps réel Redpanda** (compatible Kafka). Le
déploiement choisi est **BYOC (Bring Your Own Cloud)** : le **plan de données reste dans le compte
AWS d'InduTechData**, le plan de contrôle est managé par Redpanda — ce qui réconcilie « services
Redpanda » et le contexte AWS de l'énoncé.

| # | Composant Redpanda | Rôle dans l'architecture | Justification du choix |
|---|---|---|---|
| 1 | **Redpanda Cloud (BYOC)** — cluster | Bus temps réel central, topics partitionnés/répliqués | Absorbe les +50 Go/mois IoT ; scalabilité horizontale ; données dans le VPC AWS du client (résidence + réseau privé) |
| 2 | **Topics** (`iot.sensors`, `erp.cdc`, `client_tickets`…) | Canaux de données par source | Découple producteurs et consommateurs ; rejouabilité ; base de l'Exercice 2 |
| 3 | **Redpanda Connect** (source MQTT/HTTP) | Ingestion des capteurs IoT | Connecteurs prêts à l'emploi ; pas de consommateur maison à coder/maintenir |
| 4 | **Redpanda Connect** (source CDC) | Capture des changements SQL Server | Réplique les **changements** vers le cloud **sans migrer ni surcharger** les 40 To existants |
| 5 | **Schema Registry** (intégré) | Validation/versionnement des schémas | Garantit la compatibilité producteurs ↔ consommateurs (contrats de données) |
| 6 | **Tiered Storage → Amazon S3** (+ topics Iceberg) | Rétention longue / déchargement | Déleste la capacité datacenter saturée vers du stockage objet bon marché ; queryable en lakehouse |
| 7 | **Redpanda Console** | Supervision, lag, gestion des ACL | Observabilité et exploitation du cluster |
| 8 | **PySpark / Spark Structured Streaming** | Consommation / ETL temps réel | Traitement distribué des flux (pont vers l'Exercice 2) |
| 9 | **Sécurité** : SASL/GSSAPI (Kerberos) ou OIDC, mTLS, RBAC/ACL | Identité, chiffrement, autorisation | Réutilise l'AD ; chiffre les flux ; droits fins par topic |
| 10 | **Liaison réseau** : VPN / AWS Direct Connect + VPC peering | Lien privé on-prem ↔ cloud | Connectivité privée, chiffrée, sans exposition Internet |

**Principe directeur** : on **ne migre pas** les 40 To. Redpanda devient le **système nerveux temps
réel** qui **réplique les flux** (CDC + IoT) vers le cloud et **décharge l'historique** vers S3.
L'Active Directory reste la **source de vérité des identités**.

---

## 3. Évaluation par axe

### 3.1 Sécurité

| Critère | Mesure dans l'architecture | Verdict |
|---|---|---|
| **Authentification** | Fédération avec l'**Active Directory** existant via **SASL/GSSAPI (Kerberos)** ou **OIDC** → SSO, pas de double annuaire | ✅ Compatible |
| **Autorisation** | **RBAC + ACL** Redpanda : droits fins par topic/groupe (lecture/écriture) | ✅ Compatible |
| **Chiffrement en transit** | **mTLS** sur toutes les connexions broker ; tunnel **VPN / Direct Connect** | ✅ Compatible |
| **Chiffrement au repos** | Chiffrement des disques du cluster + objets **S3** (clés KMS du compte AWS) | ✅ Compatible |
| **Cloisonnement réseau** | **BYOC** : plan de données dans le VPC client ; **VPC peering / pas d'exposition Internet publique** | ✅ Compatible |
| **Résidence / souveraineté** | Les données restent dans le **compte AWS d'InduTechData** (région choisie) | ✅ Compatible |
| **Traçabilité** | Journaux d'audit + supervision via **Redpanda Console** | ✅ Compatible |
| **Surface d'exposition CDC** | La capture CDC lit le journal de transactions SQL Server | ⚠️ Point d'attention : ouvrir des **droits CDC dédiés** en lecture seule, pas un compte privilégié |

**Synthèse sécurité** : l'architecture **réutilise les briques de sécurité existantes** (AD) et
ajoute le chiffrement de bout en bout. Le principal point de vigilance est le **compte de service
CDC**, à cantonner au minimum de privilèges.

### 3.2 Interopérabilité

| Critère | Mesure dans l'architecture | Verdict |
|---|---|---|
| **Compatibilité applicative** | API **compatible Kafka** : les clients/outils Kafka existants fonctionnent sans réécriture | ✅ Compatible |
| **Intégration SQL Server** | Connecteur **CDC** non intrusif : pas de modification des applis ERP/CRM | ✅ Compatible |
| **Intégration IoT** | **Redpanda Connect** parle MQTT/HTTP → pas d'adaptateur maison | ✅ Compatible |
| **Identité** | Fédération **AD/Kerberos/OIDC** : un seul référentiel d'utilisateurs | ✅ Compatible |
| **Contrats de données** | **Schema Registry** : formats versionnés (Avro/JSON/Protobuf) entre équipes | ✅ Compatible |
| **Stockage analytique** | **Tiered Storage / Iceberg sur S3** : lisible par Spark, BI, lakehouse | ✅ Compatible |
| **Traitement** | **PySpark** consomme directement les topics (connecteur Spark-Kafka) | ✅ Compatible |
| **Réversibilité** | Protocole **Kafka standard** : faible verrouillage fournisseur sur le fil | ✅ Compatible |
| **Latence du lien hybride** | Trafic on-prem ↔ cloud sur la liaison privée | ⚠️ Dimensionner Direct Connect selon le débit IoT |

**Synthèse interopérabilité** : la **compatibilité Kafka** et les **connecteurs** rendent
l'intégration au SI quasi transparente, sans réécrire les applications métier ni dupliquer la
gestion des identités.

### 3.3 Coûts

| Levier | Effet sur les coûts | Sens |
|---|---|---|
| **Pas d'extension du datacenter saturé** | On évite un investissement matériel lourd (CAPEX) | ⬇️ Économie |
| **Modèle cloud à la consommation** | Bascule **CAPEX → OPEX**, on paie l'usage réel | ⬇️ / pilotable |
| **Tiered Storage → S3** | L'historique va sur du stockage objet bon marché, pas sur du disque « chaud » | ⬇️ Économie forte |
| **Efficience de Redpanda** | Pas de JVM ni de ZooKeeper → **moins de nœuds** que Kafka pour un débit donné | ⬇️ Économie infra |
| **BYOC sur compte AWS** | Coûts d'infra **visibles** dans la facture AWS du client (transparence) | ↔️ Maîtrise |
| **Frais de transfert (egress) on-prem ↔ cloud** | Le trafic sortant cloud et inter-sites est facturé | ⚠️ Coût à surveiller |
| **Licence Redpanda Cloud** | Abonnement plateforme managée | ⚠️ À budgéter |

**Mitigations coûts** : privilégier **AWS Direct Connect** (tarif au transfert plus avantageux que
l'Internet public à volume élevé) ; **garder le traitement dans le cloud** (Spark proche des topics)
pour limiter les allers-retours ; appliquer des **politiques de rétention** sur les topics +
Tiered Storage pour ne pas payer du stockage « chaud » inutile.

**Synthèse coûts** : l'architecture **évite l'extension coûteuse du datacenter** et déplace
l'historique vers du stockage bon marché. Les deux postes à surveiller sont l'**egress réseau** et
l'**abonnement** Redpanda Cloud, tous deux maîtrisables (Direct Connect, rétention, traitement
in-cloud).

---

## 4. Verdict global

| Axe | Verdict | Points d'attention résiduels |
|---|---|---|
| **Sécurité** | ✅ Compatible | Compte de service CDC à privilèges minimaux |
| **Interopérabilité** | ✅ Compatible | Dimensionnement du lien privé (débit IoT) |
| **Coûts** | ✅ Compatible / maîtrisable | Egress réseau + abonnement à budgéter |

**Conclusion** : l'infrastructure hybride proposée est **compatible avec le SI existant
d'InduTechData**. Elle **préserve** l'investissement on-premise (SQL Server 40 To, SAN, AD, ERP/CRM),
**réutilise l'Active Directory** pour l'identité, **chiffre** les flux de bout en bout, et **résout
la saturation** du datacenter en déportant l'ingestion temps réel et l'historique vers les services
Redpanda. Les points de vigilance identifiés (privilèges CDC, dimensionnement réseau, egress et
abonnement) sont **connus et adressables**, sans remettre en cause la faisabilité.

---

## 5. Questions ouvertes pour le mentor

- **AWS vs Redpanda** : on suit la consigne d'action (« services Redpanda ») avec un déploiement
  BYOC **sur AWS** ; faut-il un schéma 100 % services AWS natifs en variante ?
- **CDC SQL Server** : niveau de granularité attendu (tables ERP/CRM ciblées vs base entière) ?
- **Rétention** : durée de rétention attendue sur les topics avant déchargement Tiered Storage ?
