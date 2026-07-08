# Évaluation de compatibilité de l'infrastructure hybride — InduTechData

> **Livrable 2 de l'Exercice 1.** Ce document justifie chaque composant cloud retenu et évalue la
> compatibilité de l'architecture proposée — schéma [`architecture-hybride.pdf`](architecture-hybride.pdf) —
> avec le SI on-premise d'InduTechData : cluster SQL Server (40 To), baie SAN (10 To), Active
> Directory, serveurs ERP/CRM. Hypothèses de chiffrage détaillées : [`annexe-couts.md`](annexe-couts.md).

## 1. Composants retenus et justification

| Besoin | Composant | Justification |
|---|---|---|
| Stockage des données non structurées | **Amazon S3** | Scalabilité sans réservation préalable : absorbe les 10 To du SAN puis la croissance IoT. Sécurité par chiffrement SSE-KMS, versioning, Object Lock et politiques IAM. Interopérabilité native avec Redpanda, qui y décharge ses topics (Tiered Storage) et y écrit en Parquet/Iceberg via Redpanda Connect. |
| Entrepôt de données | **Amazon Redshift Serverless** | Requêtes SQL complexes sur l'historique sans cluster à dimensionner. Facturation à la capacité consommée, avec mise en pause automatique. Redshift Spectrum interroge les tables Iceberg directement sur S3, ce qui évite de dupliquer les données froides. |
| Streaming temps réel | **Redpanda (Cloud BYOC)** | Installation simple : binaire unique, ni JVM ni ZooKeeper à exploiter. Faible consommation de ressources : moins de nœuds que Kafka à débit égal. Fonctionnalités intégrées — Schema Registry, Redpanda Connect, Console — qui évitent d'assembler un écosystème tiers. En BYOC, le plan de données reste dans le VPC d'InduTechData. |
| Extension de l'Active Directory | **AWS Managed Microsoft AD** + **IAM Identity Center** | Annuaire Microsoft managé placé en **relation d'approbation** avec l'AD existant : celui-ci reste la source de vérité, sans double référentiel ni resaisie. IAM Identity Center projette les groupes AD en rôles IAM : une seule règle d'habilitation gouverne Redshift, S3, la console AWS et les ACL Redpanda (SASL/OIDC). |
| Reprise des 10 To du SAN | **AWS DataSync** | Réplication batch planifiée SMB/NFS → S3 sur la liaison privée, avec contrôle d'intégrité et reprise après incident. |
| Liaison hybride | **AWS Direct Connect** (+ VPN de secours) | Lien privé dédié 1 Gbps : aucun trafic sur l'Internet public, latence stable, transfert sortant moins cher. |

## 2. Synchronisation du SQL Server vers l'entrepôt

Les 40 To ne sont pas migrés. Un connecteur **CDC** lit le journal de transactions des seules tables
ERP/CRM utiles à l'analyse et publie chaque changement dans le topic `erp.cdc`. Redpanda Connect
déverse ce topic sur S3 en Parquet par micro-batchs de cinq minutes, puis Redshift charge ces
fichiers par `COPY` / auto-copy. Ce passage par S3 est délibéré : Redshift ingère mal les insertions
unitaires. Aucune application métier n'est modifiée, et le délai bout en bout reste de quelques
minutes.

## 3. Avantages

- **Sécurité homogène.** Tous les flux traversants empruntent Direct Connect et sont chiffrés en
  transit (TLS/mTLS) ; les données au repos le sont par KMS. L'authentification s'appuie sur l'AD
  existant, propagé par la relation d'approbation : identités et permissions restent gérées à un
  seul endroit.
- **Interopérabilité sans réécriture.** L'API Redpanda est compatible Kafka : clients, outils et
  connecteurs de l'écosystème fonctionnent tels quels. Le CDC est non intrusif côté SQL Server, et
  Redpanda Connect couvre MQTT/HTTP sans adaptateur maison.
- **Automatisation des flux.** Ingestion IoT, capture CDC, déchargement S3, `COPY` Redshift et
  réplication DataSync sont planifiés ou déclenchés par événement — aucune reprise manuelle en
  régime nominal.
- **Scalabilité.** Le débit se règle par le nombre de partitions et de brokers, le stockage est
  élastique, Redshift ajuste sa capacité à la charge. Passer de 50 à 500 Go/mois d'IoT ne change pas
  l'architecture.
- **Désaturation du datacenter.** L'ingestion temps réel et l'historique partent vers le cloud :
  l'extension matérielle est évitée, le CAPEX devient un OPEX pilotable.

## 4. Limitations

- **Dépendance au lien réseau.** Direct Connect devient un point de passage critique. Le VPN de
  secours a un débit inférieur : en mode dégradé, la passerelle edge bufferise, ce qui borne la
  durée d'incident tolérable.
- **Latence analytique.** Le chemin CDC → S3 → Redshift introduit quelques minutes de décalage. Le
  vrai temps réel reste du côté des topics et de Spark, pas de l'entrepôt.
- **Coût peu prévisible sur deux postes.** L'abonnement Redpanda relève d'un devis éditeur et
  Redshift Serverless facture l'usage réel : deux postes qui dépendent de décisions non techniques.
- **Compétences à acquérir.** Kafka, Spark Structured Streaming, Iceberg et l'infrastructure as code
  ne font pas partie du socle actuel des équipes.
- **Réversibilité partielle.** Le protocole Kafka limite le verrouillage sur le streaming, mais
  Redshift, S3 et IAM Identity Center créent une adhérence à AWS.

## 5. Points d'attention pour l'intégration

- **Compte de service CDC** : lecture seule sur les seules tables ciblées, jamais un compte
  privilégié — c'est le point d'exposition le plus sensible de l'architecture.
- **Dimensionnement et redondance du port Direct Connect** : un second port double ce poste de coût
  mais supprime le point unique de défaillance.
- **Rétention** : 7 jours à chaud sur les topics puis Tiered Storage 365 jours ; un rejeu au-delà
  passe par S3, donc plus lentement.
- **Gouvernance des schémas** : le Schema Registry doit être imposé aux producteurs dès le départ,
  sous peine de rupture de contrat silencieuse côté consommateurs.
- **Étiquetage systématique des ressources** : sans tags, ni Cost Explorer ni la refacturation
  interne ne sont exploitables.

## 6. Estimation des coûts et surveillance

Tarifs publics eu-west-3 (Paris), à la demande, hors taxes ; hypothèses détaillées en annexe.

| Composant | Initial | Récurrent / mois |
|---|---|---|
| Redpanda Cloud BYOC (abonnement + 3 brokers EC2/EBS) | 0 – 5 000 $ | 1 935 $ |
| Amazon Redshift Serverless (8 RPU, 6 h/jour) | — | 657 $ |
| Amazon S3 (12,5 To moyens, cycle de vie Glacier IR) | — | 235 $ |
| AWS Direct Connect 1 Gbps | 550 – 1 650 $ | 224 $ |
| AWS Managed Microsoft AD (Standard) | — | 107 $ |
| CloudWatch, Budgets, KMS | — | 36 $ |
| AWS DataSync (10 To puis deltas) | 128 $ | 4 $ |
| **Total** | **≈ 1 800 $** | **≈ 3 200 $** *(fourchette 2 700 – 3 700 $)* |

Soit environ **38 000 $ la première année**. Trois recommandations de surveillance : **AWS Budgets**,
avec un budget par composant et des alertes à 80 % et 100 % du plafond mensuel ; **Cost Explorer**,
ventilé par tag, pour repérer les dérives ; **CloudWatch**, pour corréler coût et usage réel — RPU
consommées, volume ingéré, lag des consommateurs — et détecter une anomalie avant la facture.
L'abonnement Redpanda doit être confirmé par devis avant tout engagement.

## 7. Conclusion

L'architecture est **compatible avec le SI existant**. Elle le préserve — aucune migration des
40 To, aucune réécriture applicative —, réutilise l'Active Directory comme source de vérité des
identités, chiffre les flux de bout en bout et résout la saturation du datacenter. Les limitations
sont connues et bornées ; les points d'attention relèvent de la mise en œuvre, pas de la
faisabilité.
