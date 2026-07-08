# Annexe — hypothèses et détail de l'estimation des coûts

> **Document de travail**, support du chiffrage résumé dans
> [`evaluation-compatibilite.md`](evaluation-compatibilite.md). Il n'est **pas** le livrable :
> il existe pour rendre les calculs traçables et rejouables, la contrainte de format
> (400–1200 mots) interdisant de les détailler dans l'évaluation elle-même.

---

## 1. Cadre du chiffrage

| Paramètre | Valeur retenue |
|---|---|
| Région | **eu-west-3 (Paris)** — résidence des données en France |
| Devise | **USD**, hors taxes, tarifs publics **à la demande** (aucun engagement, aucun Savings Plan) |
| Horizon | **Année 1**, en régime établi (les valeurs mensuelles sont des moyennes sur 12 mois) |
| Source des tarifs | Grilles publiques AWS constatées pour eu-west-3 · **à revalider dans l'AWS Pricing Calculator** avant tout engagement budgétaire |
| Périmètre | Services cloud uniquement. **Hors** charge projet interne (conception, IaC, recette) et **hors** coûts on-premise existants |

> Ces montants sont une **première estimation d'ordre de grandeur**, destinée à cadrer un
> budget et à hiérarchiser les postes. Ils ne valent pas devis.

---

## 2. Volumétrie retenue

| Flux | Volume | Origine de l'hypothèse |
|---|---|---|
| Capteurs IoT | **50 Go/mois** | Donnée de l'énoncé |
| CDC ERP/CRM | **20 Go/mois** | Hypothèse : les *changements* des tables métier ciblées, pas les 40 To |
| Logs applicatifs | **10 Go/mois** | Hypothèse |
| Reprise initiale du SAN | **10 To** (10 240 Go), une fois | Donnée de l'énoncé |
| Delta SAN récurrent | **300 Go/mois** | Hypothèse : ~3 %/mois de la baie |
| Croissance S3 | ~380 Go/mois après la reprise | Somme des flux ci-dessus |
| Assiette S3 moyenne année 1 | **~12 500 Go** | 10 240 Go + croissance, moyenne sur 12 mois |
| Volume « chaud » des topics | **< 20 Go** | 80 Go/mois × 7 jours de rétention |

Le volume chaud est négligeable : les brokers sont dimensionnés pour la **disponibilité** (3 nœuds,
3 zones) et non pour le stockage.

---

## 3. Détail par composant

### 3.1 Amazon S3 — stockage objet

| Poste | Calcul | Montant |
|---|---|---|
| Standard (70 % de l'assiette) | 8 750 Go × 0,024 $/Go-mois | 210,00 $ |
| Glacier Instant Retrieval (30 %, IoT brut > 90 j) | 3 750 Go × 0,006 $/Go-mois | 22,50 $ |
| Requêtes PUT (sink micro-batch + DataSync) | ~200 000 × 0,0054 $/1000 | 1,08 $ |
| Requêtes GET / listing | forfait | ~1,00 $ |
| **Total récurrent** | | **≈ 235 $/mois** |

Le cycle de vie Standard → Glacier IR au-delà de 90 jours divise le coût de stockage par 4 sur la
part froide. Sans lui, la même assiette coûterait ~300 $/mois et croîtrait linéairement.

### 3.2 Amazon Redshift Serverless — entrepôt

| Poste | Calcul | Montant |
|---|---|---|
| Capacité de calcul | 8 RPU × 6 h/jour × 30 j = 1 440 RPU-h × 0,42 $ | 604,80 $ |
| Stockage managé (RMS) | 2 000 Go × 0,0261 $/Go-mois | 52,20 $ |
| **Total récurrent** | | **≈ 657 $/mois** |

Hypothèse structurante : **6 heures de requêtes effectives par jour**. Le Serverless facture à la
seconde et se met en pause hors activité — un usage 24/7 ferait passer ce poste à ~2 400 $/mois.
C'est le poste le plus sensible au comportement des utilisateurs.

### 3.3 Redpanda Cloud (BYOC)

| Poste | Calcul | Montant |
|---|---|---|
| Abonnement plateforme (plan de contrôle managé) | fourchette éditeur | **1 000 – 2 000 $** *(retenu : 1 500 $)* |
| Brokers EC2 (3 × m6i.large) | 3 × 0,1152 $/h × 730 h | 252,29 $ |
| Volumes EBS gp3 | 3 × 500 Go × 0,0952 $/Go-mois | 142,80 $ |
| Équilibrage de charge + réseau interne | forfait | ~40,00 $ |
| **Total récurrent** | | **≈ 1 935 $/mois** |

L'abonnement Redpanda **n'est pas un tarif public au Go** : il dépend d'un devis éditeur. C'est
la principale incertitude du chiffrage — à elle seule ~47 % du total.
**Alternative à instruire** : Redpanda **self-managed** (open source) sur les mêmes instances EC2
supprime l'abonnement et ramène le poste à ~435 $/mois, au prix de l'exploitation du cluster par
les équipes internes.

### 3.4 Identité

| Poste | Calcul | Montant |
|---|---|---|
| AWS Managed Microsoft AD — Standard Edition | 0,146 $/h × 730 h | 106,58 $ |
| IAM Identity Center | — | **gratuit** |
| **Total récurrent** | | **≈ 107 $/mois** |

L'édition Standard couvre jusqu'à ~30 000 objets d'annuaire et inclut **deux contrôleurs de domaine
répartis sur deux zones de disponibilité**. Suffisant pour une entreprise de taille moyenne.

### 3.5 Liaison réseau

| Poste | Calcul | Montant |
|---|---|---|
| Port Direct Connect dédié 1 Gbps | 0,30 $/h × 730 h | 219,00 $ |
| Transfert sortant AWS → on-premise via DX | 200 Go × 0,0225 $/Go | 4,50 $ |
| Transfert entrant | — | **gratuit** |
| **Total récurrent** | | **≈ 224 $/mois** |

Le trafic **entrant** (IoT, CDC, DataSync) est gratuit : c'est le sens dominant des flux, ce qui est
favorable. Un **second port pour la redondance** est recommandé en production : **+219 $/mois**,
non retenu ici.

### 3.6 AWS DataSync

| Poste | Calcul | Montant |
|---|---|---|
| Reprise initiale du SAN | 10 240 Go × 0,0125 $/Go | **128,00 $ (une fois)** |
| Delta mensuel | 300 Go × 0,0125 $/Go | 3,75 $ |
| **Total récurrent** | | **≈ 4 $/mois** |

### 3.7 Observabilité, FinOps et chiffrement

| Poste | Calcul | Montant |
|---|---|---|
| CloudWatch — métriques personnalisées | 50 × 0,30 $ | 15,00 $ |
| CloudWatch — ingestion de logs | 20 Go × 0,63 $/Go | 12,60 $ |
| CloudWatch — alarmes | 20 × 0,10 $ | 2,00 $ |
| AWS Budgets | 5 budgets (2 gratuits) : 3 × 0,02 $ × 30 j | 1,80 $ |
| AWS Cost Explorer (console) | — | **gratuit** |
| AWS KMS | 3 clés × 1 $ + requêtes | ~5,00 $ |
| **Total récurrent** | | **≈ 36 $/mois** |

---

## 4. Synthèse

### Coûts récurrents (par mois)

| Composant | Montant | Part |
|---|---|---|
| Redpanda Cloud BYOC (abonnement + infra) | 1 935 $ | 60 % |
| Amazon Redshift Serverless | 657 $ | 21 % |
| Amazon S3 | 235 $ | 7 % |
| AWS Direct Connect | 224 $ | 7 % |
| AWS Managed Microsoft AD | 107 $ | 3 % |
| Observabilité, FinOps, KMS | 36 $ | 1 % |
| AWS DataSync | 4 $ | < 1 % |
| **Total** | **≈ 3 198 $/mois** · **~38 400 $/an** | |

**Fourchette réaliste : 2 700 – 3 700 $/mois**, l'écart tenant presque entièrement à l'abonnement
Redpanda et au volume de requêtes Redshift.

### Coûts initiaux (une fois)

| Poste | Montant |
|---|---|
| Reprise DataSync des 10 To | 128 $ |
| Raccordement Direct Connect (cross-connect, prestation opérateur) | 550 – 1 650 $ |
| Activation du CDC SQL Server | **0 $** — fonctionnalité incluse dans l'édition Standard depuis SQL Server 2016 SP1 |
| Accompagnement éditeur Redpanda (mise en place BYOC) | 0 – 5 000 $ *(à négocier)* |
| **Total hors charge interne** | **≈ 700 – 6 800 $**, hypothèse centrale **~1 800 $** |

La **charge projet interne** (conception, infrastructure as code, tests, recette, conduite du
changement) n'est pas chiffrée ici : elle relève du budget RH, pas de la facture cloud.

---

## 5. Leviers d'optimisation identifiés

| Levier | Gain estimé | Contrepartie |
|---|---|---|
| Redpanda self-managed au lieu de BYOC | **−1 500 $/mois** | Exploitation du cluster à la charge des équipes internes |
| Savings Plans / Reserved Instances sur les brokers (1 an) | −25 à −40 % sur les 252 $ EC2 | Engagement de durée |
| Cycle de vie S3 vers Glacier IR (déjà intégré) | −65 $/mois | Restauration facturée à la lecture |
| Limiter la fenêtre d'activité Redshift | jusqu'à −400 $/mois | Disponibilité analytique réduite |
| CDC limité aux tables ERP/CRM ciblées (déjà intégré) | volume et egress divisés | Périmètre analytique plus étroit |
| Compression des messages sur les topics | −20 à −40 % de stockage et de transfert | Coût CPU côté producteurs |
