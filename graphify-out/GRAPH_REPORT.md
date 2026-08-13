# Graph Report - C:\Users\Utilisateur\Documents\openclassrooms\modérlisez une infra dans le cloud\modeliser-une-infrastructure-cloud  (2026-08-14)

## Corpus Check
- Corpus is ~9,033 words - fits in a single context window. You may not need a graph.

## Summary
- 114 nodes · 228 edges · 7 communities
- Extraction: 78% EXTRACTED · 20% INFERRED · 2% AMBIGUOUS · INFERRED: 45 edges (avg confidence: 0.85)
- Token cost: 163,498 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Schema Architecture Hybride|Schema Architecture Hybride]]
- [[_COMMUNITY_Selection Composants Cloud|Selection Composants Cloud]]
- [[_COMMUNITY_Securite Couts AWS|Securite Couts AWS]]
- [[_COMMUNITY_Pipeline ETL Temps Reel|Pipeline ETL Temps Reel]]
- [[_COMMUNITY_Orchestration Multi-Agents|Orchestration Multi-Agents]]
- [[_COMMUNITY_SI On-Premise InduTechData|SI On-Premise InduTechData]]
- [[_COMMUNITY_Livrables et Evaluation OC|Livrables et Evaluation OC]]

## God Nodes (most connected - your core abstractions)
1. `Plan de travail multi-agents (Conversations Claude Code)` - 14 edges
2. `Redpanda Cloud — BYOC (cluster compatible Kafka)` - 12 edges
3. `Exercice 1 — Modélisez une infrastructure hybride dans le cloud` - 11 edges
4. `README du projet` - 11 edges
5. `Évaluation de compatibilité de l'infrastructure hybride` - 11 edges
6. `Conversation G — Regroupement, merge & push final (main)` - 10 edges
7. `Conversation B — Mise en conformité de l'Exercice 1 (conv-b-ex1-modelisation)` - 9 edges
8. `Topics Redpanda (iot.sensors, erp.cdc, client_tickets)` - 9 edges
9. `Cloud — Services Redpanda (compte AWS, mode BYOC)` - 9 edges
10. `Exercice 2 — Gérez des tickets clients avec Redpanda et PySpark` - 8 edges

## Surprising Connections (you probably didn't know these)
- `Tiered Storage Redpanda` --semantically_similar_to--> `Baie de stockage SAN (10 To non structurés)`  [INFERRED] [semantically similar]
  docs/exercice1-modelisation/evaluation-compatibilite.md → consigne.md
- `Topics Iceberg / lakehouse sur S3` --conceptually_related_to--> `Entrepôt de données cloud (exigence)`  [AMBIGUOUS]
  docs/exercice1-modelisation/evaluation-compatibilite.md → consigne.md
- `Arbitrage AWS vs Redpanda` --semantically_similar_to--> `Arbitrage entrepôt : Redshift ou Snowflake`  [INFERRED] [semantically similar]
  consigne.md → docs/plan-conversations.md
- `SASL/GSSAPI (Kerberos)` --conceptually_related_to--> `Service d'extension de l'Active Directory au cloud (exigence)`  [AMBIGUOUS]
  docs/exercice1-modelisation/evaluation-compatibilite.md → consigne.md
- `Redpanda Cloud BYOC (Bring Your Own Cloud)` --rationale_for--> `Arbitrage AWS vs Redpanda`  [INFERRED]
  docs/exercice1-modelisation/evaluation-compatibilite.md → consigne.md

## Hyperedges (group relationships)
- **Flux hybride on-premise → cloud (CDC, IoT, lien privé, déchargement S3)** — consigne_sql_server, consigne_iot, eval_redpanda_connect_cdc, eval_redpanda_connect_mqtt, eval_redpanda_cloud_byoc, eval_direct_connect, eval_tiered_storage, eval_amazon_s3 [EXTRACTED 1.00]
- **Pipeline ETL temps réel des tickets clients (Exercice 2)** — consigne_producteur_python, consigne_topic_client_tickets, consigne_pyspark, consigne_export_json_parquet, consigne_docker_compose, readme_mermaid_flowchart [EXTRACTED 1.00]
- **Chaîne de sécurité : identité fédérée, chiffrement et autorisation fine** — consigne_active_directory, eval_kerberos, eval_oidc, eval_mtls, eval_rbac_acl, eval_kms, eval_vpc_peering [EXTRACTED 1.00]
- **Chaîne d'ingestion IoT temps réel (capteurs → passerelle → liaison privée → Redpanda Connect MQTT → cluster BYOC)** — architecture_hybride_capteurs_iot, architecture_hybride_passerelle_collecte, architecture_hybride_liaison_privee, architecture_hybride_redpanda_connect_mqtt, architecture_hybride_cluster_redpanda_byoc, architecture_hybride_topic_iot_sensors [EXTRACTED 1.00]
- **Chaîne CDC ERP/CRM vers le lakehouse (SQL Server → passerelle → liaison privée → Connect CDC → cluster → PySpark → Lakehouse/BI)** — architecture_hybride_cluster_sql_server, architecture_hybride_passerelle_collecte, architecture_hybride_liaison_privee, architecture_hybride_redpanda_connect_cdc, architecture_hybride_cluster_redpanda_byoc, architecture_hybride_pyspark_streaming, architecture_hybride_lakehouse_bi [EXTRACTED 1.00]
- **Chaîne de fédération d'identité et de contrôle d'accès (AD Kerberos → OIDC/SSO → RBAC+ACL du cluster, supervisé via Console)** — architecture_hybride_active_directory, architecture_hybride_federation_identite, architecture_hybride_cluster_redpanda_byoc, architecture_hybride_rbac_acl, architecture_hybride_redpanda_console [INFERRED 0.85]

## Communities (7 total, 0 thin omitted)

### Community 0 - "Schema Architecture Hybride"
Cohesion: 0.20
Nodes (23): Active Directory (Identités · Kerberos), Baie de stockage SAN (10 To — non structuré), Capteurs IoT (+50 Go/mois, temps réel), Redpanda Cloud — BYOC (cluster compatible Kafka), Cluster SQL Server (40 To — ERP / CRM), Diagramme — Architecture hybride on-premise ↔ Cloud (InduTechData), Exercice 2 — ETL temps réel, Fédération d'identité — Kerberos / OIDC (SSO) (+15 more)

### Community 1 - "Selection Composants Cloud"
Cohesion: 0.14
Nodes (20): Active Directory on-premise, Arbitrage AWS vs Redpanda, Entrepôt de données cloud (exigence), Exercice 1 — Modélisez une infrastructure hybride dans le cloud, Service d'extension de l'Active Directory au cloud (exigence), Écosystème Kafka, Redpanda (plateforme de streaming compatible Kafka), Livrable 1 — Schéma de l'infrastructure hybride (PDF/PNG) (+12 more)

### Community 2 - "Securite Couts AWS"
Cohesion: 0.16
Nodes (16): AWS (Amazon Web Services), AWS Budgets, AWS Pricing Calculator, Amazon CloudWatch (surveillance), Livrable 2 — Évaluation de compatibilité (400–1200 mots), SSL/TLS — protection des flux en transit, Bascule CAPEX → OPEX, AWS Direct Connect (+8 more)

### Community 3 - "Pipeline ETL Temps Reel"
Cohesion: 0.22
Nodes (15): Docker, Docker Compose, Exercice 2 — Gérez des tickets clients avec Redpanda et PySpark, Export des résultats (JSON / Parquet), Diagramme Mermaid du pipeline ETL, Pipeline ETL en temps réel, POC système de gestion de tickets clients, Script producteur Python de tickets aléatoires (+7 more)

### Community 4 - "Orchestration Multi-Agents"
Cohesion: 0.27
Nodes (15): docs/contrat-ticket.md — contrat partagé du ticket, Conversation A — Cadrage & environnement (fusionnée), Conversation C — Redpanda + producteur de tickets (conv-c-redpanda-producteur), Conversation D — Traitement PySpark (conv-d-pyspark), Conversation E — Export + conteneurisation (conv-e-export-docker), Conversation F — Documentation, Mermaid & vidéo (conv-f-documentation), Conversation G — Regroupement, merge & push final (main), Plan de travail multi-agents (Conversations Claude Code) (+7 more)

### Community 5 - "SI On-Premise InduTechData"
Cohesion: 0.22
Nodes (14): Serveurs ERP / CRM, InduTechData (entreprise fictive), Capteurs IoT (+50 Go/mois temps réel), Baie de stockage SAN (10 To non structurés), SI on-premise d'InduTechData, Cluster SQL Server (40 To de données critiques), Principe directeur — ne pas migrer les 40 To, RBAC + ACL Redpanda (+6 more)

### Community 6 - "Livrables et Evaluation OC"
Cohesion: 0.20
Nodes (11): Compétences évaluées (7 compétences), Convention de nommage des livrables, Consignes officielles du projet (OpenClassrooms), Session de bilan avec le mentor, Vidéo de démonstration du POC, CDC (Change Data Capture) sur SQL Server, Point d'attention — privilèges du compte de service CDC, Verdict global — architecture compatible avec le SI existant (+3 more)

## Ambiguous Edges - Review These
- `Entrepôt de données cloud (exigence)` → `Topics Iceberg / lakehouse sur S3`  [AMBIGUOUS]
  docs/exercice1-modelisation/evaluation-compatibilite.md · relation: conceptually_related_to
- `Service d'extension de l'Active Directory au cloud (exigence)` → `SASL/GSSAPI (Kerberos)`  [AMBIGUOUS]
  docs/exercice1-modelisation/evaluation-compatibilite.md · relation: conceptually_related_to
- `Baie de stockage SAN (10 To — non structuré)` → `Topic client_tickets`  [AMBIGUOUS]
  docs/exercice1-modelisation/architecture-hybride.svg · relation: conceptually_related_to
- `Baie de stockage SAN (10 To — non structuré)` → `Tiered Storage → Amazon S3 (Iceberg, rétention)`  [AMBIGUOUS]
  docs/exercice1-modelisation/architecture-hybride.svg · relation: conceptually_related_to
- `Active Directory (Identités · Kerberos)` → `Redpanda Console (supervision · ACL)`  [AMBIGUOUS]
  docs/exercice1-modelisation/architecture-hybride.svg · relation: references

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `Entrepôt de données cloud (exigence)` and `Topics Iceberg / lakehouse sur S3`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `Service d'extension de l'Active Directory au cloud (exigence)` and `SASL/GSSAPI (Kerberos)`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `Baie de stockage SAN (10 To — non structuré)` and `Topic client_tickets`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `Baie de stockage SAN (10 To — non structuré)` and `Tiered Storage → Amazon S3 (Iceberg, rétention)`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `Active Directory (Identités · Kerberos)` and `Redpanda Console (supervision · ACL)`?**
  _Edge tagged AMBIGUOUS (relation: references) - confidence is low._
- **Why does `Exercice 1 — Modélisez une infrastructure hybride dans le cloud` connect `Selection Composants Cloud` to `Securite Couts AWS`, `SI On-Premise InduTechData`, `Livrables et Evaluation OC`?**
  _High betweenness centrality (0.109) - this node is a cross-community bridge._
- **Why does `Évaluation de compatibilité de l'infrastructure hybride` connect `Securite Couts AWS` to `Selection Composants Cloud`, `SI On-Premise InduTechData`, `Livrables et Evaluation OC`?**
  _High betweenness centrality (0.107) - this node is a cross-community bridge._