# Modélisez une infrastructure dans le cloud

Projet Data Engineer — OpenClassrooms.

L'entreprise InduTech veut moderniser sa gestion de données (IoT, plus de 50 Go/mois en temps réel)
en s'appuyant sur le cloud, sans casser son SI on-premise existant : SQL Server, SAN, Active
Directory, ERP/CRM.

Le projet se découpe en deux exercices indépendants :

1. **Modélisation** d'une infrastructure hybride on-premise ↔ cloud, avec évaluation de
   compatibilité. Exercice sur le papier — rien n'est déployé.
2. **POC** d'un pipeline ETL temps réel de gestion de tickets clients avec Redpanda et PySpark,
   conteneurisé avec Docker.

Consignes complètes : [`consigne.md`](consigne.md).

## Structure

```
docs/exercice1-modelisation/   Schéma d'architecture + évaluation de compatibilité
src/                           Code Python du pipeline
docker/                        Dockerfiles + docker-compose.yml
data/                          Exports générés par le pipeline (ignoré par git)
livrables/                     Livrables finaux pour le dépôt OpenClassrooms
```
