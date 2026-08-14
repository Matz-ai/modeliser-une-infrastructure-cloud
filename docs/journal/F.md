# Journal — Conversation F (Documentation, Mermaid & vidéo)

**Périmètre** : Exercice 2, Étape 5 · **Branche** : `conv-f-documentation`
**Dossier de travail** : `worktrees/conv-f/` (un worktree par agent — plan §2.1)

---

## 2026-08-14 — Prérequis bloquant : levé, mais pas comme prévu

Le plan conditionnait F à « un `docker compose up --build` fonctionnel de bout en bout ». Au
démarrage, ce n'était **pas** le cas : E avait livré la conteneurisation mais n'avait jamais pu
lancer le service `spark`, faute de script — et D n'avait pas commencé.

La même session a donc enchaîné **D**, puis la **validation du pipeline complet**, avant d'écrire
une seule ligne de documentation. C'était le bon ordre : documenter un pipeline qu'on n'a pas vu
tourner produit un README qui ment sur les détails, et un script de vidéo infilmable.

**Le pipeline a été validé pour de vrai avant rédaction** — 5 services, une seule commande,
18 000+ tickets traités, exports Parquet et JSON écrits, reprise après redémarrage vérifiée
(détail dans [`D.md`](D.md)). **Chaque chiffre et chaque sortie du README proviennent de cette
exécution**, aucun n'est reconstitué.

---

## 2026-08-14 — Le README

Cible fixée par la *definition of done* du plan : **un lecteur externe doit pouvoir lancer et
comprendre le POC avec le seul README**. Structure retenue, dans l'ordre de ce que ce lecteur se
demande :

1. **Le schéma de flux Mermaid** en tête de l'Exercice 2 — comprendre avant de lancer ;
2. **Prérequis** — la bonne nouvelle d'abord : *Docker, et rien d'autre* ;
3. **Démarrage rapide** — la commande, puis **« ce que vous devez voir »** ;
4. **Les insights produits** — ce que le pipeline calcule et où ça atterrit ;
5. **Performance et résilience** — les deux points de vigilance de la consigne, avec les valeurs ;
6. **Le contrat de données** — le schéma du ticket ;
7. **Dépannage** — les symptômes réellement rencontrés.

### Décisions de rédaction

- **Une vraie sortie console est collée dans le README**, pas une paraphrase. C'est ce qui permet
  au lecteur de savoir en un coup d'œil si son propre lancement se comporte normalement.
- **Une section « Ce que vous devez voir »** avec les quatre URL/commandes de vérification. Un
  README qui donne la commande de lancement mais pas le critère de succès laisse le lecteur seul.
- **La section Dépannage ne liste que des symptômes réellement observés** pendant le projet
  (`topic-init` en `Exited (0)`, code 143, `.delta` manquant, accents cassés sous PowerShell,
  conflit de port avec le cluster autonome de C). Une FAQ inventée n'aide personne.
- **Trois pièges sont documentés en tant que comportements normaux** — c'est le meilleur retour sur
  investissement de la doc : `topic-init` qui sort en `Exited (0)`, `spark` qui sort en **143**, et
  le bruit Py4J à l'arrêt. Les trois ressemblent à des pannes et n'en sont pas.
- **La section « Avancement » n'a pas été touchée** (plan §2.3) : elle reste à l'identique, réservée
  à la Conversation G.

### Le diagramme Mermaid

Trois sous-graphes **Extract / Transform / Load**, qui rendent la structure ETL lisible sans
légende. Trois choix :

- **la validation apparaît comme un losange de décision**, avec sa branche « non » vers la
  quarantaine : c'est l'exigence de résilience, rendue visible plutôt qu'affirmée ;
- **les checkpoints sont reliés en pointillés** au bloc Transform, hors du flux de données — ils
  n'en font pas partie, ils le rendent reprenable ;
- **les paramètres réels figurent sur les nœuds** (3 partitions, clé `client_id`, rétention 7 jours,
  partitionnement par `request_type`) : le schéma sert aussi d'aide-mémoire.

Syntaxe volontairement conservatrice — uniquement `<br/>`, `<b>` et `<i>` dans les libellés, tous
entre guillemets. Mermaid sur GitHub est plus strict que les éditeurs en ligne.

---

## 2026-08-14 — La vidéo

**Support : Loom** (décision §4.5 du plan). Le script complet est dans
[`docs/script-video.md`](../script-video.md) : préparation, 8 séquences minutées (~5-6 min), phrases
repères, commandes exactes, et la marche à suivre après le tournage.

> ⚠️ **La vidéo doit être tournée par Mathieu** — c'est le seul livrable non automatisable du
> projet. Tout le reste est prêt.

### Deux points de préparation qui font la différence

- **Repartir d'un état vierge** (`down -v` + suppression de `data/`). Sinon le compteur d'insights
  démarre à plusieurs milliers de tickets et on **ne voit pas** le pipeline se remplir — c'est
  pourtant ce que la vidéo doit prouver.
- **Pré-construire les images** (`build` avant de filmer). Le premier `up --build` prend 2 à
  4 minutes de construction ; filmer une barre de progression Docker n'apporte rien, et la commande
  montrée reste exactement la même.

### Ce que la vidéo montre, et pourquoi dans cet ordre

Le fil est **la preuve, pas le code** : les tickets arrivent (console Redpanda) → ils sont traités
(insights en console + interface Spark) → les résultats sortent (Parquet et JSON sur le disque) →
et ça résiste (redémarrage, reprise au micro-batch suivant). La lecture du code n'y a pas sa place :
c'est le rôle du README.

---

## Reste à faire

- [ ] **Tourner la vidéo** (Mathieu) en suivant [`docs/script-video.md`](../script-video.md).
- [ ] **Remplacer le lien Loom** dans le README — section « 🎬 Vidéo de démonstration »,
      **deux occurrences** (la note d'avertissement et le lien lui-même).
- [ ] **Screenshots** à prendre par Mathieu (`screenshots/F-*.png`), si des captures fixes sont
      souhaitées en complément de la vidéo.

## Points d'attention transmis à la Conversation G

1. **Le README de F remplace intégralement celui de `main`**, section « Avancement » exceptée, qui
   est reprise **à l'identique**. Un conflit de merge sur ce fichier est donc attendu et normal :
   **garder la version de F partout, sauf la section Avancement que G consolide elle-même** à
   partir des journaux.
2. **Deux liens du README pointent vers des fichiers d'autres branches** (`docs/contrat-ticket.md`
   de C, `docs/contrat-export.md` de E) : ils ne se résolvent qu'**une fois tout mergé**. C'est
   attendu, ce n'est pas une erreur à corriger sur la branche de F.
3. **La checklist `consigne.md` §8 « Étape 5 »** peut être cochée pour le README et le diagramme
   Mermaid ; **pas pour la vidéo**, tant que le lien Loom n'est pas en place.
4. Voir aussi la recommandation de [`D.md`](D.md) sur les checkpoints (volume nommé plutôt que
   montage lié Windows) et l'écart additif au contrat d'export (`exports/rejets/`).

## Questions ouvertes

*Aucune pour Mathieu*, hormis le tournage de la vidéo lui-même.
