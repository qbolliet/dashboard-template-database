# Revue technique — base de données, DuckLake et API GraphQL

> Date : juillet 2026. Révision : 28 août 2026 (vérification empirique + intégration
> des points 1 à 7 ci-dessous). Périmètre : package `dt_ducklake_manager` (ce dépôt) et
> API GraphQL (dépôt `dashboard-template-api`, connue via la skill
> `dashboard-api-client`).
> Ce document répond aux questions posées, analyse le code existant et sert de
> **spécification cible (« schéma v2 »)** pour la série de prompts d'implémentation
> (`prompts-migration-schema-v2.md`, à la racine du dépôt).

> **Note méthodologique (révision d'août 2026).** Les affirmations portant sur le
> comportement de DuckLake ont été **mesurées** sur l'environnement du dépôt
> (DuckDB 1.5.2, extension `ducklake` correspondante, backend catalogue fichier) et
> non déduites de la documentation. Les mesures concernées sont signalées par
> « *mesuré* ». Trois conclusions de la version de juillet en sortent **corrigées** :
> le comportement de la compaction (§7), l'état du data inlining (§7) et la portée
> réelle de la qualification des identifiants (§14).

---

## 1. Le schéma général est-il le bon ?

**Oui, le schéma d'ensemble est le bon choix pour votre objectif** (un modèle générique
réutilisable sur de nombreux projets, requêté par une interface data-driven). Les
alternatives classiques se comparent ainsi :

| Schéma | Avantages | Inconvénients pour votre cas |
|---|---|---|
| **Table large + catalogue de métadonnées + dimensions** (actuel) | Générique, typé, agrégations rapides (colonnes natives), l'UI se pilote par la table `metadata` | Complexité de maintenance des `dim_*` (voir §4) |
| Format long / EAV (`entity, variable, value`) | Encore plus générique (colonnes = lignes) | Perte du typage colonne, pivots coûteux, filtres multi-variables pénibles, métadonnées quand même nécessaires |
| Étoile classique modélisée par projet | Optimal par projet | Zéro mutualisation : un schéma et une API par projet — exactement ce que vous voulez éviter |
| Une table par graphique (pré-agrégats) | Lectures triviales | Explosion du nombre de tables, l'agrégation à la volée de DuckDB rend cela inutile |

Le cœur de votre design — **la table `metadata` comme contrat entre la base et
l'interface** — est la bonne idée et mérite d'être renforcé (c'est le sens de vos points
1 et 4). Deux réserves sur la version actuelle :

1. **`is_primary_key` porte deux sémantiques** : « identifie une ligne » (déduplication,
   upsert) et « colonne de filtre pour l'UI ». Ces deux notions coïncident souvent mais
   pas toujours (une mesure peut être filtrable ; une clé technique peut ne pas être un
   filtre pertinent). Recommandation : conserver `is_primary_key` pour l'upsert et
   introduire une notion de **rôle** (`dimension` / `measure` / `identifier`) ou au
   minimum ne pas faire dépendre l'UI de `is_primary_key`.
2. **Il manque un niveau de métadonnées « jeu de données »** (par schéma) : label,
   description, source, date de dernière mise à jour, version du schéma de tables.
   L'API l'exposerait dans `getCatalogs` et l'interface y gagnerait beaucoup (titre,
   sous-titre, mention de fraîcheur). Voir §8.

---

## 2. DuckLake est-il la bonne technologie ?

**Oui — c'est même un très bon alignement avec votre profil de charge** : écritures par
lots peu fréquentes (sorties de modèles ML), lectures analytiques fréquentes (filtres,
agrégations), besoin d'instantanés (time travel pour auditer un run), et un consommateur
en lecture seule (l'API) concurrent d'un producteur (le job de mise à jour) — que vous
gérez déjà correctement via le catalogue PostgreSQL (`DuckLakeConnector.from_postgres`).

Comparaison avec les alternatives crédibles :

- **Fichier DuckDB seul** : plus simple, mais verrou au niveau processus (pas de
  lecteur API pendant l'écriture), pas de time travel, pas de format ouvert. Vous avez
  déjà rencontré cette limite (cf. les commentaires de `connector.py`).
- **Parquet nu + vues DuckDB** : pas de transactions ni d'upsert ; vous réécririez
  DuckLake à la main.
- **Iceberg / Delta Lake** : plus matures et multi-moteurs, mais infrastructure et
  outillage beaucoup plus lourds (catalogue REST/metastore, écosystème Spark). Pertinent
  seulement si d'autres moteurs que DuckDB doivent écrire.
- **Postgres / SQLite comme base de résultats** : orientés lignes, agrégations
  analytiques nettement plus lentes, pas de format colonne ouvert.
- **ClickHouse** : excellent en lecture mais un serveur à opérer ; disproportionné pour
  des dashboards mono-équipe.

**Le vrai risque de DuckLake est sa jeunesse** (spec et extension récentes). Vous êtes
bien couvert : les données restent du Parquet ouvert + un catalogue SQL lisible, et le
code passe par `narwhals`/SQL standard. Une migration de secours vers « Parquet +
DuckDB » ou Iceberg resterait mécanique.

**Point de vigilance important** (détaillé §4, §7 et §12) : DuckLake est conçu pour des
écritures *par lots*. Chaque commit produit des fichiers ; les insertions ligne à ligne
sont un anti-pattern. Or `DimensionManager.create_dimension_table`
(`_internal/managers/dimension.py:187`) et `update_dimension_values` (ligne 277) font
des `INSERT` **ligne par ligne** dans une boucle Python. La migration vers l'option B
supprime ce code ; si une table de dimension survit (hiérarchies), l'écrire en un seul
`INSERT ... SELECT FROM temp_view`.

*Correction par rapport à la version de juillet* : le **data inlining de DuckLake est
déjà actif par défaut** (*mesuré* : un `INSERT` de 4 lignes produit un snapshot
`{inlined_insert=[1]}` et **aucun** fichier Parquet). Ce n'est donc pas « une protection
à activer » mais un paramètre à **régler consciemment** — y compris à *désactiver*
(`DATA_INLINING_ROW_LIMIT 0`) pour les constructions initiales et pour les tests qui
inspectent les fichiers Parquet. Voir §7.

---

## 3. Lacunes et manques identifiés dans le système actuel

Par ordre d'importance décroissante (toutes les localisations ont été vérifiées dans le
code au 28 août 2026) :

1. **Bug de mapping de types** — `map_python_to_sql_type` (`utils/types.py:37`) mappe
   `Int64` vers `INTEGER` (32 bits chez DuckDB) et `Float32` vers `DOUBLE`
   (`utils/types.py:56`). Un identifiant ou un compte > 2³¹−1 débordera au DDL
   explicite. Correction : `Int8→TINYINT, Int16→SMALLINT, Int32→INTEGER, Int64→BIGINT`,
   `Float32→FLOAT`, `Float64→DOUBLE` (largeurs préservées, bénéfique aussi pour le
   stockage — cf. §7).
2. **La bascule automatique catégoriel ↔ non-catégoriel est un risque opérationnel** :
   quand une colonne franchit `categorical_threshold` lors d'un update,
   `convert_to_categorical` / `convert_to_non_categorical` **réécrivent la colonne de la
   fact table** (UPDATE corrélé = réécriture de fichiers chez DuckLake, cf. §12) et
   changent la *forme publique* des données (codes ↔ labels) que l'API et le front
   consomment. Une mise à jour de données peut donc silencieusement casser des filtres
   enregistrés côté front. L'option B (§6) élimine ce mécanisme, c'est l'un de ses plus
   gros bénéfices.
3. **Les identifiants ne sont qualifiés que par le schéma, jamais par le catalogue** :
   `qualify_table` (`utils/sql.py:39`) retourne `"<schema>.<table>"`. Les requêtes se
   résolvent donc dans le catalogue **courant** de la connexion, pas dans
   `catalog_alias` — que `DatabaseUpdater` ne consulte que pour ses appels de
   maintenance. Cela fonctionne aujourd'hui *par accident*, parce que
   `DuckLakeConnector.connect()` termine par `USE {alias}.{schema}`. C'est un vrai
   défaut ; voir §14 pour l'analyse et la correction.
4. **Machinerie transactionnelle plus lourde que nécessaire** : `TransactionManager` +
   `TransactionOperation` + savepoints applicatifs recouvrent ce que la transaction
   DuckDB (`BEGIN`/`COMMIT`) et les snapshots DuckLake fournissent déjà ; la plupart des
   `_rollback_*` de `updater.py` sont des placeholders qui ne restaurent rien
   (`_rollback_dimension_changes`, `_rollback_fact_changes`, `_rollback_index_changes`,
   `_restore_database_state` se contentent de journaliser). De même, `atomic.py` et
   `recovery.py` (backups, points de restauration) dupliquent le time travel DuckLake
   (`snapshot_version`). C'est le principal gisement de réduction de dette technique
   *hors* schéma : une passe de simplification peut retirer plusieurs centaines de
   lignes sans perte fonctionnelle.
5. **Logique d'« index » vestigiale** : DuckLake ne supporte pas les index ART de
   DuckDB ; le pruning se fait par partitions, statistiques min/max des fichiers et tri
   des données. Les méthodes `_drop_column_indexes_safe`, `_restore_column_indexes`,
   `_cleanup_orphaned_indexes` (`operations/deleter.py:685` et suivantes) et
   `_rollback_index_changes` (`operations/updater.py:1012`) sont du code mort à retirer ;
   l'optimisation réelle est le `ORDER BY` à l'écriture (§7).
6. **Identifiants SQL non quotés** : noms de tables/colonnes interpolés en f-string
   partout (`f"SELECT DISTINCT {col_name} FROM ..."`, `updater.py:648` ;
   `f"f.{key} = upd.{key}"`, `updater.py:876`). Les noms viennent des colonnes du
   DataFrame d'entrée : un nom avec espace, majuscule ou mot réservé casse les requêtes,
   et c'est une surface d'injection si un jour les noms viennent de l'extérieur. Ajouter
   un utilitaire `quote_ident` dans `utils/sql.py` et l'utiliser systématiquement.
   *Note* : `DataManager._validate_insert` rejette déjà les noms non alphanumériques,
   mais ce garde-fou n'est appliqué que sur le chemin d'insertion — il ne couvre ni le
   builder, ni les `SELECT`, ni la table `metadata`.
7. **Logs écrits dans le répertoire du package** (`FILE_PATH.parents[2]/logs`,
   `connector.py:186`, `schema/persistence.py:69`, `schema/inference.py:122`,
   `_internal/managers/base.py:83`) : échoue pour un package installé (site-packages en
   lecture seule, OneDrive...). Par défaut, écrire dans le répertoire de travail ou
   accepter seulement un chemin explicite.
8. **La compaction appelée après chaque update ne compacte rien** — voir §7 et §12 ;
   c'est une découverte de la révision d'août, pas un point de la version de juillet.
9. **Journalisation trop pauvre pour un job de production** : les compteurs existent
   ligne par ligne (`"Fact table (direct): N inserted, M updated"`) mais rien ne relie
   une transaction à son bilan complet (colonnes ajoutées, métadonnées modifiées,
   fichiers créés/supprimés, snapshot avant/après). Voir §13.
10. **Pas de traçabilité des runs ML dans les données** : les snapshots DuckLake datent
    les écritures, mais rien ne relie une ligne à un `run_id` / `model_version`. Simple
    convention à documenter (colonnes de mesure dédiées) plutôt que changement de schéma.
11. **`__init__.py` racine en `import *`** : point à nuancer par rapport à la version de
    juillet — les sous-modules (`utils`, `maintenance`, `operations`, `schema`,
    `connection`) définissent bien un `__all__`, donc les exports *sont* maîtrisés.
    Reste que le `__init__.py` racine ne déclare pas de `__all__` agrégé : cosmétique,
    faible priorité.
12. **Côté API** : `dimensionDetails` impose au front une résolution valeur→label qui
    disparaîtra avec l'option B ; la pagination par offset est plafonnée à 10 000 (OK pour
    des dashboards, à documenter comme limite de conception).

---

## 4. Point 1 — menus « select » hiérarchiques (group-options)

### Votre analyse est juste, avec une distinction à faire

Il y a **deux natures de hiérarchies** à ne pas confondre, et votre proposition les
mélange légèrement :

**Cas A — hiérarchie de colonnes (niveaux aplatis)** : chaque niveau est une colonne de
la fact table (`region`, `departement`, `commune`). C'est le cas dominant dans des
résultats de modèles. Ici :

- La hiérarchie des *colonnes* se déclare par **`parent_name` dans la table
  `metadata`** (la colonne parente de `commune` est `departement`) — exactement votre
  idée, et c'est la bonne : de proche en proche on reconstruit la chaîne des niveaux,
  sans limite de profondeur.
- La hiérarchie des *valeurs* est **déjà dans la fact table** : chaque ligne porte le
  triplet (region, departement, commune). `SELECT DISTINCT departement, commune` donne
  le mapping parent→enfants sans aucune table auxiliaire. Avec l'option B (labels dans
  la fact table), c'est une requête bon marché sur colonnes dictionary-encodées —
  l'actuel `getGroupedSelectOptions` fait déjà exactement cela sur 2 niveaux.
- **Aucune table de dimension n'est nécessaire dans ce cas.** `path` et `depth` se
  dérivent à la volée si besoin.

**Cas B — hiérarchie de valeurs dans une seule colonne** (taxonomie de profondeur
arbitraire, ex. nomenclature d'activités dans une colonne `secteur`) : là, votre design
de table de dimension est le bon, sous forme **liste d'adjacence + chemin matérialisé** :

```
dim_<col>(value VARCHAR, label VARCHAR, parent_value VARCHAR, path VARCHAR, depth INTEGER)
```

- `parent_value` (adjacence) permet le parcours récursif (`WITH RECURSIVE`, supporté par
  DuckDB) ;
- `path` (ex. `'Industrie/Agroalimentaire/Boissons'`) permet le filtre de sous-arbre en
  une clause (`WHERE path LIKE 'Industrie/%'`) sans récursion — précieux pour l'API ;
- `depth` est redondant avec `path` mais quasi gratuit et pratique pour l'UI.

Matérialiser `path`/`depth` à l'écriture est le bon arbitrage : le writer les recalcule
à chaque build/update (données petites), l'API ne fait jamais de récursion.
Contrainte à valider à l'écriture : pas de cycle, et un `parent_value` doit exister dans
la même table (sinon erreur explicite).

**Recommandation** : supporter les deux cas. `metadata.parent_name` pour le cas A
(zéro table), tables de dimension hiérarchiques **opt-in et déclarées explicitement**
(argument du builder, pas d'inférence) pour le cas B. Cela rejoint votre intuition
« des tables de dimension seulement pour les colonnes ayant une hiérarchie ».

### GraphQL et profondeur non bornée

GraphQL ne sait pas exprimer une récursion de profondeur arbitraire dans une seule
requête (les fragments récursifs sont interdits ; les fragments imbriqués à N niveaux
sont un hack, et votre garde-fou `max query depth = 7` en prod le bloquerait). Les deux
solutions propres, à offrir ensemble :

1. **Liste plate + reconstruction côté client** (recommandé comme primitive) :
   `getSelectOptionsFlat(field)` retourne `[{value, label, parentValue, path, depth}]` ;
   le front reconstruit l'arbre en O(n). Paginable, cachable, filtrable
   (`pathPrefix: "Industrie/"` pour un sous-arbre).
2. **Scalaire JSON pour la commodité** : `getSelectOptionsTree(field): JSON` retourne
   l'arbre imbriqué prêt à consommer par un composant de menu. Vous avez déjà un scalaire
   `JsonValue` dans le schéma — c'est cohérent avec `getFactTableWithMetadata`.
   Parfait pour les menus (taille bornée par nature : ce sont des options de select).

Pour le cas A, le même couple d'endpoints se résout par `SELECT DISTINCT` sur la chaîne
des colonnes remontée via `parent_name`. À noter : le `<optgroup>` HTML natif ne gère
qu'un niveau — au-delà, il faut de toute façon un composant d'arbre, qui consommera
l'une des deux formes ci-dessus.

---

## 5. Point 2 — Option B : labels dans la fact table

### Verdict : oui, migrez. C'est la modification au meilleur ratio bénéfice/risque.

Votre compréhension du dictionary-encoding Parquet est exacte : c'est une optimisation
physique de page, transparente à la lecture, pas une table requêtable. Mais elle rend
précisément le schéma « codes + dim » **redondant pour son objectif initial** (ne pas
répéter des strings) : Parquet stocke le dictionnaire une fois par row group et DuckDB
lit ces colonnes efficacement (les `SELECT DISTINCT` et `GROUP BY` sur colonne
dictionary-encodée sont bon marché).

Constat déterminant relevé dans le code : **vos codes sont purement synthétiques**
(`enumerate()` dans `SchemaBuilder.create_dimension_tables`, `schema/inference.py:265` ;
`DimensionManager.create_dimension_table`, `_internal/managers/dimension.py:187`). La
table de dimension actuelle n'apporte *aucune information* que la donnée d'origine ne
contient pas : `label` **est** la valeur d'origine, `value` est un artefact interne.
Autrement dit, l'option A paie tous les coûts d'une table de dimension sans aucun de ses
bénéfices sémantiques.

Inventaire de ce que l'option B supprime (vérifié dans le code) :

| Code supprimé ou vidé | Localisation |
|---|---|
| Construction des dims + substitution labels→codes au build | `SchemaBuilder.create_dimension_tables`, `create_fact_table` |
| Écriture des dims | `DuckLakeTablesBuilder.create_duckdb_dimension_tables` |
| Assignation de codes `max+1`, INSERT ligne à ligne, verrous, parallélisme | `DimensionManager.update_dimension_values`, `batch_update_dimensions` |
| Bascules catégoriel ↔ non-catégoriel avec réécriture de la fact table | `DimensionManager.convert_to_*`, `_convert_fact_table_dimension_mapping` |
| Mapping labels→codes à chaque update | `DatabaseUpdater._prepare_dataframe_for_fact_table` |
| Nettoyage des entrées orphelines | `cleanup_orphaned_dimension_entries` |
| Enrichissement/JOIN côté API, dérive des codes entre schémas/catalogues | resolvers `dimensionDetails`, `compareFacts` |

C'est l'essentiel de la complexité du writer **et** l'une des deux étapes fragiles du
reader. Les coûts en face :

- **Stockage** : négligeable (dictionary-encoding). Seule exception théorique : très
  longues chaînes × très forte répétition × row groups nombreux — pas votre cas.
- **Performance de filtre** : comparer des strings est un peu plus lent que des
  entiers, mais le pruning par partition/zone-map fait le gros du travail avant ; à
  l'échelle de dashboards, indétectable.
- **Migration** : les catalogues v1 existants stockent des codes ; il faut un script de
  migration (décoder via les dims puis les supprimer). Prompt dédié.
- **Rupture d'API** : `dimensionDetails` et `getDimensionTable` perdent leur objet
  (value == label). Dépréciation douce côté API.

### Conséquences sur `metadata` — une nuance par rapport à votre analyse

- **`python_type` : à supprimer**, d'accord. Il est redondant avec `sql_type` (seul
  utilisé pour le DDL et suffisant pour le mapping type→graphique côté front). Attention,
  trois endroits en dépendent aujourd'hui et devront être réécrits sur `sql_type` :
  `BaseSchemaManager._resolve_type_conflicts` (`base.py:431`, hiérarchie de types),
  le DataFrame vide de repli de `_load_current_metadata` (`base.py:139`), et le filtre
  `nw.col("python_type") == "String"` de `DatabaseUpdater._update_dimensions_safe`
  (`updater.py:609`).
- **`is_categorical` : à conserver, mais changer sa nature.** Vous proposez de le
  supprimer ; je recommande de le garder comme **métadonnée d'UI pure** (« cette colonne
  se filtre par un select et peut servir de groupBy ») car le front en a besoin sans
  avoir à lancer un `COUNT(DISTINCT)` par colonne à chaque chargement, et l'API l'utilise
  déjà (`getFields(isCategorical:...)`, `getCatalogSchema`). La différence clef : il ne
  pilote **plus aucun choix de stockage** — le franchissement du seuil met à jour un
  booléen dans `metadata` (1 UPDATE) au lieu de réécrire la fact table. Le
  `categorical_threshold` survit uniquement comme paramètre d'inférence de cette
  métadonnée (avec possibilité d'override explicite par colonne).
- Cas résiduel `value ≠ label` : si un projet fournit des codes métier (ex. `"FR"`) et
  veut afficher `"France"`, prévoir un argument optionnel `label_maps={"country": {...}}`
  au builder qui crée une table d'attributs `dim_<col>(value, label)` **déclarée, jamais
  inférée**. Même mécanique opt-in que les hiérarchies du §4.

---

## 6. Point 4 — colonnes `unit`, `display_format`, `family`

Pertinent et peu coûteux. Spécification recommandée (tous nullable, valeurs par défaut
NULL) :

| Colonne | Type | Rôle UI |
|---|---|---|
| `unit` | VARCHAR | Suffixe d'axe/tooltip (`"€"`, `"%"`, `"MW"`) |
| `display_format` | VARCHAR | Chaîne **d3-format** (`",.2f"`, `".0%"`) — convention naturelle pour un front D3 |
| `family` | VARCHAR | Regroupement des variables dans les menus (familles thématiques) |
| `description` | VARCHAR | Tooltip/aide contextuelle (recommandé en plus) |
| `default_aggregation` | VARCHAR | `SUM`/`AVG`/... : agrégation par défaut d'une mesure — évite au front de deviner (recommandé en plus) |
| `parent_name` | VARCHAR | Hiérarchie de colonnes (§4) |

S'y ajoute la **table de métadonnées de jeu de données** (une ligne par schéma) évoquée
au §1 : `dataset_metadata(label, description, source, updated_at, schema_version)`.
`schema_version` est important : c'est ce qui permettra à l'API de servir des catalogues
v1 et v2 pendant la transition.

---

## 7. Point 3 — optimisations du stockage Parquet / DuckLake

Cette section a été **entièrement revérifiée** en août 2026 ; deux affirmations de la
version de juillet étaient fausses.

### 7.1 Les faits mesurés

Protocole : catalogue DuckLake neuf, `INSERT` de 20 000 lignes, puis
`UPDATE ... WHERE id < 5000` (25 % des lignes), puis chaque opération de maintenance,
avec inspection du répertoire de données après chaque étape.

| Étape | Fichiers présents dans `data/main/fact_table/` |
|---|---|
| Après `INSERT` de 20 000 lignes | 1 fichier de 20 000 lignes |
| Après `UPDATE` de 5 000 lignes | + 1 fichier de 5 000 lignes (avec une colonne `_ducklake_internal_row_id`) + 1 fichier `…-delete.parquet` de 5 000 lignes (colonnes `file_path`, `pos`) |
| Après `ducklake_merge_adjacent_files` (défauts) | **inchangé** |
| Après `ducklake_rewrite_data_files` (défauts) | **inchangé** |
| Après `ducklake_rewrite_data_files(delete_threshold := 0.1)` | + 1 fichier de 15 000 lignes (l'ancien reste sur disque) |
| Après `ducklake_expire_snapshots` + `ducklake_cleanup_old_files(cleanup_all := true)` | 2 fichiers, 20 000 lignes au total, **plus aucun doublon ni fichier de suppression** |

**Conséquence n° 1 — la compaction actuelle est un no-op.**
`DatabaseUpdater._run_ducklake_compaction` (`updater.py:918`) appelle
`ducklake_merge_adjacent_files` puis `ducklake_rewrite_data_files` **sans paramètres**.
Aucun des deux n'a rien réécrit sur ce cas pourtant très favorable (25 % de lignes
supprimées). Les vraies signatures, *mesurées* via `duckdb_functions()` :

```
ducklake_merge_adjacent_files(catalog, table, min_file_size, max_file_size,
                              max_compacted_files, schema)
ducklake_rewrite_data_files(catalog, table, delete_threshold, schema)
ducklake_expire_snapshots(catalog, older_than, versions, dry_run)
ducklake_cleanup_old_files(catalog, older_than, cleanup_all, dry_run)
```

`rewrite_data_files` ne réécrit un fichier que si sa proportion de lignes supprimées
dépasse `delete_threshold` ; il faut donc **passer explicitement ce seuil** (0,1 à 0,3
sont des valeurs raisonnables pour un job d'update). Et `merge_adjacent_files` ne fusionne
que des fichiers plus petits que `min_file_size` — utile après beaucoup de petits
updates, inutile sur un gros fichier unique.

**Conséquence n° 2 — seuls `expire_snapshots` + `cleanup_old_files` libèrent l'espace.**
C'est logique : tant qu'un snapshot référence l'ancien fichier, le time travel doit
pouvoir le lire. Le cycle est donc : *réécrire* (rewrite) → *périmer* (expire) →
*supprimer* (cleanup). Attention, `expire_snapshots` **détruit le filet de sécurité du
time travel** : ce n'est pas une opération d'update, c'est une opération de maintenance
planifiée, avec une fenêtre de rétention (`older_than`) choisie consciemment.

**Conséquence n° 3 — le data inlining est actif par défaut.** Un `INSERT` de quelques
lignes ne produit aucun fichier Parquet ; les données vivent dans le catalogue
(snapshot `{inlined_insert=[1]}`). C'est excellent pour absorber les petites mises à
jour, mais cela signifie que **tout test qui inspecte les fichiers Parquet doit soit
écrire un gros lot, soit désactiver l'inlining** (`DATA_INLINING_ROW_LIMIT 0` à l'ATTACH,
*mesuré* comme option ATTACH valide).

### 7.2 Les options réellement disponibles

*Mesuré* via `SELECT * FROM ducklake_options('<alias>')` après positionnement, et via
`CALL <alias>.set_option(name, value [, table_name := ..., schema := ...])` — les deux
derniers arguments permettent un **réglage par table ou par schéma**, pas seulement
global :

| Option | Valeurs | Effet |
|---|---|---|
| `parquet_compression` | `uncompressed, snappy, gzip, zstd, brotli, lz4, lz4_raw` | `zstd` : meilleur ratio que snappy, coût de lecture proche |
| `parquet_row_group_size` | entier (lignes) | Granularité du pruning min/max ; plus petit = pruning plus fin, plus de métadonnées |
| `parquet_version` | `1` ou `2` | V2 : encodages plus efficaces |
| `target_file_size` | taille **avec unité** (`'100MB'`, `'104857600B'`) — sans unité c'est une erreur de parsing | Taille cible des fichiers écrits |
| `data_inlining_row_limit` | entier | Lots plus petits stockés dans le catalogue ; `0` = jamais |
| `encrypted` | `true`/`false` | Chiffrement des Parquet |

`data_inlining_row_limit` est également acceptée comme option d'`ATTACH`
(`DATA_INLINING_ROW_LIMIT 0`), *mesuré*.

### 7.3 Le plan d'optimisation, par ordre de rendement

1. **Tri des données à l'écriture** (le vrai « index » du monde Parquet) : les stats
   min/max par row group ne servent que si les données sont physiquement groupées.
   Écrire la fact table avec `ORDER BY <colonnes de filtre principales>` (typiquement
   les clés primaires dans leur ordre de sélectivité) au build, et trier de même les
   lots d'update.
2. **Cycle de maintenance correct** (§7.1) : `rewrite_data_files(delete_threshold=…)`
   après un update qui a produit beaucoup de suppressions, `merge_adjacent_files` quand
   beaucoup de petits fichiers se sont accumulés, et `expire_snapshots` +
   `cleanup_old_files` **en maintenance planifiée seulement**, avec rétention explicite.
3. **Options DuckLake** : `parquet_compression = 'zstd'`, `parquet_version = 2`,
   `target_file_size = '100MB'`, `parquet_row_group_size` aligné sur la taille des
   lots, `data_inlining_row_limit` réglé selon le profil (0 au build, quelques milliers
   en update incrémental).
4. **Partitionnement : garder, mais avec parcimonie.** Déjà implémenté (`partition_by`).
   Règle à documenter : uniquement des colonnes de très faible cardinalité
   systématiquement filtrées (année, pays), jamais au point de produire des fichiers
   < ~100 Mo.
5. **Largeurs de types** : corriger le mapping (§3.1) pour préserver
   `TINYINT/SMALLINT/INTEGER/BIGINT` et `FLOAT` vs `DOUBLE`. C'est la version utile de
   l'astuce « float16 » de l'article Medium : DuckDB n'a pas de half-float, mais
   `FLOAT` (32 bits) suffit très largement pour des valeurs destinées à des graphiques,
   au choix du producteur de données (ne pas forcer silencieusement).
6. **Suppression de la logique d'index** (§3.5) : à faire dans la même passe, c'est le
   pendant « nettoyage » de cette rubrique.

Le `pd.Categorical`/dictionnaire Arrow à l'écriture ne change rien pour vous : c'est
DuckLake qui écrit les Parquet, et il applique le dictionary-encoding lui-même.

---

## 8. Récapitulatif — spécification du schéma v2 (à reporter dans la skill)

### Tables d'un schéma DuckLake (une par jeu de résultats)

**`fact_table`** — les données, colonnes typées au plus juste
(`TINYINT/SMALLINT/INTEGER/BIGINT/FLOAT/DOUBLE/...`). Les colonnes catégorielles
contiennent **directement les labels** (strings d'origine) ; plus aucun code
synthétique. Écrite triée sur les colonnes de filtre principales, partitionnement
Hive optionnel.

**`metadata`** — une ligne par colonne de la fact table :

```sql
name VARCHAR,              -- nom technique
label VARCHAR,             -- libellé d'affichage
sql_type VARCHAR,          -- type SQL (python_type supprimé)
is_categorical BOOLEAN,    -- métadonnée d'UI pure (select/groupBy) ; ne pilote plus le stockage
is_primary_key BOOLEAN,    -- clé logique d'upsert/déduplication
parent_name VARCHAR,       -- colonne parente (hiérarchie de colonnes), NULL sinon
unit VARCHAR,              -- unité d'affichage
display_format VARCHAR,    -- format d3-format
family VARCHAR,            -- famille thématique (regroupement de menus)
description VARCHAR,       -- description longue
default_aggregation VARCHAR -- agrégation par défaut pour les mesures
```

**`dataset_metadata`** — une ligne par schéma : `label, description, source,
updated_at, schema_version` (= `2`).

**`dim_<col>`** — **uniquement opt-in, déclarées au build**, deux formes :
- *Hiérarchie de valeurs* : `value, label, parent_value, path, depth`
  (adjacence + chemin matérialisé, validées sans cycle à l'écriture) ;
- *Table de labels* (`label_maps`) : `value, label` quand code métier ≠ libellé.

Plus aucune dimension inférée par seuil de cardinalité ; plus de conversion
catégoriel ↔ non-catégoriel touchant aux données.

### Impacts API (pour l'évolution du dépôt GraphQL)

- `getSelectOptions` → `SELECT DISTINCT col` (+ `searchTerm`, `limit`) ; `label = value`
  sauf table de labels déclarée.
- `getGroupedSelectOptions` → `SELECT DISTINCT parent, child` sur la fact table, la
  paire étant dérivée de `metadata.parent_name` ; généralisable à n niveaux.
- Nouveaux : `getSelectOptionsFlat(field)` (liste plate `value/label/parentValue/path/
  depth`, pour cas A et B) et `getSelectOptionsTree(field): JSON` (arbre imbriqué).
- `dimensionDetails` et `getDimensionTable` : dépréciés (value == label). Comme je suis en phase de développement je ne souhaite pas les maintenir le temps de la transition.
- `getCatalogSchema` / `getFields` exposent `unit`, `display_format`, `family`,
  `description`, `default_aggregation`, `parent_name`.
- `compareFacts` : jointure directe sur les labels — la dérive de codes disparaît.
- Nouveau : métadonnées de jeu de données dans `getCatalogs` (label, source,
  fraîcheur, `schema_version`).

### Corrections et nettoyages embarqués dans la migration

1. Mapping de types : `Int64 → BIGINT`, largeurs préservées, `Float32 → FLOAT`.
2. Suppression de la logique d'index (inopérante sous DuckLake).
3. Quoting systématique des identifiants SQL (`quote_ident`) **et qualification par le
   catalogue** (§14).
4. Options DuckLake réglables et cycle de maintenance corrigé (§7).
5. Logs hors du répertoire du package.
6. Simplification de la machinerie transactionnelle au profit de `BEGIN`/`COMMIT`
   DuckDB + snapshots DuckLake (recommandé, plus « optionnel » : la journalisation du
   §13 s'écrit beaucoup plus simplement après).

---

## 9. Point 5 — ajout de colonnes de valeurs avec un sous-ensemble de clés

### Le besoin

Un modèle produit un DataFrame qui ne porte **qu'une partie des clés primaires** de la
fact table, plus une ou plusieurs colonnes de valeurs nouvelles. Exemple : la fact table
a pour clé `(date, region, produit)` ; le nouveau DataFrame porte `(region, produit)` et
une colonne `score_moyen`. La sémantique demandée : **diffusion (broadcast)** de la
valeur sur toutes les lignes de la base partageant la combinaison de clés fournie.

### Ce que fait le code aujourd'hui

Rien de tel n'est possible : `_update_fact_table_direct` (`updater.py:720`) refuse
l'opération dès qu'une clé primaire manque (`"Primary keys missing in DataFrame"`).
À l'inverse, `DataManager._ensure_columns_exist` (`data.py:758`) ajoute
*silencieusement* toute colonne inconnue lors d'un insert/upsert — comportement implicite
qu'il faut rendre explicite au passage.

### Spécification cible

Nouvelle méthode publique de `DatabaseUpdater` :

```python
add_value_columns(
    df: IntoDataFrame,
    on: list[str] | None = None,      # sous-ensemble de clés ; défaut = PK ∩ df.columns
    overwrite: bool = False,          # autorise l'écrasement de colonnes existantes
    column_metadata: dict[str, dict[str, str]] | None = None,
    compact_after_update: bool = True,
) -> UpdateReport
```

Invariants et validations, **toutes bloquantes sauf mention contraire** :

1. `on` est non vide et **inclus dans les clés primaires** de `metadata` (un sous-ensemble
   strict est le cas nominal ; l'ensemble complet retombe sur un upsert classique).
   Une colonne de `on` absente de la fact table ou non déclarée clé primaire → `ValueError`.
2. Le DataFrame est **unique sur `on`** : sinon la valeur à diffuser est ambiguë →
   `ValueError` listant les combinaisons en doublon.
3. Les colonnes de valeurs ne doivent pas exister dans la fact table, sauf
   `overwrite=True`. Les colonnes de `df` qui ne sont ni dans `on` ni des colonnes de
   valeurs déclarées sont ignorées (avec un warning listant les ignorées).
4. Type SQL déduit par `map_python_to_sql_type` corrigé (§3.1), `ALTER TABLE … ADD COLUMN
   <col> <type> DEFAULT NULL`, puis une ligne dans `metadata` (`is_primary_key = FALSE`,
   `is_categorical` inféré, champs d'UI depuis `column_metadata`).
5. Diffusion en **un seul `UPDATE … FROM`** (jamais ligne à ligne) :
   `UPDATE fact f SET c1 = t.c1, … FROM tmp t WHERE f.k1 = t.k1 AND …`.
6. Comptages **journalisés et retournés** : lignes de la fact table mises à jour ; lignes
   de la fact table laissées à NULL faute de correspondance ; combinaisons de `df` sans
   aucune correspondance en base (warning non bloquant, avec un échantillon).
7. `dataset_metadata.updated_at` mis à jour.

### Le piège à documenter

Un `UPDATE` qui touche *toutes* les lignes est, chez DuckLake, une **réécriture complète
de la table** (copy-on-write, §12) : un nouveau fichier de la taille de la table entière
plus un fichier de suppression de même cardinalité. Sur une grosse fact table c'est
l'opération la plus coûteuse du système. Deux conséquences pratiques :

- la méthode doit **journaliser explicitement** le volume touché avant d'agir (et donc
  s'appuyer sur les compteurs du §13) ;
- elle doit être suivie d'un `rewrite_data_files` avec un `delete_threshold` bas, sans
  quoi la table conserve durablement le double de ses fichiers (§7.1).

Quand le producteur peut fournir la colonne au moment du build initial, c'est toujours
préférable — cette méthode est un rattrapage, pas un chemin nominal.

---

## 10. Point 6 — intégration du `DuckLakeConnector` (à faire à la main)

**Cette étape est réalisée manuellement, hors série de prompts.** Elle est décrite ici
pour que les prompts n'entrent pas en collision avec elle.

Constat : `DuckLakeConnector` produit une `duckdb.DuckDBPyConnection` et *perd* aussitôt
son contexte. Les classes en aval le reconstruisent morceau par morceau :
`DatabaseUpdater` reçoit `connection`, `ducklake_catalog_alias` **et** `schema` en trois
arguments indépendants (`updater.py:49`), `DuckLakeTablesBuilder` reçoit `connection` et
`schema` mais **pas** l'alias (`persistence.py:61`), `DuckLakeMaintenance` reçoit
`connection` et `catalog_alias` mais **pas** le schéma (`compaction.py:45`). Rien ne
garantit que ces trois valeurs restent cohérentes, et c'est la racine du défaut de
qualification du §14.

Cible recommandée : faire circuler le connecteur (ou un petit contexte immuable qu'il
expose) plutôt que la connexion nue.

```python
@dataclass(frozen=True)
class DuckLakeContext:
    connection: duckdb.DuckDBPyConnection
    catalog_alias: str
    schema: str
    read_only: bool
```

- `DuckLakeConnector.connect()` retourne toujours une connexion (compatibilité), et
  expose en plus `context(schema: str | None = None) -> DuckLakeContext`, permettant de
  dériver un contexte par schéma sur la même connexion.
- `BaseSchemaManager`, `DuckLakeTablesBuilder` et `DuckLakeMaintenance` acceptent soit un
  `DuckLakeContext`, soit un `DuckLakeConnector`, soit — pour compatibilité — le triplet
  actuel. `read_only` permet de refuser proprement une écriture au lieu d'échouer au
  milieu d'une transaction, et de ne pas tenter d'appliquer d'options (§7.2).
- `attach()` gagne un paramètre `activate_schema: bool = True` : dans un contexte
  multi-catalogues (§11), attacher un second catalogue ne doit pas voler le catalogue
  courant de la connexion via son `USE`.

Les prompts qui suivent supposent seulement que **l'alias du catalogue est disponible
partout où le schéma l'est déjà** ; ils fonctionnent avec le triplet actuel comme avec
un `DuckLakeContext`.

---

## 11. Point 7 — plusieurs `ATTACH` de catalogues sur une même connexion

### Ce qui marche, mesuré

- **Plusieurs catalogues DuckLake attachés simultanément : oui.** Deux `ATTACH
  'ducklake:…' AS db1 / AS db2` cohabitent sans erreur ; chacun crée sa propre base de
  métadonnées interne (`__ducklake_metadata_db1`, `__ducklake_metadata_db2`).
- **Requêtes et jointures inter-catalogues : oui.** `SELECT … FROM db1.main.fact_table a
  FULL JOIN db2.main.fact_table b USING (id)` fonctionne directement.
- **Écriture transactionnelle inter-catalogues : NON.** *Mesuré* :
  `TransactionContext Error: Attempting to write to database "db2" in a transaction that
  has already modified database "db1" — a single transaction can only write to a single
  attached database.` C'est une limite de DuckDB, pas de DuckLake, et elle est
  structurante : **aucune opération d'écriture atomique ne peut couvrir deux catalogues.**
- **Deux catalogues dans une même base de métadonnées** : l'option `METADATA_SCHEMA` de
  l'`ATTACH` est bien reconnue par la grammaire. Le test local échoue pour une raison sans
  rapport (backend fichier : le même `.ducklake` ne peut pas être ouvert deux fois dans un
  processus). Avec un backend **PostgreSQL**, où il n'y a pas de verrou de fichier, deux
  catalogues devraient pouvoir partager une base sous deux schémas de métadonnées
  distincts — **à valider sur un serveur réel avant de s'y engager.**

### Est-ce souhaitable ? Ce que cela apporterait

| Usage | Verdict |
|---|---|
| Comparer des résultats de **projets différents** dans une même requête | Le seul bénéfice réel. Aujourd'hui il faudrait deux connexions et une jointure côté Python. |
| Une API unique servant plusieurs projets | Utile : un processus, une connexion, N catalogues en lecture seule. |
| Copier/migrer des données d'un catalogue à l'autre (`INSERT INTO … SELECT`) | Possible, mais **hors transaction** : à traiter comme une opération de copie, pas de migration atomique. |
| Séparer les jeux de résultats d'**un même projet** | **Non.** C'est le rôle des schémas d'un même catalogue, déjà implémenté et déjà transactionnel. Multiplier les catalogues ici ne ferait que perdre l'atomicité. |

**Recommandation.** Oui, mais **en lecture seule et comme fonctionnalité explicite**, pas
comme mode par défaut. Concrètement : un helper `attach_catalog(conn, connector,
activate_schema=False)` (ou l'usage documenté de `DuckLakeConnector.attach()` corrigé
comme au §10), plus une note d'architecture énonçant la règle : **un catalogue par
projet, un schéma par jeu de résultats, et jamais d'écriture couvrant deux catalogues
dans une même transaction.** L'écriture multi-catalogues reste possible, mais séquentielle
et non atomique — donc à réserver à des scripts de copie assumés comme tels.

Le partage d'une base PostgreSQL par plusieurs catalogues via `METADATA_SCHEMA` est
séduisant pour limiter le nombre de bases à administrer, mais ne change rien aux limites
ci-dessus et ajoute une inconnue : à n'envisager qu'après validation sur serveur.

---

## 12. Point 4 — pourquoi les lignes semblent dupliquées dans les fichiers Parquet

### Réponse courte : c'est normal, et c'est optimisable.

DuckLake est **copy-on-write** : les fichiers Parquet sont immuables. Un `UPDATE` n'écrit
donc jamais « dans » un fichier existant — il produit :

1. **un nouveau fichier de données** contenant les lignes dans leur version mise à jour
   (avec une colonne interne supplémentaire `_ducklake_internal_row_id`) ;
2. **un fichier de suppression** `…-delete.parquet`, de schéma `(file_path VARCHAR,
   pos BIGINT)`, qui liste les positions devenues obsolètes **dans l'ancien fichier** ;
3. l'ancien fichier, **laissé intact sur disque**, parce qu'un snapshot antérieur le
   référence encore (c'est ce qui rend le time travel possible).

D'où ce que vous observez : en lisant *directement* tous les Parquet du répertoire (par
exemple avec `read_parquet('…/**/*.parquet', union_by_name=true)`), on obtient l'union de
trois familles de fichiers de schémas différents. *Mesuré* sur le cas 20 000 lignes puis
`UPDATE` de 5 000 : **30 000 lignes brutes** pour 20 000 lignes logiques, et un schéma
d'union `['id', 'country', 'value', '_ducklake_internal_row_id', 'file_path', 'pos']`.
Chaque ligne n'ayant qu'une partie de ces colonnes, les autres ressortent à `NULL` —
c'est très exactement l'impression « une observation sur deux a une colonne non
renseignée ». Ce n'est pas une anomalie de vos données : c'est un artefact de la lecture
brute. La vue logique (`SELECT * FROM fact_table`) est correcte à tout instant, et c'est
la seule qui compte pour l'API.

### Ce qui, en revanche, est un vrai défaut

Les fichiers obsolètes ne disparaissent **jamais** dans le pipeline actuel, parce que
`_run_ducklake_compaction` n'appelle que `merge_adjacent_files` et `rewrite_data_files`
sans paramètres — deux no-ops sur ce cas (§7.1) — et n'appelle jamais
`expire_snapshots` / `cleanup_old_files`. La base grossit donc indéfiniment à chaque
update. Le cycle correct, *mesuré* comme ramenant le répertoire à 2 fichiers et
20 000 lignes sans doublon :

```
rewrite_data_files(delete_threshold := 0.1)   -- après update, si beaucoup de suppressions
merge_adjacent_files(min_file_size := …)      -- quand beaucoup de petits fichiers
expire_snapshots(older_than := <rétention>)   -- maintenance planifiée uniquement
cleanup_old_files()                           -- après expiration
```

La distinction est importante : les deux premières sont sûres et peuvent suivre chaque
update ; les deux dernières **détruisent le time travel** et relèvent d'une politique de
rétention explicite (hebdomadaire, avec `older_than` = quelques jours), jamais d'un
`compact_after_update=True`.

---

## 13. Point 8 — journalisation explicite des transactions

Aujourd'hui les traces sont éparses et partielles : un compteur ici
(`"Fact table (direct): N inserted, M updated"`, `updater.py:750`), une phrase là
(`"Compaction DuckLake is finished for 'fact_table'"`, `updater.py:953`) — cette dernière
étant, on l'a vu, émise même quand rien n'a été compacté. Rien ne permet de répondre à
« qu'est-ce que ce run a changé ? ».

### Cible : un rapport d'opération structuré

```python
@dataclass
class OperationReport:
    operation: str                        # 'update' | 'add_value_columns' | 'delete' | 'maintenance'
    schema: str
    started_at: datetime
    duration_seconds: float
    rows_inserted: int
    rows_updated: int
    rows_deleted: int
    rows_before: int
    rows_after: int
    columns_added: list[str]
    columns_dropped: list[str]
    metadata_changes: list[str]           # ex. "is_categorical(region): False -> True"
    snapshot_before: int | None
    snapshot_after: int | None
    files_before: int
    files_after: int
    bytes_before: int
    bytes_after: int
    maintenance: dict[str, int]           # files_processed / files_created par procédure
    warnings: list[str]
```

Sources de ces chiffres, toutes **mesurées comme disponibles** :

- `ducklake_table_info('<alias>')` retourne `file_count`, `file_size_bytes`,
  `delete_file_count`, `delete_file_size_bytes` — à interroger avant et après
  l'opération. C'est la source publique et stable, à préférer à un accès direct aux
  tables `__ducklake_metadata_*`.
- `ducklake_snapshots('<alias>')` donne le `snapshot_id` courant et, colonne `changes`,
  un résumé lisible (`{tables_inserted_into=[1], tables_deleted_from=[1]}`).
- `ducklake_merge_adjacent_files` et `ducklake_rewrite_data_files` **retournent un jeu de
  résultats** `(schema_name, table_name, files_processed, files_created)` — actuellement
  ignoré par le code, qui journalise donc « terminé » sans savoir si quoi que ce soit a
  été fait.
- `ducklake_cleanup_old_files` retourne la liste des chemins supprimés (colonne `path`) ;
  `ducklake_expire_snapshots` retourne les snapshots périmés. Les deux acceptent
  `dry_run := true`, ce qui permet de journaliser *ce qui serait fait* avant de le faire.

### Contrat de journalisation

- Une **ligne de synthèse INFO** par opération réussie, lisible d'un coup d'œil :
  `update main: +1 240 lignes, ~380 mises à jour, +2 colonnes (score, rang), 3 → 2 fichiers (48,2 → 31,7 Mo), snapshot 17 → 18, 4,1 s`.
- Le détail (par étape) en DEBUG, pas en INFO.
- Les compteurs à zéro **explicités** plutôt que tus : « rewrite_data_files : 0 fichier
  réécrit (seuil de suppression non atteint) » vaut infiniment mieux que « terminé ».
- Chaque warning collecté dans `warnings` est aussi journalisé au fil de l'eau.
- L'API publique ne change pas : `update_database` continue de retourner `bool`, et le
  rapport est exposé via `updater.last_report`. Les nouvelles méthodes (§9) retournent
  directement un `OperationReport`.

---

## 14. Point 9 — qualification des identifiants par le catalogue

### Le constat, mesuré

`qualify_table` retourne `"<schema>.<table>"`. Le nom se résout donc dans le **catalogue
courant** de la connexion. *Mesuré* : avec `db1` et `db2` attachés et une `fact_table`
dans chacun, `SELECT * FROM main.fact_table` retourne les lignes de `db1` ou de `db2`
selon le dernier `USE` exécuté. Autrement dit, la cible d'une requête de
`dt_ducklake_manager` dépend d'un état global de session que la classe ne contrôle pas.

Cela ne casse rien aujourd'hui parce que `DuckLakeConnector.connect()` termine par
`USE {alias}.{schema}` et qu'un seul catalogue est attaché. Mais cela casse dès que :
un second catalogue est attaché (§11) ; un `USE` est exécuté ailleurs (notebook, autre
composant) ; ou deux managers visant des catalogues différents partagent une connexion.
Et le symptôme serait le pire possible : **écrire silencieusement dans la mauvaise base**.

### La correction recommandée — oui, faites-la

```python
def quote_ident(name: str) -> str: ...
def qualify_table(table: str, schema: str = "main", catalog: str | None = None) -> str: ...
```

- Retourne `"catalog"."schema"."table"` quand `catalog` est fourni, `"schema"."table"`
  sinon (compatibilité avec les connexions in-memory des tests, qui n'ont pas d'alias).
- Tous les composants exposent l'alias au même titre que le schéma (§10), et
  `BaseSchemaManager._qualified` le transmet.
- **Bonus non négligeable** : c'est exactement le même parcours de code que le quoting
  des identifiants (§3.6). Les deux corrections touchent les mêmes lignes ; les faire
  ensemble évite de repasser deux fois sur tout le paquet.
- Attention aux endroits qui *ne* doivent pas être qualifiés : les vues temporaires
  enregistrées via `conn.register()` (`temp_fact`, `temp_upsert`, `_upd_split`…) vivent
  dans le catalogue mémoire, tout comme les table functions `ducklake_*` — les qualifier
  les casserait.

---

## 15. Modifications superflues / à ne pas faire

- **Ne pas** chercher à exploiter le dictionnaire Parquet comme table requêtable
  (votre analyse l'a déjà écarté, à juste titre).
- **Ne pas** introduire de cache applicatif côté writer (le cache de métadonnées actuel
  suffit ; le reste est du one-shot batch).
- **Ne pas** généraliser les tables de dimension hiérarchiques à toutes les colonnes
  catégorielles « au cas où » : opt-in strict, sinon on recrée la complexité qu'on vient
  de supprimer.
- **Ne pas** ajouter de pré-agrégats matérialisés tant qu'aucune requête réelle n'est
  lente : DuckDB agrège à la volée très en deçà du seuil de perception sur des volumes
  de dashboard.
- **Ne pas** appeler `expire_snapshots` / `cleanup_old_files` après chaque update : ce
  serait renoncer au time travel, c'est-à-dire au seul mécanisme de récupération de
  l'architecture (§12).
- **Ne pas** répartir les jeux de résultats d'un même projet sur plusieurs catalogues :
  on y perdrait l'atomicité sans rien gagner (§11).
- Le passage du catalogue DuckDB → PostgreSQL systématique n'est pas nécessaire en
  développement ; la double voie actuelle (fichier local en dev, Postgres en prod) est
  le bon réglage.
