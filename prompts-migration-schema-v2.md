# Prompts d'implémentation — migration vers le schéma v2

> Série de prompts à exécuter **dans l'ordre**, chacun dans une session Claude Code
> fraîche, depuis la racine du dépôt. La spécification cible est dans
> `revue-technique-bdd.md` (« schéma v2 », à la racine du dépôt) : chaque prompt demande
> sa lecture, ne pas la supprimer avant la fin de la série.
>
> **Révision d'août 2026** : la série a été revue après vérification empirique du
> comportement de DuckLake (DuckDB 1.5.2). Les prompts 1, 5 et 7 ont été corrigés sur des
> points factuels (noms d'options, comportement de la compaction, data inlining) ; trois
> prompts ont été ajoutés (6, 9, 11) ; l'ancien prompt 8 « optionnel » devient obligatoire
> et remonte avant la journalisation. Voir le tableau de correspondance en fin de document.
>
> **Conventions communes** (rappelées dans chaque prompt, en complément de CLAUDE.md) :
> annotations de types partout ; commentaires en FRANÇAIS (formulations nominales) ;
> docstrings en ANGLAIS, convention Google, avec exemples ; tests pytest ciblant les
> cas limites ; lancer les tests avec `uv run --no-sync pytest` (le `--no-sync` évite
> l'échec « Accès refusé » d'uv sous OneDrive).
>
> **Choix du modèle** : Opus pour les prompts qui exigent des décisions d'architecture
> ou touchent beaucoup de fichiers interdépendants ; Sonnet pour les tâches mécaniques
> bien spécifiées. **Plan mode** : activé quand la tâche comporte des choix
> d'implémentation à valider avant d'écrire ; inutile quand la spécification ci-dessous
> est déjà un plan.

---

## Étape M (manuelle, avant le prompt 1) — intégration du `DuckLakeConnector`

**Réalisée à la main, hors prompt.** Elle est spécifiée au §10 de
`revue-technique-bdd.md` : faire circuler l'alias du catalogue, le schéma et le statut
`read_only` ensemble (via un `DuckLakeContext` ou le connecteur lui-même) au lieu des
trois arguments indépendants actuels, et ajouter `activate_schema: bool = True` à
`DuckLakeConnector.attach()`.

Les prompts qui suivent ne présupposent **qu'une seule chose** de cette étape : que
l'alias du catalogue soit accessible partout où le schéma l'est déjà
(`BaseSchemaManager`, `DuckLakeTablesBuilder`, `DuckLakeMaintenance`). Ils fonctionnent
avec le triplet d'arguments actuel comme avec un `DuckLakeContext` — mais le prompt 1
(qualification par le catalogue) est nettement plus simple si l'étape M est faite avant.
 
---

## Prompt 1 — Corrections préalables (types, quoting, qualification, index, logs)

**Modèle : Sonnet · Plan mode : non · Dépendances : étape M (recommandée)**

```text
Lis d'abord revue-technique-bdd.md (sections 3, 8 et 14) pour le contexte. Applique
cinq corrections indépendantes du schéma, sans changer aucun comportement public autre
que ceux décrits :

1) Mapping des types (dt_ducklake_manager/utils/types.py) : préserve les largeurs
   d'entiers — Int8→TINYINT, Int16→SMALLINT, Int32→INTEGER, Int64→BIGINT — et mappe
   Float32→FLOAT, Float64→DOUBLE. Mets à jour la docstring et ses exemples (l'exemple
   actuel annonce 'INTEGER' pour une colonne d'entiers polars, qui est Int64 : il doit
   devenir 'BIGINT'), puis tous les tests qui supposaient INTEGER/DOUBLE pour ces types
   (tests/unit/test_utils/test_types.py et les tests d'intégration).

2) Quoting des identifiants SQL : ajoute dans dt_ducklake_manager/utils/sql.py une
   fonction quote_ident(name: str) -> str qui entoure l'identifiant de guillemets
   doubles en doublant les guillemets internes (norme SQL). Utilise-la partout où un nom
   de table ou de colonne issu des données est interpolé dans une requête
   (schema/persistence.py, _internal/managers/*.py, operations/*.py, maintenance/*.py).
   Attention aux clauses où l'identifiant apparaît plusieurs fois : conditions de
   jointure construites par " AND ".join (updater.py:876, data.py:681), clause SET de
   _direct_upsert_data (data.py:707), listes de colonnes des INSERT ... SELECT
   (data.py:616 et 722), et build_database_duplicate_removal_query (utils/sql.py).

3) Qualification par le catalogue : qualify_table ne qualifie aujourd'hui que par le
   schéma, si bien que les requêtes se résolvent dans le catalogue COURANT de la
   connexion et non dans catalog_alias (cf. section 14 de la revue, où le problème est
   démontré). Fais évoluer la signature en
   qualify_table(table: str, schema: str = "main", catalog: str | None = None) -> str
   retournant "catalog"."schema"."table" quand catalog est fourni et "schema"."table"
   sinon (indispensable pour les connexions in-memory des tests, qui n'ont pas d'alias),
   en réutilisant quote_ident. Propage l'alias jusqu'à BaseSchemaManager._qualified et
   aux builders. NE QUALIFIE PAS : les vues temporaires enregistrées par conn.register()
   (temp_fact, temp_insert, temp_upsert, temp_metadata, temp_dim, _upd_split), ni les
   table functions ducklake_* — elles vivent dans le catalogue mémoire et la
   qualification les casserait. Ajoute un test vérifiant qu'avec deux catalogues
   attachés, un manager configuré sur l'un n'écrit jamais dans l'autre, quel que soit le
   dernier USE exécuté.

4) Suppression de la logique d'index, inopérante sous DuckLake : retire
   _drop_column_indexes_safe, _restore_column_indexes, _cleanup_orphaned_indexes dans
   operations/deleter.py, _rollback_index_changes dans operations/updater.py, ainsi que
   leurs appels (notamment les TransactionOperation 'drop_indexes' et
   'cleanup_indexes' de deleter.py) et leurs tests. Si l'auditor (maintenance/auditor.py)
   ou atomic.py référencent des index, retire aussi ces vérifications. Ne touche à rien
   d'autre dans ces fichiers.

5) Emplacement des logs : les chemins par défaut du type
   os.path.join(FILE_PATH.parents[2], "logs/...") écrivent dans le répertoire
   d'installation du package (connector.py:186, schema/persistence.py:69,
   schema/inference.py:122, _internal/managers/base.py:83). Remplace ce défaut par
   Path.cwd() / "logs" / <nom>.log (création du dossier si absent), de façon centralisée
   dans utils/logger.py plutôt que répétée dans chaque module. Conserve la possibilité de
   passer un chemin explicite. Profites-en pour corriger _init_logger, qui ajoute un
   FileHandler au logger RACINE à chaque appel : les handlers s'accumulent (une ligne de
   log dupliquée par manager instancié). Utilise un logger nommé et vérifie l'absence de
   handler équivalent avant d'en ajouter un.

Conventions : types partout ; commentaires en français (formulations nominales) ;
docstrings anglaises Google avec exemples. Termine par uv run --no-sync pytest et
corrige jusqu'au vert. Résume les fichiers touchés.
```

*Pourquoi Sonnet sans plan mode : cinq changements mécaniques, entièrement spécifiés,
sans décision d'architecture. Le point 3 est ajouté ici parce qu'il touche exactement les
mêmes lignes que le point 2 — les séparer imposerait deux passes sur tout le paquet.*

---

## Prompt 2 — Option B : labels dans la fact table, suppression des dimensions inférées

**Modèle : Opus · Plan mode : OUI · Dépendances : prompt 1**

```text
Lis d'abord revue-technique-bdd.md en entier — c'est la spécification. Objectif :
migrer le writer vers le « schéma v2 », option B : la fact_table stocke directement
les labels (strings d'origine) dans les colonnes catégorielles ; plus aucun code
synthétique, plus aucune table de dimension inférée par seuil de cardinalité. Le
dictionary-encoding Parquet absorbe le coût de stockage. Périmètre : ce dépôt
uniquement (l'API GraphQL sera adaptée séparément).

Spécification du comportement cible :

- SchemaBuilder (schema/inference.py) : create_fact_table ne substitue plus rien (la
  fact table est le DataFrame d'entrée dédupliqué) ; create_dimension_tables disparaît
  du pipeline par défaut. is_categorical reste calculé (String et n_unique <=
  categorical_threshold) mais devient une métadonnée d'UI pure. python_type est
  supprimé de la table metadata ; sql_type est conservé. Prévois un paramètre
  categorical_overrides: dict[str, bool] | None permettant de forcer le statut d'une
  colonne indépendamment du seuil.

- DuckLakeTablesBuilder (schema/persistence.py) : n'écrit plus de tables dim_* ;
  metadata est créée sans python_type. Ajoute une table dataset_metadata (une ligne :
  label, description, source, updated_at TIMESTAMP, schema_version INTEGER = 2),
  alimentée par des arguments optionnels du builder ; updated_at et schema_version
  sont toujours renseignés. Retire aussi la boucle de logging des « clés étrangères »
  de create_duckdb_fact_table, qui n'a plus d'objet.

- DimensionManager (_internal/managers/dimension.py) : vide-le de la gestion des
  codes — create_dimension_table, update_dimension_values, batch_update_dimensions,
  convert_to_categorical, convert_to_non_categorical,
  _convert_fact_table_dimension_mapping, cleanup_orphaned_dimension_entries et
  get_dimension_mapping sont supprimés (les tables dim_* réapparaîtront sous une autre
  forme, opt-in, au prompt 4 : ne conserve que ce qui resterait utile).
  Si la classe devient vide, supprime-la et retire ses usages.

- DatabaseUpdater (operations/updater.py) : _prepare_dataframe_for_fact_table ne
  mappe plus labels→codes (les données passent telles quelles) ; _update_dimensions_safe
  se réduit à la mise à jour du booléen is_categorical dans metadata quand une colonne
  String franchit le seuil dans un sens ou dans l'autre (un simple UPDATE metadata,
  plus aucune réécriture de la fact_table) ; les étapes de transaction
  « dimension_update » et « dimension_cleanup » disparaissent. ATTENTION : ce filtrage
  s'appuie aujourd'hui sur nw.col("python_type") == "String" (updater.py:609) — il doit
  passer sur sql_type == 'VARCHAR'. Le comptage du seuil doit rester fondé sur un
  COUNT(DISTINCT) de la fact_table (état post-upsert), et non sur le nombre d'entrées
  d'une dim_* qui n'existe plus. Mets à jour dataset_metadata.updated_at à chaque
  update réussi.

- BaseSchemaManager (_internal/managers/base.py) : adapte _load_current_metadata (y
  compris le DataFrame vide de repli, base.py:139, qui liste encore python_type),
  _add_column_to_metadata (dont le CREATE TABLE IF NOT EXISTS metadata doit refléter le
  schéma v2) et _resolve_type_conflicts au schéma metadata v2 : la hiérarchie de
  résolution travaille sur les types SQL — BOOLEAN < TINYINT/SMALLINT/INTEGER/BIGINT <
  FLOAT/DOUBLE < VARCHAR, avec conservation de la largeur la plus grande à l'intérieur
  d'un même niveau (un Int64 déjà enregistré ne doit pas être rétrogradé en INTEGER par
  un lot d'Int32).

- DatabaseDeleter, auditor, recovery : retire les références aux dims inférées et aux
  conversions catégorielles ; l'auditor ne valide plus la cohérence fact/dim mais
  vérifie la présence de dataset_metadata et la cohérence metadata/fact_table.

Contraintes de style : types partout ; commentaires en français (formulations
nominales) ; docstrings anglaises Google avec exemples. Réécris les tests unitaires et
d'intégration impactés (tests/) en gardant la logique « cas limites » ; supprime les
tests des mécanismes disparus. Ne crée pas de script de migration des catalogues v1
(prompt 7). Termine par uv run --no-sync pytest et corrige jusqu'au vert, puis
fournis un résumé des changements structuré par module.
```

*Pourquoi Opus + plan mode : refactor le plus large de la série, interdépendances
entre six modules, décisions résiduelles (sort de DimensionManager, forme exacte de
dataset_metadata) à valider sur plan avant l'exécution.*

---

## Prompt 3 — Métadonnées d'UI : unit, display_format, family, description, default_aggregation

**Modèle : Sonnet · Plan mode : non · Dépendances : prompt 2**

```text
Lis d'abord revue-technique-bdd.md (sections 6 et 8). Ajoute à la table metadata
les colonnes d'UI du schéma v2, toutes VARCHAR nullable (NULL par défaut) : unit,
display_format (chaîne d3-format), family, description, default_aggregation (valeurs
attendues : SUM, AVG, MAX, MIN, COUNT, MEDIAN, MODE — à valider à l'écriture, erreur
explicite sinon).

Implémentation :
- SchemaBuilder.create_metadata_table accepte un paramètre column_metadata:
  dict[str, dict[str, str]] | None mappant nom de colonne → {unit, display_format,
  family, description, default_aggregation} (clés toutes optionnelles). Le paramètre
  column_labels existant est conservé tel quel ; si un label est aussi fourni via
  column_metadata, column_metadata prime. Valide que les noms de colonnes référencés
  existent dans le DataFrame (ValueError sinon) et qu'aucune clé inconnue ne se glisse
  dans les sous-dictionnaires (ValueError listant les clés fautives).
- DuckLakeTablesBuilder écrit ces colonnes dans le DDL de metadata et les propage.
- BaseSchemaManager._add_column_to_metadata insère NULL pour ces colonnes ; ajoute une
  méthode publique update_column_metadata(column: str, **fields) permettant de
  renseigner ou corriger ces champs sur une base existante sans reconstruction
  (UPDATE + invalidation du cache ; erreur explicite si la colonne n'existe pas dans
  metadata ou si le champ n'est pas un champ v2 autorisé).
- DatabaseUpdater : lors d'un update, ces champs ne sont jamais écrasés (ils
  n'appartiennent qu'au producteur de métadonnées). Vérifie en particulier que
  _add_column_to_metadata, qui fait un UPDATE quand la colonne existe déjà, ne remet
  pas ces champs à NULL au passage.

Tests pytest sur les cas limites : colonne inconnue dans column_metadata,
default_aggregation invalide, clé inconnue dans un sous-dictionnaire, champs
partiellement renseignés, update qui ne doit pas écraser les champs,
update_column_metadata sur colonne absente. Complète le notebook d'illustration
existant (ou crée notebooks/4 - Métadonnées d'interface.ipynb) montrant l'effet de
column_metadata sur la table metadata. Conventions habituelles : types, commentaires
français nominaux, docstrings anglaises Google avec exemples. Termine par
uv run --no-sync pytest.
```

*Pourquoi Sonnet sans plan mode : schéma additif entièrement spécifié, un seul point
de passage par table.*

---

## Prompt 4 — Hiérarchies : parent_name et dimensions hiérarchiques opt-in

**Modèle : Opus · Plan mode : OUI · Dépendances : prompt 3**

```text
Lis d'abord revue-technique-bdd.md (sections 4 et 8) — la distinction cas A
(hiérarchie de colonnes) / cas B (hiérarchie de valeurs) y est spécifiée. Implémente
le support des hiérarchies pour les menus select à group-options de profondeur
arbitraire.

Cas A — hiérarchie de colonnes :
- Ajoute parent_name VARCHAR nullable à la table metadata. Renseignable via
  column_metadata (prompt 3) ou un paramètre dédié hierarchies: dict[str, str] | None
  (colonne → colonne parente) sur SchemaBuilder/DuckLakeTablesBuilder.
- Validations à l'écriture : la colonne parente existe dans le DataFrame ; pas de
  cycle dans le graphe des parent_name (parcours de proche en proche avec détection) ;
  erreur explicite sinon.
- Ajoute une fonction utilitaire publique get_column_hierarchy(conn, schema) ->
  list[list[str]] retournant les chaînes de colonnes racine→feuille reconstituées
  depuis metadata (servira de référence à l'API).

Cas B — hiérarchie de valeurs dans une colonne :
- Nouveau paramètre opt-in du builder : value_hierarchies: dict[str, IntoDataFrame]
  mappant un nom de colonne vers un DataFrame (value, label, parent_value). Pour
  chaque entrée, construis la table dim_<col>(value VARCHAR, label VARCHAR,
  parent_value VARCHAR, path VARCHAR, depth INTEGER) : path = chemin des labels
  racine→nœud joints par '/', depth = 0 pour les racines. Matérialise path et depth
  en Python au moment de l'écriture (les tables sont petites) ; écriture en un seul
  INSERT ... SELECT depuis une vue temporaire enregistrée (jamais de INSERT ligne à
  ligne — anti-pattern DuckLake, cf. section 2 de la revue).
- Validations : parent_value existant ou NULL, absence de cycle, unicité de value,
  toutes les valeurs distinctes de la colonne de la fact_table présentes dans value
  (warning loggé, non bloquant, pour les valeurs orphelines).
- Même mécanique pour un paramètre label_maps: dict[str, dict[str, str]] (colonne →
  {value: label}) créant une table dim_<col>(value, label) plate quand code métier ≠
  libellé. Aucune table n'est jamais créée sans déclaration explicite.
- DatabaseUpdater : lors d'un update de données, si une colonne possède une
  dim_<col> hiérarchique et que de nouvelles valeurs apparaissent dans la fact_table,
  logge un warning listant les valeurs absentes de la dimension (pas d'invention de
  nœud). Prévois une méthode update_value_hierarchy(col, df) pour remplacer
  atomiquement le contenu d'une dimension hiérarchique (DELETE + INSERT dans la même
  transaction, revalidation complète).

Tests pytest cas limites : cycle dans parent_name, cycle dans parent_value, valeur
orpheline, hiérarchie à un seul niveau, hiérarchie profonde (5+ niveaux), label_maps
avec valeurs manquantes, label contenant le séparateur '/' (documente le comportement
retenu). Crée notebooks/5 - Hiérarchies.ipynb illustrant les deux cas et l'effet des
paramètres. Conventions habituelles (types, commentaires français nominaux, docstrings
anglaises Google avec exemples). Termine par uv run --no-sync pytest et un résumé.
```

*Pourquoi Opus + plan mode : deux mécanismes couplés, choix d'API publique du builder
et invariants (cycles, orphelins) qui méritent une validation sur plan.*

---

## Prompt 5 — Optimisations d'écriture DuckLake/Parquet

**Modèle : Sonnet · Plan mode : OUI · Dépendances : prompt 2**

```text
Lis d'abord revue-technique-bdd.md (sections 7 et 12). La section 7 contient les noms
d'options et les signatures de fonctions VÉRIFIÉS EMPIRIQUEMENT sur la version de
DuckLake du dépôt (DuckDB 1.5.2) : appuie-toi dessus plutôt que sur ta mémoire, et
revérifie sur la version réellement installée avant de coder
(SELECT * FROM ducklake_options('<alias>'), et
SELECT parameters, parameter_types FROM duckdb_functions() WHERE function_name = ...).

Contexte factuel à ne pas perdre de vue (mesuré, cf. section 7.1) :
- la compaction actuelle de DatabaseUpdater._run_ducklake_compaction est un NO-OP :
  ducklake_rewrite_data_files sans delete_threshold explicite ne réécrit rien, même
  avec 25 % de lignes supprimées, et ducklake_merge_adjacent_files ne fusionne que des
  fichiers plus petits que min_file_size ;
- le data inlining de DuckLake est ACTIF PAR DÉFAUT : un petit INSERT ne produit aucun
  fichier Parquet.

Implémente les optimisations suivantes.

1) Tri à l'écriture : DuckLakeTablesBuilder.create_duckdb_fact_table accepte
   order_by: list[str] | None ; défaut : les clés primaires dans leur ordre de
   déclaration. L'INSERT ... SELECT (et le chemin CTAS) se font avec ORDER BY
   correspondant. Idem pour les insertions de DataManager (_direct_insert_data et
   _batch_insert_data) : tri du lot sur les mêmes colonnes avant insertion. Les colonnes
   d'order_by doivent exister (ValueError sinon).

2) Options DuckLake exposées sur DuckLakeConnector : paramètre optionnel
   ducklake_options: dict[str, str | int] | None appliqué après ATTACH via
   CALL <alias>.set_option(nom, valeur). Fournis un dictionnaire de défauts recommandés
   RECOMMENDED_DUCKLAKE_OPTIONS exporté par le module, appliqué quand l'utilisateur
   passe ducklake_options="recommended" :
     parquet_compression = 'zstd'
     parquet_version = 2
     target_file_size = '100MB'        <- l'unité est OBLIGATOIRE, sans elle c'est une
                                          erreur de parsing
     parquet_row_group_size = 122880   <- à ajuster selon la taille des lots
   Ne mets PAS data_inlining_row_limit dans les défauts : c'est un arbitrage à faire
   projet par projet (0 au build initial pour éviter des données invisibles dans les
   fichiers, quelques milliers en update incrémental). Expose-le plutôt comme option
   d'ATTACH (DATA_INLINING_ROW_LIMIT est une option ATTACH valide) via un argument
   dédié du connecteur. Aucune option ne doit être appliquée sur une connexion
   read_only ; journalise chaque option effectivement positionnée.

3) Cycle de maintenance corrigé (maintenance/compaction.py) :
   - merge_files et rewrite_data_files acceptent leurs paramètres réels
     (min_file_size / max_file_size / max_compacted_files pour la première ;
     delete_threshold pour la seconde) avec des défauts explicites et documentés
     (delete_threshold = 0.1 est un point de départ raisonnable) ;
   - ces deux procédures RETOURNENT un jeu de résultats
     (schema_name, table_name, files_processed, files_created) : récupère-le et
     journalise les compteurs réels au lieu de « terminé » ; un 0 doit être dit
     explicitement (« 0 fichier réécrit : seuil de suppression non atteint »).
   - expire_snapshots et cleanup_files retournent également des résultats exploitables
     et acceptent dry_run := true : expose ce paramètre.
   - DatabaseUpdater._run_ducklake_compaction transmet un delete_threshold et
     journalise le résultat. Il ne doit JAMAIS appeler expire_snapshots ni
     cleanup_old_files : ces deux procédures détruisent le time travel, qui est le seul
     mécanisme de récupération de l'architecture. Elles restent réservées à
     full_maintenance, avec une rétention explicite.
   - Documente ce cycle dans la docstring de module de compaction.py : rewrite après
     update → merge quand les petits fichiers s'accumulent → expire + cleanup en
     maintenance planifiée avec rétention, et pointeur vers order_by pour le
     regroupement physique.

Tests. ATTENTION au data inlining : un test qui insère quelques lignes puis cherche un
fichier Parquet échouera, car les données restent dans le catalogue. Tout test
inspectant les fichiers doit soit attacher avec DATA_INLINING_ROW_LIMIT 0, soit écrire
un lot largement au-dessus de la limite. Couvre : l'ordre physique d'insertion suit
order_by (lecture du fichier via read_parquet et contrôle de monotonie) ; les options
sont bien positionnées (SELECT * FROM ducklake_options(...) après connexion) ; une
connexion read_only ne tente pas de les appliquer ; rewrite_data_files avec un
delete_threshold bas réécrit effectivement après un UPDATE partiel, et ne réécrit rien
avec le défaut du moteur. Complète le notebook d'illustration des paramètres du
builder. Conventions habituelles. Termine par uv run --no-sync pytest.
```

*Pourquoi Sonnet + plan mode : tâche cadrée mais dont l'ancienne version reposait sur
des noms d'options supposés. Le plan sert à verrouiller les noms réels après lecture de
la doc et de `ducklake_options` avant de coder.*

---

## Prompt 6 — Ajout de colonnes de valeurs par sous-ensemble de clés (NOUVEAU)

**Modèle : Opus · Plan mode : OUI · Dépendances : prompts 2, 3 et 5**

```text
Lis d'abord revue-technique-bdd.md (section 9, qui spécifie ce comportement, ainsi que
les sections 7 et 12 pour le coût de l'opération chez DuckLake). Objectif : permettre
d'ajouter de nouvelles colonnes de VALEURS à la fact_table lors d'un update, à partir
d'un DataFrame qui ne porte qu'un SOUS-ENSEMBLE des clés primaires. Les valeurs sont
diffusées (broadcast) sur toutes les lignes de la base partageant la combinaison de clés
fournie.

Exemple : fact_table de clé (date, region, produit) ; DataFrame d'entrée
(region, produit, score_moyen) → toutes les lignes de la base partageant (region,
produit) reçoivent le même score_moyen, quelle que soit leur date.

État actuel à connaître : _update_fact_table_direct refuse l'opération dès qu'une clé
primaire manque (« Primary keys missing in DataFrame », updater.py:720), tandis que
DataManager._ensure_columns_exist ajoute silencieusement toute colonne inconnue lors
d'un insert/upsert (data.py:758). Rends ce second comportement explicite au passage :
un paramètre allow_new_columns: bool = False sur le chemin d'upsert, plutôt qu'un ajout
implicite.

Spécification de la nouvelle méthode publique de DatabaseUpdater :

    add_value_columns(
        df: IntoDataFrame,
        on: list[str] | None = None,
        overwrite: bool = False,
        column_metadata: dict[str, dict[str, str]] | None = None,
        compact_after_update: bool = True,
    ) -> OperationReport            # ou -> bool si le prompt 9 n'est pas encore passé

Validations, toutes bloquantes sauf mention contraire :
1. `on` par défaut = intersection des clés primaires de metadata et des colonnes de df.
   Il doit être non vide et INCLUS dans les clés primaires ; une colonne de `on` absente
   de la fact_table ou non déclarée clé primaire → ValueError explicite.
2. df doit être UNIQUE sur `on` : sinon la valeur à diffuser est ambiguë → ValueError
   listant un échantillon des combinaisons en doublon.
3. Les colonnes de valeurs ne doivent pas déjà exister dans la fact_table, sauf
   overwrite=True. Les colonnes de df qui ne sont ni dans `on` ni des colonnes de
   valeurs sont ignorées avec un warning les listant.
4. Type SQL via map_python_to_sql_type (corrigé au prompt 1), puis
   ALTER TABLE ... ADD COLUMN <col> <type> DEFAULT NULL, puis une ligne dans metadata
   (is_primary_key = FALSE, is_categorical inféré selon le seuil, champs d'UI issus de
   column_metadata).
5. Diffusion en UN SEUL UPDATE ... FROM (jamais ligne à ligne) :
   UPDATE fact f SET c1 = t.c1, ... FROM <vue temporaire> t WHERE f.k1 = t.k1 AND ...
   Identifiants quotés et qualifiés (prompt 1).
6. Comptages journalisés ET retournés : lignes de la fact_table effectivement mises à
   jour ; lignes laissées à NULL faute de correspondance ; combinaisons de df sans
   aucune correspondance en base (warning non bloquant avec échantillon).
7. dataset_metadata.updated_at mis à jour ; cache de métadonnées invalidé.
8. Le tout dans une transaction : en cas d'échec, ni la colonne, ni la ligne de metadata
   ne subsistent.

Point de coût à traiter explicitement, pas seulement à documenter : un UPDATE touchant
toutes les lignes est chez DuckLake une réécriture complète de la table (copy-on-write).
La méthode doit donc (a) journaliser le volume qui sera touché avant d'agir, et (b)
déclencher en fin d'opération un rewrite_data_files avec un delete_threshold bas
(prompt 5), sans quoi la table conserve durablement le double de ses fichiers.

Tests pytest cas limites : `on` vide ou non inclus dans les clés primaires ; df avec
doublons sur `on` ; colonne déjà existante avec et sans overwrite ; combinaison de clés
présente dans df mais absente en base ; lignes de la base sans correspondance (doivent
rester NULL) ; `on` égal à l'ensemble complet des clés primaires (doit se comporter
comme un update de valeurs classique) ; échec en milieu d'opération (la colonne ne doit
pas subsister). Vérifie sur un cas réel que le nombre de fichiers après
rewrite_data_files ne double pas durablement. Crée un notebook d'illustration (ou
complète le notebook « Mise à jour de la base de données ») montrant la diffusion sur
une clé partielle. Conventions habituelles. Termine par uv run --no-sync pytest.
```

*Pourquoi Opus + plan mode : sémantique nouvelle (diffusion sur clé partielle) avec
plusieurs choix d'API à arbitrer — nom et forme de la méthode, comportement par défaut de
`on`, sort des lignes sans correspondance — et une opération coûteuse dont il faut valider
le séquencement avant de l'écrire.*

---

## Prompt 7 — Script de migration des catalogues v1 → v2

**Modèle : Opus · Plan mode : OUI · Dépendances : prompts 2 et 3**

```text
Lis d'abord revue-technique-bdd.md (sections 5, 8 et 12). Écris un module de
migration dt_ducklake_manager/maintenance/migration.py transformant en place un
schéma v1 (fact_table à codes + dim_* inférées + metadata avec python_type) en
schéma v2 (labels dans la fact_table, plus de dim_* inférées, metadata enrichie,
dataset_metadata). C'est une opération destructive sur des données réelles : la
prudence prime sur la performance.

Comportement de migrate_schema(conn, schema="main", dry_run=True) :
1. Détection de version : schema_version dans dataset_metadata si présente, sinon
   heuristique (présence de python_type dans metadata et de tables dim_*). Si déjà
   v2 : no-op loggé.
2. dry_run=True (défaut) : produit et retourne un rapport (dataclass MigrationReport)
   listant les colonnes à décoder, les tables dim_* à supprimer, les colonnes
   metadata à ajouter/retirer, le nombre de lignes touchées — sans rien modifier.
3. Exécution réelle : dans une transaction unique —
   a. pour chaque colonne is_categorical avec dim_<col> : remplacement des codes par
      les labels via UPDATE ... FROM dim_<col> (jointure sur value avec CAST
      explicite), en préservant les NULL ;
   b. contrôle post-décodage : aucune valeur non-NULL de la fact_table absente des
      labels de la dimension (sinon rollback et erreur listant les valeurs) ;
   c. DROP des dim_* ; reconstruction de metadata sans python_type, avec les
      colonnes v2 à NULL ; création de dataset_metadata (schema_version=2) ;
   d. compaction DuckLake en fin de migration — avec un delete_threshold explicite et
      bas (cf. prompt 5) : les UPDATE massifs de l'étape (a) réécrivent l'intégralité
      de la table, et sans seuil explicite rewrite_data_files ne fait rien.
4. Avant toute exécution réelle, logge le numéro de snapshot DuckLake courant et
   affiche la commande de retour arrière par time travel (lecture AT VERSION) — c'est
   le filet de sécurité, ne crée pas de mécanisme de backup parallèle. Corollaire à
   documenter dans la docstring : ne PAS lancer expire_snapshots avant d'avoir validé
   la migration, sous peine de détruire ce filet.
5. Migration multi-schémas : migrate_catalog(conn) itère sur tous les schémas du
   catalogue (hors schémas internes ducklake_*).

Ajoute un point d'entrée CLI (python -m dt_ducklake_manager.maintenance.migration
--catalog ... --data-path ... [--schema ...] [--apply]) où l'absence de --apply
signifie dry_run. Tests d'intégration sur un catalogue v1 de synthèse construit dans
le test (petit DataFrame avec colonnes catégorielles, dont une avec NULL et une avec
un code non mappé pour vérifier le rollback). Conventions habituelles. Termine par
uv run --no-sync pytest.
```

*Pourquoi Opus + plan mode : migration destructive, ordre des opérations et cas
d'échec critiques ; le plan doit être relu avant toute exécution.*

---

## Prompt 8 — Simplification de la machinerie transactionnelle

**Modèle : Opus · Plan mode : OUI · Dépendances : prompt 2 (idéalement après 6 et 7)**

```text
Lis d'abord revue-technique-bdd.md (section 3, points 2 et 4). Objectif :
réduire la dette technique de la couche transactionnelle en s'appuyant sur ce que
DuckDB et DuckLake fournissent nativement, sans changer l'API publique de
DatabaseUpdater ni DatabaseDeleter (signatures et sémantique de retour conservées).

Constats à vérifier puis traiter :
- TransactionManager réimplémente une gestion de transactions applicative
  (TransactionOperation, savepoints, états) au-dessus de BEGIN/COMMIT DuckDB, et la
  plupart des rollback_func passées par updater.py sont des placeholders qui ne
  restaurent rien : _rollback_dimension_changes, _rollback_fact_changes et
  _restore_database_state se contentent d'écrire une ligne de log.
- Le couple atomic.py (backups applicatifs) / recovery.py (points de restauration)
  recouvre le time travel DuckLake (snapshots + AT VERSION) déjà exposé par
  DuckLakeConnector.

Cible : _update_database_transactional et _delete_rows_transactional deviennent un
unique bloc BEGIN/COMMIT DuckDB avec ROLLBACK sur exception, en conservant l'ordre
des étapes, la validation par l'auditor et les logs ; les classes/machinerie devenues
inutiles sont supprimées avec leurs tests ; ce qui reste utile de recovery.py
(list_ducklake_snapshots, restauration par snapshot) est conservé et documenté comme
LE mécanisme de récupération. Propose dans ton plan la liste exacte des
classes/méthodes supprimées et de celles conservées, avec justification, avant
d'implémenter.

Deux points à valider dans le plan, importants pour la suite :
- ce qui reste doit offrir un point d'accroche unique par opération (entrée/sortie de
  transaction), sur lequel le prompt 9 branchera la collecte du rapport d'opération ;
- la compaction post-update s'exécute APRÈS le commit et ne doit pas être incluse dans
  la transaction.

Réécris les tests des chemins transactionnels (échec en milieu d'update → la base
revient à l'état initial, vérifié par comptage et contenu). Conventions habituelles.
Termine par uv run --no-sync pytest.
```

*Pourquoi Opus + plan mode : suppression de code transversale où le coût d'une erreur
de périmètre est élevé ; le plan sert de revue de la liste de suppression. Ce prompt
était optionnel dans la version de juillet ; il devient obligatoire car le prompt 9
s'écrit beaucoup plus simplement sur une couche transactionnelle assainie.*

---

## Prompt 9 — Journalisation explicite des transactions (NOUVEAU)

**Modèle : Sonnet · Plan mode : OUI · Dépendances : prompts 5, 6 et 8**

```text
Lis d'abord revue-technique-bdd.md (section 13, qui donne la structure cible et les
sources de données vérifiées). Objectif : rendre les logs des transactions explicites
sur ce qui s'est réellement passé — nombre de lignes et de colonnes ajoutées ou mises
à jour, changements de métadonnées, et effet réel des optimisations.

1) Dataclass OperationReport dans un nouveau module dt_ducklake_manager/reporting.py
   (champs listés à la section 13 de la revue : operation, schema, started_at,
   duration_seconds, rows_inserted/updated/deleted, rows_before/after, columns_added,
   columns_dropped, metadata_changes, snapshot_before/after, files_before/after,
   bytes_before/after, maintenance, warnings). Ajoute une méthode summary() -> str
   produisant la ligne de synthèse lisible, et to_dict() pour un usage programmatique.

2) Collecte. Toutes ces sources ont été vérifiées comme disponibles :
   - ducklake_table_info('<alias>') retourne file_count, file_size_bytes,
     delete_file_count, delete_file_size_bytes → interroger avant et après l'opération.
     Utilise cette fonction publique plutôt qu'un accès direct aux tables internes
     __ducklake_metadata_* ;
   - ducklake_snapshots('<alias>') donne le snapshot_id courant et une colonne
     `changes` résumant l'opération ;
   - ducklake_merge_adjacent_files et ducklake_rewrite_data_files retournent
     (schema_name, table_name, files_processed, files_created) — aujourd'hui ignoré ;
   - ducklake_cleanup_old_files retourne les chemins supprimés, ducklake_expire_snapshots
     les snapshots périmés.

3) Branchement. DatabaseUpdater.update_database, DatabaseUpdater.add_value_columns,
   DatabaseDeleter et DuckLakeMaintenance.full_maintenance construisent un
   OperationReport. L'API publique NE CHANGE PAS : update_database continue de
   retourner bool et expose le rapport via updater.last_report ; add_value_columns
   (prompt 6) retourne directement son rapport.

4) Contrat de journalisation :
   - une ligne INFO de synthèse par opération réussie, du type
     « update main: +1 240 lignes, ~380 mises à jour, +2 colonnes (score, rang),
       3 → 2 fichiers (48,2 → 31,7 Mo), snapshot 17 → 18, 4,1 s » ;
   - le détail par étape en DEBUG, pas en INFO ;
   - les compteurs à zéro sont EXPLICITÉS et non tus : « rewrite_data_files : 0 fichier
     réécrit (seuil de suppression non atteint) » plutôt que « terminé » ;
   - chaque warning collecté dans report.warnings est aussi journalisé au fil de l'eau ;
   - en cas d'échec, un rapport partiel est journalisé en ERROR avec l'étape atteinte.

Tests pytest : un update dont on connaît le résultat produit les bons compteurs
(lignes, colonnes, snapshots) ; une compaction sans effet est journalisée comme telle ;
summary() est stable et lisible ; un échec produit un rapport partiel. Conventions
habituelles. Termine par uv run --no-sync pytest.
```

*Pourquoi Sonnet + plan mode : l'implémentation est mécanique, mais le placement des
points de collecte (avant/après quoi, dans ou hors transaction) mérite d'être validé sur
plan — d'autant qu'il dépend de la forme laissée par le prompt 8.*

---

## Prompt 10 — Notebooks, documentation et mise à jour de la skill

**Modèle : Sonnet · Plan mode : non · Dépendances : prompts 2 à 9**

```text
Lis d'abord revue-technique-bdd.md (section 8, « spécification du schéma v2 »).
Mets en cohérence la documentation du dépôt avec le schéma v2 maintenant implémenté :

1) Notebooks : passe en revue les notebooks existants (notebooks/0 à 3) et adapte-les
   (plus de categorical_threshold pilotant le stockage, plus de dim_* inférées) ;
   assure-toi que la série couvre : construction simple, column_metadata
   (unit/display_format/family), hiérarchies cas A et cas B, label_maps, order_by et
   options DuckLake, update avec apparition de nouvelles modalités, ajout de colonnes
   de valeurs sur clé partielle, lecture d'un OperationReport, migration v1→v2 en dry
   run. Chaque notebook illustre l'effet de la paramétrisation en variant les arguments.

2) CLAUDE.md : actualise la section « Database structure » pour décrire le schéma v2
   (labels dans la fact_table, metadata enrichie, dataset_metadata, dimensions
   opt-in uniquement).

3) docs/architecture.md : décris le cycle de vie réel des fichiers Parquet
   (copy-on-write, fichiers de suppression, cycle rewrite → merge → expire → cleanup)
   en reprenant les sections 7 et 12 de la revue. C'est le point le plus souvent
   mal compris à la lecture du répertoire de données.

4) Skill : mets à jour C:\Users\bolli\.claude\skills\dashboard-api-client\SKILL.md —
   sans modifier ce qui décrit l'API GraphQL actuelle — en ajoutant une section
   « Database schema v2 (à venir côté API) » reprenant le récapitulatif de la
   section 8 de revue-technique-bdd.md : structure des tables, impacts API
   attendus (getSelectOptions par DISTINCT, dépréciation de dimensionDetails et
   getDimensionTable, nouveaux getSelectOptionsFlat/getSelectOptionsTree, nouveaux
   champs de getCatalogSchema, schema_version dans getCatalogs). Cette section
   servira de contexte pour faire évoluer l'API dans son dépôt.

5) README/docs du dépôt : si un schéma illustré existe (docs/assets/schema_bdd.png),
   signale dans le texte qu'il décrit le schéma v1 et référence la section 8 de la
   revue technique pour le v2 (ne tente pas de régénérer l'image).

Vérifie que les notebooks s'exécutent de bout en bout (uv run --no-sync jupyter
nbconvert --execute ou exécution directe). Conventions habituelles.
```

*Pourquoi Sonnet sans plan mode : travail de documentation guidé par une spec déjà
écrite.*

---

## Prompt 11 (optionnel) — Lecture multi-catalogues

**Modèle : Sonnet · Plan mode : non · Dépendances : prompts 1 et 10**

```text
Lis d'abord revue-technique-bdd.md (section 11), qui contient les résultats mesurés :
plusieurs catalogues DuckLake peuvent être attachés à une même connexion et joints
entre eux, MAIS une transaction ne peut écrire que dans un seul catalogue attaché
(limite DuckDB, message : « a single transaction can only write to a single attached
database »).

Objectif : rendre l'usage multi-catalogues explicite et sûr, EN LECTURE SEULE.

1) DuckLakeConnector.attach(conn, activate_schema: bool = True) : quand
   activate_schema=False, ne pas exécuter le USE — attacher un second catalogue ne doit
   pas voler le catalogue courant de la connexion. (Si l'étape M a déjà introduit ce
   paramètre, vérifie-le simplement.)
2) Fonction utilitaire attach_read_only_catalog(conn, connector) qui attache un
   catalogue supplémentaire en READ_ONLY sans activer son schéma, et retourne l'alias.
3) Garde-fou : lever une erreur explicite si l'on tente d'instancier un DatabaseUpdater
   ou un DatabaseDeleter sur un alias attaché en lecture seule.
4) Documentation (docs/architecture.md) de la règle d'architecture : un catalogue par
   projet, un schéma par jeu de résultats, jamais d'écriture couvrant deux catalogues
   dans une même transaction. Les copies inter-catalogues (INSERT INTO ... SELECT) sont
   possibles mais séquentielles et non atomiques : à assumer comme telles.

Tests : deux catalogues attachés, requête de jointure inter-catalogues ; vérification
qu'un manager configuré sur le catalogue A n'écrit jamais dans B quel que soit le
dernier USE ; refus d'écriture sur un alias read-only. Conventions habituelles.
Termine par uv run --no-sync pytest.

NON couvert par ce prompt, et à ne pas tenter : le partage d'une même base PostgreSQL
par plusieurs catalogues via l'option ATTACH METADATA_SCHEMA. L'option existe dans la
grammaire mais n'a pas pu être validée sur un serveur PostgreSQL réel ; à traiter
séparément après validation manuelle.
```

*Pourquoi Sonnet sans plan mode : périmètre étroit et entièrement spécifié, et sa
valeur principale est un garde-fou plus qu'une fonctionnalité.*

---

## Ordre d'exécution et jalons

| # | Prompt | Modèle | Plan mode | Après |
|---|---|---|---|---|
| M | Intégration `DuckLakeConnector` (manuelle) | — | — | — |
| 1 | Corrections préalables (types, quoting, **qualification catalogue**, index, logs) | Sonnet | non | M |
| 2 | Option B (labels) | Opus | oui | 1 |
| 3 | Métadonnées d'UI | Sonnet | non | 2 |
| 4 | Hiérarchies | Opus | oui | 3 |
| 5 | Optimisations DuckLake **(compaction corrigée)** | Sonnet | oui | 2 |
| 6 | **Colonnes de valeurs sur clé partielle** | Opus | oui | 2, 3, 5 |
| 7 | Migration v1→v2 | Opus | oui | 2, 3 |
| 8 | Simplification transactionnelle | Opus | oui | 2 (idéalement 6, 7) |
| 9 | **Journalisation explicite** | Sonnet | oui | 5, 6, 8 |
| 10 | Notebooks + skill + docs | Sonnet | non | 2–9 |
| 11 | Lecture multi-catalogues (optionnel) | Sonnet | non | 1, 10 |

Correspondance avec la numérotation de juillet : 1→1 (étendu), 2→2, 3→3, 4→4, 5→5
(corrigé), **6 nouveau**, 6→7, 8→8 (n'est plus optionnel), **9 nouveau**, 7→10,
**11 nouveau**.

Jalons de vérification entre prompts : suite de tests verte (`uv run --no-sync
pytest`), puis un commit par prompt (message `feat:`/`refactor:` conventionnel) pour
pouvoir revenir en arrière prompt par prompt. L'évolution de l'API GraphQL se fera
ensuite dans son propre dépôt, avec la section « Impacts API » de
`revue-technique-bdd.md` (§8) en contexte.
