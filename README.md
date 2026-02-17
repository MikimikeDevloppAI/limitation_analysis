# BAG Limitation & Indication Code Database

Analyse des textes de limitation de la Liste des Specialites (LS) du BAG (Office federal de la sante publique suisse) pour en extraire les codes d'indication, les noms d'indication et les regles de cashback pharmaceutique.

## Objectif

La LS publie chaque mois des fichiers XML (`Preparations`) decrivant les medicaments rembourses en Suisse. Chaque medicament peut avoir des **limitations** : des textes reglementaires qui definissent les conditions de remboursement, incluant parfois :

- **Des codes d'indication** (ex: `20461.07`) rattaches a chaque pathologie
- **Des noms d'indication** en gras dans le texte (ex: `<b>Melanom</b>`)
- **Des regles de cashback** (remboursement pharma -> assureur)

Ce projet reconstruit l'historique complet (2018-2026) de ces donnees et les structure dans une base SQLite exploitable (`sku_indication.db`).

## Architecture

```
Matching_indication_code/
|-- run_pipeline.py              # Orchestrateur : lance tous les steps
|-- steps/
|   |-- __init__.py
|   |-- step_01_parse_xml.py     # Parse 110 XML -> DB from scratch
|   |-- step_02_init_indications.py  # NOCODE + text_complexity
|   |-- step_03_extract_bolds.py # Extraction noms bold + fusion par dossier+nom
|   |-- step_04_cashback.py      # Detection cashback
|   |-- step_99_stats.py         # Statistiques finales
|-- build_sku_normalized.py      # Utilitaire : parsing descriptions packs
|-- cashback_extractor.py        # Utilitaire : detection cashback
|-- extracted/                   # Donnees XML BAG (2018-2026, ~110 fichiers)
|-- data/                        # XML comprimes (versionnes dans git)
```

## Usage

```bash
# Lancer tout le pipeline (recree la DB from scratch)
python run_pipeline.py

# Lancer un step seul (la DB doit deja exister pour les steps 2+)
python run_pipeline.py --step 3

# Reprendre a partir d'un step
python run_pipeline.py --from 2
```

## Pipeline (5 steps)

### Step 01 : Parse XML (`step_01_parse_xml.py`)

Parse les ~110 fichiers `Preparations-YYYYMMDD.xml` et cree la DB from scratch.

**Entree :** fichiers XML dans `extracted/YYYY/`
**Sortie :** toutes les tables de base peuplees

Actions :
1. Cree la DB et le schema (DROP + CREATE)
2. Parse chaque XML : preparations, packs, limitations, companies
3. Deduplique les textes de limitation par hash MD5
4. Extrait les codes d'indication structures (post-fev 2023 uniquement)
5. Cree les indications avec `bag_dossier_no` (extrait du code : `20461.07` -> `20461`)
6. Fan-out : propage les liens preparation-limitation vers les SKUs individuels
7. Resout les dates (extract_id -> date humaine)

### Step 02 : Initialize Indications (`step_02_init_indications.py`)

Assure que chaque limitation a au moins un lien vers une indication.

Actions :
1. Cree 1 indication NOCODE (code=NULL, nom=NULL) pour les textes sans code
2. Insere un lien `limitation_indication` NOCODE pour chaque limitation pre-2023 sans code XML
3. Flagge les textes avec >1 code XML comme `XML_MULTI_CODE`
4. Verifie qu'aucune limitation n'est orpheline (uncovered = 0)

### Step 03 : Extract Bolds (`step_03_extract_bolds.py`)

Extrait les noms d'indication depuis les en-tetes bold et les codes embarques dans le texte.

**Logique de classification :**

| Cas | text_complexity | Action |
|-----|----------------|--------|
| XML + 1 bold | SIMPLE | Met a jour le nom de l'indication existante |
| XML + 0 bold | SIMPLE | Rien a ajouter (le code existe deja) |
| XML multi-code | XML_MULTI_CODE | Deja flagge (step 02) |
| Pre-2023 : 1 bold + 1 code | SIMPLE | Cree/fusionne indication avec code+nom |
| Pre-2023 : 1 bold, 0 code | SIMPLE | Cree/fusionne indication avec nom seul |
| Pre-2023 : 0 bold + 1 code | SIMPLE | Cree indication avec code seul |
| Pre-2023 : >1 bold | MULTI_BOLD | Flagge pour traitement futur |
| Pre-2023 : >1 code | MULTI_CODE | Flagge pour traitement futur |
| Pre-2023 : 0 bold + 0 code | NONE | Rien a extraire |

**Logique de fusion :**
Si deux textes differents ont le meme `bag_dossier_no` et le meme `indication_name_fr` normalise, ils pointent vers la meme ligne `indication`. Cela evite les doublons quand le meme medicament a plusieurs textes au fil des annees.

**Extraction des bolds :**
- Regex : `(?:^|<br/?>\s*(?:<br/?>\s*)*) (<b>.+?</b>)` (bold apres un saut de ligne)
- Les noms structurels sont filtres : UND, ODER, numeros, "Vor Therapiebeginn", etc.
- Les codes embarques en fin de nom sont supprimes : `"Melanom 20461.07"` -> `"Melanom"`

**Extraction des codes :**
- 7 patterns regex couvrent les 3 langues (DE/FR/IT)
- Exemples : `Indikationscode: XXXXX.XX`, `code d'indication suivant: XXXXX.XX`

### Step 04 : Cashback (`step_04_cashback.py`)

Detecte les clauses de cashback (remboursement pharma -> assureur) dans les textes francais.

Actions :
1. Ajoute les colonnes cashback sur `limitation_text` si absentes
2. Charge les donnees de reference (noms de companies, preparations)
3. Applique 8+ patterns regex + fuzzy matching sur `description_fr`
4. Extrait : company, type de calcul (%, fixe), valeur, unite

### Step 99 : Statistics (`step_99_stats.py`)

Affiche les statistiques completes de la DB :
- Nombre de rows par table
- Decomposition de la table `indication` (code+nom / code seul / nom seul / NOCODE)
- `limitation_indication` par `code_source`
- Distribution `text_complexity`
- Cashback : nombre de textes, couverture par code
- Verification : 0 limitations orphelines

## Schema de la base de donnees (`sku_indication.db`)

### `extract_info`
Metadonnees des fichiers XML source.

| Colonne | Type | Description |
|---------|------|-------------|
| extract_id | INTEGER PK | Numero sequentiel (1 = plus ancien) |
| file_name | TEXT | Nom du fichier XML |
| release_date | TEXT | Date de publication BAG |
| file_year | INTEGER | Annee du repertoire |

### `sku`
Un row par GTIN (code-barres unique d'un pack).

| Colonne | Type | Description |
|---------|------|-------------|
| sku_id | INTEGER PK | |
| gtin | TEXT UNIQUE | Code-barres EAN-13 |
| swissmedic_no8 | TEXT | Numero Swissmedic 8 chiffres |
| swissmedic_no5 | TEXT | Numero Swissmedic 5 chiffres (= preparation) |
| bag_dossier_no | TEXT | Numero de dossier BAG |
| preparation_id | INTEGER | ID interne de la preparation |
| product_name | TEXT | Nom du produit |
| atc_code | TEXT | Code ATC (classification anatomique) |
| description_de | TEXT | Description du pack en allemand |
| form_type | TEXT | Forme galenique normalisee (Tabletten, Kapseln...) |
| substance_name | TEXT | Nom de la substance active |
| substance_qty | REAL | Quantite de substance par unite |
| substance_unit | TEXT | Unite de la substance (mg, ml...) |
| total_units | INTEGER | Nombre total d'unites dans le pack |
| total_substance | REAL | Quantite totale de substance (qty * units) |
| public_price | REAL | Prix public |
| exfactory_price | REAL | Prix ex-factory |
| valid_from/to | TEXT | Periode de validite (dates) |

### `limitation_text`
Textes de limitation uniques, dedupliques par hash MD5.

| Colonne | Type | Description |
|---------|------|-------------|
| text_id | INTEGER PK | |
| content_hash | TEXT UNIQUE | MD5 de la concatenation DE+FR+IT |
| description_de/fr/it | TEXT | Texte dans les 3 langues |
| text_complexity | TEXT | SIMPLE, MULTI_BOLD, MULTI_CODE, NONE, XML_MULTI_CODE |
| is_cashback | INTEGER | 1 si cashback detecte |
| cashback_company | TEXT | Nom de la societe pharma |
| cashback_calc_type | TEXT | Type de calcul (%, fixe) |
| cashback_calc_value | REAL | Valeur du cashback |
| cashback_unit | TEXT | Unite (CHF, %) |

### `limitation`
Un row par (limitation_code, text_version) contigu dans le temps.

| Colonne | Type | Description |
|---------|------|-------------|
| limitation_id | INTEGER PK | |
| limitation_code | TEXT | Code BAG de la limitation |
| limitation_type | TEXT | Type (L, P...) |
| limitation_niveau | TEXT | Niveau (P, SL...) |
| text_id | INTEGER FK | Texte de limitation (-> limitation_text) |
| valid_from/to | TEXT | Periode de validite |

### `indication`
Table maitre des indications. **1 row par indication unique**, identifiee par son code (si disponible) ou par (dossier + nom).

| Colonne | Type | Description |
|---------|------|-------------|
| indication_id | INTEGER PK | |
| indication_code | TEXT UNIQUE | Code d'indication (ex: `20461.07`), NULL si inconnu |
| bag_dossier_no | TEXT | Numero de dossier BAG (extrait du code ou du SKU) |
| indication_name_de/fr/it | TEXT | Nom de l'indication dans les 3 langues |
| name_source | TEXT | Provenance du nom (BOLD_HEADER) |

### `limitation_indication`
Lie une limitation a une indication. Chaque limitation a au moins 1 lien.

| Colonne | Type | Description |
|---------|------|-------------|
| li_id | INTEGER PK | |
| limitation_id | INTEGER FK | -> limitation |
| indication_id | INTEGER FK | -> indication |
| code_source | TEXT | Provenance du code |
| valid_from/to | TEXT | Periode de validite |
| text_id | INTEGER | Texte source |

**code_source values :**

| Valeur | Description |
|--------|-------------|
| STRUCTURED_XML | Code du tag `<IndicationsCodes>` dans le XML (post-fev 2023) |
| TEXT_EMBEDDED | Code trouve dans le texte via regex (Indikationscode: XXXXX.XX) |
| BOLD_HEADER | Indication trouvee par nom bold, sans code |
| NOCODE | Aucun code ni nom trouve (placeholder) |

### `sku_limitation`
Lie un SKU a une limitation avec validite temporelle.

| Colonne | Type | Description |
|---------|------|-------------|
| link_id | INTEGER PK | |
| gtin | TEXT FK | -> sku |
| limitation_id | INTEGER FK | -> limitation |
| limitation_level | TEXT | PACK ou PREPARATION |
| valid_from/to | TEXT | Periode de validite |

### `company`
Societes (partners) extraites des XML.

| Colonne | Type | Description |
|---------|------|-------------|
| company_id | INTEGER PK | |
| company_name | TEXT UNIQUE | Nom de la societe |
| partner_type | TEXT | Type de partenaire |
| street, zip_code, place | TEXT | Adresse |

### `preparation_company`
Lie une preparation a une company.

### Vue `v_sku_limitation`
Vue denormalisee joignant sku + limitation + indication pour requetes faciles.

## Donnees source

Les fichiers XML proviennent du site officiel du BAG :
https://www.bag.admin.ch/bag/fr/home/versicherungen/krankenversicherung/krankenversicherung-leistungen-tarife/Arzneimittel.html

Pour decompresser les archives dans `data/` :
```bash
python -c "
import zipfile, os
for zf in sorted(os.listdir('data')):
    if not zf.endswith('.zip'): continue
    year = zf.split('_')[1].split('.')[0]
    os.makedirs(f'extracted/{year}', exist_ok=True)
    zipfile.ZipFile(f'data/{zf}').extractall(f'extracted/{year}')
    print(f'{zf} -> extracted/{year}/')
"
```

## Utilitaires

### `build_sku_normalized.py`
Parse les descriptions de packs allemandes (ex: `"Filmtabl 100mg 30 Stk"`) en attributs structures : forme galenique, nombre d'unites, dosage, etc. Utilise par step_01.

### `cashback_extractor.py`
Detecte les clauses de cashback dans les textes francais via 8+ patterns regex et fuzzy matching des noms de societes. Autonome et reutilisable. Utilise par step_04.
