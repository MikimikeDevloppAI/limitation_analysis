# BAG Limitation & Indication Code Analysis

Analyse des textes de limitation de la Liste des Spécialités (LS) du BAG (Office fédéral de la santé publique suisse) pour en extraire les codes d'indication, les segments multi-indication et les règles de cashback pharmaceutique.

## Objectif

La LS publie chaque mois des fichiers XML (`Preparations`) décrivant les médicaments remboursés en Suisse. Chaque médicament peut avoir des **limitations** : des textes réglementaires qui définissent les conditions de remboursement, incluant parfois :

- **Des codes d'indication** (ex: `1234.01`) rattachés à chaque pathologie
- **Des textes multi-indication** (un même médicament remboursé pour plusieurs maladies)
- **Des règles de cashback** (remboursement pharma → assureur, à ne pas confondre avec le remboursement standard assurance → patient)

Ce projet reconstruit l'historique complet (2018-2026) de ces données et les structure dans une base SQLite exploitable.

## Architecture

```
extracted/                      # ~110 XML mensuels BAG (non versionnés)
  2018/Preparations-20180101.xml
  ...
  2026/Preparations-20260101.xml
data/                           # XML compressés (versionnés)
  preparations_2018.zip
  ...
  preparations_2026.zip

extract_limitations.py          # Pipeline principal (6 phases)
build_sku_indication_db.py      # Pipeline SKU alternatif → sku_indication.db
build_sku_normalized.py         # Parsing des descriptions de packs (forme, dosage, unités)
cashback_extractor.py           # Extraction des règles de cashback (regex, NLP)
llm_segment_texts.py            # Segmentation LLM des textes pré-2023 sans headers bold
```

## Pipelines

### 1. `extract_limitations.py` → `swiss_pharma_limitations.db`

Pipeline complet en 6 phases :

| Phase | Description |
|-------|-------------|
| 1 | Création du schéma SQLite |
| 2 | Ingestion des XML Preparations (2018-2026) |
| 3 | Construction du mapping nom→code d'indication |
| 4 | Assignation rétroactive des codes + segmentation multi-indication |
| 4d | Matching par similarité (fuzzy, brand, cross-dossier) |
| 5 | Export CSV/XLSX |
| 6 | Extraction des cashbacks |

### 2. `build_sku_indication_db.py` → `sku_indication.db`

Pipeline SKU-centré qui reparse les XML indépendamment pour créer une vue par GTIN/pack :
- `sku` : un row par GTIN avec attributs normalisés
- `limitation_text` : textes uniques avec détection cashback
- `sku_indication` : liens SKU → code indication → texte avec validité temporelle
- `text_segment` : segmentation regex des textes multi-indication
- `text_segment_llm` : segmentation LLM des textes pré-2023

### 3. `llm_segment_texts.py` — Segmentation LLM

Pipeline async pour les textes de limitation pré-2023 qui n'ont pas de headers `<b>` (bold) permettant la segmentation regex. Utilise l'API Anthropic (Haiku pour textes courts, Sonnet pour textes longs) avec 100 appels parallèles pour :
- Détecter les textes multi-indication
- Nommer chaque indication
- Identifier le cashback pharma→assureur (vs remboursement standard)
- Extraire le type et la valeur du cashback

## Données source

Les fichiers XML proviennent du site officiel du BAG :
https://www.bag.admin.ch/bag/fr/home/versicherungen/krankenversicherung/krankenversicherung-leistungen-tarife/Arzneimittel.html

Les zips dans `data/` contiennent uniquement les `Preparations-YYYYMMDD.xml` (les fichiers `ItCodes` et `GL_DIFF_SB` ne sont pas inclus).

Pour décompresser :
```bash
# Extraire tous les XML dans extracted/
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

## Installation

```bash
pip install pandas anthropic
```

Pour le pipeline LLM, configurer la clé API :
```bash
# Windows
setx ANTHROPIC_API_KEY "sk-ant-..."
# Ou créer un fichier .env
echo ANTHROPIC_API_KEY=sk-ant-... > .env
```

## Usage

```bash
# Pipeline principal
python extract_limitations.py

# Pipeline SKU
python build_sku_indication_db.py

# Segmentation LLM (textes pré-2023 cashback sans segments regex)
python llm_segment_texts.py              # tous les textes restants
python llm_segment_texts.py --limit 5    # test sur 5 textes
python llm_segment_texts.py --dry-run    # voir sans exécuter
```
