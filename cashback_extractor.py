#!/usr/bin/env python3
"""
Extracteur autonome de règles cashback pour textes de limitation pharmaceutique.
Compatible avec n'importe quelle base SQLite contenant des textes de limitation.

Usage:
    python cashback_extractor.py --db ma_base.db                    # Analyse seule
    python cashback_extractor.py --db ma_base.db --apply            # Appliquer
    python cashback_extractor.py --db ma_base.db --export-csv out.csv
    python cashback_extractor.py --db ma_base.db --verbose --limit 10

Ce script est 100% autonome et ne dépend d'aucun autre module du projet.
Supporte le fuzzy matching avec les noms de sociétés, préparations et substances
chargés dynamiquement depuis la base de données.
"""

import sqlite3
import argparse
import re
import json
import csv
import sys
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Set
from difflib import SequenceMatcher

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')


# ============================================================================
# SECTION 1: CONFIGURATION ET CONSTANTES
# ============================================================================

# 1.1 Faux positifs (patterns à exclure - ces patterns indiquent un remboursement
# par l'ASSURANCE, pas par le FABRICANT)
FALSE_POSITIVE_PATTERNS = [
    r'garantie\s+de\s+remboursement',
    r'garantie\s+écrite\s+de\s+remboursement',
    r'cycles?\s+à\s+rembourser',
    r'durée\s+(?:maximale\s+)?de\s+remboursement',
    r'nombre\s+maximal\s+de\s+cycles\s+à\s+rembourser',
    r'exclus?\s+du\s+remboursement',
    r'remboursement\s+de\s+\w+,?\s+et\s+ceci',
    r'obtenir\s+le\s+remboursement\s+de',
    r"[A-Z][a-z]+\s+est\s+remboursé\s+jusqu",
    r"est\s+remboursé\s+jusqu['\u2019]à\s+(?:la\s+)?progression",
]

# 1.2 Patterns de détection cashback (le FABRICANT rembourse)
COMPANY_REMBOURSE_PATTERN = r'''
    (?:la\s+)?
    (?:société|entreprise|firme)?\s*
    ([A-Z][A-Za-zÀ-ÿ\-]+
    (?:\s+[A-Za-zÀ-ÿ\-\(\)&]+)*
    \s+(?:SA|AG|Sàrl|GmbH|Ltd|Suisse|Switzerland|Schweiz)
    (?:\s+[A-Za-zÀ-ÿ\-\(\)]+)?)
    \s+rembourse(?:ra)?
'''

REMBOURSE_PAR_COMPANY_PATTERN = r'''
    remboursé(?:s|e|es)?\s+par\s+
    (?:la\s+)?
    (?:société|entreprise|firme)?\s*
    ([A-Z][A-Za-zÀ-ÿ\-]+
    (?:\s+[A-Za-zÀ-ÿ\-\(\)&]+)*
    (?:\s+(?:SA|AG|Sàrl|GmbH|Ltd|Suisse|Switzerland|Schweiz))
    (?:\s+[A-Za-zÀ-ÿ\-\(\)]+)?)
'''

ASSUREUR_FACTURE_PATTERN = r'''
    (?:l['\s])?assureur\s+facture\s+à\s+
    (?:la\s+)?
    (?:société|entreprise|firme)?\s*
    (
        [A-Z][A-Za-zÀ-ÿ\-]+
        (?:\s+[A-Za-zÀ-ÿ\-\(\)&]+)*
        \s+(?:SA|AG|Sàrl|GmbH|Ltd|Suisse|Switzerland|Schweiz)
    )
    (?=\s+le\s|\s+un|\s+la\s|\s+les\s|\s*$|\s*\.)
'''

TITULAIRE_REMBOURSE_PATTERN = r'''
    (?:le\s+)?titulaire\s+
    (?:de\s+l['\u2019\s]autorisation[\s,]+)?
    (?:de\s+[A-Z][A-Za-z0-9\-]+\s+)?
    (?:[^,\n]{1,50},\s+)?
    (?:rembourse(?:ra)?|restitue)
'''

REMBOURSE_ASSURANCE_MALADIE_PATTERN = r'''
    rembourse(?:nt|ra|ront)?\s+
    (?:à\s+)?
    l['\u2019]assur(?:ance|eur)[\s\-]?maladie
'''

# 1.3 Nombres en lettres (français) pour extraction de seuils
NUMBERS_IN_WORDS = {
    'un': 1, 'une': 1, 'deux': 2, 'trois': 3, 'quatre': 4, 'cinq': 5,
    'six': 6, 'sept': 7, 'huit': 8, 'neuf': 9, 'dix': 10,
    'onze': 11, 'douze': 12, 'treize': 13, 'quatorze': 14, 'quinze': 15,
    'seize': 16, 'dix-sept': 17, 'dix-huit': 18, 'dix-neuf': 19, 'vingt': 20,
    'vingt-et-un': 21, 'vingt-deux': 22, 'vingt-quatre': 24,
    'trente': 30, 'quarante': 40, 'cinquante': 50, 'soixante': 60,
    # Ordinaux
    'premier': 1, 'première': 1, '1er': 1, '1ère': 1,
    'deuxième': 2, 'second': 2, 'seconde': 2, '2e': 2, '2ème': 2,
    'troisième': 3, '3e': 3, '3ème': 3,
    'quatrième': 4, '4e': 4, '4ème': 4,
    'cinquième': 5, '5e': 5, '5ème': 5,
    'sixième': 6, '6e': 6, '6ème': 6,
    'septième': 7, '7e': 7, '7ème': 7,
    'huitième': 8, '8e': 8, '8ème': 8,
    'neuvième': 9, '9e': 9, '9ème': 9,
    'dixième': 10, '10e': 10, '10ème': 10,
    'onzième': 11, '11e': 11, '11ème': 11,
    'douzième': 12, '12e': 12, '12ème': 12,
}

# Pattern pour nombres (chiffres ou lettres)
NUMBER_WORDS_PATTERN = '|'.join(re.escape(w) for w in sorted(NUMBERS_IN_WORDS.keys(), key=len, reverse=True))
NUMBER_OR_WORD = rf'(\d+|{NUMBER_WORDS_PATTERN})'
ORDINAL_SUFFIX = r'(?:e|[èé]me|ème|eme)?'

# 1.4 Patterns de calcul
CALCULATION_PATTERNS = [
    (r'(\d+(?:[.,]\d+)?)\s*%', 'percentage', 'value'),
    (r'(\d+(?:[.,]\d+)?)\s*(?:CHF|francs?)', 'chf_fixed', 'value'),
    (r'(?:CHF|francs?)\s*(\d+(?:[.,]\d+)?)', 'chf_fixed', 'value'),
    (r'Fr\.\s*(\d+(?:[.,]\d+)?)', 'chf_fixed', 'value'),
    (r'(\d+(?:[.,]\d+)?)\s*Fr\.', 'chf_fixed', 'value'),
    (r'(\d+(?:[.,]\d+)?)\s*(?:CHF|francs?)\s*/?\s*(?:par\s+)?mg', 'per_mg', 'value'),
    (r'(\d+(?:[.,]\d+)?)\s*(?:centimes?|cts?)\s*/?\s*(?:par\s+)?mg', 'per_mg_centimes', 'value'),
    (r'co[ûu]ts?\s+(?:de\s+)?(?:la\s+)?totalit[ée]\s+(?:de\s+)?l[\'\u2019]emballage', 'full_refund', None),
    (r'co[ûu]ts?\s+(?:de\s+)?l[\'\u2019]emballage\s+complet', 'full_refund', None),
    (r'prix\s+(?:d[\'\u2019]?[ée]part\s+)?(?:usine|fabrique|PEX|PEXF)', 'full_refund_pex', None),
    (r'[àa]\s+partir\s+d[ue]\s+(\d+)e?\s+(?:paquet|emballage|bo[îi]te)', 'threshold_box', 'value'),
    (r'rembourse(?:ra)?(?:\s+[àa]\s+[^0-9]*?)?\s+(\d+)(?:\s*\.|\s*,|\s+La)', 'amount_number_only', 'value'),
    (r'taux\s+de\s+(\d+(?:[.,]\d+)?)', 'percentage_implicit', 'value'),
    (r'co[ûu]ts?\s+correspondant', 'cost_refund', None),
    (r'(?:part|partie|montant|pourcentage)\s+(?:non\s+)?(?:divulgu[ée]e?|communiqu[ée]e?|publi[ée]e?)', 'undisclosed', None),
    (r'(?:convenu|n[ée]goci[ée])\s+(?:avec|entre)', 'undisclosed', None),
    (r'selon\s+(?:accord|convention|contrat)', 'undisclosed', None),
    (r'(?:partie|part)\s+fixe\s+(?:du\s+)?prix', 'undisclosed_fixed', None),
    (r'rembourse(?:ra)?[^.]*?(?:Fr\.|montant)', 'amount_unspecified', None),
]

# 1.5 Patterns d'unités (enrichis pour capturer "pour chaque boîte" - CAS STANDARD)
UNIT_PATTERNS = [
    # Standard "pour chaque boîte" - CAS LE PLUS COURANT
    (r'pour\s+chaque\s+(?:emballage|bo[îi]te)', 'per_box'),
    (r'pour\s+chaque\s+(?:emballage|bo[îi]te)\s+(?:de\s+)?[A-Z]', 'per_box'),
    (r'chaque\s+(?:emballage|bo[îi]te)\s+(?:de\s+|achet[ée])', 'per_box'),
    (r'par\s+(?:emballage|bo[îi]te)(?:\s+ou)?', 'per_box'),
    (r'par\s+(?:emballage|bo[îi]te)\s+(?:resp\.?|respectivement)', 'per_box'),
    # Flacons
    (r'(?:pour\s+chaque|par|chaque)\s+flacon', 'per_flacon'),
    # Seringues/Stylos
    (r'(?:pour\s+chaque|par|chaque)\s+seringue', 'per_syringe'),
    (r'(?:pour\s+chaque|par|chaque)\s+stylo', 'per_pen'),
    (r'(?:pour\s+chaque|par|chaque)\s+(?:auto[\-]?injecteur|pen)', 'per_pen'),
    (r'(?:pour\s+chaque|par|chaque)\s+solution', 'per_syringe'),
    # Doses/Injections
    (r'(?:pour\s+chaque|par|chaque)\s+(?:dose|injection|administration)', 'per_dose'),
    # Par mg (oncologie)
    (r'(?:par|pour\s+chaque)\s+mg', 'per_mg'),
    (r'/\s*mg\b', 'per_mg'),
    (r'par\s+milligramme', 'per_mg'),
    (r'Fr\.\s*[\d\',]+\s*(?:par\s+)?mg', 'per_mg'),
    # Cycles
    (r'(?:pour\s+chaque|par|chaque)\s+cycle', 'per_cycle'),
    (r'par\s+cycle\s+(?:combiné|de\s+traitement)?', 'per_cycle'),
    # Mois
    (r'(?:par|pour\s+chaque)\s+mois', 'per_month'),
    (r'mensuel(?:lement)?', 'per_month'),
    # Patient
    (r'(?:pour\s+chaque|par)\s+patient', 'per_patient'),
    # Année
    (r'par\s+an(?:née)?(?:\s+civile)?', 'per_year'),
    # Traitement
    (r'(?:par|pour\s+chaque)\s+traitement', 'per_treatment'),
    # Semaine
    (r'(?:par|pour\s+chaque)\s+semaine', 'per_week'),
    # Paquet (variante de boîte)
    (r'(?:pour\s+chaque|par|chaque)\s+paquet', 'per_box'),
]

# 1.6 Patterns de seuils (déclencheurs de remboursement)
THRESHOLD_PATTERNS = [
    # À partir du Xe
    (rf'[àa]\s+partir\s+d[ue]\s+{NUMBER_OR_WORD}{ORDINAL_SUFFIX}\s+(?:paquet|emballage|bo[îi]te|flacon)', 'from_box', 'value'),
    (rf'[àa]\s+partir\s+d[ue]\s+{NUMBER_OR_WORD}{ORDINAL_SUFFIX}\s+cycle', 'from_cycle', 'value'),
    (rf'[àa]\s+partir\s+d[ue]\s+{NUMBER_OR_WORD}{ORDINAL_SUFFIX}\s+mois', 'from_month', 'value'),
    (rf'[àa]\s+partir\s+d[ue]\s+{NUMBER_OR_WORD}{ORDINAL_SUFFIX}\s+semaines?', 'from_weeks', 'value'),
    # Après X durée
    (rf'apr[èe]s\s+{NUMBER_OR_WORD}\s+mois', 'after_months', 'value'),
    (rf'apr[èe]s\s+{NUMBER_OR_WORD}\s+semaines?', 'after_weeks', 'value'),
    (rf'apr[èe]s\s+{NUMBER_OR_WORD}\s+jours?', 'after_days', 'value'),
    (rf'apr[èe]s\s+{NUMBER_OR_WORD}\s+cycles?', 'after_cycles', 'value'),
    # Pendant X durée
    (rf'(?:pendant|durant)\s+{NUMBER_OR_WORD}\s+mois', 'during_months', 'value'),
    (rf'(?:pendant|durant)\s+{NUMBER_OR_WORD}\s+semaines?', 'during_weeks', 'value'),
    # Durée de thérapie
    (rf'dur[ée]e\s+(?:de\s+)?(?:th[ée]rapie|traitement)\s+(?:de\s+)?{NUMBER_OR_WORD}\s+mois', 'therapy_duration_months', 'value'),
    (rf'dur[ée]e\s+(?:de\s+)?(?:th[ée]rapie|traitement)\s+(?:de\s+)?{NUMBER_OR_WORD}\s+semaines?', 'therapy_duration_weeks', 'value'),
    # Au-delà de
    (rf'au[- ]?del[àa]\s+(?:de\s+)?{NUMBER_OR_WORD}\s+(?:mois|semaines?|jours?|cycles?)', 'beyond', 'value'),
    # Supérieur/plus de
    (rf'sup[ée]rieur[es]?\s+[àa]\s+{NUMBER_OR_WORD}\s+(?:mois|semaines?|jours?|cycles?)', 'exceeding', 'value'),
    (rf'plus\s+de\s+{NUMBER_OR_WORD}\s+(?:mois|semaines?|jours?|cycles?)', 'more_than', 'value'),
    # Au moins X
    (rf'au\s+moins\s+{NUMBER_OR_WORD}\s+cycles?', 'min_cycles', 'value'),
    (rf'au\s+moins\s+{NUMBER_OR_WORD}\s+mois', 'min_months', 'value'),
    # Dès le Xe
    (rf'd[èe]s\s+(?:le\s+)?{NUMBER_OR_WORD}{ORDINAL_SUFFIX}\s+(?:paquet|emballage|bo[îi]te|cycle|mois|flacon)', 'from_nth', 'value'),
    # Période de X jours
    (rf'p[ée]riode\s+de\s+{NUMBER_OR_WORD}\s+jours?', 'period_days', 'value'),
    # X jours suivant
    (rf'{NUMBER_OR_WORD}\s+jours?\s+(?:suivant|pr[ée]c[ée]dant|apr[èe]s)', 'days_after', 'value'),
    (rf'(?:au\s+cours\s+des|dans\s+les)\s+{NUMBER_OR_WORD}\s+jours?', 'within_days', 'value'),
    # Ne se poursuit pas au-delà
    (rf'ne\s+se\s+poursuit\s+pas\s+au[- ]?del[àa]\s+(?:de[s]?\s+)?{NUMBER_OR_WORD}\s+(?:semaines?|mois|jours?)', 'not_beyond', 'value'),
    # Traitement de X
    (rf'(?:traitement|th[ée]rapie|cure)\s+de\s+{NUMBER_OR_WORD}\s+(?:semaines?|mois|jours?|cycles?)', 'treatment_of_duration', 'value'),
    # Par an
    (r'par\s+an(?:n[ée]e)?(?:\s+civile)?', 'per_year', None),
    (rf'{NUMBER_OR_WORD}\s+(?:flacons?|emballages?|bo[îi]tes?)\s+par\s+(?:patient\s+(?:et\s+)?)?(?:par\s+)?an', 'annual_limit', 'value'),
    # Maximum
    (rf'max(?:imum|imal|\.?)?\s*(?:de\s+)?{NUMBER_OR_WORD}\s+(?:flacons?|emballages?|bo[îi]tes?|cycles?|mois|semaines?)', 'maximum', 'value'),
    # Plafond
    (rf'plafond\s+(?:de\s+)?{NUMBER_OR_WORD}', 'ceiling', 'value'),
    # > X cycles
    (rf'>\s*{NUMBER_OR_WORD}\s+(?:cycles?|mois|semaines?|jours?)', 'greater_than', 'value'),
    # Jusqu'à X
    (rf"jusqu['\u2019]?[àa]\s+{NUMBER_OR_WORD}\s+(?:emballages?|bo[îi]tes?|flacons?|cycles?|mois)", 'up_to', 'value'),
    # Premiers X mois
    (rf'(?:les\s+)?(?:premiers?|premi[èe]res?)\s+{NUMBER_OR_WORD}\s+(?:mois|semaines?|jours?)', 'first_n', 'value'),
    (rf'(?:au\s+cours\s+des?|dans\s+les?)\s+{NUMBER_OR_WORD}\s+(?:premiers?|premi[èe]res?)?\s*(?:mois|semaines?|jours?)', 'within_first', 'value'),
    # X semaines suivant le début
    (rf'{NUMBER_OR_WORD}\s+(?:semaines?|mois|jours?)\s+suivant\s+(?:le\s+)?d[ée]but', 'following_start', 'value'),
    # Forfait unique
    (r'(?:montant\s+)?forfaitaire\s+unique', 'flat_fee_unique', None),
    (r'forfait\s+unique', 'flat_fee_unique', None),
    # Nombre de cycles
    (rf'{NUMBER_OR_WORD}\s+cycles?\s+(?:de\s+)?(?:traitement|th[ée]rapie|chimioth[ée]rapie)', 'n_cycles', 'value'),
    (rf'(?:apr[èe]s|au[- ]del[àa]\s+de)\s+{NUMBER_OR_WORD}\s+administrations?', 'after_administrations', 'value'),
    # Patterns additionnels pour seuils
    # Nombre de boîtes/flacons total
    (rf'{NUMBER_OR_WORD}\s+(?:emballages?|bo[îi]tes?|flacons?)\s+(?:au\s+total|en\s+tout)', 'total_boxes', 'value'),
    # X premiers mois gratuits
    (rf'(?:les\s+)?{NUMBER_OR_WORD}\s+premiers?\s+(?:mois|semaines?|jours?)\s+(?:gratuits?|offerts?|rembours[ée]s?)', 'first_free', 'value'),
    # Dose supérieure à
    (rf'dose\s+sup[ée]rieure?\s+[àa]\s+{NUMBER_OR_WORD}', 'dose_above', 'value'),
    # Si besoin de plus de X
    (rf'(?:si|lorsque)\s+(?:le\s+)?(?:patient\s+)?(?:a\s+)?besoin\s+(?:de\s+)?(?:plus\s+de\s+)?{NUMBER_OR_WORD}', 'need_above', 'value'),
    # À partir de X mg/jour ou X g/jour
    (rf'[àa]\s+partir\s+de\s+{NUMBER_OR_WORD}(?:[.,]\d+)?\s*(?:mg|g|ml)\s*(?:/|\s+par\s+)(?:jour|j)', 'from_daily_dose', 'value'),
    # Après échec de X lignes
    (rf'apr[èe]s\s+(?:[ée]chec\s+(?:de\s+)?)?{NUMBER_OR_WORD}\s+(?:ligne|lignes)\s+(?:de\s+)?(?:traitement|th[ée]rapie)?', 'after_lines', 'value'),
    # En cas de progression
    (r'en\s+cas\s+de\s+progression', 'on_progression', None),
    # Rechute/récidive
    (r'(?:en\s+cas\s+de\s+)?(?:rechute|r[ée]cidive)', 'on_relapse', None),
    # X flacons par patient par an
    (rf'{NUMBER_OR_WORD}\s+(?:flacons?|emballages?)\s+par\s+patient\s+(?:et\s+)?par\s+an', 'annual_per_patient', 'value'),
    # Après X jours de traitement
    (rf'apr[èe]s\s+{NUMBER_OR_WORD}\s+jours?\s+de\s+traitement', 'after_treatment_days', 'value'),
]

# 1.7 Patterns d'exclusion (délais de demande de remboursement - à ignorer)
EXCLUSION_PATTERNS_REQUEST_DEADLINE = [
    r'demande\s+de\s+remboursement',
    r'demande\s+doit\s+(?:être\s+)?(?:effectu[ée]e|faite|soumise|envoy[ée]e|introduite)',
    r'doit\s+(?:être\s+)?(?:effectu[ée]e|faite|soumise|envoy[ée]e|introduite)\s+(?:dans|au\s+cours)',
    r'doit\s+intervenir\s+(?:dans|au\s+cours)',
    r'(?:effectu[ée]e|faite|soumise)\s+dans\s+(?:les|un\s+d[ée]lai)',
    r'd[ée]lai\s+de\s+(?:demande|soumission|envoi)',
    r'formuler\s+la\s+demande',
    r'demande\s+formul[ée]e',
    r'(?:demande|requ[êe]te)\s+adress[ée]e',
    r'(?:dans|au\s+cours\s+des?)\s+(?:\d+|premier|premi[èe]re).*?(?:suivant|apr[èe]s).*?(?:d[ée]but|prescription|initiation)',
]

# 1.8 Patterns de conditions
CONDITION_PATTERNS = {
    'treatment_stop': [
        r'arr[êe]t\s+(?:du\s+|de\s+(?:la\s+)?)?(?:traitement|th[ée]rapie)',
        r'(?:traitement|th[ée]rapie)\s+(?:est\s+)?arr[êe]t[ée]',
        r'interr(?:ompu|uption)\s+(?:du\s+|de\s+(?:la\s+)?)?(?:traitement|th[ée]rapie)',
        r'cessation\s+(?:du\s+|de\s+(?:la\s+)?)?(?:traitement|th[ée]rapie)',
        r'fin\s+(?:du\s+|de\s+(?:la\s+)?)?(?:traitement|th[ée]rapie)',
    ],
    'adverse_effects': [
        r'effets?\s+(?:ind[ée]sirables?|secondaires?|n[ée]fastes?)',
        r'toxicit[ée]s?',
        r'intol[ée]rance',
        r'r[ée]actions?\s+(?:ind[ée]sirables?|adverses?)',
        r'(?:événement|ev[ée]nement)s?\s+(?:ind[ée]sirables?|adverses?)',
        r'EI\b',
    ],
    'treatment_failure': [
        r'[ée]chec\s+(?:du\s+|de\s+(?:la\s+)?)?(?:traitement|th[ée]rapie)',
        r'(?:non[- ]?)?r[ée]ponse',
        r'progression\s+(?:de\s+la\s+)?(?:maladie|tumeur)?',
        r'r[ée]cidive',
        r'rechute',
    ],
}

# 1.9 Médicaments connus pour co-traitements (enrichi)
KNOWN_DRUGS = [
    # Immunothérapies (PD-1/PD-L1/CTLA-4)
    'KEYTRUDA', 'OPDIVO', 'TECENTRIQ', 'IMFINZI', 'YERVOY', 'BAVENCIO', 'LIBTAYO',
    # Anti-HER2
    'HERCEPTIN', 'PERJETA', 'ENHERTU', 'KADCYLA', 'PHESGO',
    # Anti-VEGF
    'AVASTIN', 'CYRAMZA', 'ZALTRAP',
    # Oncologie ciblée
    'TRODELVY', 'IBRANCE', 'KISQALI', 'VERZENIOS', 'LYNPARZA', 'ZEJULA', 'RUBRACA',
    'TAGRISSO', 'ALECENSA', 'LORBRENA', 'XALKORI', 'TAFINLAR', 'MEKINIST',
    'ZELBORAF', 'COTELLIC', 'BRAFTOVI', 'MEKTOVI', 'VITRAKVI', 'ROZLYTREK',
    'CABOMETYX', 'COMETRIQ', 'LENVIMA', 'NEXAVAR', 'STIVARGA', 'VOTRIENT',
    'SUTENT', 'INLYTA', 'TARCEVA', 'IRESSA', 'GILOTRIF', 'VIZIMPRO',
    'ALUNBRIG', 'GAVRETO', 'RETEVMO', 'TABRECTA', 'TEPMETKO', 'EXKIVITY',
    'LUMAKRAS', 'KRAZATI',
    # Hématologie
    'JAKAVI', 'IMBRUVICA', 'CALQUENCE', 'BRUKINSA', 'VENCLEXTA',
    'GAZYVA', 'RITUXAN', 'MABTHERA', 'DARZALEX', 'SARCLISA', 'EMPLICITI',
    'KYPROLIS', 'NINLARO', 'VELCADE', 'REVLIMID', 'POMALYST', 'BLINCYTO',
    # Chimiothérapies classiques (souvent en association)
    'CARBOPLATINE', 'CARBOPLATIN', 'PACLITAXEL', 'DOCETAXEL', 'GEMCITABINE',
    'PEMETREXED', 'ALIMTA', 'CISPLATINE', 'OXALIPLATINE', 'IRINOTECAN',
    'FLUOROURACILE', 'CAPECITABINE', 'XELODA', 'CYCLOPHOSPHAMIDE',
    # Corticoïdes et immunomodulateurs
    'DEXAMETHASONE', 'LENALIDOMIDE', 'POMALIDOMIDE', 'THALIDOMIDE',
    # Autres médicaments fréquents en association
    'ELOTUZUMAB', 'IXAZOMIB', 'CARFILZOMIB', 'BORTEZOMIB', 'DARATUMUMAB',
    'ISATUXIMAB', 'BELANTAMAB',
    # Maladies rares
    'SOLIRIS', 'ULTOMIRIS', 'HEMLIBRA', 'SPINRAZA', 'ZOLGENSMA',
    # Hépatite / Antiviral
    'MAVIRET', 'EPCLUSA', 'HARVONI', 'SOVALDI',
    # Autres oncologie
    'ERBITUX', 'VECTIBIX', 'ADCETRIS', 'BESPONSA', 'MYLOTARG',
]

# Patterns pour co-traitements (enrichis)
COTREATMENT_PATTERNS = [
    # Association explicite
    r"(?:en\s+)?(?:association|combinaison)\s+(?:avec|[àa])\s+(?:le\s+|la\s+|l['\u2019])?([A-Z][A-Za-z\-]+)",
    r"(?:associ[ée]|combin[ée])\s+(?:[àa]|avec)\s+(?:le\s+|la\s+|l['\u2019])?([A-Z][A-Za-z\-]+)",
    # Traitement concomitant
    r'(?:traitement\s+)?(?:concomitant|simultan[ée])\s+(?:avec|par|de)\s+([A-Z][A-Za-z\-]+)',
    # "avec" suivi d'un nom de médicament avec suffixe typique
    r'(?:avec|plus|et)\s+(?:le\s+|la\s+)?([A-Z][A-Za-z\-]+(?:inib|mab|nib|zumab|ximab|umab|tinib))',
    # Schéma thérapeutique
    r'sch[ée]ma\s+(?:th[ée]rapeutique\s+)?(?:avec|incluant)\s+([A-Z][A-Za-z\-]+)',
    # "pour l'association X et Y"
    r"pour\s+l['\u2019]association\s+([A-Z][A-Za-z\-]+)\s+et\s+",
    # "X en association avec Y" - capturer Y
    r'([A-Z][A-Za-z\-]+)\s+en\s+association\s+avec',
    # Médicaments entre parenthèses "(en association avec X)"
    r'\(en\s+association\s+avec\s+([A-Z][A-Za-z\-]+)\)',
]

# Patterns de phrases de cashback (pour extraction de la phrase)
CASHBACK_SENTENCE_PATTERNS = [
    r"Au[\s\-]?del[àa]\s+d['\u2019]une?\s+dur[ée]e[^.]*?(?:rembourse|restitue)[^.]*\.",
    r"[ÀAa]\s+partir\s+d[ue][^.]*?(?:rembourse|restitue)[^.]*\.",
    r"Pour\s+chaque\s+(?:emballage|bo[îi]te|flacon)[^.]*?(?:rembourse|restitue)[^.]*\.",
    r"(?:Sur|À|A)\s+(?:première\s+)?demande\s+de\s+l['\u2019](?:assur(?:eur|ance)|caisse)[\s\-]?maladie[^.]*?(?:rembourse|restitue)[^.]*\.",
    r"[A-Z][a-zA-ZÀ-ÿ\-]+(?:\s+[A-Za-zÀ-ÿ\-\(\)&]+)*\s+(?:SA|AG|GmbH|Sàrl)\s+(?:rembourse(?:ra)?|restitue)[^.]*\.",
    r"(?:Bristol[\s\-]?Myers[\s\-]?Squibb|Pfizer|Novartis|Roche|Merck|Bayer|AstraZeneca|GlaxoSmithKline|GSK|Sanofi|AbbVie|Eli\s+Lilly|Lilly|Amgen|Johnson\s*&?\s*Johnson|Boehringer\s+Ingelheim)\s+(?:rembourse(?:ra)?|restitue)[^.]*\.",
    r"[Ll][ea]\s+titulaire\s+(?:de\s+l['\u2019]autorisation)?[^.]*?(?:rembourse|restitue)[^.]*\.",
    r"[Dd][èe]s\s+(?:la\s+)?(?:première\s+)?demande\s+[^.]*?(?:titulaire|le\s+titulaire)[^.]*?(?:rembourse|restitue)[^.]*\.",
    r"Dans\s+cette\s+indication[^.]*?[A-Z][a-zA-ZÀ-ÿ\-]+\s+(?:SA|AG|GmbH|Sàrl)\s+(?:rembourse|restitue)[^.]*\.",
    r"En\s+cas\s+d['\u2019]arr[êe]t[^.]*?(?:rembourse(?:ra)?|restitue)[^.]*\.",
]

# Patterns pour phrases supplémentaires pertinentes (à inclure après la phrase principale)
EXTRA_SENTENCE_PATTERNS = [
    # TVA et taxes (diverses formulations)
    r'^\s*(La\s+(?:taxe|TVA|T\.?V\.?A\.?)[^.]*\.)',
    r'^\s*((?:Hors|Sans|Exclu(?:ant|s)?)\s+(?:la\s+)?(?:TVA|T\.?V\.?A\.?|taxe)[^.]*\.)',
    r'^\s*((?:TVA|T\.?V\.?A\.?)\s+(?:non\s+)?(?:comprise?|incluse?|exclue?)[^.]*\.)',
    r'^\s*(Ce\s+(?:montant|prix|remboursement)\s+(?:est\s+)?(?:hors|sans)\s+(?:TVA|taxe)[^.]*\.)',
    r'^\s*(Un\s+remboursement\s+(?:de\s+)?(?:la\s+)?(?:TVA|T\.?V\.?A\.?)[^.]*\.)',
    r'^\s*(Une?\s+demande\s+de\s+remboursement\s+(?:de\s+)?(?:la\s+)?(?:TVA|T\.?V\.?A\.?)[^.]*\.)',
    # Phrases mentionnant TVA avec différentes structures
    r'^\s*([^.]*(?:TVA|T\.?V\.?A\.?)[^.]*(?:admissible|exclu|compris|inclus)[^.]*\.)',
    # Rabais et remboursements supplémentaires
    r'^\s*(Un\s+rabais\s+suppl[ée]mentaire[^.]*\.)',
    r'^\s*(Le\s+remboursement[^.]*\.)',
    r'^\s*(Ce\s+remboursement[^.]*\.)',
    r'^\s*(Le\s+montant[^.]*(?:rembours[^.]*|vers[^.]*)\.)',
    # Demandes et délais
    r'^\s*(La\s+demande\s+de\s+remboursement[^.]*\.)',
    r'^\s*(Cette\s+demande[^.]*\.)',
    # Conditions supplémentaires
    r'^\s*(En\s+cas\s+d[^.]*(?:rembours[^.]*|restitue[^.]*)\.)',
    r'^\s*(Si\s+le\s+traitement[^.]*(?:rembours[^.]*|restitue[^.]*)\.)',
]


# ============================================================================
# SECTION 1B: CHARGEMENT DES DONNÉES DE RÉFÉRENCE (FUZZY MATCHING)
# ============================================================================

class ReferenceDataLoader:
    """Charge les données de référence depuis la base pour fuzzy matching."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.companies: Set[str] = set()          # Noms de sociétés cashback
        self.partners: Set[str] = set()           # Partenaires (titulaires)
        self.preparations: Set[str] = set()       # Noms de préparations
        self.substances: Set[str] = set()         # Noms de substances

        # Noms de base normalisés (sans suffixes juridiques)
        self.company_bases: Set[str] = set()
        self.preparation_bases: Set[str] = set()

    def load_all(self) -> bool:
        """
        Charge toutes les données de référence.
        Returns True si des données ont été chargées.
        """
        tables = self._get_tables()

        # Charger sociétés depuis cashback existant (si table existe)
        if 'cashback' in tables:
            self._load_companies_from_cashback()

        # Charger préparations (notre table = 'preparation')
        if 'preparation' in tables:
            self._load_preparations_from_preparation()
        elif 'preparations' in tables:
            self._load_preparations()

        if 'partners' in tables:
            self._load_partners()

        if 'substances' in tables:
            self._load_substances()

        return len(self.companies) > 0 or len(self.preparations) > 0

    def _get_tables(self) -> List[str]:
        """Liste les tables de la base."""
        cursor = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        return [row[0] for row in cursor.fetchall()]

    def _load_companies_from_cashback(self):
        """Charge les sociétés depuis la table cashback."""
        try:
            cursor = self.conn.execute('''
                SELECT DISTINCT cashback_company
                FROM cashback
                WHERE cashback_company IS NOT NULL AND LENGTH(cashback_company) < 60
            ''')
            for row in cursor:
                name = row[0]
                if name and not self._is_invalid_company(name):
                    self.companies.add(name)
                    base = self._extract_base_name(name)
                    if base:
                        self.company_bases.add(base)
        except sqlite3.OperationalError:
            pass

    def _load_partners(self):
        """Charge les partenaires."""
        try:
            cursor = self.conn.execute('SELECT DISTINCT name FROM partners WHERE name IS NOT NULL')
            for row in cursor:
                name = row[0]
                if name:
                    self.partners.add(name)
                    base = self._extract_base_name(name)
                    if base:
                        self.company_bases.add(base)
        except sqlite3.OperationalError:
            pass

    def _load_preparations_from_preparation(self):
        """Charge les noms de préparations depuis notre table 'preparation'."""
        try:
            cursor = self.conn.execute('''
                SELECT DISTINCT name_de
                FROM preparation
                WHERE name_de IS NOT NULL AND name_de != ''
            ''')
            for row in cursor:
                name = row[0]
                if name:
                    self.preparations.add(name)
                    base = self._extract_drug_base(name)
                    if base:
                        self.preparation_bases.add(base)
        except sqlite3.OperationalError:
            pass

    def _load_preparations(self):
        """Charge les noms de préparations (ancien schéma)."""
        try:
            cursor = self.conn.execute('''
                SELECT DISTINCT name_fr
                FROM preparations
                WHERE name_fr IS NOT NULL AND name_fr != ''
            ''')
            for row in cursor:
                name = row[0]
                if name:
                    self.preparations.add(name)
                    base = self._extract_drug_base(name)
                    if base:
                        self.preparation_bases.add(base)
        except sqlite3.OperationalError:
            pass

    def _load_substances(self):
        """Charge les noms de substances."""
        try:
            cursor = self.conn.execute('''
                SELECT DISTINCT description_la
                FROM substances
                WHERE description_la IS NOT NULL AND description_la != ''
            ''')
            for row in cursor:
                if row[0]:
                    self.substances.add(row[0])
        except sqlite3.OperationalError:
            pass

    def _is_invalid_company(self, name: str) -> bool:
        """Filtre les faux positifs d'extraction de société."""
        invalid_words = ['remboursera', 'recherche', 'solution', 'cadre',
                        'combinaison', 'chaque', 'pour', 'si ']
        return any(w in name.lower() for w in invalid_words)

    def _extract_base_name(self, name: str) -> Optional[str]:
        """Extrait le nom de base sans suffixes juridiques."""
        suffixes = r'(AG|SA|GmbH|Ltd|Inc|International|Switzerland|Schweiz|Suisse|Pharma|Pharmaceuticals?|Healthcare|Biosciences?|Sàrl|S\.?à\.?r\.?l\.?)'
        base = re.sub(rf'\s*{suffixes}\s*', ' ', name, flags=re.IGNORECASE)
        base = re.sub(r'[()]', '', base)
        base = re.sub(r'\s+', ' ', base).strip(' -')
        return base.upper() if len(base) > 2 else None

    def _extract_drug_base(self, name: str) -> Optional[str]:
        """Extrait le nom de base du médicament."""
        base = re.sub(r'\d+\s*(mg|ml|g|mcg|µg)', '', name, flags=re.IGNORECASE)
        base = re.sub(r'(depot|retard|SR|XR|CR|forte|comp\.?|caps\.?)', '', base, flags=re.IGNORECASE)
        base = re.sub(r'\s+', ' ', base).strip()
        return base.upper() if len(base) > 2 else None

    def get_stats(self) -> Dict[str, int]:
        """Retourne les statistiques de chargement."""
        return {
            'companies': len(self.companies),
            'partners': len(self.partners),
            'company_bases': len(self.company_bases),
            'preparations': len(self.preparations),
            'substances': len(self.substances),
        }


def fuzzy_match(text: str, candidates: Set[str], threshold: float = 0.85) -> Optional[str]:
    """
    Trouve la meilleure correspondance fuzzy.

    Args:
        text: Texte à matcher
        candidates: Ensemble de candidats
        threshold: Seuil minimum de similarité (0.85 = 85%)

    Returns:
        Le candidat le plus proche si au-dessus du seuil, sinon None
    """
    if not text or not candidates:
        return None

    text_upper = text.upper()
    best_match = None
    best_ratio = 0.0

    for candidate in candidates:
        candidate_upper = candidate.upper()

        # Match exact rapide
        if text_upper == candidate_upper:
            return candidate

        # Match substring rapide (pour les noms de base)
        if len(text_upper) >= 4:
            if candidate_upper in text_upper or text_upper in candidate_upper:
                return candidate

        # Fuzzy match avec SequenceMatcher
        ratio = SequenceMatcher(None, text_upper, candidate_upper).ratio()
        if ratio > best_ratio and ratio >= threshold:
            best_ratio = ratio
            best_match = candidate

    return best_match


def find_company_in_text(text: str, ref_data: ReferenceDataLoader) -> Optional[str]:
    """
    Trouve un nom de société dans le texte par fuzzy matching.
    """
    text_lower = text.lower()

    # 1. Chercher les noms de sociétés exacts connus
    for company in ref_data.companies:
        if company.lower() in text_lower:
            return company

    # 2. Chercher par nom de base (sans suffixes)
    words = re.findall(r'[A-ZÀÂÄÉÈÊËÏÎÔÖÙÛÜŸÇ][a-zA-ZÀ-ÿ\-]{3,}', text)
    for word in words:
        match = fuzzy_match(word, ref_data.company_bases, threshold=0.90)
        if match:
            # Retrouver le nom complet de la société
            for company in ref_data.companies:
                if match in company.upper():
                    return company
            for partner in ref_data.partners:
                if match in partner.upper():
                    return partner

    return None


def find_drugs_in_text(text: str, ref_data: ReferenceDataLoader) -> List[str]:
    """
    Trouve les noms de médicaments dans le texte par fuzzy matching.
    """
    found = []
    text_upper = text.upper()

    # Chercher les préparations connues
    for prep in ref_data.preparations:
        prep_upper = prep.upper()
        if prep_upper in text_upper and prep not in found:
            found.append(prep)

    # Chercher les substances (noms latins souvent terminés en -um, -as, -is)
    for substance in ref_data.substances:
        # Chercher le radical de la substance
        substance_base = re.sub(r'(um|as|is|icum)$', '', substance, flags=re.IGNORECASE)
        if len(substance_base) >= 5 and substance_base.upper() in text_upper:
            if substance not in found:
                found.append(substance)

    return found


# ============================================================================
# SECTION 2: FONCTIONS UTILITAIRES
# ============================================================================

def convert_number(s: str) -> Optional[int]:
    """Convertit un nombre en lettres ou chiffres vers int."""
    if s is None:
        return None
    s_lower = s.lower().strip()
    if s_lower in NUMBERS_IN_WORDS:
        return NUMBERS_IN_WORDS[s_lower]
    try:
        return int(s)
    except ValueError:
        return None


def parse_decimal(s: str) -> Optional[float]:
    """Parse un nombre avec virgule ou point."""
    if not s:
        return None
    return float(s.replace(',', '.'))


def clean_html(text: str) -> str:
    """Nettoie le HTML et normalise les espaces."""
    text = text.replace('<br>', ' ').replace('<br/>', ' ')
    text = text.replace('<b>', '').replace('</b>', '')
    text = text.replace('<u>', '').replace('</u>', '')
    text = text.replace('&nbsp;', ' ')
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def protect_text(text: str) -> str:
    """Protège dates, montants et abréviations avant découpage en phrases."""
    # 1. Dates suisses (DD.MM.YYYY ou D.M.YY)
    text = re.sub(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', r'\1__DATE__\2__DATE__\3', text)
    text = re.sub(r'(\d{1,2})\.(\d{1,2})\.(\d{2})(?!\d)', r'\1__DATE__\2__DATE__\3', text)

    # 2. Montants avec préfixe monétaire (Fr., CHF)
    # IMPORTANT: Inclure apostrophe ASCII (') ET typographique (') pour nombres suisses
    # Ex: "Fr. 68.89" -> "Fr__MONTANT__68__DOT__89"
    # Ex: "Fr. 6'702.71" ou "Fr. 6'702.71" -> "Fr__MONTANT__6'702__DOT__71"
    text = re.sub(r"Fr\.\s*([\d''\u2019]+)\.(\d+)", r"Fr__MONTANT__\1__DOT__\2", text)
    text = re.sub(r"CHF\s*([\d''\u2019]+)\.(\d+)", r"CHF__MONTANT__\1__DOT__\2", text)
    # Fr. seul (sans décimale)
    text = re.sub(r"Fr\.\s+(?=\d)", r"Fr__DOT__ ", text)

    # 3. Montants avec suffixe "francs" (sans préfixe)
    # Ex: "de 6'702.71 francs" ou "6'702.71 francs" (apostrophe typographique)
    text = re.sub(r"([\d''\u2019]+)\.(\d+)\s+francs", r"\1__DOT__\2 francs", text)

    # 4. Montants isolés avec apostrophe suisse (pattern nombre décimal)
    # Ex: "rembourse 1'234.56" ou "1'234.56" (apostrophe typographique)
    text = re.sub(r"(\d+[''\u2019]\d+)\.(\d{2})(?!\d)", r"\1__DOT__\2", text)

    # 5. Pourcentages avec décimales
    # Ex: "12.5%" -> "12__DOT__5%"
    text = re.sub(r"(\d+)\.(\d+)\s*%", r"\1__DOT__\2%", text)

    # 6. Abréviations courantes
    text = text.replace('T.V.A.', '__TVA__')
    text = text.replace('T.V.A', '__TVA__')
    text = re.sub(r'(?<!\w)etc\.(?!\w)', '__ETC__', text)
    text = re.sub(r'(?<!\w)max\.(?!\w)', '__MAX__', text)
    text = re.sub(r'(?<!\w)art\.(?!\w)', '__ART__', text)
    text = re.sub(r'(?<!\w)al\.(?!\w)', '__AL__', text)

    return text


def restore_text(text: str) -> str:
    """Restaure dates, montants et abréviations après découpage."""
    # Montants
    text = text.replace('Fr__MONTANT__', 'Fr. ')
    text = text.replace('CHF__MONTANT__', 'CHF ')
    text = text.replace('Fr__DOT__', 'Fr.')

    # Abréviations
    text = text.replace('__TVA__', 'T.V.A.')
    text = text.replace('__ETC__', 'etc.')
    text = text.replace('__MAX__', 'max.')
    text = text.replace('__ART__', 'art.')
    text = text.replace('__AL__', 'al.')

    # Dates et points décimaux (en dernier)
    text = text.replace('__DATE__', '.')
    text = text.replace('__DOT__', '.')

    return text


# ============================================================================
# SECTION 3: DÉTECTION CASHBACK
# ============================================================================

def is_false_positive(text: str) -> bool:
    """Vérifie si le texte contient des patterns de faux positifs."""
    text_lower = text.lower()
    for pattern in FALSE_POSITIVE_PATTERNS:
        if re.search(pattern, text_lower, re.IGNORECASE):
            return True
    return False


def detect_cashback(text: str, ref_data: Optional[ReferenceDataLoader] = None) -> Dict:
    """
    Détecte si le texte contient un cashback (fabricant → assurance).

    Args:
        text: Le texte de limitation à analyser
        ref_data: Données de référence pour fuzzy matching (optionnel)

    Returns:
        {'is_cashback': bool, 'company': str, 'patterns_matched': list}
    """
    result = {'is_cashback': False, 'company': None, 'patterns_matched': []}

    # Vérifier faux positifs
    has_fp = is_false_positive(text)

    # Pattern 1: [Société] rembourse
    match = re.search(COMPANY_REMBOURSE_PATTERN, text, re.IGNORECASE | re.VERBOSE)
    if match:
        result['is_cashback'] = True
        result['company'] = match.group(1).strip() if match.groups() else None
        result['patterns_matched'].append('company_rembourse')
        return result

    # Pattern 2: remboursé par [Société]
    match = re.search(REMBOURSE_PAR_COMPANY_PATTERN, text, re.IGNORECASE | re.VERBOSE)
    if match:
        result['is_cashback'] = True
        result['company'] = match.group(1).strip() if match.groups() else None
        result['patterns_matched'].append('rembourse_par')
        return result

    # Pattern 3: L'assureur facture à [Société]
    match = re.search(ASSUREUR_FACTURE_PATTERN, text, re.IGNORECASE | re.VERBOSE)
    if match:
        result['is_cashback'] = True
        result['company'] = match.group(1).strip() if match.groups() else None
        result['patterns_matched'].append('assureur_facture')
        return result

    # Pattern 4: Le titulaire rembourse
    if re.search(TITULAIRE_REMBOURSE_PATTERN, text, re.IGNORECASE | re.VERBOSE):
        result['is_cashback'] = True
        result['patterns_matched'].append('titulaire_rembourse')
        return result

    # Pattern 5: rembourse à l'assurance-maladie
    if re.search(REMBOURSE_ASSURANCE_MALADIE_PATTERN, text, re.IGNORECASE | re.VERBOSE):
        result['is_cashback'] = True
        result['patterns_matched'].append('rembourse_assurance')
        return result

    # Patterns supplémentaires
    if re.search(r'rembourse.*\d+[.,]?\d*\s*%\s*(?:du|de|des)', text, re.IGNORECASE):
        result['is_cashback'] = True
        result['patterns_matched'].append('percentage')
    elif re.search(r"rembourse(?:ra)?\s+(?:à\s+l'assureur)?.*?(?:CHF|Fr\.)\s*[\d']+", text, re.IGNORECASE):
        result['is_cashback'] = True
        result['patterns_matched'].append('amount')
    elif re.search(r'rembourse.*partie\s*fixe.*prix', text, re.IGNORECASE):
        result['is_cashback'] = True
        result['patterns_matched'].append('fixed_part')
    elif re.search(r'rembourse(?:ra)?\s+(?:intégralement|complètement)', text, re.IGNORECASE):
        result['is_cashback'] = True
        result['patterns_matched'].append('full_refund')

    # NOUVEAU: Fuzzy matching sur sociétés connues si pas encore détecté
    if ref_data and not result['is_cashback']:
        # Chercher si une société connue est mentionnée avec un verbe de remboursement
        company = find_company_in_text(text, ref_data)
        if company:
            # Vérifier si le contexte suggère un cashback
            company_lower = company.lower()
            text_lower = text.lower()
            company_pos = text_lower.find(company_lower[:min(10, len(company_lower))])
            if company_pos >= 0:
                # Extraire le contexte autour du nom de société
                context_start = max(0, company_pos - 50)
                context_end = min(len(text), company_pos + len(company) + 100)
                context = text_lower[context_start:context_end]

                # Vérifier présence de verbes de remboursement
                cashback_verbs = ['rembourse', 'restitue', 'verse', 'paie', 'prend en charge']
                if any(verb in context for verb in cashback_verbs):
                    result['is_cashback'] = True
                    result['company'] = company
                    result['patterns_matched'].append('fuzzy_company')

    # Annuler si faux positif sans pattern fort
    if has_fp and not result['patterns_matched']:
        result['is_cashback'] = False

    return result


# ============================================================================
# SECTION 4: EXTRACTION DE PHRASE
# ============================================================================

def find_cost_section(text: str) -> str:
    """Trouve la section 'Coûts thérapeutiques' si elle existe."""
    # Avec balises HTML
    match = re.search(r'<u>Co[ûu]ts?\s+th[ée]rapeutiques?</u>', text, re.IGNORECASE)
    if match:
        return text[match.end():]
    # Sans balises
    match = re.search(r'Co[ûu]ts?\s+th[ée]rapeutiques?\s*[:\n]', text, re.IGNORECASE)
    if match:
        return text[match.end():]
    return text


def extract_cashback_sentence(text: str) -> Dict:
    """
    Extrait la phrase de cashback d'un texte.

    Returns:
        {
            'has_cashback': bool,
            'cashback_sentence': str,
            'company': str,
            'pattern_matched': str
        }
    """
    result = {
        'has_cashback': False,
        'cashback_sentence': None,
        'company': None,
        'pattern_matched': None
    }

    # Limiter à la section coûts thérapeutiques si elle existe
    working_text = find_cost_section(text)

    # Nettoyer et protéger
    clean_text = clean_html(working_text)
    clean_text = protect_text(clean_text)

    # Chercher les patterns de phrase cashback
    for pattern in CASHBACK_SENTENCE_PATTERNS:
        match = re.search(pattern, clean_text, re.IGNORECASE)
        if match:
            # Trouver le vrai début de la phrase
            start_pos = match.start()
            text_before = clean_text[:start_pos]

            last_end = max(
                text_before.rfind('. '),
                text_before.rfind('.<br'),
                text_before.rfind('.\n'),
                -1
            )

            real_start = last_end + 2 if last_end >= 0 else 0
            sentence = clean_text[real_start:match.end()].strip()

            # Chercher phrases supplémentaires (TVA, demandes, etc.)
            remaining = clean_text[match.end():match.end()+800]
            extra_count = 0

            # Méthode 1: Patterns explicites
            while extra_count < 3:
                found = None
                for extra_pat in EXTRA_SENTENCE_PATTERNS:
                    extra_match = re.match(extra_pat, remaining, re.IGNORECASE)
                    if extra_match:
                        found = extra_match
                        break
                if found:
                    sentence += ' ' + found.group(1).strip()
                    remaining = remaining[found.end():]
                    extra_count += 1
                else:
                    break

            # Méthode 2: Si TVA/T.V.A. apparaît dans les phrases suivantes, les inclure
            # Chercher dans les 2-3 phrases suivantes
            if 'TVA' not in sentence.upper() and 'T__TVA__' not in sentence:
                # Découper remaining en phrases
                sentences_after = re.split(r'(?<=[.!?])\s+', remaining[:500])
                for next_sent in sentences_after[:3]:
                    if re.search(r'(?:TVA|T\.?V\.?A\.?|__TVA__)', next_sent, re.IGNORECASE):
                        sentence += ' ' + next_sent.strip()
                        break

            # Restaurer
            sentence = restore_text(sentence)

            # Extraire société
            company = None
            company_match = re.search(
                r'([A-ZÀÂÄÉÈÊËÏÎÔÖÙÛÜŸÇ][a-zA-ZÀ-ÿ\-]*(?:[\s\-][A-Za-zÀ-ÿ\-\(\)&]+)*(?:\s*\([^)]+\))?\s*(?:SA|AG|GmbH|Sàrl))',
                sentence
            )
            if company_match:
                company = company_match.group(1).strip()

            result['has_cashback'] = True
            result['cashback_sentence'] = sentence
            result['company'] = company
            result['pattern_matched'] = 'sentence_isolated'
            return result

    return result


# ============================================================================
# SECTION 5: EXTRACTION DE RÈGLES
# ============================================================================

def extract_calculation(text: str) -> Dict:
    """Extrait le type et la valeur du calcul."""
    text_lower = text.lower()

    for pattern, calc_type, has_value in CALCULATION_PATTERNS:
        match = re.search(pattern, text_lower if has_value is None else text, re.IGNORECASE)
        if match:
            value = None
            if has_value == 'value' and match.groups():
                try:
                    value = parse_decimal(match.group(1))
                except:
                    pass
            return {'type': calc_type, 'value': value, 'match': match.group(0)}

    return {'type': 'unknown', 'value': None, 'match': None}


def extract_unit(text: str) -> str:
    """Extrait l'unité de remboursement."""
    text_lower = text.lower()
    for pattern, unit_type in UNIT_PATTERNS:
        if re.search(pattern, text_lower):
            return unit_type
    return 'unknown'


def is_request_deadline_context(text: str, match_start: int, match_end: int) -> bool:
    """Vérifie si le seuil est dans un contexte de délai de demande."""
    text_lower = text.lower()
    context_start = max(0, match_start - 150)
    context_end = min(len(text_lower), match_end + 150)
    context = text_lower[context_start:context_end]

    for pattern in EXCLUSION_PATTERNS_REQUEST_DEADLINE:
        if re.search(pattern, context, re.IGNORECASE):
            return True
    return False


def extract_threshold(text: str) -> Tuple[Optional[Dict], List[Dict]]:
    """Extrait les seuils de déclenchement du remboursement.

    Returns:
        Tuple[Optional[Dict], List[Dict]]:
            - Le seuil principal (priorité aux seuils avec valeur)
            - La liste de tous les seuils détectés
    """
    text_lower = text.lower()
    thresholds = []

    for pattern, threshold_type, has_value in THRESHOLD_PATTERNS:
        match = re.search(pattern, text_lower, re.IGNORECASE)
        if match:
            # Exclure les délais de demande
            if is_request_deadline_context(text, match.start(), match.end()):
                continue

            value = None
            if has_value == 'value' and match.groups():
                value = convert_number(match.group(1))

            # Détecter l'unité
            unit = None
            matched = match.group(0).lower()
            if any(u in matched for u in ['mois', 'month']):
                unit = 'months'
            elif any(u in matched for u in ['semaine', 'week']):
                unit = 'weeks'
            elif any(u in matched for u in ['jour', 'day']):
                unit = 'days'
            elif 'cycle' in matched:
                unit = 'cycles'
            elif any(u in matched for u in ['paquet', 'emballage', 'boîte', 'boite', 'flacon']):
                unit = 'boxes'
            elif any(u in matched for u in ['an ', 'année', 'par an']):
                unit = 'years'
            elif 'administration' in matched:
                unit = 'administrations'
            elif 'forfait' in matched or 'unique' in matched:
                unit = 'flat_fee'

            thresholds.append({
                'type': threshold_type,
                'value': value,
                'unit': unit,
                'match': match.group(0)
            })

    if thresholds:
        # Prioriser les seuils avec valeur pour le seuil principal
        with_value = [t for t in thresholds if t['value'] is not None]
        primary = with_value[0] if with_value else thresholds[0]
        return primary, thresholds

    return None, []


def extract_conditions(text: str) -> Dict:
    """Extrait les conditions de remboursement."""
    text_lower = text.lower()
    conditions = {}

    for cond_type, patterns in CONDITION_PATTERNS.items():
        found = False
        for pattern in patterns:
            if re.search(pattern, text_lower):
                found = True
                break
        conditions[cond_type] = found

    return conditions


def extract_cotreatments(text: str, ref_data: Optional[ReferenceDataLoader] = None) -> List[str]:
    """Extrait les co-traitements mentionnés avec fuzzy matching."""
    cotreatments = []
    text_upper = text.upper()

    # Mots à exclure (faux positifs fréquents)
    EXCLUDED_WORDS = {'AVEC', 'POUR', 'DANS', 'CETTE', 'ENTRE', 'APRES', 'AVANT',
                      'CHEZ', 'SUITE', 'SELON', 'LEURS', 'NOTRE', 'VOTRE', 'AINSI'}

    # 1. Patterns explicites d'association
    for pattern in COTREATMENT_PATTERNS:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            drug = match.strip().upper()
            # Nettoyer le symbole ®
            drug = drug.replace('®', '').strip()
            if len(drug) >= 4 and drug not in cotreatments and drug not in EXCLUDED_WORDS:
                cotreatments.append(drug)

    # 2. Médicaments connus (liste enrichie)
    for drug in KNOWN_DRUGS:
        if re.search(r'\b' + re.escape(drug) + r'(?:®)?\b', text_upper):
            if drug not in cotreatments:
                cotreatments.append(drug)

    # 3. Fuzzy matching avec préparations si disponible
    if ref_data and ref_data.preparations:
        drugs_found = find_drugs_in_text(text, ref_data)
        for drug in drugs_found:
            drug_upper = drug.upper()
            if drug_upper not in cotreatments and len(drug_upper) >= 5 and drug_upper not in EXCLUDED_WORDS:
                cotreatments.append(drug_upper)

    return cotreatments


def extract_company(text: str) -> Optional[str]:
    """Extrait le nom de la société."""
    match = re.search(
        r'([A-ZÀÂÄÉÈÊËÏÎÔÖÙÛÜŸÇ][a-zA-ZÀ-ÿ\-]*(?:[\s\-]+[A-Za-zÀ-ÿ\-\(\)&]+)*\s*(?:SA|AG|GmbH|Sàrl))',
        text
    )
    return match.group(1).strip() if match else None


# ============================================================================
# SECTION 6: PIPELINE PRINCIPAL
# ============================================================================

class CashbackExtractor:
    """Pipeline complet d'extraction cashback."""

    CASHBACK_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS cashback (
        cashback_id             INTEGER PRIMARY KEY AUTOINCREMENT,
        limitation_id           INTEGER NOT NULL REFERENCES limitation(limitation_id),
        preparation_id          INTEGER REFERENCES preparation(preparation_id),
        limitation_code         TEXT,
        product_name            TEXT,
        cashback_extract        TEXT,
        cashback_company        TEXT,
        detection_patterns      TEXT,
        rule_calc_type          TEXT,
        rule_calc_value         REAL,
        rule_unit               TEXT,
        rule_threshold_type     TEXT,
        rule_threshold_value    INTEGER,
        rule_threshold_unit     TEXT,
        rule_thresholds_all     TEXT,
        rule_cond_treatment_stop    BOOLEAN,
        rule_cond_adverse_effects   BOOLEAN,
        rule_cond_treatment_failure BOOLEAN,
        rule_cotreatments       TEXT,
        UNIQUE(limitation_id)
    );
    """

    CASHBACK_SEGMENT_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS cashback_segment (
        cashback_segment_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        segment_id              INTEGER NOT NULL REFERENCES limitation_indication_segment(segment_id),
        limitation_id           INTEGER NOT NULL REFERENCES limitation(limitation_id),
        preparation_id          INTEGER REFERENCES preparation(preparation_id),
        limitation_code         TEXT,
        product_name            TEXT,
        indication_name         TEXT,
        indication_code         TEXT,
        cashback_extract        TEXT,
        cashback_company        TEXT,
        detection_patterns      TEXT,
        rule_calc_type          TEXT,
        rule_calc_value         REAL,
        rule_unit               TEXT,
        rule_threshold_type     TEXT,
        rule_threshold_value    INTEGER,
        rule_threshold_unit     TEXT,
        rule_thresholds_all     TEXT,
        rule_cond_treatment_stop    BOOLEAN,
        rule_cond_adverse_effects   BOOLEAN,
        rule_cond_treatment_failure BOOLEAN,
        rule_cotreatments       TEXT,
        UNIQUE(segment_id)
    );
    """

    def __init__(self, db_path: str, table: str = 'limitation',
                 text_col: str = 'description_fr', id_col: str = 'limitation_id'):
        self.db_path = db_path
        self.table = table
        self.text_col = text_col
        self.id_col = id_col
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.stats = defaultdict(int)
        self.results = []
        self.ref_data: Optional[ReferenceDataLoader] = None

    def _load_reference_data(self):
        """Charge les données de référence pour le fuzzy matching."""
        if self.ref_data is not None:
            return  # Déjà chargé

        print("Chargement des données de référence pour fuzzy matching...")
        self.ref_data = ReferenceDataLoader(self.conn)
        loaded = self.ref_data.load_all()

        if loaded:
            stats = self.ref_data.get_stats()
            print(f"  - Sociétés connues: {stats['companies']}")
            print(f"  - Partenaires: {stats['partners']}")
            print(f"  - Noms de base: {stats['company_bases']}")
            print(f"  - Préparations: {stats['preparations']}")
            print(f"  - Substances: {stats['substances']}")
        else:
            print("  Tables de référence non trouvées, fuzzy matching désactivé")
            self.ref_data = None

    def process_text(self, text_id: int, raw_text: str) -> Optional[Dict]:
        """Pipeline complet pour un texte."""
        if not raw_text:
            return None

        # Nettoyer le HTML avant détection (nos textes ont <b>, <br> etc.)
        text = clean_html(raw_text)

        # Étape 1: Détection cashback (avec fuzzy matching si ref_data disponible)
        detection = detect_cashback(text, self.ref_data)
        if not detection['is_cashback']:
            self.stats['not_cashback'] += 1
            return None

        self.stats['detected'] += 1
        # Tracer les détections par fuzzy matching
        if 'fuzzy_company' in detection.get('patterns_matched', []):
            self.stats['fuzzy_detections'] += 1

        # Étape 2: Extraction phrase
        extraction = extract_cashback_sentence(text)
        if not extraction['has_cashback']:
            # Fallback: utiliser le texte complet nettoyé
            sentence = text
            company = detection['company']
        else:
            sentence = extraction['cashback_sentence']
            company = extraction['company'] or detection['company']

        # Étape 3: Extraction règles
        calculation = extract_calculation(sentence)
        unit = extract_unit(sentence)
        threshold, all_thresholds = extract_threshold(sentence)
        conditions = extract_conditions(sentence)
        cotreatments = extract_cotreatments(sentence, self.ref_data)

        # Stats
        self.stats['processed'] += 1
        self.stats[f'calc_{calculation["type"]}'] += 1
        if threshold:
            self.stats['with_threshold'] += 1
            self.stats[f'threshold_{threshold["type"]}'] += 1
        if len(all_thresholds) > 1:
            self.stats['with_multiple_thresholds'] += 1
        for cond, val in conditions.items():
            if val:
                self.stats[f'cond_{cond}'] += 1
        if cotreatments:
            self.stats['with_cotreatments'] += 1

        return {
            'id': text_id,
            'is_cashback': True,
            'cashback_extract': sentence,
            'cashback_company': company,
            'detection_patterns': ','.join(detection.get('patterns_matched', [])),
            'rule_calc_type': calculation['type'],
            'rule_calc_value': calculation['value'],
            'rule_unit': unit,
            'rule_threshold_type': threshold['type'] if threshold else None,
            'rule_threshold_value': threshold['value'] if threshold else None,
            'rule_threshold_unit': threshold['unit'] if threshold else None,
            'rule_thresholds_all': json.dumps(all_thresholds, ensure_ascii=False) if all_thresholds else None,
            'rule_cond_treatment_stop': conditions.get('treatment_stop', False),
            'rule_cond_adverse_effects': conditions.get('adverse_effects', False),
            'rule_cond_treatment_failure': conditions.get('treatment_failure', False),
            'rule_cotreatments': json.dumps(cotreatments) if cotreatments else None,
        }

    def process_all(self, dry_run: bool = True, limit: int = None, verbose: bool = False) -> List[Dict]:
        """Traite tous les textes."""
        # Charger les données de référence pour fuzzy matching
        self._load_reference_data()
        print("=" * 80)

        # Récupérer les textes avec contexte (preparation_id, limitation_code, product_name)
        query = f"""
            SELECT l.{self.id_col}, l.{self.text_col},
                   l.preparation_id, l.limitation_code,
                   p.name_de AS product_name
            FROM {self.table} l
            LEFT JOIN preparation p ON l.preparation_id = p.preparation_id
            WHERE l.{self.text_col} IS NOT NULL
        """

        if limit:
            query += f" LIMIT {limit}"

        cursor = self.conn.execute(query)
        rows = cursor.fetchall()

        print(f"Traitement de {len(rows)} textes...")

        self.results = []
        self._row_context = {}  # Store context for DB write
        for row in rows:
            text_id = row[self.id_col]
            text = row[self.text_col]

            result = self.process_text(text_id, text)
            if result:
                # Attach context from the row
                result['preparation_id'] = row['preparation_id']
                result['limitation_code'] = row['limitation_code']
                result['product_name'] = row['product_name']
                self.results.append(result)

                if verbose and len(self.results) <= 5:
                    print(f"\n[ID {text_id}] {result['product_name']} / {result['limitation_code']}")
                    print(f"  Type: {result['rule_calc_type']}")
                    print(f"  Société: {result['cashback_company']}")
                    print(f"  Extrait: {result['cashback_extract'][:120]}...")

        # Rapport
        self.print_report()

        # Mise à jour base
        if not dry_run and self.results:
            self.create_cashback_table()
            self.insert_cashback_results()
        elif dry_run:
            print(f"\n[DRY RUN] Utilisez --apply pour sauvegarder les résultats.")

        return self.results

    def create_cashback_table(self):
        """Crée la table cashback (drop + recreate)."""
        print("\nCréation de la table cashback...")
        self.conn.execute("DROP TABLE IF EXISTS cashback")
        self.conn.executescript(self.CASHBACK_TABLE_SQL)
        self.conn.commit()

    def insert_cashback_results(self):
        """Insère les résultats dans la table cashback."""
        print(f"Insertion de {len(self.results)} enregistrements dans cashback...")

        for r in self.results:
            self.conn.execute('''
                INSERT OR REPLACE INTO cashback (
                    limitation_id, preparation_id, limitation_code, product_name,
                    cashback_extract, cashback_company, detection_patterns,
                    rule_calc_type, rule_calc_value, rule_unit,
                    rule_threshold_type, rule_threshold_value, rule_threshold_unit,
                    rule_thresholds_all,
                    rule_cond_treatment_stop, rule_cond_adverse_effects,
                    rule_cond_treatment_failure, rule_cotreatments
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                r['id'],
                r.get('preparation_id'),
                r.get('limitation_code'),
                r.get('product_name'),
                r['cashback_extract'],
                r['cashback_company'],
                r.get('detection_patterns'),
                r['rule_calc_type'],
                r['rule_calc_value'],
                r['rule_unit'],
                r['rule_threshold_type'],
                r['rule_threshold_value'],
                r['rule_threshold_unit'],
                r['rule_thresholds_all'],
                1 if r['rule_cond_treatment_stop'] else 0,
                1 if r['rule_cond_adverse_effects'] else 0,
                1 if r['rule_cond_treatment_failure'] else 0,
                r['rule_cotreatments'],
            ))

        self.conn.commit()
        print("Insertion terminée!")

    # ------------------------------------------------------------------
    # Segment-level cashback extraction
    # ------------------------------------------------------------------

    def process_segments(self, dry_run: bool = True, verbose: bool = False) -> List[Dict]:
        """Process cashback at segment level (per-indication within a limitation).

        For limitations that have segments in limitation_indication_segment,
        each segment's French text is analyzed individually.  For limitations
        without segments, the full description_fr is used (limitation-level).

        Returns list of segment-level cashback results.
        """
        self._load_reference_data()
        print("=" * 80)

        # 1. Get all segments with their French text
        try:
            seg_rows = self.conn.execute("""
                SELECT s.segment_id, s.limitation_id, s.preparation_id,
                       s.indication_name_de, s.indication_name_fr,
                       s.segment_text_fr, s.matched_code_value,
                       l.limitation_code, p.name_de AS product_name
                FROM limitation_indication_segment s
                JOIN limitation l ON s.limitation_id = l.limitation_id
                LEFT JOIN preparation p ON s.preparation_id = p.preparation_id
                WHERE s.segment_text_fr IS NOT NULL
                ORDER BY s.limitation_id, s.segment_order
            """).fetchall()
        except Exception as e:
            print(f"Table limitation_indication_segment not found: {e}")
            print("Falling back to limitation-level processing.")
            return self.process_all(dry_run=dry_run, verbose=verbose)

        # 2. Also get limitation-level limitations that have NO segments
        #    (single-indication or no bold names)
        lim_ids_with_segments = set()
        for row in seg_rows:
            lim_ids_with_segments.add(row['limitation_id'])

        unsegmented_rows = self.conn.execute("""
            SELECT l.limitation_id, l.preparation_id,
                   l.description_fr, l.limitation_code,
                   p.name_de AS product_name
            FROM limitation l
            LEFT JOIN preparation p ON l.preparation_id = p.preparation_id
            WHERE l.description_fr IS NOT NULL
            AND l.limitation_id NOT IN (
                SELECT DISTINCT limitation_id FROM limitation_indication_segment
            )
        """).fetchall()

        print(f"Processing {len(seg_rows)} segments + {len(unsegmented_rows)} unsegmented limitations...")

        self.segment_results = []
        seg_stats = defaultdict(int)

        # 3. Process each segment individually
        for row in seg_rows:
            raw_text = row['segment_text_fr']
            result = self.process_text(row['segment_id'], raw_text)
            if result:
                result['segment_id'] = row['segment_id']
                result['limitation_id'] = row['limitation_id']
                result['preparation_id'] = row['preparation_id']
                result['indication_name'] = row['indication_name_de'] or row['indication_name_fr']
                result['indication_code'] = row['matched_code_value']
                result['limitation_code'] = row['limitation_code']
                result['product_name'] = row['product_name']
                self.segment_results.append(result)
                seg_stats['segment_cashback'] += 1
            else:
                seg_stats['segment_no_cashback'] += 1

        # 4. Process unsegmented limitations (limitation-level)
        self.results = []
        for row in unsegmented_rows:
            result = self.process_text(row['limitation_id'], row['description_fr'])
            if result:
                result['preparation_id'] = row['preparation_id']
                result['limitation_code'] = row['limitation_code']
                result['product_name'] = row['product_name']
                self.results.append(result)
                seg_stats['unseg_cashback'] += 1
            else:
                seg_stats['unseg_no_cashback'] += 1

        # 5. Report
        print(f"\n{'=' * 80}")
        print("RAPPORT D'EXTRACTION CASHBACK (SEGMENT-LEVEL)")
        print(f"{'=' * 80}")
        print(f"\nSegments analysés: {len(seg_rows)}")
        print(f"  - Avec cashback: {seg_stats['segment_cashback']}")
        print(f"  - Sans cashback: {seg_stats['segment_no_cashback']}")
        print(f"\nLimitations non-segmentées: {len(unsegmented_rows)}")
        print(f"  - Avec cashback: {seg_stats['unseg_cashback']}")
        print(f"  - Sans cashback: {seg_stats['unseg_no_cashback']}")
        print(f"\nTotal cashback: {seg_stats['segment_cashback'] + seg_stats['unseg_cashback']} "
              f"({seg_stats['segment_cashback']} segment + {seg_stats['unseg_cashback']} limitation)")

        # Print detailed stats
        self.print_report()

        # 6. Save to DB
        if not dry_run:
            # Segment-level table
            if self.segment_results:
                self.create_cashback_segment_table()
                self.insert_segment_results()
            # Limitation-level table (for unsegmented)
            if self.results:
                self.create_cashback_table()
                self.insert_cashback_results()
        elif dry_run:
            print(f"\n[DRY RUN] Utilisez --apply pour sauvegarder les résultats.")

        return self.segment_results

    def create_cashback_segment_table(self):
        """Create the cashback_segment table (drop + recreate)."""
        print("\nCréation de la table cashback_segment...")
        self.conn.execute("DROP TABLE IF EXISTS cashback_segment")
        self.conn.executescript(self.CASHBACK_SEGMENT_TABLE_SQL)
        self.conn.commit()

    def insert_segment_results(self):
        """Insert segment-level results into cashback_segment table."""
        print(f"Insertion de {len(self.segment_results)} enregistrements dans cashback_segment...")

        for r in self.segment_results:
            self.conn.execute('''
                INSERT OR REPLACE INTO cashback_segment (
                    segment_id, limitation_id, preparation_id,
                    limitation_code, product_name,
                    indication_name, indication_code,
                    cashback_extract, cashback_company, detection_patterns,
                    rule_calc_type, rule_calc_value, rule_unit,
                    rule_threshold_type, rule_threshold_value, rule_threshold_unit,
                    rule_thresholds_all,
                    rule_cond_treatment_stop, rule_cond_adverse_effects,
                    rule_cond_treatment_failure, rule_cotreatments
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                r['segment_id'],
                r['limitation_id'],
                r.get('preparation_id'),
                r.get('limitation_code'),
                r.get('product_name'),
                r.get('indication_name'),
                r.get('indication_code'),
                r['cashback_extract'],
                r['cashback_company'],
                r.get('detection_patterns'),
                r['rule_calc_type'],
                r['rule_calc_value'],
                r['rule_unit'],
                r['rule_threshold_type'],
                r['rule_threshold_value'],
                r['rule_threshold_unit'],
                r['rule_thresholds_all'],
                1 if r['rule_cond_treatment_stop'] else 0,
                1 if r['rule_cond_adverse_effects'] else 0,
                1 if r['rule_cond_treatment_failure'] else 0,
                r['rule_cotreatments'],
            ))

        self.conn.commit()
        print("Insertion cashback_segment terminée!")

    def export_csv(self, path: str):
        """Exporte les résultats en CSV."""
        if not self.results:
            print("Aucun résultat à exporter.")
            return

        print(f"Export CSV: {path}")

        with open(path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f, delimiter=';')

            # En-tête
            headers = [
                'id', 'company', 'calc_type', 'calc_value', 'unit',
                'threshold_type', 'threshold_value', 'threshold_unit',
                'thresholds_all', 'thresholds_count',
                'treatment_stop', 'adverse_effects', 'treatment_failure',
                'cotreatments', 'cashback_extract'
            ]
            writer.writerow(headers)

            for r in self.results:
                # Compter le nombre de seuils
                thresholds_all = r.get('rule_thresholds_all') or ''
                thresholds_count = 0
                if thresholds_all:
                    try:
                        thresholds_count = len(json.loads(thresholds_all))
                    except:
                        pass

                writer.writerow([
                    r['id'],
                    r['cashback_company'] or '',
                    r['rule_calc_type'] or '',
                    r['rule_calc_value'] or '',
                    r['rule_unit'] or '',
                    r['rule_threshold_type'] or '',
                    r['rule_threshold_value'] or '',
                    r['rule_threshold_unit'] or '',
                    thresholds_all,
                    thresholds_count,
                    1 if r['rule_cond_treatment_stop'] else 0,
                    1 if r['rule_cond_adverse_effects'] else 0,
                    1 if r['rule_cond_treatment_failure'] else 0,
                    r['rule_cotreatments'] or '',
                    r['cashback_extract'] or ''
                ])

        print(f"Exporté {len(self.results)} lignes.")

    def print_report(self):
        """Affiche le rapport de statistiques."""
        print("\n" + "=" * 80)
        print("RAPPORT D'EXTRACTION CASHBACK")
        print("=" * 80)

        print(f"\nTextes analysés: {self.stats['detected'] + self.stats['not_cashback']}")
        print(f"  - Cashback détecté: {self.stats['detected']}")
        print(f"  - Non cashback: {self.stats['not_cashback']}")
        print(f"  - Traités: {self.stats['processed']}")

        # Détections par fuzzy matching
        if self.stats['fuzzy_detections'] > 0:
            print(f"  - Détections par fuzzy matching: {self.stats['fuzzy_detections']}")

        # Types de calcul
        print("\nTypes de calcul:")
        calc_types = {k: v for k, v in self.stats.items() if k.startswith('calc_')}
        for k, v in sorted(calc_types.items(), key=lambda x: -x[1]):
            print(f"  - {k.replace('calc_', '')}: {v}")

        # Seuils
        if self.stats['with_threshold']:
            print(f"\nAvec seuil: {self.stats['with_threshold']}")
            threshold_types = {k: v for k, v in self.stats.items() if k.startswith('threshold_')}
            for k, v in sorted(threshold_types.items(), key=lambda x: -x[1]):
                print(f"  - {k.replace('threshold_', '')}: {v}")

        # Conditions
        cond_types = {k: v for k, v in self.stats.items() if k.startswith('cond_')}
        if cond_types:
            print("\nConditions:")
            for k, v in sorted(cond_types.items(), key=lambda x: -x[1]):
                print(f"  - {k.replace('cond_', '')}: {v}")

        # Co-traitements
        if self.stats['with_cotreatments']:
            print(f"\nAvec co-traitements: {self.stats['with_cotreatments']}")

    def export_segments_csv(self, path: str):
        """Export segment-level cashback results to CSV."""
        results = getattr(self, 'segment_results', [])
        if not results:
            print("Aucun résultat segment à exporter.")
            return

        print(f"Export CSV segments: {path}")
        with open(path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f, delimiter=';')
            headers = [
                'segment_id', 'limitation_id', 'product_name', 'limitation_code',
                'indication_name', 'indication_code',
                'company', 'calc_type', 'calc_value', 'unit',
                'threshold_type', 'threshold_value', 'threshold_unit',
                'thresholds_all', 'thresholds_count',
                'treatment_stop', 'adverse_effects', 'treatment_failure',
                'cotreatments', 'cashback_extract'
            ]
            writer.writerow(headers)

            for r in results:
                thresholds_all = r.get('rule_thresholds_all') or ''
                thresholds_count = 0
                if thresholds_all:
                    try:
                        thresholds_count = len(json.loads(thresholds_all))
                    except:
                        pass
                writer.writerow([
                    r.get('segment_id', ''),
                    r.get('limitation_id', ''),
                    r.get('product_name', ''),
                    r.get('limitation_code', ''),
                    r.get('indication_name', ''),
                    r.get('indication_code', ''),
                    r.get('cashback_company') or '',
                    r.get('rule_calc_type') or '',
                    r.get('rule_calc_value') or '',
                    r.get('rule_unit') or '',
                    r.get('rule_threshold_type') or '',
                    r.get('rule_threshold_value') or '',
                    r.get('rule_threshold_unit') or '',
                    thresholds_all,
                    thresholds_count,
                    1 if r.get('rule_cond_treatment_stop') else 0,
                    1 if r.get('rule_cond_adverse_effects') else 0,
                    1 if r.get('rule_cond_treatment_failure') else 0,
                    r.get('rule_cotreatments') or '',
                    r.get('cashback_extract') or ''
                ])

        print(f"Exporté {len(results)} lignes segments.")

    def close(self):
        """Ferme la connexion."""
        if self.conn:
            self.conn.close()


# ============================================================================
# SECTION 7: CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Extracteur autonome de règles cashback pour textes de limitation pharmaceutique',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Exemples:
  python cashback_extractor.py --db swiss_pharma_limitations.db
  python cashback_extractor.py --db swiss_pharma_limitations.db --apply
  python cashback_extractor.py --db swiss_pharma_limitations.db --apply --segments
  python cashback_extractor.py --db swiss_pharma_limitations.db --apply --export-csv cashback_results.csv
  python cashback_extractor.py --db swiss_pharma_limitations.db --verbose --limit 10
        '''
    )

    parser.add_argument('--db', default='swiss_pharma_limitations.db', help='Chemin vers la base SQLite (défaut: swiss_pharma_limitations.db)')
    parser.add_argument('--table', default='limitation', help='Nom de la table (défaut: limitation)')
    parser.add_argument('--text-column', default='description_fr', help='Colonne du texte (défaut: description_fr)')
    parser.add_argument('--id-column', default='limitation_id', help='Colonne ID (défaut: limitation_id)')
    parser.add_argument('--apply', action='store_true', help='Appliquer les modifications à la base (créer table cashback)')
    parser.add_argument('--segments', action='store_true', help='Process at segment level (per-indication) instead of limitation level')
    parser.add_argument('--export-csv', type=str, help='Exporter les résultats en CSV')
    parser.add_argument('--verbose', action='store_true', help='Mode verbeux avec exemples')
    parser.add_argument('--limit', type=int, help='Limiter le nombre de textes à traiter')

    args = parser.parse_args()

    # Vérifier que la base existe
    if not Path(args.db).exists():
        print(f"ERREUR: Base de données non trouvée: {args.db}")
        sys.exit(1)

    # Lancer le pipeline
    extractor = CashbackExtractor(
        db_path=args.db,
        table=args.table,
        text_col=args.text_column,
        id_col=args.id_column
    )

    try:
        if args.segments:
            extractor.process_segments(
                dry_run=not args.apply,
                verbose=args.verbose,
            )
            if args.export_csv:
                extractor.export_segments_csv(args.export_csv)
        else:
            extractor.process_all(
                dry_run=not args.apply,
                limit=args.limit,
                verbose=args.verbose,
            )
            if args.export_csv:
                extractor.export_csv(args.export_csv)

    finally:
        extractor.close()


if __name__ == '__main__':
    main()
