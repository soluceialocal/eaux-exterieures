# MTDC-Eau — Surveillance qualité des eaux

Détection automatique de transitions de qualité dans les **cours d'eau français** (couverture nationale — tous bassins hydrographiques métropolitains) et les **lacs NTL-LTER** à l'aide du modèle MTDC (*Modèle de Transition Dynamique de Cohérence*).

> **Premier déploiement terrain prévu en Lot-et-Garonne (47)** — l'outil est opérationnel sur l'ensemble du territoire métropolitain (6 bassins, 38+ cours d'eau référencés, toutes stations Hub'Eau accessibles).

## Fonctionnalités

- **Rivières & fleuves français** — connexion directe à [Hub'Eau](https://hubeau.eaufrance.fr/) (API officielle OFB/DREAL) — couverture nationale
- **Lacs NTL-LTER** — analyse sur données CSV North Temperate Lakes
- **Classification hydrologique automatique** — 3 niveaux (géographique, relationnel, type DCE/SANDRE)
- **Détection MTDC** — alarme à 0 % de faux positifs sur dérives saisonnières normales
- **Auto-sélection des paramètres** — `run_auto()` choisit n_base, smooth, T selon le type détecté
- **Dashboard interactif** — interface Streamlit avec graphiques et export CSV
- **Cache local** — évite les téléchargements répétés

## Installation

```bash
pip install -r requirements.txt
```

## Lancement

```bash
# Dashboard interactif
python run.py

# Vérification des dépendances + connexion Hub'Eau (nationale)
python run.py --check

# Démonstration rapide (Garonne à Agen, nitrates, sans interface)
python run.py --demo
```

Ouvrir ensuite [http://localhost:8501](http://localhost:8501).

## Tests

```bash
python -m pytest tests/ -v
# ou sans pytest :
python tests/test_mtdc_core.py
python tests/test_topology.py
```

33 tests au total : 4 tests MTDC core (propriétés mathématiques) + 29 tests topologie (classification hydrologique, zéro appel réseau, compatible CI/CD).

## Algorithme MTDC

Le MTDC détecte les **accélérations anormales** dans un signal en comparant chaque mesure à sa baseline locale :

| Étape | Formule | Description |
|-------|---------|-------------|
| Baseline | `Rs(t) = médiane(X, n_base derniers pas)` | Référence glissante robuste |
| Ratio | `Φ(t) = X(t) / Rs(t)` | Φ=1 → normal, Φ=1.5 → +50% |
| Lissage | `Φ̃(t) = moyenne_centrée(Φ, smooth)` | Supprime le bruit haute fréquence |
| Gradient | `G(t) = dΦ̃/dt` | Détecte l'accélération |
| Alarme | `Φ̃ > phi1 ET G > 0` pendant T pas | Double condition : niveau + tendance |

### Paramètres auto-sélectionnés par type hydrologique

Le module `water/topology.py` classifie chaque station en 3 niveaux et sélectionne automatiquement les paramètres MTDC adaptés via `run_auto()` :

| Type | n_base | smooth | T | Unité de temps | Critère |
|------|--------|--------|---|----------------|---------|
| Ruisseau | 12 mois | 2 mois | 2 mois | Mensuel | Module < 0,5 m³/s |
| Rivière | 24 mois | 3 mois | 3 mois | Mensuel | Module 0,5–100 m³/s |
| Fleuve | 36 mois | 6 mois | 4 mois | Mensuel | Exutoire maritime (SANDRE) |
| Lac | 6 ans | 2 ans | 3 ans | Annuel | Plan d'eau stagnant |
| Retenue | 6 ans | 2 ans | 3 ans | Annuel | Ouvrage hydraulique |

Le paramètre `phi1` est auto-calibré sur la variabilité naturelle de chaque série (percentile 95 du ratio Φ en période stable).

## Classification hydrologique en 3 niveaux

`water/topology.py` implémente une classification à 3 niveaux basée sur les normes professionnelles françaises :

**Niveau 1 — Géographique**
Rattachement au bassin versant via les coordonnées Lambert 93 (BDCarthage) et l'identifiant `libelle_cours_eau` Hub'Eau. Référentiel des 6 bassins hydrographiques métropolitains (Adour-Garonne, Loire-Bretagne, Rhône-Méditerranée, Seine-Normandie, Rhin-Meuse, Artois-Picardie).

**Niveau 2 — Relationnel**
Ordonnancement amont→aval des stations par ACP sur les coordonnées Lambert 93. Donne le rang relatif de chaque station sur son cours d'eau, utile pour l'interprétation multi-stations.

**Niveau 3 — Type de configuration**
Classification selon deux normes combinées :
- **Fleuves côtiers** (référentiel SANDRE, 51 fleuves) : critère = exutoire maritime, indépendant du débit. La Garonne est un fleuve ; le Lot est une rivière (affluent, pas d'exutoire maritime propre).
- **Typologie DCE** (circulaire 2005/12) : < 0,5 m³/s = tête de bassin, 0,5–5 = petit cours d'eau, 5–100 = rivière, > 100 = grand cours d'eau.

## Utilisation programmatique

```python
from domains.rivieres import run_auto

# Détection automatique : classification + paramètres + analyse
res = run_auto(
    code_station="05064500",   # Garonne à Agen
    parametre_key="nitrates",
    date_debut="2000-01-01",
)

print(res["type_config"])    # "fleuve"
print(res["alarm_date"])     # première alarme détectée (ou None)
print(res["phi1"])           # seuil auto-calibré

# Surcharge experte (usage avancé uniquement)
from domains.rivieres import run
res = run(
    code_station="05064500",
    parametre_key="nitrates",
    date_debut="2000-01-01",
    n_base=36, smooth=6, t_persistance=4,
)
```

## Données

### Rivières — Hub'Eau

Les données sont issues de [Hub'Eau](https://hubeau.eaufrance.fr/api/v2/qualite_rivieres/) — API officielle du Système d'Information sur l'Eau français.

**Attribution obligatoire :** *"Données Hub'Eau / NAÏADES / OFB / DREAL / Agences de l'eau — Licence Ouverte 2.0"*

Paramètres disponibles :

| Paramètre | Code SANDRE | Unité |
|-----------|-------------|-------|
| Nitrates NO₃ | 1340 | mg/L |
| Phosphore total | 1350 | mg/L |
| Oxygène dissous | 1311 | mg/L |
| pH | 1302 | pH |
| Conductivité | 1303 | µS/cm |
| Turbidité | 1295 | NTU |
| Température | 1301 | °C |
| MES | 1305 | mg/L |
| DBO₅ | 1313 | mg O₂/L |
| Ammonium NH₄ | 1335 | mg/L |

### Pré-chargement hors ligne

```bash
# Toutes les stations du référentiel, tous paramètres
python download_hubeau.py

# Station et paramètre spécifiques
python download_hubeau.py --station 05064500 --param nitrates

# Glisser-déposer le fichier JSON généré dans le dashboard pour une analyse hors connexion
```

### Lacs — NTL-LTER

Télécharger le fichier `ntl1_v14.csv` sur [lter.limnology.wisc.edu](https://lter.limnology.wisc.edu/).

**Licence :** CC BY 4.0 — North Temperate Lakes Long-Term Ecological Research Program, University of Wisconsin-Madison.

## Structure du projet

```
MTDC-Eau-OpenSource/
├── app/
│   └── dashboard.py          # Interface Streamlit (run_auto() par défaut, expert override optionnel)
├── mtdc/
│   ├── core.py               # Algorithme MTDC canonique
│   └── visualiser.py         # Graphiques Plotly interactifs (fallback matplotlib)
├── water/
│   ├── topology.py           # Classification 3 niveaux (géographique / relationnel / type)
│   ├── parameters.py         # Codes SANDRE + seuils DCE + référentiel 38 cours d'eau
│   └── hubeau_loader.py      # Client API Hub'Eau + cache
├── domains/
│   ├── rivieres.py           # Pipeline rivières/fleuves : run() + run_auto()
│   └── lacs.py               # Pipeline lacs NTL-LTER
├── tests/
│   ├── test_mtdc_core.py     # 4 tests : propriétés mathématiques MTDC
│   └── test_topology.py      # 29 tests : classification hydrologique (zéro réseau)
├── run.py                    # Point d'entrée (dashboard / check / demo)
├── download_hubeau.py        # Pré-chargement Hub'Eau (couverture nationale)
├── extract_naiades.py        # Export NAÏADES bulk (couverture nationale)
├── requirements.txt
└── LICENSE
```

## Seuils réglementaires DCE

Les seuils indicatifs de la Directive Cadre sur l'Eau (2000/60/CE) sont affichés en référence dans le dashboard :

| Paramètre | Bon état | Mauvais état |
|-----------|----------|--------------|
| Nitrates | < 10 mg/L | > 50 mg/L |
| Phosphore total | < 0.05 mg/L | > 0.2 mg/L |
| DBO₅ | < 3 mg O₂/L | > 9 mg O₂/L |
| Oxygène dissous | > 8 mg/L | < 5 mg/L |
| Turbidité | < 10 NTU | > 100 NTU |
| Ammonium | < 0.1 mg/L | > 1.0 mg/L |

> **Note :** Le MTDC ne remplace pas les seuils DCE — il les complète. Les seuils DCE indiquent si un paramètre dépasse une valeur absolue ; le MTDC détecte les **transitions accélérantes** avant que ces seuils soient atteints.

## Licence

Ce logiciel est distribué sous licence **MIT** — voir [LICENSE](LICENSE).

Les données Hub'Eau sont sous **Licence Ouverte 2.0** (Etalab).
Les données NTL-LTER sont sous **CC BY 4.0**.
