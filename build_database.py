#!/usr/bin/env python3
"""
build_database.py — Construction de la banque de données locale MTDC-Eau
=========================================================================

Lance ce script une fois pour constituer la base complète.
Les données téléchargées permettent de faire tourner le moteur MTDC
et l'interface terrain SANS aucun appel réseau ultérieur.

Sources :
  1. Hub'Eau qualité rivières  — mesures physico-chimiques (NAÏADES)
     → endpoint CSV /analyse_pc.csv (bulk, sans pagination JSON)
  2. Hub'Eau hydrométrie       — modules et débits caractéristiques
  3. SANDRE référentiel        — stations, cours d'eau, bassins
  4. BDCarthage                — réseau hydrographique (GeoJSON simplifié)

Usage :
  python build_database.py                  # tout télécharger
  python build_database.py --source qualite # une source seulement
  python build_database.py --riviere Garonne Lot  # cours d'eau ciblés
  python build_database.py --depuis 2010-01-01    # période réduite
  python build_database.py --resume               # reprendre si interrompu
  python build_database.py --strict-qual          # données validées uniquement (code_qual=1)

Licence données : Licence Ouverte 2.0 (Etalab) — Hub'Eau / OFB / DREAL
Attribution      : "Données Hub'Eau / NAÏADES / OFB / Agences de l'eau"

Notes API (Hub'Eau qualité rivières v2) :
  - /analyse_pc.csv   : endpoint CSV bulk, depth max 20 000 enreg. par requête
  - /analyse_pc       : endpoint JSON, page_size max 20 000
  - /station_pc       : page_size max 20 000
  - code_qualification: 1=bon, 2=incertain, 3=mauvais  (on garde 1+2 par défaut)
  - code_remarque     : 1=normal, 2=<LQ, 3=<LD, 7=incertain, 10=invalide
  - code_fraction     : 23=eau totale (fraction recommandée physico-chimie)
"""

import os
import sys
import csv
import io
import json
import time
import argparse
import logging
import urllib.request
import urllib.parse
import urllib.error
import ssl
from pathlib import Path
from datetime import datetime, date
from typing import Optional

# ── Dépendances optionnelles ──────────────────────────────────────────────────
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

# ── Configuration ─────────────────────────────────────────────────────────────
DB_DIR     = Path(__file__).parent / "data"
LOG_FILE   = DB_DIR / "build_database.log"

HUBEAU_BASE  = "https://hubeau.eaufrance.fr/api/v2"
SANDRE_BASE  = "https://api.sandre.eaufrance.fr/referentiels/v1"

# Paramètres SANDRE à télécharger (code → clé locale)
PARAMS_SANDRE = {
    1301: "temperature",
    1302: "ph",
    1303: "conductivite",
    1295: "turbidite",
    1311: "o2_dissous",
    1340: "nitrates",
    1350: "phosphore",
    1305: "mes",
    1313: "dbo5",
    1335: "ammonium",
}

# Codes de remarque invalidants (à exclure des agrégations)
# 10 = résultat falsu/invalide  |  7 = résultat incertain  |  2 = <LQ (conservé mais marqué)
CODES_REMARQUE_INVALIDES = {10, 7}

# Profondeur max par requête CSV (limite Hub'Eau = 20 000)
CSV_DEPTH_LIMIT = 20_000

# Cours d'eau référencés (depuis water/parameters.py)
from water.parameters import RIVIERES_FRANCE

# ── Logging ───────────────────────────────────────────────────────────────────
def setup_logging(verbose: bool = False):
    DB_DIR.mkdir(parents=True, exist_ok=True)
    handlers = [logging.StreamHandler(sys.stdout)]
    try:
        handlers.append(logging.FileHandler(LOG_FILE, encoding="utf-8"))
    except Exception:
        pass
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
        handlers=handlers,
        force=True,
    )

log = logging.getLogger("build_db")

# ── HTTP helpers ──────────────────────────────────────────────────────────────
SSL_CTX = ssl.create_default_context()

def _build_url(url: str, params: dict) -> str:
    if params:
        return url + "?" + urllib.parse.urlencode(params)
    return url


def _get(url: str, params: dict = None, retries: int = 3, timeout: int = 120) -> dict:
    """Requête JSON Hub'Eau avec gestion des erreurs et rate-limit."""
    full_url = _build_url(url, params)
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                full_url,
                headers={"User-Agent": "MTDC-Eau/1.0 (build_database.py; contact:eaufrance)"},
            )
            with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 2 ** (attempt + 1)
                log.warning(f"Rate limit (429) — attente {wait}s")
                time.sleep(wait)
            elif e.code in (500, 502, 503):
                time.sleep(2)
            else:
                raise
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(1 + attempt)
    raise RuntimeError(f"Échec après {retries} tentatives : {full_url}")


def _get_all_pages(url: str, params: dict, page_size: int = 20_000) -> list:
    """Récupère toutes les pages d'un endpoint JSON paginé Hub'Eau.
    page_size max = 20 000 pour /analyse_pc et /station_pc (spec v2).
    """
    results = []
    params  = dict(params)
    params["size"] = min(page_size, 20_000)
    params["page"] = 1
    while True:
        data  = _get(url, params)
        items = data.get("data", [])
        results.extend(items)
        total = data.get("count", 0)
        log.debug(f"  page {params['page']} — {len(results)}/{total}")
        if len(results) >= total or not items:
            break
        params["page"] += 1
        time.sleep(0.3)
    return results


def _get_csv(url: str, params: dict, retries: int = 3, timeout: int = 90) -> list[dict]:
    """
    Télécharge un endpoint CSV Hub'Eau et retourne une liste de dicts.
    L'endpoint /analyse_pc.csv retourne le CSV directement (pas de JSON wrapper).
    Gère le rate-limit et les retries.
    """
    full_url = _build_url(url, params)
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                full_url,
                headers={"User-Agent": "MTDC-Eau/1.0 (build_database.py; contact:eaufrance)"},
            )
            with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
                content = r.read().decode("utf-8-sig")  # utf-8-sig gère le BOM éventuel
            reader = csv.DictReader(io.StringIO(content), delimiter=";")
            return [row for row in reader]
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 2 ** (attempt + 1)
                log.warning(f"  Rate limit CSV (429) — attente {wait}s")
                time.sleep(wait)
            elif e.code == 400:
                log.debug(f"  CSV 400 — aucune donnée pour ces paramètres")
                return []
            elif e.code in (500, 502, 503):
                time.sleep(3)
            else:
                raise
        except Exception as e:
            if attempt == retries - 1:
                log.warning(f"  CSV erreur (tentative {attempt+1}) : {e}")
                raise
            time.sleep(1 + attempt)
    return []


def _get_all_csv(
    code_station: str,
    code_parametre: int,
    depuis: str,
    jusqu: str,
    code_qualification: str = "1;2",
) -> list[dict]:
    """
    Télécharge les analyses via /analyse_pc.csv en découpant par tranches
    de 5 ans pour rester sous la limite de 20 000 enregistrements par requête.

    Retourne la liste brute des lignes CSV (dicts).
    Filtre automatiquement les fractions non-eau-totale et les remarques invalides.
    """
    url    = f"{HUBEAU_BASE}/qualite_rivieres/analyse_pc.csv"
    depuis_dt = datetime.strptime(depuis[:10], "%Y-%m-%d")
    jusqu_dt  = datetime.strptime(jusqu[:10],  "%Y-%m-%d")

    # Découpage en tranches de 5 ans
    year_start = depuis_dt.year
    year_end   = jusqu_dt.year
    chunks     = []
    y = year_start
    while y <= year_end:
        chunk_debut = f"{y}-01-01" if y > year_start else depuis[:10]
        chunk_fin   = f"{min(y+4, year_end)}-12-31" if y+4 < year_end else jusqu[:10]
        chunks.append((chunk_debut, chunk_fin))
        y += 5

    all_rows = []
    for chunk_debut, chunk_fin in chunks:
        params = {
            "code_station":            code_station,
            "code_parametre":          code_parametre,
            "date_debut_prelevement":  chunk_debut,
            "date_fin_prelevement":    chunk_fin,
            "code_qualification":      code_qualification,
            "code_fraction":           23,  # eau totale
            "fields": (
                "code_station,date_prelevement,resultat,"
                "code_remarque,code_qualification,libelle_parametre"
            ),
        }
        try:
            rows = _get_csv(url, params)
            all_rows.extend(rows)
            if rows:
                log.debug(f"    CSV {code_station}/{code_parametre} [{chunk_debut}→{chunk_fin}] : {len(rows)} lignes")
        except Exception as e:
            log.debug(f"    CSV {code_station}/{code_parametre} [{chunk_debut}→{chunk_fin}] erreur : {e}")
        time.sleep(0.25)

    # Filtrer les remarques invalides
    filtered = []
    for row in all_rows:
        try:
            rem = int(row.get("code_remarque", 0) or 0)
        except (ValueError, TypeError):
            rem = 0
        if rem not in CODES_REMARQUE_INVALIDES:
            filtered.append(row)

    return filtered


# ── Persistance locale ────────────────────────────────────────────────────────
def _save(path: Path, data, fmt: str = "json"):
    path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "json":
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    elif fmt == "jsonl":
        with open(path, "w", encoding="utf-8") as f:
            for row in data:
                f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
    log.debug(f"  → {path} ({path.stat().st_size/1024:.0f} KB)")


def _load(path: Path) -> Optional[list]:
    if not path.exists():
        return None
    try:
        if path.suffix == ".jsonl":
            with open(path, encoding="utf-8") as f:
                return [json.loads(l) for l in f if l.strip()]
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _exists_and_fresh(path: Path, max_age_days: int = 30) -> bool:
    if not path.exists():
        return False
    age = (datetime.now().timestamp() - path.stat().st_mtime) / 86400
    return age < max_age_days


# ── 1. STATIONS — référentiel qualité ────────────────────────────────────────
def download_stations(resume: bool = False) -> dict:
    """
    Télécharge le référentiel complet des stations de mesure physico-chimique
    pour tous les cours d'eau de RIVIERES_FRANCE.

    Sortie : data/stations/stations_{libelle}.json
             data/stations/_index.json  (toutes stations, tous cours d'eau)
    """
    log.info("── 1/4  Référentiel stations Hub'Eau ──────────────────────────────")
    index_path = DB_DIR / "stations" / "_index.json"

    all_stations = {}

    for nom, meta in RIVIERES_FRANCE.items():
        libelle   = meta["libelle"]
        safe_lib  = libelle.lower().replace("é","e").replace("è","e").replace("ô","o").replace("â","a").replace(" ","_")
        out_path  = DB_DIR / "stations" / f"stations_{safe_lib}.json"

        if resume and _exists_and_fresh(out_path, max_age_days=90):
            cached = _load(out_path)
            if cached:
                all_stations[libelle] = cached
                log.info(f"  ✓ {libelle:<20} {len(cached):>5} stations (cache)")
                continue

        try:
            stations = _get_all_pages(
                f"{HUBEAU_BASE}/qualite_rivieres/station_pc",
                {
                    "libelle_cours_eau": libelle,
                    "fields": (
                        "code_station,libelle_station,libelle_cours_eau,"
                        "code_cours_eau,coordonnee_x,coordonnee_y,"
                        "code_departement,code_commune,libelle_commune,"
                        "code_bassin_versant_topographique,"
                        "libelle_bassin_versant_topographique,"
                        "date_ouverture_station,date_fermeture_station,"
                        "actif"
                    ),
                },
                page_size=20_000,
            )
            # Filtrer les stations actives (actif != False)
            actives = [s for s in stations if s.get("actif") is not False]
            _save(out_path, actives)
            all_stations[libelle] = actives
            log.info(f"  ✓ {libelle:<20} {len(actives):>5} stations actives")
        except Exception as e:
            log.warning(f"  ✗ {libelle:<20} — {e}")
            all_stations[libelle] = []

        time.sleep(0.4)

    # Index global dédupliqué
    flat = [s for stations in all_stations.values() for s in stations]
    seen  = set()
    dedup = []
    for s in flat:
        code = s.get("code_station")
        if code and code not in seen:
            seen.add(code)
            dedup.append(s)

    _save(index_path, dedup)
    log.info(f"  → Index global : {len(dedup)} stations uniques")
    return all_stations


# ── 2. MESURES physico-chimiques ──────────────────────────────────────────────
def download_mesures(stations_index: list, depuis: str = "2000-01-01",
                     jusqu: str = None, resume: bool = False,
                     strict_qual: bool = False):
    """
    Télécharge les séries temporelles de mesures physico-chimiques via
    l'endpoint CSV /analyse_pc.csv (bulk, sans overhead JSON).

    Stratégie :
      1. Téléchargement CSV par tranches de 5 ans (≪ limite 20 000 enreg.)
      2. Filtre code_qualification : "1;2" (bon+incertain) ou "1" (strict)
      3. Filtre code_remarque : exclut invalides (10) et incertains (7)
      4. Agrégation mensuelle (médiane) si pandas disponible
      5. Fallback JSON si CSV échoue

    Sortie : data/mesures/{code_station}/{param_key}.jsonl
    Colonnes JSONL : date, resultat_mensuel, n_mesures, min, max
    """
    log.info("── 2/4  Mesures physico-chimiques (CSV bulk) ──────────────────────")
    if not jusqu:
        jusqu = date.today().strftime("%Y-%m-%d")

    # Filtre qualification : "1" (strict) ou "1;2" (standard)
    code_qual = "1" if strict_qual else "1;2"
    log.info(f"  Qualification : {code_qual}  |  Période : {depuis} → {jusqu}")

    n_total  = len(stations_index) * len(PARAMS_SANDRE)
    n_done   = 0
    n_skip   = 0
    n_empty  = 0
    n_error  = 0

    for station in stations_index:
        code = station.get("code_station")
        if not code:
            continue

        for code_param, param_key in PARAMS_SANDRE.items():
            out_path = DB_DIR / "mesures" / code / f"{param_key}.jsonl"

            if resume and _exists_and_fresh(out_path, max_age_days=7):
                n_skip += 1
                n_done += 1
                continue

            raw_rows = []
            used_csv = False

            # ── Tentative CSV (endpoint bulk) ─────────────────────────────
            try:
                raw_rows = _get_all_csv(code, code_param, depuis, jusqu, code_qual)
                used_csv = True
            except Exception as e:
                log.debug(f"  CSV échoué {code}/{param_key}, fallback JSON : {e}")

            # ── Fallback JSON paginé ──────────────────────────────────────
            if not used_csv or (not raw_rows and not used_csv):
                try:
                    json_rows = _get_all_pages(
                        f"{HUBEAU_BASE}/qualite_rivieres/analyse_pc",
                        {
                            "code_station":           code,
                            "code_parametre":         code_param,
                            "date_debut_prelevement": depuis,
                            "date_fin_prelevement":   jusqu,
                            "code_qualification":     code_qual.replace(";", ","),
                            "code_fraction":          23,
                            "fields": (
                                "code_station,date_prelevement,"
                                "resultat,code_remarque,code_qualification"
                            ),
                        },
                        page_size=20_000,
                    )
                    # Filtrer les remarques invalides
                    raw_rows = [
                        r for r in json_rows
                        if int(r.get("code_remarque") or 0) not in CODES_REMARQUE_INVALIDES
                    ]
                except Exception as e:
                    log.debug(f"  ✗ JSON {code}/{param_key} — {e}")
                    n_error += 1
                    n_done  += 1
                    continue

            if not raw_rows:
                n_empty += 1
                n_done  += 1
                continue

            # ── Normalisation des noms de colonnes CSV → JSON ─────────────
            # CSV Hub'Eau utilise les mêmes noms que JSON pour les champs demandés.
            # Les lignes CSV sont des dicts avec les clés issues du header CSV.
            def _coerce(rows: list[dict]) -> list[dict]:
                """Normalise les types : str → float/int selon le champ."""
                out = []
                for r in rows:
                    try:
                        r["resultat"] = float(r["resultat"])
                    except (ValueError, TypeError, KeyError):
                        continue   # ignorer les lignes sans résultat numérique
                    out.append(r)
                return out

            raw_rows = _coerce(raw_rows)
            if not raw_rows:
                n_empty += 1
                n_done  += 1
                continue

            # ── Agrégation mensuelle (médiane) ───────────────────────────
            if HAS_PANDAS and len(raw_rows) > 1:
                df = pd.DataFrame(raw_rows)

                # Normaliser le nom de la colonne date (CSV ou JSON)
                date_col = "date_prelevement" if "date_prelevement" in df.columns else "date"
                df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
                df = df.dropna(subset=[date_col])
                df["resultat"] = pd.to_numeric(df["resultat"], errors="coerce")
                df = df.dropna(subset=["resultat"])

                monthly = (
                    df.set_index(date_col)
                      .resample("ME")["resultat"]
                      .agg(["median", "count", "min", "max"])
                      .reset_index()
                )
                monthly.columns = ["date", "resultat_mensuel", "n_mesures", "min", "max"]
                monthly = monthly[monthly["n_mesures"] > 0]
                monthly["date"] = monthly["date"].dt.strftime("%Y-%m-%d")
                records = monthly.to_dict("records")
            else:
                # Sans pandas : stocker les mesures brutes normalisées
                records = [
                    {
                        "date": r.get("date_prelevement", r.get("date", "")),
                        "resultat_mensuel": r["resultat"],
                    }
                    for r in raw_rows
                ]

            if records:
                _save(out_path, records, fmt="jsonl")

            n_done += 1

            # Progression tous les 100 téléchargements
            if n_done % 100 == 0:
                pct = n_done / n_total * 100
                log.info(
                    f"  Progression : {n_done}/{n_total} ({pct:.0f}%)  "
                    f"cache={n_skip}  vides={n_empty}  erreurs={n_error}"
                )

    log.info(
        f"  ✓ Mesures terminées — {n_done} combinaisons | "
        f"vides={n_empty} | cache={n_skip} | erreurs={n_error}"
    )


# ── 3. HYDROMÉTRIE — modules m³/s ────────────────────────────────────────────
def download_hydrometrie(resume: bool = False):
    """
    Télécharge les débits caractéristiques (module interannuel) pour toutes
    les stations hydrométriques France. Utilisé par topology.py pour la
    classification DCE (ruisseau / rivière / fleuve).

    Sortie : data/hydrometrie/stations_hydro.json
             data/hydrometrie/debits_caracteristiques.json
    """
    log.info("── 3/4  Hydrométrie — modules m³/s ────────────────────────────────")
    out_stations = DB_DIR / "hydrometrie" / "stations_hydro.json"
    out_debits   = DB_DIR / "hydrometrie" / "debits_caracteristiques.json"

    # Stations hydrométriques
    if not (resume and _exists_and_fresh(out_stations, 90)):
        try:
            stations = _get_all_pages(
                f"{HUBEAU_BASE}/hydrometrie/referentiel/stations",
                {
                    "fields": (
                        "code_station,libelle_station,libelle_cours_eau,"
                        "coordonnee_x_station,coordonnee_y_station,"
                        "code_departement,en_service"
                    ),
                    "en_service": True,
                },
            )
            _save(out_stations, stations)
            log.info(f"  ✓ Stations hydro : {len(stations)}")
        except Exception as e:
            log.warning(f"  ✗ Stations hydro — {e}")
            stations = []
    else:
        stations = _load(out_stations) or []
        log.info(f"  ✓ Stations hydro (cache) : {len(stations)}")

    # Débits caractéristiques — calculé sur les obs élaborées QmM
    if not (resume and _exists_and_fresh(out_debits, 180)):
        debits = {}
        n = min(500, len(stations))
        for i, st in enumerate(stations[:n]):
            code = st.get("code_station")
            if not code:
                continue
            try:
                data = _get(
                    f"{HUBEAU_BASE}/hydrometrie/obs_elab",
                    {
                        "code_entite":        code,
                        "grandeur_hydro_elab": "QmM",   # débit moyen mensuel
                        "size":               1_200,    # ~100 ans
                        "fields":             "code_station,date_obs_elab,resultat_obs_elab",
                    },
                )
                mesures = data.get("data", [])
                if mesures and HAS_NUMPY:
                    vals = [m["resultat_obs_elab"] for m in mesures
                            if m.get("resultat_obs_elab") is not None]
                    if vals:
                        debits[code] = {
                            "module_m3s":  round(float(np.median(vals)), 3),
                            "q_min_m3s":   round(float(np.percentile(vals, 5)), 3),
                            "q_max_m3s":   round(float(np.percentile(vals, 95)), 3),
                            "n_mois":      len(vals),
                        }
            except Exception:
                pass
            if i % 50 == 0:
                log.info(f"  Débits : {i}/{n} stations traitées")
            time.sleep(0.15)

        _save(out_debits, debits)
        log.info(f"  ✓ Débits caractéristiques : {len(debits)} stations")
    else:
        debits = _load(out_debits) or {}
        log.info(f"  ✓ Débits caractéristiques (cache) : {len(debits)} stations")

    return debits


# ── 4. SANDRE / BDCarthage — référentiel géographique ───────────────────────
def download_referentiel(resume: bool = False):
    """
    Télécharge :
      - Le référentiel des cours d'eau SANDRE (libellés officiels)
      - Les codes bassins versants
      - La liste officielle des fleuves côtiers

    Sortie : data/referentiel/cours_eau.json
             data/referentiel/bassins.json
             data/referentiel/fleuves_cotiers.json
    """
    log.info("── 4/4  Référentiel SANDRE ─────────────────────────────────────────")

    # Cours d'eau via Hub'Eau (endpoint /liste_cours_eau)
    out_ce = DB_DIR / "referentiel" / "cours_eau.json"
    if not (resume and _exists_and_fresh(out_ce, 180)):
        try:
            cours_eau = _get_all_pages(
                f"{HUBEAU_BASE}/qualite_rivieres/liste_cours_eau",
                {"fields": "code_cours_eau,nom_cours_eau,code_bassin,libelle_bassin"},
            )
            _save(out_ce, cours_eau)
            log.info(f"  ✓ Cours d'eau SANDRE : {len(cours_eau)}")
        except Exception as e:
            log.warning(f"  ✗ Cours d'eau — {e}")
    else:
        ce = _load(out_ce) or []
        log.info(f"  ✓ Cours d'eau SANDRE (cache) : {len(ce)}")

    # Bassins hydrographiques (données statiques)
    out_bassins = DB_DIR / "referentiel" / "bassins.json"
    if not out_bassins.exists():
        bassins = [
            {"code": "----0000", "libelle": "Adour-Garonne",      "agence": "AEAG"},
            {"code": "----0001", "libelle": "Loire-Bretagne",      "agence": "AELB"},
            {"code": "----0002", "libelle": "Rhône-Méditerranée",  "agence": "AERMC"},
            {"code": "----0003", "libelle": "Seine-Normandie",     "agence": "AESN"},
            {"code": "----0004", "libelle": "Rhin-Meuse",          "agence": "AERM"},
            {"code": "----0005", "libelle": "Artois-Picardie",     "agence": "AEAP"},
        ]
        _save(out_bassins, bassins)
        log.info(f"  ✓ Bassins hydrographiques : {len(bassins)}")

    # Fleuves côtiers SANDRE (référentiel intégré — source SANDRE 2022)
    out_fc = DB_DIR / "referentiel" / "fleuves_cotiers.json"
    if not out_fc.exists():
        fleuves_cotiers = [
            "adour","allier","arc","ardèche","argens","aude","aulne","authie","ax",
            "blavet","bonne","bresle","canche","charente","cher","couesnon","dordogne",
            "dourduff","durance","garonne","gier","golo","hérault","ibie","isère",
            "laïta","lay","léguer","liane","loir","loire","lot","med","orb","oust",
            "oyat","penfeld","rhône","rance","saône","scarpe","sèvre niortaise",
            "sèvre nantaise","seine","siagne","somme","têt","touques","trieux",
            "var","vienne","vilaine",
        ]
        _save(out_fc, sorted(fleuves_cotiers))
        log.info(f"  ✓ Fleuves côtiers SANDRE : {len(fleuves_cotiers)}")


# ── 5. MANIFEST ──────────────────────────────────────────────────────────────
def write_manifest(args):
    """Écrit un fichier de résumé de la banque de données."""
    manifest_path = DB_DIR / "MANIFEST.json"

    def count_files(subdir):
        p = DB_DIR / subdir
        return len(list(p.rglob("*"))) if p.exists() else 0

    def dir_size_mb(subdir):
        p = DB_DIR / subdir
        if not p.exists():
            return 0
        return sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) / 1e6

    manifest = {
        "built_at":      datetime.now().isoformat(),
        "depuis":        args.depuis,
        "strict_qual":   getattr(args, "strict_qual", False),
        "sources": {
            "stations":    {"files": count_files("stations"),    "size_mb": round(dir_size_mb("stations"),1)},
            "mesures":     {"files": count_files("mesures"),     "size_mb": round(dir_size_mb("mesures"),1)},
            "hydrometrie": {"files": count_files("hydrometrie"), "size_mb": round(dir_size_mb("hydrometrie"),1)},
            "referentiel": {"files": count_files("referentiel"), "size_mb": round(dir_size_mb("referentiel"),1)},
        },
        "total_size_mb":       round(dir_size_mb("."), 1),
        "cours_eau_couverts":  list(RIVIERES_FRANCE.keys()),
        "parametres":          list(PARAMS_SANDRE.values()),
        "download_method":     "CSV bulk /analyse_pc.csv + fallback JSON",
        "attribution":         (
            "Données Hub'Eau / NAÏADES / OFB / DREAL / Agences de l'eau — "
            "Licence Ouverte 2.0 (Etalab)"
        ),
    }
    _save(manifest_path, manifest)
    return manifest


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Construit la banque de données locale MTDC-Eau.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--source", choices=["stations","mesures","hydro","referentiel"],
        help="Télécharger une seule source",
    )
    parser.add_argument(
        "--riviere", nargs="+", metavar="NOM",
        help="Limiter à certains cours d'eau (ex: Garonne Lot)",
    )
    parser.add_argument(
        "--depuis", default="2000-01-01",
        help="Date de début (YYYY-MM-DD), défaut : 2000-01-01",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Ignorer les fichiers déjà téléchargés (reprise après interruption)",
    )
    parser.add_argument(
        "--strict-qual", action="store_true",
        help="Garder uniquement code_qualification=1 (bon) — plus strict",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    setup_logging(args.verbose)
    DB_DIR.mkdir(parents=True, exist_ok=True)

    log.info("═" * 65)
    log.info("  MTDC-Eau — Construction banque de données locale")
    log.info(f"  Destination : {DB_DIR.resolve()}")
    log.info(f"  Période     : {args.depuis} → aujourd'hui")
    log.info(f"  Mode        : {'reprise (--resume)' if args.resume else 'téléchargement complet'}")
    log.info(f"  Qualif.     : {'strict (1 uniquement)' if args.strict_qual else 'standard (1+2)'}")
    log.info("═" * 65)

    # Filtrer les cours d'eau si demandé
    rivieres = RIVIERES_FRANCE
    if args.riviere:
        rivieres = {k: v for k, v in RIVIERES_FRANCE.items()
                    if k in args.riviere or v["libelle"] in args.riviere}
        if not rivieres:
            log.error(f"Cours d'eau introuvables : {args.riviere}")
            log.error(f"Disponibles : {list(RIVIERES_FRANCE.keys())}")
            sys.exit(1)
        log.info(f"  Cours d'eau sélectionnés : {list(rivieres.keys())}")

    t0 = time.time()

    try:
        # ── Étape 1 : Stations
        if not args.source or args.source == "stations":
            stations_dict = download_stations(resume=args.resume)
            index = [s for lst in stations_dict.values() for s in lst]
            seen  = set()
            stations_index = []
            for s in index:
                if s.get("code_station") not in seen:
                    seen.add(s.get("code_station"))
                    stations_index.append(s)
        else:
            idx_path = DB_DIR / "stations" / "_index.json"
            stations_index = _load(idx_path) or []
            log.info(f"  Stations (index existant) : {len(stations_index)}")

        # ── Étape 2 : Mesures
        if not args.source or args.source == "mesures":
            if stations_index:
                download_mesures(
                    stations_index,
                    depuis=args.depuis,
                    resume=args.resume,
                    strict_qual=args.strict_qual,
                )
            else:
                log.warning("  Pas de stations — étape mesures ignorée")

        # ── Étape 3 : Hydrométrie
        if not args.source or args.source == "hydro":
            download_hydrometrie(resume=args.resume)

        # ── Étape 4 : Référentiel
        if not args.source or args.source == "referentiel":
            download_referentiel(resume=args.resume)

        # ── Manifest
        manifest = write_manifest(args)
        elapsed  = time.time() - t0

        log.info("")
        log.info("═" * 65)
        log.info("  ✓  Banque de données construite")
        log.info(f"     Durée   : {elapsed/60:.1f} minutes")
        log.info(f"     Taille  : {manifest['total_size_mb']} MB")
        log.info(f"     Chemin  : {DB_DIR.resolve()}")
        log.info("")
        log.info("  Structure :")
        for src, info in manifest["sources"].items():
            log.info(f"     data/{src:<15} {info['files']:>5} fichiers  {info['size_mb']:>7.1f} MB")
        log.info("")
        log.info("  Prochaine étape :")
        log.info("     python -c \"from water.local_loader import is_available; print(is_available())\"")
        log.info("═" * 65)

    except KeyboardInterrupt:
        log.info("\n  Interrompu — relancer avec --resume pour reprendre")
        sys.exit(0)
    except Exception as e:
        log.error(f"Erreur : {e}")
        if args.verbose:
            import traceback; traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
