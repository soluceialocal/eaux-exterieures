#!/usr/bin/env python3
"""
generate_stations_json.py — Génère stations.json pour le frontend État des Eaux
================================================================================

Lit les données Hub'Eau téléchargées par build_database.py, applique
le moteur MTDC v3.3 station par station, et produit stations.json
(déployé sur GitHub Pages avec les fichiers statiques).

Pipeline complet :
  1. python build_database.py --riviere Garonne Lot Dordogne
  2. python generate_stations_json.py
  3. git add stations.json && git commit -m "data: update" && git push

Usage :
  python generate_stations_json.py                   # tout générer
  python generate_stations_json.py --n-max 50        # limiter
  python generate_stations_json.py --dept 31 47 46   # par département
  python generate_stations_json.py --phi1 1.3465     # seuil MTDC
  python generate_stations_json.py --out stations.json
"""

import json
import sys
import logging
from pathlib import Path
from datetime import date, timedelta

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

sys.path.insert(0, str(Path(__file__).parent))
try:
    from mtdc.core import run_mtdc
    HAS_MTDC = True
except ImportError:
    HAS_MTDC = False

# ── Chemins ───────────────────────────────────────────────────────────────────
DB_DIR   = Path(__file__).parent / "data"
OUT_FILE = Path(__file__).parent / "stations.json"

# ── Paramètres MTDC (calibrés sur données France) ─────────────────────────────
PHI1    = 1.3465   # seuil structurant calibré
N_BASE  = 20       # longueur médiane causale
SMOOTH  = 5        # lissage Φ̃
T_ALARM = 5        # durée minimale d'alarme (mois consécutifs)

# ── Référentiels ──────────────────────────────────────────────────────────────
PARAMS_LABELS = {
    "temperature":  "Température",
    "ph":           "pH",
    "conductivite": "Conductivité",
    "turbidite":    "Turbidité",
    "o2_dissous":   "Oxygène dissous",
    "nitrates":     "Nitrates",
    "phosphore":    "Phosphore",
    "mes":          "MES",
    "dbo5":         "DBO5",
    "ammonium":     "Ammonium",
}

PARAMS_UNITS = {
    "temperature":  "°C",
    "ph":           "",
    "conductivite": "µS/cm",
    "turbidite":    "NTU",
    "o2_dissous":   "mg/L",
    "nitrates":     "mg/L",
    "phosphore":    "µg/L",
    "mes":          "mg/L",
    "dbo5":         "mg/L",
    "ammonium":     "mg/L",
}

# Ordre de priorité pour l'affichage des paramètres
PARAMS_PRIORITY = [
    "ph", "temperature", "turbidite", "conductivite",
    "nitrates", "o2_dissous", "phosphore", "mes", "ammonium", "dbo5"
]

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# I/O helpers
# ─────────────────────────────────────────────────────────────────────────────

def load_jsonl(path: Path) -> list:
    """Charge un fichier JSONL ligne par ligne."""
    if not path.exists():
        return []
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Inférences métier (usage, type, coordonnées)
# ─────────────────────────────────────────────────────────────────────────────

def infer_usage(libelle: str) -> str:
    lib = libelle.lower()
    if any(w in lib for w in ["plage", "baignade", "bain", "piscine", "loisir", "camping", "nautique"]):
        return "baignade"
    if any(w in lib for w in ["pompage", "prise d", "captage", "adduction", "aep"]):
        return "irrigation"
    if any(w in lib for w in ["pêche", "peche", "piscicole", "alevin"]):
        return "peche"
    if any(w in lib for w in ["réserve", "reserve", "écologi", "ecologi", "ornith", "natura"]):
        return "ecologie"
    return "polyvalent"


def infer_type(libelle: str) -> str:
    lib = libelle.lower()
    if any(w in lib for w in ["lac", "étang", "etang", "plan d", "retenue", "barrage", "réservoir"]):
        return "lac"
    return "riviere"


def get_coords(station: dict):
    """
    Extrait les coordonnées WGS84 [lat, lon] d'une station Hub'Eau.
    Gère Lambert 93 → WGS84 si pyproj est disponible.
    """
    # Essaie d'abord des champs latitude/longitude explicites
    lat = station.get("latitude")
    lon = station.get("longitude")
    if lat and lon:
        try:
            return [round(float(lat), 5), round(float(lon), 5)]
        except (ValueError, TypeError):
            pass

    cx = station.get("coordonnee_x")
    cy = station.get("coordonnee_y")
    if not cx or not cy:
        return None
    try:
        cx, cy = float(cx), float(cy)
    except (ValueError, TypeError):
        return None

    # Si ça ressemble à du WGS84 (France : lon ∈ [-6, 10], lat ∈ [41, 52])
    if -6 <= cx <= 10 and 41 <= cy <= 52:
        return [round(cy, 5), round(cx, 5)]

    # Lambert 93 → WGS84 via pyproj
    try:
        from pyproj import Transformer
        t = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)
        lon_wgs, lat_wgs = t.transform(cx, cy)
        if 41 <= lat_wgs <= 52 and -6 <= lon_wgs <= 10:
            return [round(lat_wgs, 5), round(lon_wgs, 5)]
    except Exception:
        pass

    # Approximation linéaire grossière si pyproj absent
    # Lambert 93 : x ∈ [100 000, 1 300 000], y ∈ [6 000 000, 7 200 000]
    if 100_000 < cx < 1_300_000 and 6_000_000 < cy < 7_200_000:
        lon_approx = -5.0 + (cx - 100_000) / (1_300_000 - 100_000) * 15.0
        lat_approx = 41.0 + (cy - 6_000_000) / (7_200_000 - 6_000_000) * 11.0
        return [round(lat_approx, 3), round(lon_approx, 3)]

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Calcul MTDC et interprétation
# ─────────────────────────────────────────────────────────────────────────────

def run_param(signal_vals: list, dates: list) -> dict | None:
    """Applique le pipeline MTDC sur une série d'une station/paramètre."""
    if not HAS_MTDC or not HAS_NUMPY:
        return None
    if len(signal_vals) < max(N_BASE, 10):
        return None

    try:
        arr = np.array(signal_vals, dtype=float)
        df  = run_mtdc(arr, n_base=N_BASE, smooth=SMOOTH, phi1=PHI1, T=T_ALARM)
        df["date"] = dates[:len(df)]

        last_alarm = int(df["alarm"].iloc[-1])
        last_phis  = float(df["PhiS"].iloc[-1])
        last_g     = float(df["G"].iloc[-1])
        g_min      = float(df["G_min"].iloc[-1])

        if last_alarm == 1:
            status = "alert"
        elif last_phis > PHI1 * 0.85 and last_g > g_min * 0.5:
            status = "watch"
        else:
            status = "stable"

        return {"status": status, "df": df}
    except Exception as e:
        log.debug(f"  MTDC erreur : {e}")
        return None


def determine_status(param_results: dict) -> str:
    statuses = [r["status"] for r in param_results.values() if r]
    if "alert" in statuses:
        return "alert"
    if "watch" in statuses:
        return "watch"
    return "stable"


def derive_days(df) -> tuple:
    """Retourne (jours_stable, jours_derive) depuis la série MTDC."""
    if df is None or len(df) == 0:
        return 0, 0
    alarms = df["alarm"].values
    phis   = df["PhiS"].values

    derive = 0
    for i in range(len(alarms) - 1, -1, -1):
        if alarms[i] == 1 or phis[i] > PHI1 * 0.9:
            derive += 1
        else:
            break

    stable = 0
    start = len(alarms) - 1 - derive
    for i in range(start, -1, -1):
        if alarms[i] == 0 and phis[i] <= PHI1:
            stable += 1
        else:
            break

    # Données mensuelles → ×30 jours
    return stable * 30, derive * 30


def build_signal_text(param_results: dict, status: str) -> str:
    """Construit le texte du signal principal (HTML)."""
    # Trouver le paramètre le plus dégradé
    worst_param = None
    worst_score = -999.0
    for pkey, res in param_results.items():
        if not res or res["df"] is None:
            continue
        df = res["df"]
        score = float(df["PhiS"].iloc[-1]) * (1 + max(0.0, float(df["G"].iloc[-1])))
        if score > worst_score:
            worst_score = score
            worst_param = pkey

    if not worst_param:
        return "Surveillance en cours"

    label = PARAMS_LABELS.get(worst_param, worst_param)
    df    = param_results[worst_param]["df"]
    last_g = float(df["G"].iloc[-1])
    direction = "en hausse" if last_g > 0 else "en baisse"

    if status == "alert":
        return (f'<span class="highlight up">{label} {direction} accélérante</span>'
                f' — seuil MTDC franchi')
    elif status == "watch":
        return (f'<span class="highlight warn">{label} {direction}</span>'
                f' — dérive détectée, surveillance renforcée')
    else:
        return f'{label} stable — dans la variabilité normale'


def build_history(df_best, n: int = 25) -> str:
    """Chaîne d'historique 's'/'w'/'a' sur les n derniers points mensuels."""
    if df_best is None:
        return "s" * n
    phis   = df_best["PhiS"].values
    alarms = df_best["alarm"].values
    take   = min(n, len(phis))
    chars  = []
    for i in range(len(phis) - take, len(phis)):
        if alarms[i] == 1:
            chars.append("a")
        elif phis[i] > PHI1 * 0.85:
            chars.append("w")
        else:
            chars.append("s")
    while len(chars) < n:
        chars.insert(0, "s")
    return "".join(chars)


def build_params_list(param_results: dict) -> list:
    """Liste des 3 paramètres à afficher sur la fiche station."""
    shown = []
    for pkey in PARAMS_PRIORITY:
        if len(shown) >= 3:
            break
        res = param_results.get(pkey)
        if not res or res["df"] is None:
            continue
        df = res["df"]
        if len(df) == 0:
            continue

        last_val  = float(df["signal"].iloc[-1])
        last_phis = float(df["PhiS"].iloc[-1])
        last_g    = float(df["G"].iloc[-1])
        unit      = PARAMS_UNITS.get(pkey, "")
        label     = PARAMS_LABELS.get(pkey, pkey)

        if pkey == "ph":
            value_str = f"{last_val:.1f}"
        elif pkey == "temperature":
            value_str = f"{last_val:.1f}°C"
        elif pkey in ("conductivite",):
            value_str = f"{last_val:.0f} {unit}".strip()
        else:
            value_str = f"{last_val:.1f} {unit}".strip()

        if last_g > 0.02:
            trend_txt = "↑ en hausse"
            cls = "up" if last_phis > PHI1 * 0.9 else "warn"
        elif last_g < -0.02:
            trend_txt = "↓ en baisse"
            cls = "warn" if last_phis < 0.9 else "ok"
        else:
            trend_txt = "stable"
            cls = "ok"

        shown.append({"label": label, "value": value_str, "trend": trend_txt, "cls": cls})

    return shown


def build_advice(status: str) -> dict:
    if status == "alert":
        return {
            "cls":   "alert",
            "title": "⚠ Vérification recommandée",
            "text":  ("Le moteur MTDC détecte une dérive active sur ce point de mesure. "
                      "Un contrôle terrain est recommandé avant toute utilisation."),
        }
    elif status == "watch":
        return {
            "cls":   "watch",
            "title": "Surveillance renforcée",
            "text":  ("Une dérive est détectée mais reste dans les seuils normatifs. "
                      "Surveiller l'évolution sur les 7 prochains jours."),
        }
    else:
        return {
            "cls":   "stable",
            "title": "Qualité dans la variabilité normale",
            "text":  ("Aucune dérive significative détectée. Les paramètres suivis "
                      "restent dans le comportement habituel de ce point."),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Traitement d'une station
# ─────────────────────────────────────────────────────────────────────────────

def process_station(station: dict, idx: int) -> dict | None:
    code    = station.get("code_station", "")
    libelle = (station.get("libelle_station") or
               station.get("libelle_commune") or code)
    cours   = station.get("libelle_cours_eau", "")
    dept    = station.get("code_departement", "")

    mesures_dir = DB_DIR / "mesures" / code
    if not mesures_dir.exists():
        return None

    param_results = {}

    for pkey in PARAMS_LABELS:
        rows = load_jsonl(mesures_dir / f"{pkey}.jsonl")
        if len(rows) < 5:
            continue
        rows.sort(key=lambda r: r.get("date", ""))
        dates  = [r["date"] for r in rows]
        values = []
        for r in rows:
            try:
                values.append(float(r["resultat_mensuel"]))
            except (KeyError, ValueError, TypeError):
                pass
        if len(values) < 5:
            continue

        result = run_param(values, dates)
        if result:
            param_results[pkey] = result

    if not param_results:
        return None

    status     = determine_status(param_results)
    signal_txt = build_signal_text(param_results, status)

    # Paramètre avec le plus de données (pour sparklines et historique)
    best_param = max(
        param_results,
        key=lambda p: len(param_results[p]["df"]) if param_results[p] else 0,
        default=None,
    )
    df_best = param_results[best_param]["df"] if best_param else None

    jours_stable, jours_derive = derive_days(df_best)
    history     = build_history(df_best)
    params_list = build_params_list(param_results)
    advice      = build_advice(status)
    coords      = get_coords(station)

    # Datasets pour sparklines réelles
    datasets = None
    if df_best is not None and len(df_best) >= 10:
        take = min(90, len(df_best))
        datasets = {
            "dates":  list(df_best["date"].values[-take:]) if "date" in df_best.columns else [],
            "signal": [round(float(v), 3) for v in df_best["signal"].values[-take:]],
            "phis":   [round(float(v), 4) for v in df_best["PhiS"].values[-take:]],
        }

    # Date de dernière mesure
    last_date = None
    if df_best is not None and "date" in df_best.columns and len(df_best):
        last_date = str(df_best["date"].iloc[-1])

    today = date.today()
    if last_date and last_date >= today.isoformat():
        update_txt = "Aujourd'hui"
    elif last_date and last_date >= (today - timedelta(days=35)).isoformat():
        update_txt = "Ce mois-ci"
    else:
        update_txt = last_date or "Inconnu"

    river_txt = cours or code
    if dept:
        river_txt = f"{river_txt} ({dept})"

    return {
        "id":           f"s{idx + 1}",
        "code":         code,
        "name":         libelle,
        "river":        river_txt,
        "dept":         dept,
        "type":         infer_type(libelle + " " + cours),
        "usage":        infer_usage(libelle),
        "coords":       coords,
        "status":       status,
        "lastUpdate":   update_txt,
        "signal":       signal_txt,
        "jours_stable": jours_stable,
        "jours_derive": jours_derive,
        "params":       params_list,
        "advice":       advice,
        "history":      history,
        "datasets":     datasets,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Génère stations.json depuis les données Hub'Eau + MTDC"
    )
    parser.add_argument("--out",   default=str(OUT_FILE), help="Fichier de sortie")
    parser.add_argument("--n-max", type=int, default=200,  help="Nombre max de stations")
    parser.add_argument("--phi1",  type=float, default=PHI1, help="Seuil phi1 MTDC")
    parser.add_argument("--dept",  nargs="+", help="Filtrer par département(s) (ex: 31 47)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    global PHI1
    PHI1 = args.phi1

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s  %(message)s",
    )

    if not HAS_NUMPY or not HAS_PANDAS:
        log.error("numpy et pandas requis : pip install numpy pandas --break-system-packages")
        sys.exit(1)
    if not HAS_MTDC:
        log.error("Module mtdc introuvable. Lance ce script depuis le dossier MTDC-Eau-OpenSource/")
        sys.exit(1)

    index_path = DB_DIR / "stations" / "_index.json"
    if not index_path.exists():
        log.error(f"Index stations introuvable : {index_path}")
        log.error("Lance d'abord : python build_database.py")
        sys.exit(1)

    with open(index_path, encoding="utf-8") as f:
        stations = json.load(f)

    log.info(f"{len(stations)} stations dans l'index")

    if args.dept:
        stations = [s for s in stations if s.get("code_departement") in args.dept]
        log.info(f"→ {len(stations)} stations après filtre départements {args.dept}")

    stations = stations[:args.n_max]

    results = []
    for i, station in enumerate(stations):
        result = process_station(station, i)
        if result:
            results.append(result)
        if (i + 1) % 20 == 0:
            log.info(f"  {i + 1}/{len(stations)} — {len(results)} stations avec données")

    # Trier : alertes → vigilances → stables
    ORDER = {"alert": 0, "watch": 1, "stable": 2}
    results.sort(key=lambda s: ORDER.get(s["status"], 99))
    for i, s in enumerate(results):
        s["id"] = f"s{i + 1}"

    out_path = Path(args.out)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    n_alert  = sum(1 for s in results if s["status"] == "alert")
    n_watch  = sum(1 for s in results if s["status"] == "watch")
    n_stable = sum(1 for s in results if s["status"] == "stable")

    log.info(f"\n✓ {len(results)} stations exportées → {out_path}")
    log.info(f"  🔴 Alertes   : {n_alert}")
    log.info(f"  🟡 Vigilance : {n_watch}")
    log.info(f"  🟢 Stables   : {n_stable}")
    log.info(f"\nÉtape suivante :")
    log.info(f"  git add {out_path.name}")
    log.info(f"  git commit -m \"data: mise à jour données Hub'Eau\"")
    log.info(f"  git push origin main")


if __name__ == "__main__":
    main()
