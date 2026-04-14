#!/usr/bin/env python3
"""
bundle.py — Génère un fichier HTML auto-contenu pour distribution
==================================================================
Télécharge Chart.js et Leaflet depuis les CDN, les intègre directement
dans le HTML source. Produit un seul fichier .html portable (~1.5 Mo)
qui fonctionne hors-ligne et sans serveur.

Usage :
  python bundle.py                          # génère dans release/
  python bundle.py --out mon_fichier.html   # chemin personnalisé
  python bundle.py --version 1.0.0         # tag de version dans le nom

Le fichier généré peut être attaché directement à une GitHub Release.
"""

import re
import sys
import time
import argparse
import urllib.request
import urllib.error
import ssl
from pathlib import Path

# ── Sources CDN ───────────────────────────────────────────────────────────────
CDN_LIBS = [
    {
        "placeholder": "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.css",
        "tag": "link",
        "attr": "href",
    },
    {
        "placeholder": "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.js",
        "tag": "script",
        "attr": "src",
    },
    {
        "placeholder": "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js",
        "tag": "script",
        "attr": "src",
    },
]

SSL_CTX = ssl.create_default_context()

def fetch(url: str, retries: int = 3) -> str:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "MTDC-Eau bundle.py/1.0"},
            )
            with urllib.request.urlopen(req, timeout=30, context=SSL_CTX) as r:
                return r.read().decode("utf-8")
        except Exception as e:
            if attempt == retries - 1:
                raise RuntimeError(f"Impossible de télécharger {url} : {e}")
            print(f"  ⚠ Tentative {attempt+1} échouée, retry…")
            time.sleep(2)

def inline_css(content: str) -> str:
    return f"<style>\n{content}\n</style>"

def inline_js(content: str) -> str:
    return f"<script>\n{content}\n</script>"

def bundle(src_html: str, version: str = "") -> str:
    html = src_html

    for lib in CDN_LIBS:
        url = lib["placeholder"]
        print(f"  → Téléchargement {url.split('/')[-1]}…", end=" ", flush=True)
        try:
            content = fetch(url)
            print(f"OK ({len(content)//1024} Ko)")
        except RuntimeError as e:
            print(f"ERREUR — {e}")
            print("  ⚠ Lib non intégrée — le fichier bundle nécessitera une connexion pour cette lib.")
            continue

        if lib["tag"] == "link":
            # Remplace <link rel="stylesheet" href="URL"/> par <style>…</style>
            pattern = rf'<link[^>]+href=["\']?{re.escape(url)}["\']?[^>]*/?\s*>'
            html = re.sub(pattern, inline_css(content), html, flags=re.IGNORECASE)
        else:
            # Remplace <script src="URL"></script> par <script>…</script>
            pattern = rf'<script[^>]+src=["\']?{re.escape(url)}["\']?[^>]*>\s*</script>'
            html = re.sub(pattern, inline_js(content), html, flags=re.IGNORECASE)

    # Retirer la référence au service worker (inutile sans serveur)
    html = re.sub(
        r"if\s*\('serviceWorker'\s*in\s*navigator\).*?}\s*\)\s*;",
        "/* Service Worker désactivé en mode bundle */",
        html,
        flags=re.DOTALL,
    )

    # Retirer le lien manifest (pas de SW = pas de PWA install en mode bundle)
    html = re.sub(r'<link rel="manifest"[^>]+>', '', html)

    # Ajouter un commentaire de version en tête
    tag = f" — v{version}" if version else ""
    header = f"<!-- État des Eaux en Extérieur{tag} — bundle auto-contenu -->\n"
    html = header + html

    return html


def main():
    parser = argparse.ArgumentParser(description="Bundle HTML auto-contenu.")
    parser.add_argument("--src",     default="surveillance_eaux_v4.html",
                        help="Fichier source (défaut : surveillance_eaux_v4.html)")
    parser.add_argument("--out",     default=None,
                        help="Fichier de sortie (défaut : release/eaux-exterieures-vX.X.X.html)")
    parser.add_argument("--version", default="1.0.0", help="Numéro de version")
    args = parser.parse_args()

    src_path = Path(__file__).parent / args.src
    if not src_path.exists():
        print(f"Erreur : fichier source introuvable : {src_path}")
        sys.exit(1)

    out_name = args.out or f"eaux-exterieures-v{args.version}.html"
    out_path = Path(__file__).parent / "release" / out_name
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Bundle — État des Eaux en Extérieur v{args.version}")
    print(f"Source  : {src_path.name}")
    print(f"Sortie  : {out_path}")
    print()

    src_html = src_path.read_text(encoding="utf-8")
    bundled  = bundle(src_html, args.version)
    out_path.write_text(bundled, encoding="utf-8")

    size_kb = out_path.stat().st_size // 1024
    print()
    print(f"✓  Fichier généré : {out_path}")
    print(f"   Taille         : {size_kb} Ko")
    print()
    print("Prochaine étape :")
    print(f"  → Attacher '{out_path.name}' à la GitHub Release v{args.version}")


if __name__ == "__main__":
    main()
