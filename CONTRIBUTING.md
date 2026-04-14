# Contribuer — État des Eaux en Extérieur

Merci de l'intérêt pour ce projet ! Voici comment contribuer.

## Ce dont le projet a besoin en priorité

| Priorité | Type | Description |
|----------|------|-------------|
| 🔴 Haute | Validation terrain | Tester l'app sur de vraies données Hub'Eau |
| 🔴 Haute | Événement réel | Confirmer une alarme MTDC sur un bloom documenté (Lot 2019, Garonne 2003) |
| 🟠 Moyenne | Traduction | Adapter les conseils terrain par usage (pêche, irrigation…) |
| 🟠 Moyenne | Nouveaux cours d'eau | Ajouter des stations manquantes dans `water/parameters.py` |
| 🟡 Basse | UX mobile | Tests sur iPhone < iOS 16 et Android < 10 |

## Structure du projet

```
water/          → moteur MTDC (Python)
tests/          → tests unitaires (python3 tests/test_*.py)
surveillance_eaux_v4.html  → interface PWA
build_database.py          → téléchargement données Hub'Eau
```

## Faire tourner les tests

```bash
python3 tests/test_mtdc_core.py
python3 tests/test_topology.py
```

Les deux suites doivent passer à 100 % avant toute PR.

## Soumettre une Pull Request

1. Fork le repo
2. Crée une branche : `git checkout -b feature/ma-contribution`
3. Lance les tests : `python3 tests/test_mtdc_core.py`
4. Commit : `git commit -m "feat: description courte"`
5. Push et ouvre une PR vers `main`

## Convention de commits

```
feat:  nouvelle fonctionnalité
fix:   correction de bug
data:  ajout/correction données stations
docs:  documentation uniquement
test:  tests uniquement
```

## Données

Les données Hub'Eau sont sous **Licence Ouverte 2.0 (Etalab)**.
Ne jamais committer le dossier `data/` (ignoré par `.gitignore`).
Chaque contributeur télécharge sa propre banque avec `build_database.py`.

## Questions

Ouvre une Issue GitHub — toutes les questions sont bienvenues.
