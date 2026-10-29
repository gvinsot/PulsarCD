# Tests

- Python : `bash scripts/dev.sh python tests/test_<domaine>.py -q`.
  JavaScript : `bash scripts/dev.sh frontend tests/test_<domaine>.cjs`.
  Ensemble : `bash scripts/dev.sh check`.
- `pytest.ini` active asyncio et les xfail stricts. Lancer pytest depuis la
  racine avec `python -m pytest` (le script le fait automatiquement).
- Réutiliser `conftest.py` et les doubles du module voisin ; `tmp_path` et
  `monkeypatch` isolent les fichiers et variables d'environnement.
- Les fixtures HTTP de session partagent leurs doubles : restaurer les
  modifications apportées aux services ou à l'état global à la fin du test.
- Les tests Bash utilisent des exécutables factices dans un PATH temporaire.
  Préserver ce modèle : pas de registre, cluster ou dépôt distant réels.
- Les tests de stockage nécessitent VictoriaLogs (`vlogs`), ceux du coffre de
  récupération PostgreSQL (`PULSARCD_TEST_DATABASE_URL`). Employer uniquement
  des instances jetables, les tests y écrivent des données.
- Vérifier le résumé `-ra` : un service manquant produit des skips. Ne pas
  masquer un échec avec un skip ou un xfail ajouté pour rendre la suite verte.
