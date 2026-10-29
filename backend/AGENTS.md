# Backend

- `api.py` assemble l'application, son lifespan et ses services partagés.
  Ajouter les endpoints dans le module de domaine de `routes/` ; reprendre la
  façon dont les routes voisines accèdent aux services pour éviter les imports
  circulaires. Les routes preview, notifications et historique ont aussi leurs
  modules dédiés à la racine de `backend/`.
- Pour une route nouvelle ou modifiée, vérifier la politique centrale dans
  `auth_middleware.py` et tester les rôles non authentifié, viewer et admin
  selon les droits attendus. Examiner aussi les appelants MCP concernés.
- Garder les opérations réseau asynchrones dans les chemins async ; respecter
  les délais, annulations et fermetures des clients existants.
- Réutiliser `durable_json.py` pour la persistance JSON durable. Vérifier la
  compatibilité des données existantes lors d'un changement de schéma.
- Une modification de configuration peut toucher `config.py`, `config_file.py`,
  les routes de configuration, l'interface Settings et le compose Swarm.
- Pour les tests HTTP, partir des fixtures `client` et `auth_headers` de
  `tests/conftest.py` : leur lifespan remplace l'infrastructure par des doubles.
  Les tests de stockage utilisent VictoriaLogs via la fixture `vlogs`.
- Validation ciblée : `bash scripts/dev.sh python tests/test_api.py -q`
  et les fichiers `tests/test_<domaine>.py` concernés, puis le contrôle complet.
