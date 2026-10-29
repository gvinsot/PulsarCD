# Agent de collecte

- `main.py` assemble le collecteur Docker, le writer VictoriaLogs et le poller
  d'actions. `config.py` charge les paramètres `AGENT_*`.
- Les journaux et métriques vont directement à VictoriaLogs ; le backend reçoit
  les demandes/réponses d'actions. Préserver cette séparation et l'identité de
  l'hôte dans les écritures.
- Ne pas bloquer la boucle asyncio. Garder les délais réseau, les reprises et
  l'arrêt propre des tâches et clients lors des modifications.
- `shared/` est aussi importé par le backend. Ses changements doivent rester
  compatibles avec `agent/requirements.txt` et `Dockerfile.agent` ; ne pas y
  introduire une dépendance propre au backend sans mettre à jour l'image agent.
- Les tests sont dans `tests/` à la racine, pas dans ce répertoire. Mock les
  appels Docker/backend et utilise des fichiers temporaires ; ne pas tester
  d'action contre le socket Docker de production.
