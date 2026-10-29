# Développement par agents IA

PulsarCD est un backend Python/FastAPI, un agent de collecte Python et une
interface HTML/CSS/JavaScript sans bundler. Lire ce fichier au démarrage, puis
les `AGENTS.md` du périmètre modifié. Le guide de travail est
[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) ; le README décrit le produit et son
déploiement.

## Boucle de travail

1. Lire `git status --short`, puis récupérer `origin/main`. Intégrer les commits
   distants avant de commencer, sans écraser les modifications parallèles.
2. Identifier les fichiers concernés et leurs tests avec `rg`. Lire les modules
   utiles plutôt que charger tout le dépôt ou l'intégralité du README.
3. Préparer l'environnement avec `bash scripts/dev.sh setup` une seule fois.
   Cette commande installe les dépendances dans `.venv` sans activation manuelle.
4. Faire un changement ciblé, avec un test de régression pour un comportement
   corrigé. Réutiliser les conventions et dépendances déjà présentes.
5. Exécuter les tests ciblés, puis `bash scripts/dev.sh check`. Les tests ignorés
   doivent être signalés avec leur cause ; ils ne prouvent pas une validation.
6. Examiner `git diff --check` et le diff, committer uniquement ses fichiers,
   puis pousser sur la branche demandée. Un push déclenche la CI/CD.

## Repères

| Besoin | Point de départ |
| --- | --- |
| Routes HTTP et autorisations | `backend/routes/`, `backend/auth_middleware.py` |
| Initialisation et état partagé | `backend/api.py`, `backend/main.py` |
| Configuration | `backend/config.py`, `backend/config_file.py`, `backend/models.py` |
| Pipeline et déploiement | `backend/pipeline_runner.py`, `backend/github_service.py`, `scripts/` |
| Collecte sur les hôtes | `agent/`, `shared/` |
| Journaux et historique | `backend/log_store.py`, `backend/run_history.py`, `shared/victorialogs.py` |
| Interface | `frontend/index.html`, `frontend/static/js/views/`, `frontend/static/css/` |
| Tests et fixtures | `tests/`, `tests/conftest.py` |
| Images et infrastructure | `Dockerfile`, `Dockerfile.agent`, `devops/docker-compose.swarm.yml` |

## Invariants

- Utiliser `scripts/dev.sh` pour vérifier ce dépôt. `scripts/test.sh` est un
  outil d'orchestration des dépôts déployés : il modifie leur checkout Git et
  lance Docker Compose.
- Les dépendances runtime restent dans `requirements.txt` et
  `agent/requirements.txt`, les outils de développement dans `requirements-dev.txt`.
  Respecter `security-constraints.txt` et les incompatibilités documentées des
  versions FastAPI/Starlette, Pydantic et httpx.
- Ne pas introduire un framework frontend, un bundler ou un nouveau gestionnaire
  de paquets pour une modification ordinaire. Ne pas reformater les fichiers
  voisins sans rapport avec la tâche.
- Les tests utilisent des doubles ou des services jetables. Ne pas exécuter un
  déploiement, une restauration ou une action sur un hôte réel pour les valider.
- Ne jamais versionner de secrets, `.env`, clés SSH ou données de `/data`.
  Garder les contrôles d'autorisation, la CSP, la rédaction des secrets dans les
  logs et la vérification des clés hôtes SSH lors des modifications.
- Actualiser les consignes et la documentation si les commandes, points d'entrée
  ou invariants changent. Ne pas dupliquer les instructions propres à un outil IA.

## Fin de session

À la fin de chaque session ayant modifié ce dépôt, créer systématiquement un commit des changements de cette session.
Préserver les modifications parallèles et ne pas les inclure dans ce commit.
