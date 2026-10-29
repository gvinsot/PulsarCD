# Développement par IA

Le contrat d'entrée des agents est [AGENTS.md](../AGENTS.md). Les instructions
spécifiques sont placées dans les dossiers backend, agent, frontend et tests.
Cette documentation est indépendante du fournisseur d'IA.

## Préparer un checkout

Prévoir Python compatible avec les images du dépôt (la version de référence est
dans `Dockerfile`), Bash, Node.js avec `node --test`, Git, awk et `envsubst`
(`gettext-base` sur Debian). La CI utilise les paquets Node et shell de Debian
Trixie. Aucun paquet npm ni compilation frontend n'est nécessaire.

```bash
git status --short
git fetch origin main
# Sur un checkout propre de main :
git merge --ff-only origin/main
bash scripts/dev.sh setup
bash scripts/dev.sh check
```

`setup` crée `.venv` et installe `requirements-dev.txt`, qui inclut les dépendances
runtime et les mêmes outils que l'image de test. Il nécessite le réseau. Les
versions des outils statiques sont fixées ; les dépendances runtime gardent les
plages existantes : ce fichier n'est pas un verrouillage transitif complet.
Les commandes suivantes détectent `.venv` sans avoir à l'activer.
`PULSARCD_PYTHON=/chemin/vers/python` permet de choisir un autre interpréteur.
Les scripts ciblent Bash sur Linux/macOS ; sous Windows, utiliser WSL.

## Boucle courte et validation

```bash
bash scripts/dev.sh python tests/test_api.py -q
bash scripts/dev.sh python tests/test_security.py -k viewer -q
bash scripts/dev.sh frontend tests/test_stack_env.cjs
bash scripts/dev.sh static
bash scripts/dev.sh check
git diff --check
```

`check` exécute les contrôles statiques, la syntaxe Bash et les deux suites de
tests, puis renvoie un code non nul si une étape échoue. Les étapes indépendantes
continuent après un échec pour fournir plusieurs diagnostics dans une seule
itération. Une absence de Node est une erreur, pas un succès silencieux.
`python` et `frontend` transmettent les arguments aux runners sans les réinterpréter.
Ces commandes ne modifient pas le checkout Git et ne lancent aucun déploiement.

Ruff et mypy bloquent les erreurs probables suivant `pyproject.toml` ; l'audit de
dépendances reste consultatif suivant `scripts/static-checks.sh`.
`PULSARCD_SKIP_AUDIT=1 bash scripts/dev.sh check` évite cet accès réseau après
installation. Signaler cette omission dans le compte rendu.

## Intégration et CI/CD

La plupart des tests remplacent l'infrastructure par des doubles. Les tests
VictoriaLogs démarrent un binaire `victoria-logs` / `victoria-logs-prod` trouvé sur
PATH, ou utilisent `VL_TEST_URL`. Sans service disponible, ils sont ignorés.
L'image de test fournit VictoriaLogs dans la version du compose Swarm.
Les tests PostgreSQL nécessitent `PULSARCD_TEST_DATABASE_URL`, pointant vers une
base jetable. Le résumé pytest affiche les raisons des tests ignorés.

Pour valider dans l'environnement conteneurisé, avec un daemon Docker disponible :

```bash
docker build --target test -t pulsarcd-test .
docker run --rm pulsarcd-test
```

La construction exécute les contrôles statiques et le conteneur lance les tests
Python **et** JavaScript via `scripts/dev.sh test`. Le stage de production ne
contient ni Node ni les dépendances de développement. `scripts/test.sh` sert à
orchestrer les projets déployés par PulsarCD ; ce n'est pas le lanceur local.

## Livraison d'une tâche

Limiter le diff au périmètre demandé. Tester le comportement modifié et ses
limites (erreurs, autorisations, reprise ou annulation selon le domaine).
Actualiser les guides affectés. Relire le diff et n'ajouter à l'index que les
fichiers de la session, puis committer et pousser sur la branche demandée.

Le compte rendu doit fournir le commit, les changements, les commandes de
validation et leurs résultats, ainsi que les tests ignorés ou non exécutés.
Un push déclenche la CI/CD ; une vérification locale ne prouve pas à elle seule
que le déploiement distant a réussi.
