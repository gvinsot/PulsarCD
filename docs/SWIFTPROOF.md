# SwiftProof dans le pipeline PulsarCD

SwiftProof est la dernière étape de l'étape **Test** : une fois la suite
automatisée passée, la revue compare le candidat au code réellement en
production. Son verdict fait partie du résultat de l'étape Test.

Déployer en QA ou en production n'est qu'un déploiement : le chemin de
déploiement n'appelle plus SwiftProof, ne pose plus de garde et n'épingle plus
les digests. Une revue refusée met l'étape Test en échec, et c'est la transition
**Test → QA/Deploy** qui décide alors de la suite, avec son propre mode :
`auto_with_success` s'arrête, `manual` attend, `agent` juge. En mode `auto`,
qui ne vérifie pas le succès de l'étape précédente, le pipeline poursuit malgré
le refus — ce mode reste un choix explicite de l'administrateur.

## Activer un projet

1. Publier cette version de PulsarCD et mettre à jour ses scripts sur l'hôte
   de déploiement. Depuis une copie relue de PulsarCD sur cet hôte Linux :
   `sudo bash scripts/install-swiftproof.sh`. L'installateur vérifie le SHA-256
   de la version SwiftProof [v0.2.0](https://github.com/gvinsot/SwiftProof/releases/tag/v0.2.0),
   dernière version publiée vérifiée le 20 septembre 2026, pour amd64 ou arm64.
   Relancer cet installateur sur les hôtes déjà équipés de v0.1.0 et vérifier
   `swiftproof version` ; modifier PulsarCD seul ne remplace pas leur binaire.
   Un autre emplacement
   est possible avec un argument de l'installateur, puis la variable
   `PULSARCD_SWIFTPROOF_BINARY` dans le service PulsarCD.
2. Vérifier Python 3, PyYAML, Git, Docker et Buildx sur cet hôte. Utiliser le
   même compte SSH et le même répertoire personnel pour les builds et les
   déploiements. Pour utiliser le LLM via SSH, autoriser le transfert de port
   distant vers `127.0.0.1` ; aucun port public supplémentaire n'est nécessaire.
3. Faire relire et intégrer `.swiftproof.json` dans le projet. Préparer puis
   précharger sur l'hôte l'image de tests déclarée dans `sandbox.image`, avec
   toutes les dépendances, idéalement par digest. Les tests SwiftProof ne
   téléchargent pas les dépendances et le bac à sable n'a pas de réseau : une
   image absente fait échouer tous les contrôles en `exit 125` et la revue sort
   en code 4. Vérifier avec `docker image inspect <image>` avant d'activer.
   La politique sera lue depuis le commit de production, pas depuis le candidat.
4. Après ce premier déploiement de la politique, relever et vérifier le SHA Git
   complet réellement en production. Dans **Stacks → Build → Test**, cocher
   **Run SwiftProof after the automated tests**, renseigner ce SHA initial et
   enregistrer. Ce SHA ne sert que tant qu'aucune provenance de build ne
   correspond aux images en production ; ensuite la référence est déduite de la
   release que Swarm exécute réellement. SwiftProof ne peut pas déduire le
   commit d'anciennes images sans provenance.
5. Laisser **Use the LLM configured in PulsarCD** coché. Aucune URL, clé ou
   sélection de modèle supplémentaire n'est nécessaire. Reconstruire la prochaine
   version avec les nouveaux scripts pour disposer de sa provenance.

Si une de ces étapes manque, la modale **Test Logs** nomme le point à corriger
plutôt qu'un nom d'exception : provenance de build absente, tag de release absent
du dépôt de l'hôte, `.swiftproof.json` absent du commit de référence, binaire
absent de l'hôte. Ces messages n'exposent jamais la sortie d'erreur des commandes.

La fonctionnalité est désactivée par défaut sur les projets existants. Le pilote
GitHub de SwiftProof est informatif ; le contrôle PulsarCD, une fois activé,
bloque effectivement les déploiements qui ne satisfont pas la politique.

Avec v0.2.0, les signaux `uncovered_change` apparaissent également parmi les
risques cliquables. La mesure des lignes ajoutées exécutées concerne Go et
nécessite une commande `coverage` dans la politique de référence, avec le jeton
`{coverage_out}` exactement une fois ; une politique existante n'est pas modifiée
automatiquement. Le rapport complet conserve les mesures et leurs limites.
PulsarCD passe explicitement `--reviewer=true` ou `--reviewer=false`, donc le
nouveau démarrage automatique du reviewer ne change pas le choix LLM du projet.

## Réutilisation du LLM

Chaque revue lit la configuration LLM effective de PulsarCD, y compris ses
surcharges d'environnement. Une passerelle temporaire transmet les appels Chat
Completions au modèle configuré. La clé du fournisseur reste dans le processus
PulsarCD ; SwiftProof reçoit un jeton temporaire par l'entrée standard SSH.
Le candidat ne choisit ni le fournisseur ni le modèle. Les outils MCP et les
droits de déploiement de l'agent PulsarCD ne sont pas exposés à cette revue.

Limites : 20 appels, 4 096 tokens de sortie maximum par appel (ou la limite
PulsarCD si inférieure), 128 Kio par requête, 1 Mio par réponse, 10 tests générés
et 10 minutes pour l'investigation LLM. La politique SwiftProof conserve ses
limites plus basses. La commande complète a une limite de 30 minutes et les
preuves transférées une limite de 8 Mio. Le code transmis au modèle bénéficie
du masquage de SwiftProof, qui reste une protection au mieux.

Décocher l'option LLM conserve les analyses et tests déterministes. Une absence
de modèle/URL désactive l'investigateur. Une panne du fournisseur laisse une
investigation incomplète à examiner ; elle ne produit pas une approbation.
Les transitions PulsarCD en mode `agent` sont désormais refusées si leur agent
LLM est indisponible.

## Décisions et preuves

Dans **Test Logs**, activer SwiftProof place le dernier résultat du projet en
tête de la modale. Le statut est actualisé toutes les quatre secondes, même
après la fin des tests. La release et le SHA distinguent cette revue du run de
tests ouvert. Cliquer sur un constat ouvre ses preuves, les sorties des contrôles
de référence/candidat et l'extrait des changements aux lignes concernées.
Les titres du rapport servent de raccourcis ; l'archive reste téléchargeable.

L'explorateur regroupe par défaut les résultats sous **ERROR** (échecs et erreurs)
et **SUCCESS**, puis les tests ignorés ou indéterminés. Il propose aussi les
groupements par type, framework, fichier ou thème automatique, une recherche
et un filtre de résultat. Les formats pytest, Jest/Vitest, `node --test`
(rapporteur `spec` comme TAP), TAP, .NET et Go sont reconnus au mieux ; les
suites et totaux du runner restent distincts des tests nommés. Pour `node
--test`, un test ouvrant des sous-tests devient leur suite, et le récapitulatif
`failing tests` apporte le fichier et l'erreur au test déjà listé plutôt qu'un
doublon. Chaque entrée pointe vers sa ligne de log. Les dernières 20 000 lignes
sont conservées dans la modale, affichées par fenêtres de 500 lignes ; le suivi
automatique s'arrête lorsqu'on remonte ou consulte une entrée.

**Group with LLM** est une action administrateur facultative : elle classe
jusqu'à 300 noms de tests et chemins avec le modèle configuré dans PulsarCD,
sans transmettre les logs bruts ni modifier les résultats ou les décisions
SwiftProof. Les nouveaux tests et ceux sans thème restent dans **Other / new
tests**. Le classement automatique reste disponible si le LLM est indisponible.
L'API correspondante est `POST /api/stacks/actions/{id}/logs/themes`, avec
`{"entries":[{"id":"test-1","name":"test_login","file":"tests/auth.py"}]}`.

| Résultat SwiftProof | Étape Test |
| --- | --- |
| 0 | Succès ; les autres contrôles de la transition restent applicables |
| 1 | Échec : problème élevé/critique reproduit |
| 2 | Échec jusqu'à approbation humaine |
| 3 ou 4, rapport absent/invalide | Échec : configuration ou exécution à corriger |

Pour un code 3 ou 4, le motif affiché nomme le premier contrôle en erreur, son
code de sortie et sa sortie enregistrée — par exemple `test failed (exit 125) :
No such image: …` — au lieu du seul « SwiftProof execution failed ». Ces champs
viennent du rapport v1, jamais de la sortie d'erreur brute d'une commande.
L'archive contient en plus `pulsarcd-run.log`, la console du binaire, tronquée
aux 64 derniers Kio.

Les rapports Markdown sont affichés comme du texte, et l'archive contient le
JSON ainsi que les preuves conservées. Pour un code 2, un administrateur
connecté peut approuver avec un motif, puis relancer explicitement le pipeline.
Il ne peut pas approuver un code 1, 3 ou 4. Le bouton de nouvelle revue invalide
le résultat courant ; la prochaine tentative regénère les preuves. Les anciennes archives restent disponibles pour l'audit.

L'API `GET /api/stacks/pipeline/{repo}/swiftproof/{id}/report?format=json`
fournit le verdict (`result`, avec version de release et SHA), le rapport
SwiftProof original (`report`), son texte (`markdown`) et un index `findings`
pour naviguer vers les observations et les lignes concernées. Cet index
conserve la gravité, le statut et les preuves enregistrés ; un signal de
l'analyseur n'est pas présenté comme un défaut reproduit. Les coordonnées
`side: old` désignent la référence, et `side: new` le candidat. Les appels
sans `format=json` et avec `download=true` conservent leur format existant.
Le contrôle d'appartenance au projet et d'intégrité de l'archive s'applique
également au rapport structuré. Le statut en cours et l'activation restent
disponibles dans `GET /api/stacks/pipeline/{repo}/transition/build_to_test`.
Les projets configurés avant ce déplacement voient leurs réglages migrés
automatiquement depuis `test_to_deploy` à la lecture de `pipeline_state.json`.

L'identifiant du rapport lie le SHA de production, le SHA candidat, les digests
des images, le binaire, la politique de référence et la configuration du modèle.
Tout changement de ces éléments impose une nouvelle revue. Les builds créent
une provenance dans `~/.local/share/pulsarcd/swiftproof/builds/`. Les versions
doivent être exactes (`1.2.3`, tag `v1.2.3`). Réutiliser des images exige leur
provenance existante correspondant au même commit. Sinon, reconstruire **toutes**
les images avec `--no-cache`, plutôt que d'attribuer un ancien binaire à un
nouveau commit. Les projets sans images construites ne sont pas couverts par
ce premier adaptateur de provenance.

La référence de production n'est plus enregistrée par un déploiement : elle est
déduite. Au début de chaque revue, les images que Swarm exécute réellement dans
le stack de production sont rapprochées des provenances de build, et le commit
de la release correspondante devient la base de comparaison. Un tag de release
identifie un build sans ambiguïté et prime donc sur un digest, que deux builds
peuvent partager lorsque des images ont été réutilisées ; à égalité, la release
la plus basse gagne, car une revue trop large se corrige alors qu'une revue trop
étroite ne se voit pas. La release candidate n'est jamais sa propre référence.
Si aucune provenance n'explique les images en production, le SHA initial déclaré
par l'administrateur est utilisé. Un déploiement QA utilise son propre stack et
ne change donc rien à cette déduction ; la promotion en production reste
manuelle.

Les rapports PulsarCD sont dans `<data_dir>/swiftproof/`. Sauvegarder ce
répertoire, `pipeline_state.json` et les données de provenance de l'hôte.
La provenance est un enregistrement de confiance du build, pas une attestation
cryptographique indépendante. L'administrateur de l'hôte, Docker et les scripts
PulsarCD font partie du périmètre de confiance.

Le périmètre a changé avec ce déplacement, et il faut en tenir compte : la revue
qualifie un candidat à l'étape Test, elle ne garantit plus que c'est bien
l'artefact revu qui part en production. Un déploiement reste un déploiement, y
compris hors de PulsarCD. La déduction de la référence rend ce cas visible à la
revue suivante — la base devient ce qui tourne vraiment — mais elle ne
l'empêche pas.

Les détails des interfaces utilisées sont documentés par
[Docker Buildx](https://docs.docker.com/reference/cli/docker/buildx/imagetools/inspect/)
et [AsyncSSH](https://asyncssh.readthedocs.io/en/latest/api.html#asyncssh.SSHClientConnection.forward_remote_port).

## Vérification locale

```sh
python -m pytest tests/test_swiftproof.py tests/test_api.py tests/test_security.py tests/test_build_push.py -q
python -m pytest tests/test_test_log_themes.py tests/test_test_log_themes_api.py -q
node --test tests/test_test_log_model.cjs tests/test_test_log_viewer.cjs
SWIFTPROOF_TEST_BINARY=/chemin/swiftproof python -m pytest tests/test_swiftproof.py -q
```

Le second appel ajoute le vrai binaire au test de bout en bout avec un
fournisseur local simulé. Les tests ne déploient pas sur le cluster et ne
contactent pas le LLM de production.
