# Sauvegardes chiffrées des fichiers de déploiement

PulsarCD peut conserver dans PostgreSQL des versions chiffrées des `.env`
présents sous le répertoire des dépôts et des fichiers du répertoire SSH monté
dans le backend. Cette fonctionnalité ne sauvegarde pas `/data`.

## Réplication sur les serveurs de build

Chaque enregistrement de `devops/.env`, `devops/.env.qa` ou `devops/.env.build` via l'éditeur web
ou le MCP écrit d'abord le fichier sur l'hôte principal (`github.ssh_host`),
puis réplique ce fichier sur tous les serveurs supplémentaires configurés dans
`github.build_hosts`. L'adresse du principal est exclue et les adresses répétées
ne reçoivent qu'une copie. Les autres nœuds Swarm ne sont pas concernés.

Les copies passent par SSH sur stdin, sans contenu dans les arguments de
commande ni dans les logs. Le remplacement est atomique avec des permissions
`0600`. Cette réplication fonctionne indépendamment des sauvegardes chiffrées
PostgreSQL décrites ci-dessous.

La modale indique le nœud principal qui lit le fichier (son nom réel, pas son
alias SSH) et les cibles configurées pour la réplication à l'enregistrement.
Ces cibles ne constituent pas une vérification de l'état actuel des copies.
Si le fichier n'existe pas encore, la modale indique où il sera créé. Sans
serveur secondaire configuré, elle le signale explicitement.

Le succès confirme l'enregistrement et la réplication sur toutes les cibles.
Si une copie échoue, le fichier reste enregistré sur le principal et les copies
réussies sont conservées ; la modale reste ouverte et indique les serveurs en
échec. Enregistrer de nouveau relance la réplication, même avec un contenu
inchangé. La synchronisation depuis le principal avant un build ou un test sur
un secondaire reste également active.

Le principal reste la référence : il n'y a pas de bascule automatique en cas
de perte de ce serveur. Les changements effectués directement par SSH ne sont
pas répliqués immédiatement ; ils sont repris à la prochaine synchronisation
avant build/test ou à un nouvel enregistrement via PulsarCD. Cette réplication
ne fournit pas d'historique et ne remplace pas une sauvegarde indépendante.

## Activation

Configurer dans le `devops/.env` de **PulsarCD** :

```dotenv
PULSARCD_BACKUP__ENABLED=true
DATABASE_CONNECTION_STRING=<connexion PostgreSQL existante>
ENCRYPTION_KEY=<clé existante, sans en générer une nouvelle>
PULSARCD_BACKUP__SCOPE=production
PULSARCD_BACKUP__INTERVAL_SECONDS=60
PULSARCD_BACKUP__ENV_HISTORY=10
```

Les deux valeurs sensibles sont converties en secrets Docker par le processus
de déploiement existant, comme les autres variables à suffixe `_KEY` et
`_CONNECTION_STRING`. Un simple `docker stack deploy` sans cette conversion ne
doit pas servir à les publier en clair. Le backend rejoint le réseau externe
`postgresqlcluster_internal`, qui doit exister. Son compte PostgreSQL doit
pouvoir créer la table `recovery_versions` et son index, puis lire, insérer et
mettre à jour ses lignes. Aucun accès superutilisateur n'est nécessaire.

La fonctionnalité reste désactivée par défaut tant que les accès ne sont pas
provisionnés. Une fois activée, une erreur de clé ou de base interdit une
modification de `.env` via PulsarCD. Le relevé périodique signale ses erreurs
sans arrêter le reste du serveur. Le serveur de build doit disposer de Python 3.

La connexion peut également être fournie via
`PULSARCD_BACKUP__CONNECTION_STRING` pour utiliser une base distincte. Il n'existe
aucun accès PostgreSQL implicite via VictoriaLogs. Ne réutiliser les accès d'une
autre application qu'après avoir identifié explicitement la base voulue.

## La clé et le problème du démarrage après sinistre

`ENCRYPTION_KEY` accepte une valeur base64 ou hexadécimale représentant au moins
32 octets aléatoires (32 octets pour le format hexadécimal). HKDF-SHA256 en dérive
une clé spécifique au format `pulsarcd/recovery/v1`, utilisée avec AES-256-GCM.
Chaque version reçoit un nonce aléatoire de 12 octets. Le dépôt/chemin, le type,
le périmètre, l'identifiant, l'auteur et la date font partie des métadonnées
authentifiées. Le contenu n'est jamais envoyé aux logs ni stocké en clair en base.

**Une copie récupérable de la clé et des accès PostgreSQL doit exister hors de
server-b et hors du cluster**, par exemple dans le gestionnaire de mots de passe
de l'administrateur. Ils peuvent continuer à être fournis par le `.env` en
exploitation. Sauvegarder ce même `.env` sous forme chiffrée ne suffit pas à
retrouver la clé lorsque toutes ses copies en clair ont disparu.

La clé peut aussi être lue depuis `ENCRYPTION_KEY_FILE`, puis à défaut depuis
`ENCRYPTION_KEY` ou `/run/secrets/ENCRYPTION_KEY`. Aucune clé n'est créée
automatiquement. Garder les anciennes clés si une rotation devient nécessaire :
cette première version ne ré-encrypte pas automatiquement l'historique. Restaurer
une ancienne version exige sa clé d'origine. Un changement de clé inattendu
provoque un échec d'authentification des versions existantes et bloque le relevé.

## Ce qui est sauvegardé

- Toute écriture par l'éditeur web ou le MCP sauvegarde d'abord le fichier
  existant, puis la nouvelle version. Celle-ci est `pending` jusqu'au remplacement
  atomique du fichier local, puis `applied`. Le fichier est écrit avec des
  permissions `0600` sur Linux. Les contenus circulent sur stdin, jamais dans
  les arguments de commande. Les retours à la ligne sont préservés.
- Dès le démarrage, puis toutes les 60 secondes par défaut, un relevé recherche
  les fichiers nommés exactement `.env`, `.env.qa` (surcharges QA) ou `.env.build` (secrets de build) sous le répertoire configuré des dépôts,
  y compris les fichiers à leur racine et dans `devops`. Les répertoires `.git`,
  `node_modules`, `.venv`, `venv` et `__pycache__` sont exclus. Les liens
  symboliques ne sont pas suivis. Une version `observed` est créée si le contenu
  diffère de la dernière version confirmée.
- Les builds et déploiements déclenchés par le backend vérifient aussi les
  `.env` du dépôt avant les opérations Git. L'appel direct des scripts Bash
  échappe à ce contrôle synchrone ; le relevé périodique reste applicable.
- Les fichiers réguliers sous `PULSARCD_BACKUP__SSH_PATH` (par défaut `~/.ssh`,
  `/root/.ssh` dans le compose Swarm) sont sauvegardés : clés privées/publiques,
  configuration, `known_hosts`, `authorized_keys`. Les sockets sont ignorés et
  les liens symboliques refusés. Une identité située ailleurs doit être ajoutée
  au périmètre explicitement ; le système ne parcourt pas tout le serveur.

Le relevé constate l'état au moment du passage : il ne garantit pas de capturer
chaque version intermédiaire d'une modification SSH. Une garantie à chaque
écriture demande de passer par PulsarCD. Une disparition du fichier ne supprime
jamais ses sauvegardes. Chaque fichier `.env` / `.env.qa` / `.env.build` garde ses **10 dernières
versions** (`PULSARCD_BACKUP__ENV_HISTORY`) : chaque nouvelle sauvegarde supprime
les plus anciennes de ce fichier, sous le même verrou que l'insertion. La plus
récente, même encore `pending`, est toujours conservée. Les sauvegardes SSH n'ont
pas de purge automatique. Les fichiers sont limités à 4 MiB chacun.

Les écritures et relevés `.env` sont sérialisés dans l'unique backend actuel.
Ne pas augmenter son nombre de réplicas avant de mettre en place un verrou de
fichier distribué. Une vérification avant remplacement refuse une modification
manuelle déjà visible ; elle ne verrouille pas les éditeurs externes au processus.

## Suivi et restauration par API

Ces routes nécessitent un compte administrateur et ne renvoient aucun contenu
secret dans les listes :

| Route | Fonction |
|---|---|
| `GET /api/admin/recovery/status` | Dernier relevé réussi, erreur éventuelle et nombre de fichiers |
| `GET /api/admin/recovery/env/{repo}/history` | Les versions conservées de `devops/.env` (`?env=qa` : `devops/.env.qa`, `?env=build` : `devops/.env.build`), sans contenu |
| `GET /api/admin/recovery/env/{repo}/versions/{revision}` | Le contenu d'une version, pour l'historique de l'éditeur (`?env=qa` pour `devops/.env.qa`, `?env=build` pour `devops/.env.build`) |
| `POST /api/admin/recovery/env/{repo}/restore/{revision}` | Restaure une version, en sauvegardant l'état actuel avant écriture (`?env=qa` pour `devops/.env.qa`, `?env=build` pour `devops/.env.build`) |

Une restauration ne redéploie pas automatiquement la stack. Les fichiers SSH
se restaurent uniquement via l'outil autonome ci-dessous, dans un nouveau dossier.

Dans l'interface web, le bouton **History** de la modale d'édition d'un `.env`
liste les versions conservées du fichier affiché (date, origine, auteur). **Load**
place une version dans l'éditeur comme une modification non enregistrée ;
**Save** la restaure, par le même chemin — et avec la même sauvegarde — qu'une
édition ordinaire. Sans sauvegarde chiffrée activée, le panneau l'indique.

## Restauration sans PulsarCD

Sur une machine de récupération : cloner ce dépôt, installer les dépendances
Python et fournir la connexion PostgreSQL ainsi que la clé sauvegardées
indépendamment. Les secrets peuvent être montés sous `/run/secrets` ou injectés
dans l'environnement depuis un coffre. `ENCRYPTION_KEY_FILE` permet de fournir
un fichier protégé sans mettre la clé dans l'historique du shell. Le périmètre
`PULSARCD_BACKUP__SCOPE` doit être identique à celui des sauvegardes.

```bash
python -m backend.backup_cli list
python -m backend.backup_cli verify
python -m backend.backup_cli history env PulsarCD/devops/.env
python -m backend.backup_cli restore-all --output-dir /chemin/recuperation-neuve
```

Le répertoire de sortie doit être **inexistant**, avec un parent déjà présent.
On retrouve les fichiers sous `env/<depot>/<chemin>` et `ssh/<chemin>`, avec des
permissions restrictives. Le programme n'écrase jamais un répertoire existant.
Une erreur peut laisser une restauration partielle dans le nouveau dossier ;
vérifier le code de retour avant de l'utiliser. Les propriétaires doivent être
ajustés lors de la remise en place sur le nouveau nœud.

Par défaut, seules les dernières versions `observed` ou `applied` sont utilisées.
Une écriture interrompue reste récupérable explicitement :

```bash
python -m backend.backup_cli restore env MonProjet/devops/.env \
  --revision <identifiant> --output-dir /chemin/autre-recuperation-neuve
```

Le test de reprise consiste à exécuter ces commandes depuis une autre machine,
puis à vérifier une stack avec les fichiers restaurés sans accéder à server-b.
La présence d'enregistrements chiffrés en base ne suffit pas à valider ce test.

## Disponibilité de PostgreSQL

Le stockage doit survivre à la perte de server-b. Le commit PostgreSQL garantit
l'écriture selon la configuration de la base ; une réplication asynchrone peut
encore perdre les derniers commits lors d'une promotion. Vérifier le placement,
la réplication et les sauvegardes de cette base. Prévoir aussi un export
PostgreSQL chiffré/hors cluster avec une rétention indépendante : la réplication
ne protège pas d'une suppression de table. Cette fonctionnalité ne configure
pas elle-même les sauvegardes ou la réplication de PostgreSQL.

## Tests

Les tests unitaires s'exécutent normalement. Pour les tests d'intégration et le
test de restauration autonome, fournir `PULSARCD_TEST_DATABASE_URL` vers une
base PostgreSQL **jetable** puis lancer :

```bash
python -m pytest tests/test_recovery.py -q
```

Les tests n'utilisent aucune clé réelle. Ils couvrent le contenu chiffré, la
falsification, une mauvaise clé, les états intermédiaires, une base indisponible,
les modifications manuelles, les clés SSH et une restauration dans un nouveau
répertoire sans serveur web.
