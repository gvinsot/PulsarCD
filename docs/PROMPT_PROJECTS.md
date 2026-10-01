# Guide de Configuration pour Projets Externes

Ce document explique comment configurer la solution pour qu'elle s'intègre avec l'infrastructure edge (Traefik v3 + Coraza WAF) de ce homelab.

## Architecture de l'Infrastructure

L'infrastructure edge déjà existante fournit :
- **Traefik v3** comme reverse proxy
- **Certificats Let's Encrypt** automatiques (HTTP-01 challenge)
- **Coraza WAF** avec OWASP CRS (mode prévention)
- **Middlewares globaux** : WAF, security headers, rate limiting, redirect HTTP→HTTPS
- **Réseau overlay `proxy`** partagé pour toutes les applications
- **Docker Registry interne** pour stocker les images Docker des projets
- **MinIO** pour le stockage S3-compatible
- **MongoDB** Si besoin de collections de documents
- **PostgreSQL** Si besoin d'une base de données

## Services de données disponibles dans le cluster

Des services de données sont déjà déployés. **Demander à l’admin** de créer les accès et de fournir les paramètres de connexion pour votre projet.

| Service | Usage | Dans l’application |
|--------|--------|---------------------|
| **MongoDB** | Base de documents (collections) | Utiliser la connection string fournie par l’admin (ex. `MONGODB_CONNECTION_STRING`) avec un driver MongoDB. |
| **PostgreSQL** | Base relationnelle | Utiliser la connection string fournie par l’admin (ex. `DATABASE_CONNECTION_STRING`) avec un client PostgreSQL. |
| **MinIO** | Stockage d’objets compatible S3 | Utiliser l’endpoint API et les identifiants (`MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY`) fournis avec un client S3 (AWS SDK, boto3, mc, etc.). |

> ⚠️ **Ces valeurs sont des secrets.** Une connection string contient des identifiants : ne la mettez **jamais** en clair dans le compose ni dans git. Nommez ces variables avec un suffixe reconnu (`_CONNECTION_STRING`, `_SECRET`, `_KEY`, `_PASSWORD`, `_TOKEN`) pour qu’elles soient converties automatiquement en secrets Docker au déploiement. Voir [Gestion des secrets](#gestion-des-secrets).

**Réseaux à rejoindre** (ajouter le réseau au service qui se connecte au cluster) :

- **MongoDB** : rejoindre le réseau overlay `mongocluster_internal` (externe). Plus d’info : `C:\Repos\MongoCluster\README.md`
- **PostgreSQL** : rejoindre le réseau overlay `postgresqlcluster_internal` (externe). Plus d’info : `C:\Repos\PostgreSqlCluster\README.md`
- **MinIO** : exposé via Traefik ; pas de réseau overlay à rejoindre si l’app accède par l’URL publique. Plus d’info : `C:\Repos\MinioCluster\README.md`

Exemple pour un service qui utilise MongoDB :

```yaml
services:
  mon-app:
    image: mon-app:latest
    environment:
      # Suffixe _CONNECTION_STRING ⇒ converti automatiquement en secret Docker au déploiement
      - MONGODB_CONNECTION_STRING=${MONGODB_CONNECTION_STRING}   # connection string fournie par l’admin
    networks:
      - proxy
      - mongocluster_internal

networks:
  proxy:
    external: true
  mongocluster_internal:
    external: true
```

Pour créer une solution intégrée avec le swarm, il faut créer:
- un dossier devops

Puis dans ce dossier devops ajouter
- un fichier docker-compose.swarm.yml qui contiendra la description des déploiements pour le projet.

**Example de structure de fichiers**

```
mon-projet/
├── devops/
│   ├── docker-compose.swarm.yml       # Fichier de stack Docker Swarm (requis)
│   ├── .env.example                   # Exemple de fichier .env
│   └── .env                           # Variables d'environnement pour le déploiement
├── my-app/
│   ├── Dockerfile                     # Dockerfile pour construire l'image
│   └─── src/                           # Code source de l'application
│       └── ...
├── .env                               # Variables d'environnement pour le déploiement
├── .env.example                       # Exemple de fichier .env
└── README.md                          # Documentation du projet
```


Dans docker-compose.swarm.yml, les images specifiques au projet doivent pouvoir être buildées avec docker compose build et cibler la registry registry.methodinfo.fr qui est automatiquement accessible.


**Exemple d'utilisation avec un projet specifique** :
```yaml
services:
  mon-app:
    image: registry.methodinfo.fr/mon-app:latest
    build:
      context: ..
      dockerfile: mon-app/Dockerfile
    # ...
```

Le projet doit avoir dans le dossier devops un fichier .env specifique.
Il doit y avoir un .env.example


## Gestion des secrets

Les secrets (mots de passe, tokens, clés d’API, connection strings…) ne doivent **jamais** apparaître en clair dans le `docker-compose.swarm.yml` ni être committés dans git. PulsarCD les convertit automatiquement en **secrets Docker Swarm** au moment du déploiement.

### Principe

1. Les valeurs sensibles vivent uniquement dans `devops/.env` (qui est **gitignoré** — voir plus bas).
2. Dans le compose, vous référencez ces valeurs comme des variables d’environnement : `- MA_VAR=${MA_VAR}`.
3. Au déploiement, le script [`process_secrets.py`](../scripts/process_secrets.py) :
   - détecte les variables sensibles (par convention de nommage, voir ci-dessous) ;
   - crée un secret Docker nommé `<stack>_<NOM_VARIABLE>` à partir de la valeur du `.env` ;
   - **retire** la variable du bloc `environment:` (elle n’apparaît donc plus en clair) ;
   - monte le secret dans le conteneur sous `/run/secrets/<NOM_VARIABLE>` ;
   - déclare le secret au niveau racine comme `external: true`.

L’application lit ensuite le secret depuis le fichier `/run/secrets/<NOM_VARIABLE>`.

### Convention de nommage (conversion automatique)

Toute variable d’un bloc `environment:` dont le **nom se termine** par l’un de ces suffixes (insensible à la casse) est automatiquement transformée en secret Docker :

| Suffixe | Exemple |
|---------|---------|
| `_SECRET` | `JWT_SECRET`, `APP_SECRET` |
| `_KEY` | `MINIO_SECRET_KEY`, `STRIPE_API_KEY` |
| `_TOKEN` | `GITHUB_TOKEN`, `MA_VAR_TOKEN` |
| `_PASSWORD` | `DB_PASSWORD`, `SMTP_PASSWORD` |
| `_CONNECTIONSTRING` / `_CONNECTION_STRING` | `MONGODB_CONNECTION_STRING`, `DATABASE_CONNECTION_STRING` |

> 💡 Nommez donc vos variables sensibles en conséquence. Une variable comme `MONGODB_URI` ou `DATABASE_URL` ne correspond à **aucun** suffixe : elle serait déployée **en clair** (visible dans `docker service inspect`). Préférez `MONGODB_CONNECTION_STRING`, `DATABASE_CONNECTION_STRING`, etc.

### Mode d’emploi (cas courant)

**1. Déclarer la valeur dans `devops/.env`** (jamais committé) :

```bash
MONGODB_CONNECTION_STRING=mongodb://user:p4ssw0rd@mongo:27017/mabase
GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx
```

**2. Référencer la variable dans `docker-compose.swarm.yml`** :

```yaml
services:
  mon-app:
    image: registry.methodinfo.fr/mon-app:latest
    environment:
      - MONGODB_CONNECTION_STRING=${MONGODB_CONNECTION_STRING}
      - GITHUB_TOKEN=${GITHUB_TOKEN}
    networks:
      - proxy
```

**3. Déployer.** Le script transforme automatiquement le compose résolu en :

```yaml
services:
  mon-app:
    image: registry.methodinfo.fr/mon-app:latest
    # plus de MONGODB_CONNECTION_STRING / GITHUB_TOKEN dans environment:
    secrets:
      - source: mon-app_MONGODB_CONNECTION_STRING
        target: MONGODB_CONNECTION_STRING
      - source: mon-app_GITHUB_TOKEN
        target: GITHUB_TOKEN
    networks:
      - proxy

secrets:
  mon-app_MONGODB_CONNECTION_STRING:
    external: true
  mon-app_GITHUB_TOKEN:
    external: true
```

Aucune action manuelle n’est requise : il suffit de respecter la convention de nommage et de fournir la valeur dans `.env`.

### Lire un secret dans l’application

Chaque secret est monté en tant que **fichier** sous `/run/secrets/<NOM_VARIABLE>` (le contenu du fichier = la valeur). Deux approches :

- **Lire le fichier directement** (toutes stacks/langages) :
  ```python
  with open("/run/secrets/MONGODB_CONNECTION_STRING") as f:
      mongo_uri = f.read().strip()
  ```
- **Recharger les secrets dans l’environnement au démarrage** (approche utilisée par PulsarCD, cf. [`shared/secrets.py`](../shared/secrets.py)) : au boot, copier chaque fichier de `/run/secrets/` dans une variable d’environnement du même nom, puis lire la config via `os.environ` comme d’habitude. Les variables déjà définies dans l’environnement **ont priorité** et ne sont pas écrasées.

Équivalents pour d’autres écosystèmes : Node.js `fs.readFileSync('/run/secrets/MA_VAR', 'utf8')`, ou la plupart des frameworks supportent le pattern `*_FILE` (ex. `MONGODB_CONNECTION_STRING_FILE=/run/secrets/MONGODB_CONNECTION_STRING`).

### Secrets déclarés explicitement (variante)

Si vous préférez déclarer le secret vous-même (par exemple pour le monter sous un autre nom de cible), déclarez-le au niveau racine en `external: true`. Le script créera le secret Docker à partir de la variable `.env` correspondante (la clé doit elle aussi respecter la convention de nommage) :

```yaml
services:
  mon-app:
    secrets:
      - DB_CONNECTION_STRING

secrets:
  DB_CONNECTION_STRING:
    external: true
    name: mon-app_DB_CONNECTION_STRING   # optionnel ; défaut: <stack>_<clé>
```

### Règles importantes

- **`.env` ne doit jamais être committé.** Ajoutez-le à `.gitignore` et ne committez que `.env.example` avec des valeurs factices (placeholders), jamais de vraie valeur. Lors d’un déploiement, le script sauvegarde puis restaure automatiquement les fichiers `.env` autour des opérations git (ils ne sont donc pas écrasés par un `git reset`).
- **Ne loggez jamais** la valeur d’un secret et ne la passez pas en `build arg` / `ARG` de Dockerfile (les build args restent visibles dans l’historique de l’image).
- **Les secrets Docker sont immuables.** Si un secret `<stack>_<VAR>` existe déjà, il est **réutilisé tel quel** (la valeur n’est pas mise à jour). Pour faire une rotation : `docker secret rm <stack>_<VAR>` puis redéployer (Docker Swarm refusera la suppression tant que le secret est utilisé — il faut d’abord retirer le service ou utiliser une mise à jour qui ne le référence plus).
- **Valeur vide = pas de secret.** Si la variable n’a pas de valeur dans `.env`, elle est laissée comme variable d’environnement classique (avec un avertissement). Assurez-vous que toutes les valeurs sensibles sont bien renseignées.
- **Ne définissez pas la même clé à la fois en secret et en variable d’environnement** : une variable d’environnement déjà présente a priorité sur le fichier `/run/secrets/` au chargement.


## Environnement QA (`.env.qa`)

Quand l’étape QA est activée, le déploiement QA (stack `qa-<stack>`) lit le **même** `devops/.env` que la production. Les variables `*_HOST` / `*_DOMAIN` contenant un point reçoivent automatiquement le préfixe `qa.` (`app.example.com` → `qa.app.example.com`). Toutes les autres valeurs (base de données, clés, tokens…) sont **celles de la production**.

Pour isoler la QA, créez `devops/.env.qa` (éditeur web : onglet `.env.qa` de la modale `.env`, visible quand la QA est activée ; MCP : `set_stack_env(..., env="qa")`). En déploiement QA uniquement, ce fichier est chargé **par-dessus** `devops/.env` :

- chaque `CLE=valeur` remplace la valeur de `.env` pour la QA ;
- `CLE=` (sans valeur) vide la variable pour la QA (par exemple, pas de base de données partagée) ;
- une clé définie dans `.env.qa` est utilisée telle quelle, **sans** préfixe `qa.` automatique ;
- les clés absentes de `.env.qa` gardent la valeur de `.env`.

```dotenv
# devops/.env.qa : la QA ne partage pas la base de prod
APP_DATABASE_CONNECTION_STRING=
APP_SESSION_KEY=<clé propre à la QA>
```

Comme `.env`, `.env.qa` ne doit **jamais** être committé : ajoutez `.env.qa` au `.gitignore`. Il est sauvegardé/restauré autour des opérations git et versionné chiffré comme `.env`.

Attention aux secrets Docker : les secrets QA s’appellent `qa-<stack>_<VAR>` et, comme indiqué plus haut, un secret existant est **réutilisé sans mise à jour**. Si un déploiement QA a déjà créé `qa-<stack>_<VAR>` avec la valeur de prod, changer `<VAR>` dans `.env.qa` n’a d’effet qu’après un `docker secret rm qa-<stack>_<VAR>`. Ce n’est pas nécessaire quand la variable est vidée, car aucun secret n’est alors monté.


## Configuration pour Docker Swarm (Stack)

Chaque service doit :
1. Rejoindre le réseau `proxy`
2. Avoir les labels Traefik appropriés
3. **Utiliser le middleware global `global@file`** pour tous les endpoints exposés à Internet (obligatoire pour la sécurité)

⚠️ **Sécurité** : Sans `global@file`, vos endpoints ne sont **PAS protégés** par le WAF (Coraza + OWASP CRS). Ne l'omettez jamais pour les services accessibles depuis Internet.

### Template de base

```yaml
version: "3.9"

services:
  mon-app:
    image: mon-app:latest
    networks:
      - proxy
    deploy:
      labels:
        # Activer Traefik
        - "traefik.enable=true"
        
        # Définir le port du service
        - "traefik.http.services.mon-app.loadbalancer.server.port=8080"
        
        # Router HTTPS (principal)
        - "traefik.http.routers.mon-app.rule=Host(`mon-app.example.com`)"
        - "traefik.http.routers.mon-app.entrypoints=websecure"
        - "traefik.http.routers.mon-app.tls.certresolver=letsencrypt"
        - "traefik.http.routers.mon-app.middlewares=global@file"
        
        # Router HTTP (redirige vers HTTPS)
        - "traefik.http.routers.mon-app-http.rule=Host(`mon-app.example.com`)"
        - "traefik.http.routers.mon-app-http.entrypoints=web"
        - "traefik.http.routers.mon-app-http.middlewares=redirect-to-https@file"
        - "traefik.http.routers.mon-app-http.service=noop@internal"

networks:
  proxy:
    external: true
```

### Exemple complet avec plusieurs services

```yaml
version: "3.9"

services:
  web:
    image: nginx:alpine
    networks:
      - proxy
    deploy:
      replicas: 1
      labels:
# Activer Traefik
        - "traefik.enable=true"
        
        # Définir le port du service
        - "traefik.http.services.art-retrainer-frontend.loadbalancer.server.port=80"
        
        # Router HTTPS (principal) - Frontend principal
        - "traefik.http.routers.art-retrainer-frontend.rule=Host(`expert-art.com`) || Host(`www.expert-art.com`)"
        - "traefik.http.routers.art-retrainer-frontend.entrypoints=websecure"
        - "traefik.http.routers.art-retrainer-frontend.tls.certresolver=letsencrypt"
        - "traefik.http.routers.art-retrainer-frontend.middlewares=global@file"
        - "traefik.http.routers.art-retrainer-frontend.service=art-retrainer-frontend"
        - "traefik.http.routers.art-retrainer-frontend.priority=1"
        
        # Router HTTP (redirige vers HTTPS)
        - "traefik.http.routers.art-retrainer-frontend-http.rule=Host(`expert-art.com`) || Host(`www.expert-art.com`)"
        - "traefik.http.routers.art-retrainer-frontend-http.entrypoints=web"
        - "traefik.http.routers.art-retrainer-frontend-http.middlewares=redirect-to-https@file"
        - "traefik.http.routers.art-retrainer-frontend-http.service=noop@internal"
      restart_policy:
        condition: on-failure
      placement:
        constraints:
          - node.labels.gpu == none

  api:
    image: registry.methodinfo.fr/api:latest
    build:
      context: ..
      dockerfile: api/Dockerfile
    networks:
      - proxy
    deploy:
      replicas: 1
      labels:
        # Activer Traefik
        - "traefik.enable=true"
        
        # Définir le port du service
        - "traefik.http.services.art-retrainer-api.loadbalancer.server.port=8000"
        
        # Router HTTPS (principal) - API avec path prefix
        # IMPORTANT: Utiliser global@file pour appliquer le WAF aux endpoints exposés à Internet
        - "traefik.http.routers.art-retrainer-api.rule=Host(`expert-art.com`) && PathPrefix(`/api`)"
        - "traefik.http.middlewares.api-strip.stripprefix.prefixes=/api"
        - "traefik.http.routers.art-retrainer-api.entrypoints=websecure"
        - "traefik.http.routers.art-retrainer-api.tls.certresolver=letsencrypt"
        - "traefik.http.routers.art-retrainer-api.middlewares=global@file,api-stripprefix@file"
        - "traefik.http.routers.art-retrainer-api.service=art-retrainer-api"
        - "traefik.http.routers.art-retrainer-api.priority=100"
        - "traefik.http.routers.art-retrainer-api.middlewares=api-strip"

        
        # Router HTTP (redirige vers HTTPS)
        - "traefik.http.routers.art-retrainer-api-http.rule=Host(`expert-art.com`) && PathPrefix(`/api`)"
        - "traefik.http.routers.art-retrainer-api-http.entrypoints=web"
        - "traefik.http.routers.art-retrainer-api-http.middlewares=redirect-to-https@file"
        - "traefik.http.routers.art-retrainer-api-http.service=noop@internal"
      placement:
        constraints:
          - node.labels.gpu == none
networks:
  proxy:
    external: true
```

## Middlewares disponibles

### Middleware global (OBLIGATOIRE pour endpoints exposés à Internet)

⚠️ **IMPORTANT** : Le middleware `global@file` est **OBLIGATOIRE** pour tous les endpoints exposés à Internet. Il applique automatiquement :
- **WAF** (Coraza avec OWASP CRS) - Protection contre les attaques
- **Security headers** (HSTS, X-Frame-Options, etc.) - Headers de sécurité
- **Rate limiting** (100 req/s moyenne, 50 burst) - Protection contre le DDoS

**Utilisation** :
```yaml
- "traefik.http.routers.app.middlewares=global@file"
```

**Pour les APIs avec path prefix** (combiner avec stripprefix) :
```yaml
- "traefik.http.routers.app.middlewares=global@file,api-stripprefix@file"
```

> **Note** : Le middleware `api-stripprefix` doit être défini dans `edge/dynamic.yml` (voir la configuration edge pour la syntaxe exacte).

### Middlewares individuels (si besoin)

Si vous ne voulez pas le middleware global, vous pouvez utiliser :

| Middleware | Description | Référence |
|------------|-------------|-----------|
| `waf@file` | WAF uniquement | `traefik.http.routers.app.middlewares=waf@file` |
| `security-headers@file` | Headers de sécurité uniquement | `traefik.http.routers.app.middlewares=security-headers@file` |
| `rate-limit@file` | Rate limiting uniquement | `traefik.http.routers.app.middlewares=rate-limit@file` |
| `dashboard-allowlist@file` | Restriction IP (réseau privé uniquement) | `traefik.http.routers.app.middlewares=dashboard-allowlist@file` |

> **Note** : Le middleware `dashboard-allowlist@file` est défini dans `edge/dynamic.yml` et utilise les plages IP privées configurées dans `edge/.env` (`TRAEFIK_DASHBOARD_ALLOW_IP1`, `TRAEFIK_DASHBOARD_ALLOW_IP2`).

## Cas d'usage spécifiques

### Service accessible uniquement depuis le réseau privé

Pour restreindre l'accès d'un service au réseau privé uniquement (pas accessible depuis Internet) :

#### Utiliser le middleware existant `dashboard-allowlist@file`

Ce middleware est déjà configuré :

```yaml
services:
  internal-api:
    image: internal-api:latest
    networks:
      - proxy
    deploy:
      labels:
        - "traefik.enable=true"
        - "traefik.http.services.internal-api.loadbalancer.server.port=8000"
        # Router HTTPS - accessible uniquement depuis le réseau privé
        - "traefik.http.routers.internal-api.rule=Host(`internal-api.example.com`)"
        - "traefik.http.routers.internal-api.entrypoints=websecure"
        - "traefik.http.routers.internal-api.tls.certresolver=letsencrypt"
        # Utiliser dashboard-allowlist pour restreindre au réseau privé
        # Note: Pas de global@file car pas besoin de WAF pour un service interne
        - "traefik.http.routers.internal-api.middlewares=dashboard-allowlist@file"
        # Router HTTP (redirection)
        - "traefik.http.routers.internal-api-http.rule=Host(`internal-api.example.com`)"
        - "traefik.http.routers.internal-api-http.entrypoints=web"
        - "traefik.http.routers.internal-api-http.middlewares=redirect-to-https@file"
        - "traefik.http.routers.internal-api-http.service=noop@internal"
```


### Application avec authentification basique supplémentaire

```yaml
services:
  app:
    image: app:latest
    networks:
      - proxy
    deploy:
      labels:
        - "traefik.enable=true"
        - "traefik.http.services.app.loadbalancer.server.port=80"
        - "traefik.http.routers.app.rule=Host(`app.example.com`)"
        - "traefik.http.routers.app.entrypoints=websecure"
        - "traefik.http.routers.app.tls.certresolver=letsencrypt"
        # Chaîne de middlewares : global + auth personnalisée
        - "traefik.http.routers.app.middlewares=global@file,app-auth@file"
        # Définir l'auth dans edge/dynamic.yml
```

### Application WebSocket

```yaml
services:
  app:
    image: app:latest
    networks:
      - proxy
    deploy:
      labels:
        - "traefik.enable=true"
        - "traefik.http.services.app.loadbalancer.server.port=8080"
        - "traefik.http.routers.app.rule=Host(`app.example.com`)"
        - "traefik.http.routers.app.entrypoints=websecure"
        - "traefik.http.routers.app.tls.certresolver=letsencrypt"
        - "traefik.http.routers.app.middlewares=global@file"
        # Support WebSocket
        - "traefik.http.services.app.loadbalancer.server.scheme=ws"
```

### Application avec healthcheck

```yaml
services:
  app:
    image: app:latest
    networks:
      - proxy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3
    deploy:
      labels:
        - "traefik.enable=true"
        - "traefik.http.services.app.loadbalancer.server.port=8080"
        - "traefik.http.routers.app.rule=Host(`app.example.com`)"
        - "traefik.http.routers.app.entrypoints=websecure"
        - "traefik.http.routers.app.tls.certresolver=letsencrypt"
        - "traefik.http.routers.app.middlewares=global@file"
```

### Application nécessitant un GPU

Pour déployer un service sur un nœud avec GPU, vous devez :

1. **Ajouter la variable d'environnement** pour exposer les devices GPU AMD:
```yaml
services:
  gpu-service:
    environment:
      - AMD_VISIBLE_DEVICES=all
```

2. **Ajouter la contrainte de placement** pour cibler les nœuds avec GPU AMD:
```yaml
deploy:
  replicas: 1
  placement:
    constraints:
      - node.labels.gpu == amd
```
**Ajouter la contrainte de placement** pour cibler les nœuds avec GPU NVIDIA:
```yaml
deploy:
  replicas: 1
  placement:
    constraints:
      - node.labels.gpu == nvidia
```

> **Pour exclure les nœuds GPU** (déployer uniquement sur les nœuds sans GPU) :
> ```yaml
> constraints:
>   - node.labels.gpu == none
> ```
