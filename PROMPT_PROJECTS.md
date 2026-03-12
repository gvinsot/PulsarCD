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
| **MongoDB** | Base de documents (collections) | Utiliser la connection string fournie par l’admin (ex. `MONGODB_URI`) avec un driver MongoDB. |
| **PostgreSQL** | Base relationnelle | Utiliser la connection string fournie par l’admin (ex. `DATABASE_URL`) avec un client PostgreSQL. |
| **MinIO** | Stockage d’objets compatible S3 | Utiliser l’endpoint API et les identifiants (access key / secret) fournis avec un client S3 (AWS SDK, boto3, mc, etc.). |

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
      - MONGODB_URI=${MONGODB_URI}   # connection string fournie par l’admin
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
          - node.labels.gpu != true

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
          - node.labels.gpu != true
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

## Opérations via MCP (Model Context Protocol)

PulsarCD expose un serveur MCP permettant aux agents IA d'interagir avec la plateforme. Le MCP est sécurisé par authentification Bearer token.

### Endpoint

```
POST https://<TRAEFIK_HOST>/ai/mcp
```

### Authentification

Chaque requête MCP nécessite un header `Authorization: Bearer <token>`.
Deux types de tokens sont acceptés :
- **Clé API MCP** : configurée via `PULSARCD_MCP__API_KEY` (auto-générée au démarrage si non fournie, affichée dans les logs)
- **JWT** : les mêmes tokens utilisés par l'interface web (obtenus via `/api/auth/login`)

### Tools disponibles

| Tool | Description | Paramètres |
|------|-------------|------------|
| `list_stacks` | Lister les stacks disponibles (repos GitHub starred) | aucun |
| `build_stack` | Builder une image Docker depuis un repo GitHub | `repo_name`, `ssh_url`, `version`, `branch?`, `commit?` |
| `deploy_stack` | Déployer une stack sur Docker Swarm | `repo_name`, `ssh_url`, `version`, `tag?` |
| `list_containers` | Lister les containers et leur état | `host?`, `status?` |
| `list_computers` | Lister les hosts/machines monitorés | aucun |
| `search_logs` | Rechercher dans les logs collectés | `query?`, `hosts?`, `containers?`, `levels?`, `start_time?`, `end_time?`, `size?` |
| `get_action_status` | Vérifier le statut d'un build/deploy | `action_id` |

### Configuration dans Claude Desktop

```json
{
  "mcpServers": {
    "pulsarcd": {
      "url": "https://<TRAEFIK_HOST>/ai/mcp",
      "transport": "streamable-http",
      "headers": {
        "Authorization": "Bearer <MCP_API_KEY>"
      }
    }
  }
}
```

### Variables d'environnement MCP

| Variable | Description | Défaut |
|----------|-------------|--------|
| `PULSARCD_MCP__ENABLED` | Activer/désactiver le serveur MCP | `true` |
| `PULSARCD_MCP__API_KEY` | Clé API dédiée pour le MCP | auto-générée (UUID) |
