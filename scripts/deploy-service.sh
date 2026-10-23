#!/bin/bash
#
# Deploy Service
# ==============
# Deploys a service stack using previously built and tagged Docker images
#
# Usage:
#   ./deploy-service.sh [--qa | --preview <branch>] <folder> <version> [branch] [commit]
#
# Options:
#   --qa    Deploy as QA environment: prefix the stack name with "qa-" and
#           prepend "qa." to TRAEFIK_HOST and any *_HOST / *_DOMAIN variables.
#           This produces a fully isolated environment alongside production
#           (e.g. https://pulsarteam.io → https://qa.pulsarteam.io,
#           stack "pulsarcd" → "qa-pulsarcd").
#           devops/.env.qa, when present, is loaded over devops/.env: its
#           variables override the production values for the QA stack only
#           and are used verbatim (no qa. prefix).
#   --preview <branch>
#           Deploy a per-branch preview environment. Same machinery as --qa
#           (devops/.env.qa overlay, Traefik label isolation, host prefixing)
#           with a branch-derived prefix instead of "qa":
#             slug  = DNS-safe, collision-safe name of <branch>
#                     (see shared/preview_naming.py, also used by the API)
#             stack = "pv-<slug>-<repo>"      host = "<slug>.<TRAEFIK_HOST>"
#           TRAEFIK_HOST is always prefixed in preview mode, even when
#           .env.qa sets it, so a preview can never answer on the prod or QA
#           host. A preview can never deploy over the prod or QA stack: the
#           script refuses any stack name that does not start with "pv-".
#
# Arguments:
#   folder  - Repository folder name (e.g., "Art Retrainer") or full path
#   version - Version/build number to deploy (e.g., "42", "main", "abc1234")
#   branch  - (Optional) Branch name to checkout for compose file
#   commit  - (Optional) Specific commit hash to checkout
#
# Examples:
#   ./deploy-service.sh "Art Retrainer" 42
#   ./deploy-service.sh --qa "Art Retrainer" 42 main
#   ./deploy-service.sh --preview feature/login "Art Retrainer" pv-feature-login-df7c7a feature/login
#   ./deploy-service.sh "Art Retrainer" 42 main abc1234
#   ./deploy-service.sh "Art Retrainer" main          # Deploy using branch tag
#
# Expected folder structure:
#   <folder>/devops/
#     ├── docker-compose.swarm.yml   (required)
#     ├── docker-compose.pre.sh      (optional - runs before deployment)
#     └── docker-compose.post.sh     (optional - runs after deployment)
#
# The script will:
#   1. Update repository (checkout branch/commit if specified, or pull latest on current branch)
#   2. Run docker-compose.pre.sh if it exists
#   3. Update image tags in docker-compose.swarm.yml to use the specified version
#   4. Deploy the stack using docker stack deploy
#   5. Wait until every service converges on the new version (verify-deploy.sh)
#   6. Run docker-compose.post.sh if it exists
#   7. Optionally probe an HTTP health URL through Traefik
#
# Post-deploy verification (variables read from the environment / devops/.env):
#   DEPLOY_VERIFY=false            skip convergence and health checks
#   DEPLOY_CONVERGE_TIMEOUT=180    seconds to wait for all replicas (see
#                                  verify-deploy.sh for the other knobs)
#   DEPLOY_HEALTHCHECK_PATH=/health  probe https://$TRAEFIK_HOST/health
#   DEPLOY_HEALTHCHECK_URL=...     or probe this exact URL
#   A failed verification exits with code 3 and prints a line starting with
#   PULSARCD_DEPLOY_VERIFICATION_FAILED (the stack stays deployed).
#

set -e

# ============================================================================
# Configuration
# ============================================================================
# Registry host the images are pushed to / pulled from (no scheme). PulsarCD
# passes its configured registry (PULSARCD_GITHUB__REGISTRY_URL) as REGISTRY;
# the default keeps standalone runs working as before.
REGISTRY="${REGISTRY:-registry.methodinfo.fr}"
REGISTRY="${REGISTRY#http://}"
REGISTRY="${REGISTRY#https://}"
REGISTRY="${REGISTRY%/}"
DEVOPS_FOLDER="devops"
COMPOSE_FILE="docker-compose.swarm.yml"
PRE_SCRIPT="docker-compose.pre.sh"
POST_SCRIPT="docker-compose.post.sh"

# Post-deploy verification (see verify-deploy.sh). A failed verification exits
# with this code and prints this marker; the backend keys its automatic
# rollback on the marker.
DEPLOY_VERIFY_EXIT_CODE=3
DEPLOY_VERIFY_MARKER="PULSARCD_DEPLOY_VERIFICATION_FAILED"

# QA mode flags (set via --qa)
QA_MODE=false
QA_STACK_PREFIX="qa-"
QA_DOMAIN_PREFIX="qa."

# Preview mode (set via --preview <branch>): names computed later from the branch
PREVIEW_MODE=false
PREVIEW_BRANCH=""
PREVIEW_SLUG=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ============================================================================
# Functions
# ============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

show_usage() {
    echo "Usage: $0 [--qa | --preview <branch>] <folder> <version> [branch] [commit]"
    echo ""
    echo "Options:"
    echo "  --qa                Deploy as QA environment (stack prefixed 'qa-', domains 'qa.')"
    echo "  --preview <branch>  Deploy a preview of <branch> (stack 'pv-<slug>-<repo>', domains '<slug>.')"
    echo ""
    echo "Arguments:"
    echo "  folder  - Repository folder name (sibling to this repo) or full path"
    echo "  version - Version/build number to deploy (e.g., 42, main, abc1234)"
    echo "  branch  - (Optional) Branch to checkout for compose file"
    echo "  commit  - (Optional) Specific commit hash to checkout"
    echo ""
    echo "Expected folder structure:"
    echo "  <folder>/devops/"
    echo "    ├── docker-compose.swarm.yml   (required)"
    echo "    ├── docker-compose.pre.sh      (optional)"
    echo "    └── docker-compose.post.sh     (optional)"
    echo ""
    echo "Examples:"
    echo "  $0 \"Art Retrainer\" 42"
    echo "  $0 \"Art Retrainer\" 42 main"
    echo "  $0 \"Art Retrainer\" 42 main abc1234"
    exit 1
}

# Backup .env files before git operations
backup_env_files() {
    local repo_path="$1"
    ENV_BACKUP_DIR=$(mktemp -d)
    
    # Find and backup all .env files (and the .env.qa QA overrides)
    while IFS= read -r -d '' env_file; do
        local rel_path="${env_file#$repo_path/}"
        local backup_path="$ENV_BACKUP_DIR/$rel_path"
        mkdir -p "$(dirname "$backup_path")"
        cp "$env_file" "$backup_path"
        log_info "Backed up: $rel_path"
    done < <(find "$repo_path" \( -name ".env" -o -name ".env.qa" \) -type f -print0 2>/dev/null)
}

# Restore .env files after git operations
restore_env_files() {
    local repo_path="$1"
    
    if [ -n "$ENV_BACKUP_DIR" ] && [ -d "$ENV_BACKUP_DIR" ]; then
        while IFS= read -r -d '' backup_file; do
            local rel_path="${backup_file#$ENV_BACKUP_DIR/}"
            local target_path="$repo_path/$rel_path"
            mkdir -p "$(dirname "$target_path")"
            cp "$backup_file" "$target_path"
            log_info "Restored: $rel_path"
        done < <(find "$ENV_BACKUP_DIR" \( -name ".env" -o -name ".env.qa" \) -type f -print0 2>/dev/null)
        
        # Cleanup backup directory
        rm -rf "$ENV_BACKUP_DIR"
    fi
}

# Derive stack name from folder name
get_stack_name() {
    local folder="$1"
    # Get basename, convert to lowercase, replace spaces/special chars with hyphens
    basename "$folder" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/-/g' | sed 's/--*/-/g' | sed 's/^-//;s/-$//'
}

# Update image tags in compose file
update_image_tags() {
    local compose_file="$1"
    local version="$2"
    local output_file="$3"
    local registry_url="${REGISTRY_URL:-${DOCKER_REGISTRY_URL:-$REGISTRY}}"

    # Only resolve deploy-related variables, preserve all others (like DATABASE_URL)
    # envsubst without args replaces ALL ${VAR} — which destroys runtime variables
    sed -e "s|\${REGISTRY_URL:-[^}]*}|${registry_url}|g" \
        -e "s|\${REGISTRY_URL}|${registry_url}|g" \
        -e "s|\${DOCKER_REGISTRY_URL:-[^}]*}|${registry_url}|g" \
        -e "s|\${DOCKER_REGISTRY_URL}|${registry_url}|g" \
        -e "s|\${REPO_NAME:-[^}]*}|${REPO_NAME}|g" \
        -e "s|\${REPO_NAME}|${REPO_NAME}|g" \
        -e "s|\${VERSION:-[^}]*}|${version}|g" \
        -e "s|\${VERSION}|${version}|g" \
        -e "s|\(${registry_url}/[^:]*\):[^[:space:]\"']*|\1:${version}|g" \
        "$compose_file" > "$output_file"
}

# Abort after a failed post-deploy verification. The stack IS deployed (and may
# be crash-looping), so this exits with its own code and prints a stable marker
# line: the PulsarCD backend looks for that marker to decide on an automatic
# rollback to the previously deployed version.
fail_verification() {
    local check="$1"
    local reason="$2"
    log_error "Post-deploy verification failed ($check): $reason"
    echo "${DEPLOY_VERIFY_MARKER} check=${check} stack=${STACK_NAME} version=${VERSION}"
    rm -f "$DEPLOY_COMPOSE"
    exit "$DEPLOY_VERIFY_EXIT_CODE"
}

# Hostnames served by the deployed stack, one per line, de-duplicated:
#   1. TRAEFIK_HOST from the loaded .env (already qa-prefixed if applicable)
#   2. Host(`...`) rules declared in the deploy compose file
collect_probe_hosts() {
    {
        [ -n "${TRAEFIK_HOST:-}" ] && echo "$TRAEFIK_HOST"
        if [ -f "$DEPLOY_COMPOSE" ]; then
            grep -oE 'Host\(`[^`]+`\)' "$DEPLOY_COMPOSE" \
                | sed -E 's/Host\(`([^`]+)`\)/\1/' \
                | sort -u
        fi
    } | awk 'NF && !seen[$0]++'
}

# Run a hook script if it exists
run_hook() {
    local script_path="$1"
    local script_name="$2"
    local devops_dir="$3"
    
    if [ -f "$script_path" ]; then
        log_info "Running $script_name..."
        
        # Make sure it's executable
        chmod +x "$script_path"
        
        # Run the script from the devops directory
        (cd "$devops_dir" && bash "$script_path") || {
            log_error "$script_name failed!"
            return 1
        }
        
        log_success "$script_name completed successfully"
        return 0
    else
        log_info "No $script_name found, skipping..."
        return 0
    fi
}

# ============================================================================
# Main Script
# ============================================================================

# Parse optional flags. Accept --qa anywhere before/among the positional args.
POSITIONAL=()
while [ $# -gt 0 ]; do
    case "$1" in
        --qa)
            QA_MODE=true
            shift
            ;;
        --preview)
            if [ $# -lt 2 ] || [ -z "$2" ]; then
                log_error "--preview requires a branch name"
                show_usage
            fi
            PREVIEW_MODE=true
            PREVIEW_BRANCH="$2"
            shift 2
            ;;
        --preview=*)
            PREVIEW_MODE=true
            PREVIEW_BRANCH="${1#--preview=}"
            shift
            ;;
        --help|-h)
            show_usage
            ;;
        --)
            shift
            while [ $# -gt 0 ]; do POSITIONAL+=("$1"); shift; done
            break
            ;;
        *)
            POSITIONAL+=("$1")
            shift
            ;;
    esac
done
set -- "${POSITIONAL[@]}"

if [ "$QA_MODE" = "true" ] && [ "$PREVIEW_MODE" = "true" ]; then
    log_error "--qa and --preview are mutually exclusive"
    exit 1
fi

# Check arguments
if [ $# -lt 2 ]; then
    log_error "Missing required arguments"
    show_usage
fi

REPO_FOLDER="$1"
VERSION="$2"
BRANCH="${3:-}"
COMMIT="${4:-}"

# Get the script's directory and the repos root (two levels up since scripts are in PulsarCD/scripts)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPOS_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
VERIFY_SCRIPT="$SCRIPT_DIR/verify-deploy.sh"

# Resolve absolute path
if [[ "$REPO_FOLDER" = /* ]]; then
    # Absolute path
    REPO_PATH="$REPO_FOLDER"
elif [[ "$REPO_FOLDER" = ./* ]] || [[ "$REPO_FOLDER" = ../* ]]; then
    # Relative path starting with ./ or ../
    REPO_PATH="$(cd "$SCRIPT_DIR" && cd "$REPO_FOLDER" && pwd)"
else
    # Just a folder name - look in parent directory (sibling repos)
    REPO_PATH="$REPOS_DIR/$REPO_FOLDER"
fi

# Validate repository exists
if [ ! -d "$REPO_PATH" ]; then
    log_error "Repository folder not found: $REPO_FOLDER"
    exit 1
fi

# Define devops folder path
# Safety: if REPO_PATH already ends with /devops, don't append it again
if [[ "$REPO_PATH" == */devops ]]; then
    DEVOPS_PATH="$REPO_PATH"
    REPO_PATH="$(dirname "$REPO_PATH")"
else
    DEVOPS_PATH="$REPO_PATH/$DEVOPS_FOLDER"
fi

# Derive stack name from the resolved REPO_PATH (not raw REPO_FOLDER)
BASE_STACK_NAME=$(get_stack_name "$REPO_PATH")
if [ "$QA_MODE" = "true" ]; then
    STACK_NAME="${QA_STACK_PREFIX}${BASE_STACK_NAME}"
elif [ "$PREVIEW_MODE" = "true" ]; then
    # Names come from shared/preview_naming.py, the same code the API uses, so
    # the stack the API records is exactly the stack deployed here.
    PREVIEW_NAMING="$(dirname "$SCRIPT_DIR")/shared/preview_naming.py"
    PREVIEW_PYTHON="$(command -v python3 || command -v python || true)"
    if [ -z "$PREVIEW_PYTHON" ] || [ ! -f "$PREVIEW_NAMING" ]; then
        log_error "Preview mode needs python3 and $PREVIEW_NAMING"
        exit 1
    fi
    PREVIEW_NAMES=$("$PREVIEW_PYTHON" "$PREVIEW_NAMING" "$PREVIEW_BRANCH" "$BASE_STACK_NAME") || {
        log_error "Invalid preview branch: $PREVIEW_BRANCH"
        exit 1
    }
    PREVIEW_NAMES="${PREVIEW_NAMES%$'\r'}"
    IFS=$'\t' read -r PREVIEW_SLUG STACK_NAME _PREVIEW_TAG <<< "$PREVIEW_NAMES"
    # Defence in depth: whatever the helper printed, a preview must never
    # target the production or QA stack of this repository.
    if [ -z "$PREVIEW_SLUG" ] || [[ "$STACK_NAME" != pv-* ]] \
        || [ "$STACK_NAME" = "$BASE_STACK_NAME" ] \
        || [ "$STACK_NAME" = "${QA_STACK_PREFIX}${BASE_STACK_NAME}" ] \
        || ! [[ "$PREVIEW_SLUG" =~ ^[a-z0-9]([a-z0-9-]*[a-z0-9])?$ ]]; then
        log_error "Refusing preview stack '$STACK_NAME' (slug '$PREVIEW_SLUG')"
        exit 1
    fi
    # The preview stack's isolation prefix (Traefik names, named networks) and
    # host prefix replace QA's "qa-" / "qa." in the shared code paths below.
    QA_STACK_PREFIX="pv-${PREVIEW_SLUG}-"
    QA_DOMAIN_PREFIX="${PREVIEW_SLUG}."
else
    STACK_NAME="$BASE_STACK_NAME"
fi

# Shared by QA and preview deployments: overlay .env.qa, prefix hosts and
# isolate Traefik labels. ENV_LABEL only names the mode in log lines.
ISOLATED_MODE=false
ENV_LABEL=""
if [ "$QA_MODE" = "true" ]; then
    ISOLATED_MODE=true
    ENV_LABEL="QA"
elif [ "$PREVIEW_MODE" = "true" ]; then
    ISOLATED_MODE=true
    ENV_LABEL="Preview"
fi

echo ""
echo "=============================================="
if [ "$QA_MODE" = "true" ]; then
    echo "  Deploy Service (QA environment)"
elif [ "$PREVIEW_MODE" = "true" ]; then
    echo "  Deploy Service (preview of $PREVIEW_BRANCH)"
else
    echo "  Deploy Service"
fi
echo "=============================================="
echo ""
log_info "Repository: $REPO_PATH"
log_info "DevOps folder: $DEVOPS_PATH"
log_info "Stack name: $STACK_NAME"
[ "$QA_MODE" = "true" ] && log_info "Mode: QA (domains will be prefixed with '${QA_DOMAIN_PREFIX}')"
[ "$PREVIEW_MODE" = "true" ] && log_info "Mode: preview of '$PREVIEW_BRANCH' (domains will be prefixed with '${QA_DOMAIN_PREFIX}')"
log_info "Version: $VERSION"
[ -n "$BRANCH" ] && log_info "Branch: $BRANCH"
[ -n "$COMMIT" ] && log_info "Commit: $COMMIT"
log_info "Compose file: $DEVOPS_FOLDER/$COMPOSE_FILE"
log_info "Registry: $REGISTRY"
echo ""

# ============================================================================
# Step 1: Update repository (checkout branch/commit or pull latest)
# ============================================================================
if [ -n "$BRANCH" ] || [ -n "$COMMIT" ]; then
    # Validate it's a git repository
    if [ ! -d "$REPO_PATH/.git" ]; then
        log_error "Not a git repository: $REPO_PATH"
        exit 1
    fi
    
    log_info "Checking out code..."

    cd "$REPO_PATH"
    
    # Backup .env files before any git operations
    backup_env_files "$REPO_PATH"

    # Save current state
    ORIGINAL_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
    STASHED=false

    # Fetch latest changes
    log_info "Fetching latest changes from remote..."
    git fetch --all --prune --tags --force

    # Checkout the branch or tag
    BRANCH_IS_MOVABLE=false
    if [ -n "$BRANCH" ]; then
        log_info "Checking out branch: $BRANCH"

        # Check if branch exists locally
        if git show-ref --verify --quiet "refs/heads/$BRANCH"; then
            # Local branch exists - checkout
            BRANCH_IS_MOVABLE=true
            git checkout "$BRANCH" || {
                log_error "Failed to checkout branch: $BRANCH"
                exit 1
            }
        elif git show-ref --verify --quiet "refs/remotes/origin/$BRANCH"; then
            # Branch exists on remote but not locally - create tracking branch
            log_info "Creating local tracking branch for origin/$BRANCH"
            BRANCH_IS_MOVABLE=true
            git checkout -b "$BRANCH" "origin/$BRANCH" || {
                log_error "Failed to checkout remote branch: origin/$BRANCH"
                exit 1
            }
        elif git show-ref --verify --quiet "refs/tags/$BRANCH"; then
            # It's a tag - checkout directly (use refs/tags/ to avoid ambiguity with branches)
            log_info "Checking out tag: $BRANCH"
            git checkout "refs/tags/$BRANCH" || {
                log_error "Failed to checkout tag: $BRANCH"
                exit 1
            }
        elif git rev-parse --verify --quiet "${BRANCH}^{commit}" >/dev/null; then
            # A commit ID (pinned deployment)
            log_info "Checking out commit: $BRANCH"
            git checkout --detach "$BRANCH" || {
                log_error "Failed to checkout commit: $BRANCH"
                exit 1
            }
        else
            # Not found anywhere
            log_error "Branch or tag not found: $BRANCH (checked locally, remotely, and tags)"
            exit 1
        fi

        # Force reset to remote only for an actual branch with no pinned commit
        if [ -z "$COMMIT" ] && [ "$BRANCH_IS_MOVABLE" = "true" ]; then
            log_info "Resetting to latest remote version..."
            git reset --hard "origin/$BRANCH" 2>/dev/null || {
                log_warning "Could not reset to origin/$BRANCH (may be a local-only branch)"
            }
        fi
    fi

    # Checkout specific commit if provided
    if [ -n "$COMMIT" ]; then
        log_info "Checking out commit: $COMMIT"
        git checkout "$COMMIT" || {
            log_error "Failed to checkout commit: $COMMIT"
            exit 1
        }
    fi
    
    # Get the current commit hash
    CURRENT_COMMIT_SHORT=$(git rev-parse --short HEAD)
    log_success "Checked out: $CURRENT_COMMIT_SHORT"
    
    # Restore .env files after git operations
    restore_env_files "$REPO_PATH"
else
    # No branch/commit specified - update current branch to latest
    cd "$REPO_PATH"
    ORIGINAL_BRANCH=""
    STASHED=false

    # Check if it's a git repository
    if [ -d "$REPO_PATH/.git" ]; then
        # Backup .env files before any git operations
        backup_env_files "$REPO_PATH"
        
        # Get current branch name
        CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
        
        if [ -z "$CURRENT_BRANCH" ] || [ "$CURRENT_BRANCH" = "HEAD" ]; then
            log_warning "Currently in detached HEAD state"
            CURRENT_COMMIT_SHORT=$(git rev-parse --short HEAD)
            log_info "Using commit: $CURRENT_COMMIT_SHORT"
        else
            log_info "Updating current branch to latest..."
            log_info "Current branch: $CURRENT_BRANCH"

            # Fetch latest
            log_info "Fetching latest changes from remote..."
            git fetch --all --prune --tags --force

            log_info "Resetting to latest remote version for branch: $CURRENT_BRANCH"
            git reset --hard "origin/$CURRENT_BRANCH" 2>/dev/null || log_warning "Could not reset to origin/$CURRENT_BRANCH (may be a local-only branch)"

            CURRENT_COMMIT_SHORT=$(git rev-parse --short HEAD)
            log_success "Updated to: $CURRENT_COMMIT_SHORT"
        fi
        
        # Restore .env files after git operations
        restore_env_files "$REPO_PATH"
    else
        CURRENT_COMMIT_SHORT="unknown"
        log_warning "Not a git repository, skipping update"
    fi
fi

# ============================================================================
# Step 2: Validate devops folder and compose file exist
# ============================================================================
if [ ! -d "$DEVOPS_PATH" ]; then
    log_error "DevOps folder not found: $DEVOPS_PATH"
    log_error "Expected folder structure: <repo>/devops/docker-compose.swarm.yml"
    exit 1
fi

COMPOSE_PATH="$DEVOPS_PATH/$COMPOSE_FILE"
if [ ! -f "$COMPOSE_PATH" ]; then
    log_error "Compose file not found: $COMPOSE_PATH"
    exit 1
fi

# Define paths for optional scripts
PRE_SCRIPT_PATH="$DEVOPS_PATH/$PRE_SCRIPT"
POST_SCRIPT_PATH="$DEVOPS_PATH/$POST_SCRIPT"

# Show what files were found
log_info "Files found in devops folder:"
echo "  - $COMPOSE_FILE (required)"
[ -f "$PRE_SCRIPT_PATH" ] && echo "  - $PRE_SCRIPT (will run before deployment)"
[ -f "$POST_SCRIPT_PATH" ] && echo "  - $POST_SCRIPT (will run after deployment)"
echo ""

# ============================================================================
# Step 3: Load environment variables if .env exists
# ============================================================================
# Check devops folder first, then repo root
if [ -f "$DEVOPS_PATH/.env" ]; then
    log_info "Loading environment variables from devops/.env file..."
    set +u
    set -a
    source "$DEVOPS_PATH/.env"
    set +u
elif [ -f "$REPO_PATH/.env" ]; then
    log_info "Loading environment variables from .env file..."
    set +u
    set -a
    source "$REPO_PATH/.env"
    set +u
else
    log_warning ".env file not found, using defaults"
    set +u
fi

# QA mode: .env.qa, next to the .env, overrides some of its values for the QA
# stack only (e.g. a separate database or session key). Variables it sets are
# explicit QA values, so the qa. domain prefixing below leaves them alone.
# Preview deployments load the same overlay (non-production settings).
QA_OVERRIDE_KEYS=" "
if [ "$ISOLATED_MODE" = "true" ]; then
    ENV_QA_FILE=""
    if [ -f "$DEVOPS_PATH/.env.qa" ]; then
        ENV_QA_FILE="$DEVOPS_PATH/.env.qa"
    elif [ -f "$REPO_PATH/.env.qa" ]; then
        ENV_QA_FILE="$REPO_PATH/.env.qa"
    fi
    if [ -n "$ENV_QA_FILE" ]; then
        log_info "Loading QA overrides from ${ENV_QA_FILE#$REPO_PATH/}..."
        set +u
        set -a
        source "$ENV_QA_FILE"
        # Key names only: the values are secrets.
        QA_OVERRIDE_KEYS=" $(sed -nE 's/^[[:space:]]*(export[[:space:]]+)?([A-Za-z_][A-Za-z0-9_]*)=.*/\2/p' "$ENV_QA_FILE" | sort -u | tr '\n' ' ')"
        log_info "QA overrides:${QA_OVERRIDE_KEYS% }"
    fi
fi

# Export VERSION and STACK_NAME for use in hook scripts
export DEPLOY_VERSION="$VERSION"
export DEPLOY_STACK_NAME="$STACK_NAME"
export DEPLOY_REGISTRY="$REGISTRY"

# Export registry variables for compose files (support both REGISTRY_URL and DOCKER_REGISTRY_URL)
export REGISTRY_URL="${REGISTRY_URL:-$REGISTRY}"
export DOCKER_REGISTRY_URL="${DOCKER_REGISTRY_URL:-$REGISTRY}"
export REPO_NAME="${REPO_NAME:-$STACK_NAME}"

# ============================================================================
# QA mode: prepend "qa." to host/domain env vars so the QA stack runs on
# qa.<domain> instead of <domain>. Skip values that already start with "qa.".
# ============================================================================
if [ "$ISOLATED_MODE" = "true" ]; then
    if [ "$PREVIEW_MODE" = "true" ]; then
        export DEPLOY_ENV="preview"
        export DEPLOY_PREVIEW_BRANCH="$PREVIEW_BRANCH"
        export DEPLOY_PREVIEW_SLUG="$PREVIEW_SLUG"
    else
        export DEPLOY_ENV="qa"
    fi

    # Preview: the variables Traefik routes on (TRAEFIK_HOST and any ${VAR}
    # inside a "rule" label). Their .env.qa value is a QA host, so a preview
    # using it verbatim would compete with the QA stack's routers.
    PREVIEW_ROUTED_VARS=" TRAEFIK_HOST "
    if [ "$PREVIEW_MODE" = "true" ] && [ -f "$COMPOSE_PATH" ]; then
        PREVIEW_ROUTED_VARS+="$( { grep -E 'rule' "$COMPOSE_PATH" || true; } \
            | grep -oE '\$\{?[A-Za-z_][A-Za-z0-9_]*' | sed -E 's/^\$\{?//' \
            | sort -u | tr '\n' ' ')"
    fi

    apply_qa_domain_prefix() {
        local var_name="$1"
        local cur_value="${!var_name:-}"
        if [ -z "$cur_value" ]; then
            return 0
        fi
        # Set explicitly by .env.qa: already the QA value. A preview still
        # prefixes the variables Traefik routes on: the verbatim QA value
        # would steal the QA stack's traffic.
        if [[ "$QA_OVERRIDE_KEYS" == *" ${var_name} "* ]] \
            && ! { [ "$PREVIEW_MODE" = "true" ] && [[ "$PREVIEW_ROUTED_VARS" == *" ${var_name} "* ]]; }; then
            log_info "${ENV_LABEL}: ${var_name} set by .env.qa, not prefixed"
            return 0
        fi
        # Already prefixed? leave it alone
        if [[ "$cur_value" == ${QA_DOMAIN_PREFIX}* ]]; then
            return 0
        fi
        local new_value="${QA_DOMAIN_PREFIX}${cur_value}"
        export "$var_name"="$new_value"
        log_info "${ENV_LABEL}: ${var_name} = ${cur_value} → ${new_value}"
    }

    # Always rewrite TRAEFIK_HOST when present (most common case)
    apply_qa_domain_prefix "TRAEFIK_HOST"

    # Auto-detect any other *_HOST or *_DOMAIN env vars that look like
    # public hostnames (contain a dot) and prefix them too.
    while IFS='=' read -r name value; do
        # Only consider matching variable names
        case "$name" in
            *_HOST|*_DOMAIN|HOST|DOMAIN)
                ;;
            *)
                continue
                ;;
        esac
        # Skip already-handled and skip non-public values
        [ "$name" = "TRAEFIK_HOST" ] && continue
        # Must contain a dot (looks like a real domain) and no slashes/spaces
        if [[ "$value" == *.* && "$value" != *" "* && "$value" != *"/"* ]]; then
            apply_qa_domain_prefix "$name"
        fi
    done < <(env)
else
    export DEPLOY_ENV="prod"
fi

# ============================================================================
# Step 4: Run pre-deployment script if it exists
# ============================================================================
run_hook "$PRE_SCRIPT_PATH" "$PRE_SCRIPT" "$DEVOPS_PATH" || {
    log_error "Pre-deployment script failed, aborting deployment"
    exit 1
}

# ============================================================================
# Step 5: Create deployment compose file with updated tags
# ============================================================================
DEPLOY_COMPOSE="$DEVOPS_PATH/${COMPOSE_FILE%.yml}.deploy.yml"

log_info "Updating image tags to version: $VERSION"
update_image_tags "$COMPOSE_PATH" "$VERSION" "$DEPLOY_COMPOSE"

# ----------------------------------------------------------------------------
# Convert sensitive env vars (*_SECRET, *_KEY, *_TOKEN, *_PASSWORD,
# *_CONNECTIONSTRING, *_CONNECTION_STRING) into Docker secrets and rewrite
# the compose file accordingly.
# ----------------------------------------------------------------------------
PYTHON_BIN="$(command -v python3 || command -v python || true)"

SECRETS_PROCESSOR="$SCRIPT_DIR/process_secrets.py"
if [ -f "$SECRETS_PROCESSOR" ]; then
    if [ -n "$PYTHON_BIN" ]; then
        log_info "Converting sensitive env vars into Docker secrets..."
        TMP_PROCESSED="${DEPLOY_COMPOSE}.processed"
        if "$PYTHON_BIN" "$SECRETS_PROCESSOR" "$DEPLOY_COMPOSE" "$TMP_PROCESSED" "$STACK_NAME"; then
            mv "$TMP_PROCESSED" "$DEPLOY_COMPOSE"
            log_success "Sensitive env vars processed"
        else
            log_error "Failed to process secrets — aborting deployment"
            rm -f "$DEPLOY_COMPOSE" "$TMP_PROCESSED"
            exit 1
        fi
    else
        log_warning "python not found — skipping Docker secrets conversion"
    fi
else
    log_warning "process_secrets.py not found at $SECRETS_PROCESSOR — skipping"
fi

# ----------------------------------------------------------------------------
# QA mode: prefix Traefik router/service/middleware names so the QA stack's
# labels do not clash with the prod stack's. Without this, both stacks declare
# routers with identical names and Traefik silently drops one — making the QA
# instance unreachable even when its Host() rule is already qa-prefixed.
# ----------------------------------------------------------------------------
if [ "$ISOLATED_MODE" = "true" ]; then
    QA_LABELS_PROCESSOR="$SCRIPT_DIR/process_qa_labels.py"
    if [ -f "$QA_LABELS_PROCESSOR" ] && [ -n "$PYTHON_BIN" ]; then
        log_info "Prefixing Traefik labels with '${QA_STACK_PREFIX}' for ${ENV_LABEL} isolation..."
        TMP_LABELS="${DEPLOY_COMPOSE}.qa-labels"
        # QA_HOST_APEX is a QA-only override (an already qa-prefixed apex): a
        # preview always derives its hosts from the compose file's own apex.
        if [ "$PREVIEW_MODE" = "true" ]; then
            LABELS_APEX=""
        else
            LABELS_APEX="${QA_HOST_APEX:-}"
        fi
        if QA_HOST_APEX="$LABELS_APEX" "$PYTHON_BIN" "$QA_LABELS_PROCESSOR" \
            "$DEPLOY_COMPOSE" "$TMP_LABELS" "$QA_STACK_PREFIX" "$QA_DOMAIN_PREFIX"; then
            mv "$TMP_LABELS" "$DEPLOY_COMPOSE"
            log_success "Traefik labels prefixed for ${ENV_LABEL}"
        else
            log_error "Failed to prefix Traefik labels — aborting deployment"
            rm -f "$DEPLOY_COMPOSE" "$TMP_LABELS"
            exit 1
        fi
    elif [ ! -f "$QA_LABELS_PROCESSOR" ]; then
        log_warning "process_qa_labels.py not found at $QA_LABELS_PROCESSOR — Traefik labels NOT isolated for ${ENV_LABEL}"
    fi
fi

# Function to resolve environment variables in image names
# If the image has a version tag, use it; otherwise resolve variables
resolve_image_name() {
    local img="$1"
    local version="$2"  # Optional: version to use if variable resolution fails
    
    # Try envsubst first (if available)
    local resolved=$(echo "$img" | envsubst 2>/dev/null || echo "$img")
    
    # If still contains ${}, try to resolve with defaults using sed
    if [[ "$resolved" =~ \$\{ ]]; then
        # Replace ${VAR:-default} with default or VAR value
        resolved=$(echo "$resolved" | sed -E 's/\$\{([^:}]+):-([^}]+)\}/\2/g')
        # Replace remaining ${VAR} with VAR name (fallback)
        resolved=$(echo "$resolved" | sed -E 's/\$\{([^}]+)\}/\1/g')
    fi
    
    # If resolved image still looks wrong (contains variable-like syntax) and we have a version, use it
    if [[ "$resolved" =~ \$\{|^[^:]+$ ]] && [ -n "$version" ]; then
        local base_image="${img%:*}"
        # Extract base without any tag
        if [[ "$base_image" =~ ^(.+): ]]; then
            base_image="${BASH_REMATCH[1]}"
        fi
        # If base_image contains the registry, use version tag
        if [[ "$base_image" =~ ^${REGISTRY}/ ]]; then
            resolved="${base_image}:${version}"
        fi
    fi
    
    echo "$resolved"
}

# Show the images that will be deployed
log_info "Images to deploy:"
grep -E "^\s+image:" "$DEPLOY_COMPOSE" | \
    sed 's/.*image:\s*//' | \
    sed 's/"//g' | \
    sed "s/'//g" | \
    tr -d ' ' | \
    sed "s|\${DOCKER_REGISTRY_URL}|${REGISTRY}|g" | \
    grep "^${REGISTRY}/" | \
    while read -r img; do
        RESOLVED_IMG=$(resolve_image_name "$img" "$VERSION")
        echo "  - $RESOLVED_IMG"
    done
echo ""

# ============================================================================
# Step 6: Pull images from registry
# ============================================================================
log_info "Pulling images from registry..."

IMAGES=$(grep -E "^\s+image:" "$DEPLOY_COMPOSE" | \
    sed 's/.*image:\s*//' | \
    sed 's/"//g' | \
    sed "s/'//g" | \
    tr -d ' ' | \
    sed "s|\${DOCKER_REGISTRY_URL}|${REGISTRY}|g" | \
    grep "^${REGISTRY}/" || true)

for img in $IMAGES; do
    RESOLVED_IMG=$(resolve_image_name "$img" "$VERSION")
    log_info "Pulling $RESOLVED_IMG"
    
    # Try to pull with resolved name
    if docker pull "$RESOLVED_IMG" 2>/dev/null; then
        log_success "Pulled $RESOLVED_IMG"
    else
        # If that fails and resolved tag is "latest", try with version tag
        if [[ "$RESOLVED_IMG" =~ :latest$ ]]; then
            BASE_IMAGE="${RESOLVED_IMG%:*}"
            log_info "Trying ${BASE_IMAGE}:${VERSION} instead..."
            if docker pull "${BASE_IMAGE}:${VERSION}" 2>/dev/null; then
                log_success "Pulled ${BASE_IMAGE}:${VERSION}"
                # Tag it as the resolved name for docker stack deploy
                docker tag "${BASE_IMAGE}:${VERSION}" "$RESOLVED_IMG"
            else
                log_error "Failed to pull image: $RESOLVED_IMG (also tried ${BASE_IMAGE}:${VERSION})"
                log_error "Make sure the image exists and you're logged into the registry"
                rm -f "$DEPLOY_COMPOSE"
                exit 1
            fi
        else
            log_error "Failed to pull image: $RESOLVED_IMG"
            log_error "Make sure the image exists and you're logged into the registry"
            rm -f "$DEPLOY_COMPOSE"
            exit 1
        fi
    fi
done

log_success "All images pulled successfully!"

# Show image SHAs
echo ""
log_info "Image details (SHA256 digests):"
MISMATCHED_IMAGES=()
for img in $IMAGES; do
    RESOLVED_IMG=$(resolve_image_name "$img" "$VERSION")
    IMAGE_ID=$(docker images --no-trunc --format "{{.ID}}" "$RESOLVED_IMG" 2>/dev/null | head -1)
    
    if [ -n "$IMAGE_ID" ]; then
        echo "  - $RESOLVED_IMG"
        echo "    Local SHA: $IMAGE_ID"
        
        # Get the registry digest (RepoDigests field shows the actual registry SHA)
        REPO_DIGEST=$(docker image inspect "$RESOLVED_IMG" --format '{{range .RepoDigests}}{{.}}{{end}}' 2>/dev/null | head -1)
        
        if [ -n "$REPO_DIGEST" ]; then
            # Extract just the SHA part after @sha256:
            REGISTRY_SHA=$(echo "$REPO_DIGEST" | sed 's/.*@sha256:/sha256:/')
            echo "    Registry SHA: $REGISTRY_SHA"
            
            # Verify the image was pulled from registry (not locally built)
            # by checking that the RepoDigests reference matches our registry
            if echo "$REPO_DIGEST" | grep -q "^${REGISTRY}/"; then
                echo "    Source: Registry (verified)"
            else
                log_warning "Image may not have been pulled from expected registry: $REGISTRY"
                echo "    RepoDigest: $REPO_DIGEST"
            fi
        else
            log_warning "No registry digest found - image may be locally built"
            MISMATCHED_IMAGES+=("$RESOLVED_IMG (no registry digest)")
        fi
    fi
done
echo ""

# ============================================================================
# Step 7: Deploy the stack
# ============================================================================
log_info "Deploying stack: $STACK_NAME"

# Remember which tasks exist before the deploy, so the convergence check below
# only blames this deploy for failures of the tasks it creates.
DEPLOY_START_EPOCH=$(date +%s)
BASELINE_TASKS_FILE=$(mktemp)
docker stack ps "$STACK_NAME" -q --no-trunc > "$BASELINE_TASKS_FILE" 2>/dev/null || true

# Deploy the stack (rolling update if it already exists)
docker stack deploy -c "$DEPLOY_COMPOSE" "$STACK_NAME" --with-registry-auth --prune || {
    log_error "Failed to deploy stack!"
    rm -f "$DEPLOY_COMPOSE" "$BASELINE_TASKS_FILE"
    exit 1
}

log_success "Stack submitted to the swarm"

# ============================================================================
# Step 7b: Wait for every service to converge on the new version
# ============================================================================
# "docker stack deploy" returns as soon as the specs are accepted; a service
# whose new containers crash in a loop would still count as deployed. Wait
# until all replicas run the new image (see verify-deploy.sh for the rules).
if [ "${DEPLOY_VERIFY:-true}" = "false" ]; then
    log_warning "DEPLOY_VERIFY=false — skipping the convergence check"
elif [ ! -f "$VERIFY_SCRIPT" ]; then
    log_warning "verify-deploy.sh not found at $VERIFY_SCRIPT — skipping the convergence check"
else
    VERIFY_RC=0
    bash "$VERIFY_SCRIPT" converge "$STACK_NAME" "$BASELINE_TASKS_FILE" "$DEPLOY_START_EPOCH" || VERIFY_RC=$?
    if [ "$VERIFY_RC" -ne 0 ]; then
        rm -f "$BASELINE_TASKS_FILE"
        fail_verification "converge" "stack $STACK_NAME did not converge on version $VERSION"
    fi
fi
rm -f "$BASELINE_TASKS_FILE"

log_success "Stack deployed successfully!"

# ============================================================================
# Step 8: Run post-deployment script if it exists
# ============================================================================
run_hook "$POST_SCRIPT_PATH" "$POST_SCRIPT" "$DEVOPS_PATH" || {
    log_warning "Post-deployment script failed (stack is already deployed)"
}

# ============================================================================
# Step 8b: Resync privatenetwork_traefik only if the deployed domain
#          responds with "Bad Gateway" (stale backend IPs after a redeploy).
# ============================================================================
# Skip when deploying the privatenetwork stack itself (already updated above).
if [ "$STACK_NAME" != "privatenetwork" ] && docker service inspect privatenetwork_traefik >/dev/null 2>&1; then
    readarray -t CHECK_HOSTS < <(collect_probe_hosts)

    if [ ${#CHECK_HOSTS[@]} -eq 0 ]; then
        log_info "No deployed host found to probe — skipping Traefik resync check"
    else
        NEEDS_TRAEFIK_RESYNC=false
        BAD_GATEWAY_HOST=""
        # The page goes to a file, never into a variable: sourcing .env left
        # allexport on, so a variable holding it would be exported, and one
        # over Linux's 128 KiB per-variable limit makes every later command
        # fail with "Argument list too long".
        PROBE_BODY_FILE=$(mktemp)

        for host in "${CHECK_HOSTS[@]}"; do
            log_info "Probing https://$host for Bad Gateway..."
            # Retry a few times so we don't false-positive while replicas
            # are still starting up after the rolling update.
            for attempt in 1 2 3; do
                : > "$PROBE_BODY_FILE"
                STATUS=$(curl -k -sS \
                    --connect-timeout 5 --max-time 10 \
                    -o "$PROBE_BODY_FILE" -w '%{http_code}' \
                    "https://$host" 2>/dev/null || true)

                if [ "$STATUS" = "502" ] || grep -qi "Bad Gateway" "$PROBE_BODY_FILE"; then
                    log_warning "Bad Gateway from $host (HTTP ${STATUS:-?}, attempt ${attempt}/3)"
                    if [ "$attempt" -lt 3 ]; then
                        sleep 3
                        continue
                    fi
                    NEEDS_TRAEFIK_RESYNC=true
                    BAD_GATEWAY_HOST="$host"
                    break
                fi

                log_info "Got HTTP ${STATUS:-?} from $host — no Bad Gateway"
                break
            done

            [ "$NEEDS_TRAEFIK_RESYNC" = true ] && break
        done
        rm -f "$PROBE_BODY_FILE"

        if [ "$NEEDS_TRAEFIK_RESYNC" = true ]; then
            log_info "Persistent Bad Gateway on $BAD_GATEWAY_HOST — force-updating privatenetwork_traefik..."
            if docker service update --force privatenetwork_traefik >/dev/null 2>&1; then
                log_success "privatenetwork_traefik resynced"
            else
                log_warning "Could not force-update privatenetwork_traefik (continuing)"
            fi
        else
            log_info "No Bad Gateway detected — leaving privatenetwork_traefik untouched"
        fi
    fi
fi

# ============================================================================
# Step 8c: Optional HTTP health check through Traefik
# ============================================================================
# Enabled by DEPLOY_HEALTHCHECK_URL (full URL) or DEPLOY_HEALTHCHECK_PATH (e.g.
# /health, probed on TRAEFIK_HOST, or else on the first Host() rule of the
# compose file). Runs after the Traefik resync above so a stale route does not
# fail it.
if [ "${DEPLOY_VERIFY:-true}" != "false" ] && [ -f "$VERIFY_SCRIPT" ]; then
    HEALTH_URL="${DEPLOY_HEALTHCHECK_URL:-}"
    if [ -z "$HEALTH_URL" ] && [ -n "${DEPLOY_HEALTHCHECK_PATH:-}" ]; then
        HEALTH_HOST=$(collect_probe_hosts | head -n 1)
        if [ -n "$HEALTH_HOST" ]; then
            HEALTH_PATH="$DEPLOY_HEALTHCHECK_PATH"
            [[ "$HEALTH_PATH" == /* ]] || HEALTH_PATH="/$HEALTH_PATH"
            HEALTH_URL="https://${HEALTH_HOST}${HEALTH_PATH}"
        else
            log_warning "DEPLOY_HEALTHCHECK_PATH is set but no TRAEFIK_HOST / Host() rule was found — skipping health check"
        fi
    fi
    if [ -n "$HEALTH_URL" ]; then
        bash "$VERIFY_SCRIPT" health "$HEALTH_URL" \
            || fail_verification "health" "$HEALTH_URL is not healthy after deploying version $VERSION"
    fi
fi

# ============================================================================
# Step 9: Cleanup and restore
# ============================================================================
# Remove temporary deploy file
rm -f "$DEPLOY_COMPOSE"

# Restore original branch/commit if we changed it
if [ -n "$ORIGINAL_BRANCH" ] && [ "$ORIGINAL_BRANCH" != "HEAD" ]; then
    # We were on a real branch, not detached HEAD
    CURRENT_BRANCH_CHECK=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "HEAD")
    if [ -n "$BRANCH" ] && [ "$CURRENT_BRANCH_CHECK" != "$ORIGINAL_BRANCH" ]; then
        log_info "Restoring original branch: $ORIGINAL_BRANCH"
        cd "$REPO_PATH" 2>/dev/null || true
        git checkout "$ORIGINAL_BRANCH" 2>/dev/null || log_warning "Could not restore original branch"
    fi
fi


# ============================================================================
# Summary
# ============================================================================
echo ""
echo "=============================================="
echo "  Deployment Complete!"
echo "=============================================="
echo ""
log_success "Stack: $STACK_NAME"
log_success "Version: $VERSION"
log_success "Compose commit: $CURRENT_COMMIT_SHORT"
echo ""
log_info "Useful commands:"
echo "  View services:    docker stack services $STACK_NAME"
echo "  View logs:        docker service logs -f ${STACK_NAME}_<service>"
echo "  View replicas:    docker service ps ${STACK_NAME}_<service> --no-trunc"
echo "  Remove stack:     docker stack rm $STACK_NAME"
echo ""

# Show service status
log_info "Service status:"
docker stack services "$STACK_NAME" 2>/dev/null || true
echo ""

# Display warnings for images without registry digests (locally built)
if [ ${#MISMATCHED_IMAGES[@]} -gt 0 ]; then
    echo ""
    echo -e "${RED}============================================="
    echo -e "  ⚠  WARNING: LOCALLY BUILT IMAGES DETECTED"
    echo -e "=============================================${NC}"
    echo ""
    log_error "The following images have no registry digest (may be locally built):"
    for mismatch_img in "${MISMATCHED_IMAGES[@]}"; do
        echo -e "${RED}  ✗ $mismatch_img${NC}"
    done
    echo ""
    echo -e "${YELLOW}This may indicate:${NC}"
    echo "  - Image was built locally and not pushed to registry"
    echo "  - Image tag does not exist in the registry"
    echo ""
    echo -e "${YELLOW}Recommended actions:${NC}"
    echo "  - Verify image versions: docker image inspect <image>"
    echo "  - Force pull latest: docker pull <image>"
    echo "  - Push local images: docker push <image>"
    echo ""
fi
