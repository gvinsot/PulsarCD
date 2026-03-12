#!/bin/bash
#
# Deploy Service
# ==============
# Deploys a service stack using previously built and tagged Docker images
#
# Usage:
#   ./deploy-service.sh <folder> <version> [branch] [commit]
#
# Arguments:
#   folder  - Repository folder name (e.g., "Art Retrainer") or full path
#   version - Version/build number to deploy (e.g., "42", "main", "abc1234")
#   branch  - (Optional) Branch name to checkout for compose file
#   commit  - (Optional) Specific commit hash to checkout
#
# Examples:
#   ./deploy-service.sh "Art Retrainer" 42
#   ./deploy-service.sh "Art Retrainer" 42 main
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
#   5. Run docker-compose.post.sh if it exists
#

set -e

# ============================================================================
# Configuration
# ============================================================================
REGISTRY="registry.methodinfo.fr"
DEVOPS_FOLDER="devops"
COMPOSE_FILE="docker-compose.swarm.yml"
PRE_SCRIPT="docker-compose.pre.sh"
POST_SCRIPT="docker-compose.post.sh"

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
    echo "Usage: $0 <folder> <version> [branch] [commit]"
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
    
    # Find and backup all .env files
    while IFS= read -r -d '' env_file; do
        local rel_path="${env_file#$repo_path/}"
        local backup_path="$ENV_BACKUP_DIR/$rel_path"
        mkdir -p "$(dirname "$backup_path")"
        cp "$env_file" "$backup_path"
        log_info "Backed up: $rel_path"
    done < <(find "$repo_path" -name ".env" -type f -print0 2>/dev/null)
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
        done < <(find "$ENV_BACKUP_DIR" -name ".env" -type f -print0 2>/dev/null)
        
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

# Derive stack name
STACK_NAME=$(get_stack_name "$REPO_FOLDER")

echo ""
echo "=============================================="
echo "  Deploy Service"
echo "=============================================="
echo ""
log_info "Repository: $REPO_PATH"
log_info "DevOps folder: $DEVOPS_PATH"
log_info "Stack name: $STACK_NAME"
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
    if [ -n "$BRANCH" ]; then
        log_info "Checking out branch: $BRANCH"
        
        # Check if branch exists locally
        if git show-ref --verify --quiet "refs/heads/$BRANCH"; then
            # Local branch exists - checkout
            git checkout "$BRANCH" || {
                log_error "Failed to checkout branch: $BRANCH"
                exit 1
            }
        elif git show-ref --verify --quiet "refs/remotes/origin/$BRANCH"; then
            # Branch exists on remote but not locally - create tracking branch
            log_info "Creating local tracking branch for origin/$BRANCH"
            git checkout -b "$BRANCH" "origin/$BRANCH" || {
                log_error "Failed to checkout remote branch: origin/$BRANCH"
                exit 1
            }
        elif git show-ref --verify --quiet "refs/tags/$BRANCH"; then
            # It's a tag - checkout directly
            log_info "Checking out tag: $BRANCH"
            git checkout "$BRANCH" || {
                log_error "Failed to checkout tag: $BRANCH"
                exit 1
            }
        else
            # Not found anywhere
            log_error "Branch or tag not found: $BRANCH (checked locally, remotely, and tags)"
            exit 1
        fi

        # Force reset to remote if no specific commit requested and not a tag
        if [ -z "$COMMIT" ] && ! git show-ref --verify --quiet "refs/tags/$BRANCH"; then
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

# Export VERSION and STACK_NAME for use in hook scripts
export DEPLOY_VERSION="$VERSION"
export DEPLOY_STACK_NAME="$STACK_NAME"
export DEPLOY_REGISTRY="$REGISTRY"

# Export registry variables for compose files (support both REGISTRY_URL and DOCKER_REGISTRY_URL)
export REGISTRY_URL="${REGISTRY_URL:-$REGISTRY}"
export DOCKER_REGISTRY_URL="${DOCKER_REGISTRY_URL:-$REGISTRY}"
export REPO_NAME="${REPO_NAME:-$STACK_NAME}"

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
    sed "s/\${DOCKER_REGISTRY_URL}/${REGISTRY}/g" | \
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
    sed "s/\${DOCKER_REGISTRY_URL}/${REGISTRY}/g" | \
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
            
            # Get the manifest digest from registry (could be manifest list or single manifest)
            MANIFEST_OUTPUT=$(docker manifest inspect "$RESOLVED_IMG" 2>/dev/null)
            
            if [ -n "$MANIFEST_OUTPUT" ]; then
                # Check if it's a manifest list (has "manifests" field) or single manifest
                if echo "$MANIFEST_OUTPUT" | grep -q '"manifests"'; then
                    # It's a manifest list - extract the manifest list digest
                    # The RepoDigest should match the manifest list digest
                    MANIFEST_LIST_DIGEST=$(echo "$MANIFEST_OUTPUT" | grep -o '"digest"[[:space:]]*:[[:space:]]*"sha256:[^"]*"' | head -1 | sed 's/.*"sha256:/sha256:/' | sed 's/".*//')
                    
                    # For manifest lists, we should compare against the list digest, not platform-specific digest
                    # Actually, docker automatically returns the manifest list digest when we inspect it
                    # So we need to recalculate or accept that RepoDigest is the source of truth
                    
                    # The RepoDigest is what was actually pulled/pushed, so that's our source of truth
                    echo "    Format: Multi-arch manifest list"
                else
                    # Single platform manifest
                    SINGLE_MANIFEST_DIGEST=$(echo "$MANIFEST_OUTPUT" | grep -o '"digest"[[:space:]]*:[[:space:]]*"sha256:[^"]*"' | head -1 | sed 's/.*"sha256:/sha256:/' | sed 's/".*//')
                    
                    if [ -n "$SINGLE_MANIFEST_DIGEST" ] && [ "$REGISTRY_SHA" != "$SINGLE_MANIFEST_DIGEST" ]; then
                        log_warning "Registry manifest SHA differs from RepoDigest!"
                        echo "    Manifest SHA: $SINGLE_MANIFEST_DIGEST"
                        MISMATCHED_IMAGES+=("$RESOLVED_IMG")
                    fi
                fi
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

# Deploy the stack (rolling update if it already exists)
docker stack deploy -c "$DEPLOY_COMPOSE" "$STACK_NAME" --with-registry-auth || {
    log_error "Failed to deploy stack!"
    rm -f "$DEPLOY_COMPOSE"
    exit 1
}

log_success "Stack deployed successfully!"

# ============================================================================
# Step 8: Run post-deployment script if it exists
# ============================================================================
run_hook "$POST_SCRIPT_PATH" "$POST_SCRIPT" "$DEVOPS_PATH" || {
    log_warning "Post-deployment script failed (stack is already deployed)"
}

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

# Display SHA mismatch warnings if any
if [ ${#MISMATCHED_IMAGES[@]} -gt 0 ]; then
    echo ""
    echo -e "${RED}============================================="
    echo -e "  ⚠  WARNING: IMAGE SHA MISMATCH DETECTED"
    echo -e "=============================================${NC}"
    echo ""
    log_error "The following images have SHA mismatches or missing registry digests:"
    for mismatch_img in "${MISMATCHED_IMAGES[@]}"; do
        echo -e "${RED}  ✗ $mismatch_img${NC}"
    done
    echo ""
    echo -e "${YELLOW}This may indicate:${NC}"
    echo "  - Local cached image is stale"
    echo "  - Image tag was overwritten in registry"
    echo "  - Deploying a locally built image instead of registry image"
    echo ""
    echo -e "${YELLOW}Recommended actions:${NC}"
    echo "  - Verify image versions: docker image inspect <image>"
    echo "  - Force pull latest: docker pull <image>"
    echo "  - Check registry: docker manifest inspect <image>"
    echo ""
fi
