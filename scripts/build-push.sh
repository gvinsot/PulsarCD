#!/bin/bash
#
# Build and Push Docker Images
# ============================
# Builds Docker images from other repositories and pushes to registry.methodinfo.fr
#
# Usage:
#   ./build-push.sh <folder> <version> [branch] [commit]
#
# Arguments:
#   folder  - Repository folder name (e.g., "Art Retrainer") or full path
#   version - Major.minor version (e.g., "1.0", "2.3"). Build number is auto-incremented as patch.
#   branch  - (Optional) Branch name to build from. If not specified, uses current branch
#   commit  - (Optional) Specific commit hash to checkout
#
# Examples:
#   ./build-push.sh "Art Retrainer" 1.0                # Build v1.0.1, v1.0.2, etc.
#   ./build-push.sh "Art Retrainer" 1.0 main           # Build from main branch
#   ./build-push.sh "Art Retrainer" 2.0 develop abc123 # Build specific commit
#
# The script will:
#   1. Update repository (checkout branch/commit if specified, or pull latest on current branch)
#   2. Build all images defined in devops/docker-compose.swarm.yml
#   3. Tag the git repo with semantic version (e.g., v1.0.42)
#   4. Tag and push Docker images with the version
#

set -e

# ============================================================================
# Configuration
# ============================================================================
REGISTRY="registry.methodinfo.fr"
DEVOPS_FOLDER="devops"
COMPOSE_FILE="docker-compose.swarm.yml"

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
    echo "  version - Major.minor version (e.g., 1.0, 2.3). Patch is auto-incremented."
    echo "  branch  - (Optional) Branch name to build from. If not specified, uses current branch"
    echo "  commit  - (Optional) Specific commit hash to checkout"
    echo ""
    echo "Examples:"
    echo "  $0 \"Art Retrainer\" 1.0                # Build v1.0.1, v1.0.2, etc."
    echo "  $0 \"Art Retrainer\" 1.0 main           # Build from main branch"
    echo "  $0 \"Art Retrainer\" 2.0 develop abc123 # Build specific commit"
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

# Get the next patch number for a given major.minor version
get_next_patch_number() {
    local repo_path="$1"
    local version="$2"
    local current_max=0
    
    # Fetch all tags
    git -C "$repo_path" fetch --tags 2>/dev/null || true
    
    # Find the highest patch number for this version (format: v1.0.X)
    local tags=$(git -C "$repo_path" tag -l "v${version}.*" 2>/dev/null)
    
    for tag in $tags; do
        # Extract the patch number from vX.Y.Z
        local patch="${tag##*.}"
        if [[ "$patch" =~ ^[0-9]+$ ]] && [ "$patch" -gt "$current_max" ]; then
            current_max=$patch
        fi
    done
    
    echo $((current_max + 1))
}

# Extract image names from docker-compose.swarm.yml
# Returns images from services that have a 'build:' section
get_images_from_compose() {
    local compose_file="$1"
    
    # Simple approach: use awk to find services with both image: and build:
    awk '
    /^[[:space:]]{2}[a-zA-Z][a-zA-Z0-9_-]*:[[:space:]]*$/ {
        # New service at indent 2 - save any previous service with image+build
        if (current_image != "" && has_build) {
            print current_image
        }
        current_image = ""
        has_build = 0
    }
    /^[[:space:]]{4}image:[[:space:]]*/ {
        # image: at indent 4 (direct child of service)
        gsub(/^[[:space:]]+image:[[:space:]]*/, "")
        gsub(/[[:space:]]*$/, "")
        gsub(/"/, "")
        gsub(/\047/, "")  # single quote
        current_image = $0
    }
    /^[[:space:]]{4}build:/ {
        # build: at indent 4
        has_build = 1
    }
    END {
        # Output last service if it has image+build
        if (current_image != "" && has_build) {
            print current_image
        }
    }
    ' "$compose_file" | while read -r img; do
        # Resolve all environment variables (REGISTRY_URL, REPO_NAME, VERSION, etc.)
        resolved=$(echo "$img" | envsubst 2>/dev/null)
        if [ -n "$resolved" ] && ! echo "$resolved" | grep -q '\${'; then
            echo "$resolved"
        else
            # Fallback: resolve ${VAR:-default} patterns manually
            echo "$img" | sed -E 's/\$\{([^:}]+):-([^}]+)\}/\2/g' | sed -E 's/\$\{([^}]+)\}/\1/g'
        fi
    done
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
NO_CACHE="${5:-}"

# Validate version format (major.minor or major.minor.patch)
FULL_VERSION_PROVIDED=false
if [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    # Full version provided (e.g., 1.0.5) - skip auto-increment and git tagging
    FULL_VERSION_PROVIDED=true
elif [[ "$VERSION" =~ ^[0-9]+\.[0-9]+$ ]]; then
    # Major.minor provided (e.g., 1.0) - will auto-increment patch
    :
else
    log_error "Invalid version format: $VERSION"
    log_error "Version must be major.minor (e.g., 1.0) or major.minor.patch (e.g., 1.0.5)"
    exit 1
fi

# Get the script's directory and the parent of the parent (where repositories are)
# Script is in: LogsCrawler/scripts/
# Repos are in: parent of LogsCrawler/ (e.g., ~/repos/)
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

# Define devops folder path (after REPO_PATH is set)
# Safety: if REPO_PATH already ends with /devops, don't append it again
if [[ "$REPO_PATH" == */devops ]]; then
    DEVOPS_PATH="$REPO_PATH"
    REPO_PATH="$(dirname "$REPO_PATH")"
else
    DEVOPS_PATH="$REPO_PATH/$DEVOPS_FOLDER"
fi
COMPOSE_PATH="$DEVOPS_PATH/$COMPOSE_FILE"

# Validate repository exists
if [ ! -d "$REPO_PATH" ]; then
    log_error "Repository folder not found: $REPO_FOLDER"
    exit 1
fi

# Validate it's a git repository
if [ ! -d "$REPO_PATH/.git" ]; then
    log_error "Not a git repository: $REPO_PATH"
    exit 1
fi


echo ""
echo "=============================================="
echo "  Build and Push Docker Images"
echo "=============================================="
echo ""
log_info "Repository: $REPO_PATH"
log_info "DevOps folder: $DEVOPS_PATH"
log_info "Version: $VERSION.x (patch auto-incremented)"
[ -n "$BRANCH" ] && log_info "Branch: $BRANCH" || log_info "Branch: (current)"
[ -n "$COMMIT" ] && log_info "Commit: $COMMIT"
log_info "Compose file: $DEVOPS_FOLDER/$COMPOSE_FILE"
log_info "Registry: $REGISTRY"
echo ""

# ============================================================================
# Step 1: Update repository (checkout branch/commit or pull latest)
# ============================================================================
cd "$REPO_PATH"

# Backup .env files before any git operations
backup_env_files "$REPO_PATH"

# Save current state
ORIGINAL_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
ORIGINAL_COMMIT=$(git rev-parse HEAD 2>/dev/null || echo "")

# Stash any local changes
if ! git diff --quiet HEAD 2>/dev/null; then
    log_warning "Stashing local changes..."
    git stash push -m "build-push-script-autostash-$(date +%Y%m%d%H%M%S)"
    STASHED=true
else
    STASHED=false
fi

# Fetch latest changes
log_info "Fetching latest changes from remote..."
git fetch --all --prune

if [ -n "$BRANCH" ]; then
    # Branch specified - checkout that branch
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
        # It's a tag - checkout in detached HEAD
        log_info "Checking out tag: $BRANCH"
        git checkout "refs/tags/$BRANCH" || {
            log_error "Failed to checkout tag: $BRANCH"
            exit 1
        }
    else
        # Branch/tag doesn't exist locally or remotely
        log_error "Branch or tag not found: $BRANCH (checked locally and on origin)"
        exit 1
    fi

    # Pull latest if no specific commit (and not in detached HEAD)
    if [ -z "$COMMIT" ]; then
        log_info "Pulling latest changes..."
        git pull origin "$BRANCH" || log_warning "Could not pull (may be a local-only branch or a tag)"
    else
        # Checkout specific commit
        log_info "Checking out commit: $COMMIT"
        git checkout "$COMMIT" || {
            log_error "Failed to checkout commit: $COMMIT"
            exit 1
        }
    fi
else
    # No branch specified - use current branch
    if [ -z "$ORIGINAL_BRANCH" ] || [ "$ORIGINAL_BRANCH" = "HEAD" ]; then
        log_warning "Currently in detached HEAD state"
        if [ -n "$COMMIT" ]; then
            # Checkout specific commit
            log_info "Checking out commit: $COMMIT"
            git checkout "$COMMIT" || {
                log_error "Failed to checkout commit: $COMMIT"
                exit 1
            }
        else
            log_warning "Staying in detached HEAD state - consider specifying a branch"
        fi
        BRANCH="detached"
    else
        # We're on a branch
        BRANCH="$ORIGINAL_BRANCH"
        log_info "Using current branch: $BRANCH"
        
        if [ -n "$COMMIT" ]; then
            # Checkout specific commit
            log_info "Checking out commit: $COMMIT"
            git checkout "$COMMIT" || {
                log_error "Failed to checkout commit: $COMMIT"
                exit 1
            }
        else
            # Pull latest on current branch
            log_info "Pulling latest changes for branch: $BRANCH"
            git pull origin "$BRANCH" 2>/dev/null || log_warning "Could not pull (may be a local-only branch)"
        fi
    fi
fi

# Get the current commit hash
CURRENT_COMMIT=$(git rev-parse HEAD)
CURRENT_COMMIT_SHORT=$(git rev-parse --short HEAD)
log_success "Checked out: $CURRENT_COMMIT_SHORT"

# Restore .env files after git operations
restore_env_files "$REPO_PATH"

# ============================================================================
# Step 2: Get version (major.minor.patch)
# ============================================================================
if [ "$FULL_VERSION_PROVIDED" = true ]; then
    FULL_VERSION="$VERSION"
    VERSION_TAG="v${FULL_VERSION}"
    log_info "Version: $FULL_VERSION (tag: $VERSION_TAG) [exact version, no auto-increment]"
else
    PATCH_NUMBER=$(get_next_patch_number "$REPO_PATH" "$VERSION")
    FULL_VERSION="${VERSION}.${PATCH_NUMBER}"
    VERSION_TAG="v${FULL_VERSION}"
    log_info "Version: $FULL_VERSION (tag: $VERSION_TAG)"
fi

# ============================================================================
# Step 3: Get images to build
# ============================================================================
log_info "Analyzing $COMPOSE_FILE..."

# Validate devops folder exists
if [ ! -d "$DEVOPS_PATH" ]; then
    log_error "DevOps folder not found: $DEVOPS_PATH"
    log_error "Expected folder structure: <repo>/devops/docker-compose.swarm.yml"
    exit 1
fi

# Validate docker-compose.swarm.yml exists
if [ ! -f "$COMPOSE_PATH" ]; then
    log_error "Compose file not found: $COMPOSE_PATH"
    exit 1
fi

# Save script arguments before sourcing .env (which might override VERSION, etc.)
SCRIPT_VERSION="$VERSION"
SCRIPT_REPO_FOLDER="$REPO_FOLDER"

# Load environment variables from .env BEFORE reading compose file
# so that variable substitution (e.g., ${REGISTRY_URL}) works correctly
if [ -f "$DEVOPS_PATH/.env" ]; then
    log_info "Loading environment variables from devops/.env..."
    set -a
    source "$DEVOPS_PATH/.env"
    set +a
elif [ -f "$REPO_PATH/.env" ]; then
    log_info "Loading environment variables from .env..."
    set -a
    source "$REPO_PATH/.env"
    set +a
fi

# Restore script arguments (they take priority over .env values)
VERSION="$SCRIPT_VERSION"

# Export build-time variables so envsubst/compose can resolve them
export REGISTRY_URL="${REGISTRY_URL:-$REGISTRY}"
export REPO_NAME="${REPO_NAME:-$(basename "$REPO_PATH")}"
export VERSION="${VERSION}"
export DOCKER_REGISTRY_URL="${DOCKER_REGISTRY_URL:-$REGISTRY}"

log_info "REGISTRY_URL=$REGISTRY_URL"
log_info "REPO_NAME=$REPO_NAME"

log_info "Reading images from compose file..."
IMAGES=$(get_images_from_compose "$COMPOSE_PATH")

if [ -z "$IMAGES" ]; then
    log_error "No services with 'build:' section found in $COMPOSE_FILE"
    echo ""
    log_error "The script looks for services that have:"
    log_error "  1. A 'build:' section (to build the image)"
    log_error "  2. An 'image:' line"
    echo ""
    log_info "Checking compose file for debugging..."
    
    # Check if file has any services
    if ! grep -qE "^\s+[a-zA-Z0-9_-]+:" "$COMPOSE_PATH" 2>/dev/null; then
        log_warning "  - No services found in the compose file"
    else
        log_info "  - Services found in compose file:"
        grep -E "^\s+[a-zA-Z0-9_-]+:" "$COMPOSE_PATH" 2>/dev/null | sed 's/^/    /' || true
    fi
    
    # Check for build sections
    if ! grep -qE "^\s+build:" "$COMPOSE_PATH" 2>/dev/null; then
        log_warning "  - No 'build:' sections found"
    else
        log_info "  - Services with 'build:' section found:"
        # Find services that have build section
        awk '/^[[:space:]]*[a-zA-Z0-9_-]+:[[:space:]]*$/{service=$0; gsub(/[[:space:]]*:|[[:space:]]*$/, "", service)} /^[[:space:]]+build:/{print "    - " service}' "$COMPOSE_PATH" 2>/dev/null || true
    fi
    
    # Check for images with registry
    if ! grep -qE "^\s+image:.*${REGISTRY}" "$COMPOSE_PATH" 2>/dev/null; then
        log_warning "  - No 'image:' lines found starting with $REGISTRY/"
    else
        log_info "  - Images found starting with $REGISTRY/:"
        grep -E "^\s+image:.*${REGISTRY}" "$COMPOSE_PATH" 2>/dev/null | sed 's/^/    /' || true
    fi
    
    echo ""
    log_info "Example of correct configuration:"
    echo "  services:"
    echo "    my-service:"
    echo "      image: $REGISTRY/my-project/my-service:latest"
    echo "      build:"
    echo "        context: .."
    echo "        dockerfile: my-service/Dockerfile"
    echo ""
    exit 1
fi

echo ""
log_info "Images to build:"
for img in $IMAGES; do
    echo "  - $img"
done
echo ""

# ============================================================================
# Step 4: Build images
# ============================================================================
log_info "Building Docker images..."

# Build command with optional --no-cache flag
BUILD_ARGS=""
if [ "$NO_CACHE" = "--no-cache" ]; then
    BUILD_ARGS="--no-cache"
    log_info "Building with --no-cache (forced fresh build)"
fi

# Build all images using docker compose (env already loaded in Step 3)
if ! docker compose -f "$COMPOSE_PATH" build $BUILD_ARGS; then
    log_error "Docker build failed!"
    
    # Restore original state
    if [ -n "$ORIGINAL_BRANCH" ]; then
        git checkout "$ORIGINAL_BRANCH" 2>/dev/null || true
    fi
    if [ "$STASHED" = true ]; then
        git stash pop 2>/dev/null || true
    fi
    
    exit 1
fi

log_success "All images built successfully!"

# ============================================================================
# Step 5: Resolve environment variables in image names and tag images
# ============================================================================
log_info "Tagging images with version: $FULL_VERSION"

for img in $IMAGES; do
    # Resolve environment variables in image name (e.g., ${LLM_VERSION:-latest} -> latest or actual value)
    # Use eval to expand variables, but be safe about it
    RESOLVED_IMG=$(echo "$img" | envsubst 2>/dev/null || echo "$img")
    
    # If still contains ${}, try to resolve with defaults
    if [[ "$RESOLVED_IMG" =~ \$\{ ]]; then
        # Replace ${VAR:-default} with default or VAR value
        RESOLVED_IMG=$(echo "$RESOLVED_IMG" | sed -E 's/\$\{([^:}]+):-([^}]+)\}/\2/g' | sed -E 's/\$\{([^}]+)\}/\1/g')
    fi
    
    # Extract base image name (remove tag)
    BASE_IMAGE="${RESOLVED_IMG%:*}"
    
    # Get the actual built image name (docker compose uses the image name from compose file)
    # We need to find what docker compose actually built
    BUILT_IMAGE=$(docker images --format "{{.Repository}}:{{.Tag}}" | grep "^${BASE_IMAGE}:" | head -1)
    
    if [ -z "$BUILT_IMAGE" ]; then
        # Fallback: try to find by repository name only
        BUILT_IMAGE=$(docker images --format "{{.Repository}}:{{.Tag}}" | grep "^${BASE_IMAGE}" | head -1)
    fi
    
    if [ -z "$BUILT_IMAGE" ]; then
        log_warning "Could not find built image for $BASE_IMAGE, skipping tags"
        continue
    fi
    
    log_info "Tagging $BUILT_IMAGE as ${BASE_IMAGE}:${FULL_VERSION}"
    
    # Tag with full version (e.g., 1.0.1)
    docker tag "$BUILT_IMAGE" "${BASE_IMAGE}:${FULL_VERSION}"
    log_success "Tagged: ${BASE_IMAGE}:${FULL_VERSION}"
    
    # Tag with major.minor version (e.g., 1.0)
    docker tag "$BUILT_IMAGE" "${BASE_IMAGE}:${VERSION}"
    log_success "Tagged: ${BASE_IMAGE}:${VERSION}"
    
    # Tag with commit hash
    docker tag "$BUILT_IMAGE" "${BASE_IMAGE}:${CURRENT_COMMIT_SHORT}"
    log_success "Tagged: ${BASE_IMAGE}:${CURRENT_COMMIT_SHORT}"
done

# ============================================================================
# Step 6: Push images to registry
# ============================================================================
log_info "Pushing images to $REGISTRY..."

for img in $IMAGES; do
    # Resolve environment variables in image name (same as tagging step)
    RESOLVED_IMG=$(echo "$img" | envsubst 2>/dev/null || echo "$img")
    
    # If still contains ${}, try to resolve with defaults
    if [[ "$RESOLVED_IMG" =~ \$\{ ]]; then
        RESOLVED_IMG=$(echo "$RESOLVED_IMG" | sed -E 's/\$\{([^:}]+):-([^}]+)\}/\2/g' | sed -E 's/\$\{([^}]+)\}/\1/g')
    fi
    
    BASE_IMAGE="${RESOLVED_IMG%:*}"
    RESOLVED_TAG="${RESOLVED_IMG##*:}"
    
    # Push all tags
    log_info "Pushing ${BASE_IMAGE}:${FULL_VERSION}"
    docker push "${BASE_IMAGE}:${FULL_VERSION}" || log_error "Failed to push ${BASE_IMAGE}:${FULL_VERSION}"
    
    log_info "Pushing ${BASE_IMAGE}:${VERSION}"
    docker push "${BASE_IMAGE}:${VERSION}" || log_error "Failed to push ${BASE_IMAGE}:${VERSION}"
    
    log_info "Pushing ${BASE_IMAGE}:${CURRENT_COMMIT_SHORT}"
    docker push "${BASE_IMAGE}:${CURRENT_COMMIT_SHORT}" || log_error "Failed to push ${BASE_IMAGE}:${CURRENT_COMMIT_SHORT}"
    
    # Also push the resolved tag (e.g., "latest") if it's different from version tags
    if [ "$RESOLVED_TAG" != "$FULL_VERSION" ] && [ "$RESOLVED_TAG" != "$VERSION" ] && [ "$RESOLVED_TAG" != "$CURRENT_COMMIT_SHORT" ]; then
        log_info "Pushing ${BASE_IMAGE}:${RESOLVED_TAG}"
        docker push "${BASE_IMAGE}:${RESOLVED_TAG}" || log_warning "Could not push ${BASE_IMAGE}:${RESOLVED_TAG}"
    fi
done

log_success "All images pushed successfully!"

# ============================================================================
# Step 7: Tag git repository with version (skip if full version was provided)
# ============================================================================
if [ "$FULL_VERSION_PROVIDED" = true ]; then
    log_info "Skipping git tagging (exact version $FULL_VERSION provided, tag already exists)"
else
    log_info "Creating git tag: $VERSION_TAG"

    # Create annotated tag
    git tag -a "$VERSION_TAG" -m "Version $FULL_VERSION from branch $BRANCH (commit $CURRENT_COMMIT_SHORT)"

    # Push tag to remote
    log_info "Pushing tag to remote..."
    git push origin "$VERSION_TAG" || log_warning "Could not push tag (check remote permissions)"

    log_success "Git tag created: $VERSION_TAG"
fi

# ============================================================================
# Step 8: Restore original state (optional)
# ============================================================================
# Go back to original branch/commit if different
if [ -n "$ORIGINAL_BRANCH" ] && [ "$ORIGINAL_BRANCH" != "HEAD" ]; then
    # We were on a real branch, not detached HEAD
    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "HEAD")
    if [ "$CURRENT_BRANCH" != "$ORIGINAL_BRANCH" ]; then
        log_info "Restoring original branch: $ORIGINAL_BRANCH"
        git checkout "$ORIGINAL_BRANCH" 2>/dev/null || log_warning "Could not restore original branch"
    fi
elif [ -n "$ORIGINAL_COMMIT" ] && [ "$ORIGINAL_BRANCH" = "HEAD" ]; then
    # We were in detached HEAD state, restore the commit
    CURRENT_COMMIT_CHECK=$(git rev-parse HEAD 2>/dev/null || echo "")
    if [ "$CURRENT_COMMIT_CHECK" != "$ORIGINAL_COMMIT" ]; then
        log_info "Restoring original detached HEAD state: ${ORIGINAL_COMMIT:0:7}"
        git checkout "$ORIGINAL_COMMIT" 2>/dev/null || log_warning "Could not restore original commit"
    fi
fi

# Restore stashed changes
if [ "$STASHED" = true ]; then
    log_info "Restoring stashed changes..."
    git stash pop 2>/dev/null || log_warning "Could not restore stash"
fi

# ============================================================================
# Summary
# ============================================================================
echo ""
echo "=============================================="
echo "  Build Complete!"
echo "=============================================="
echo ""
log_success "Repository: $REPO_PATH"
log_success "Branch: $BRANCH"
log_success "Commit: $CURRENT_COMMIT_SHORT"
log_success "Version: $FULL_VERSION"
log_success "Git Tag: $VERSION_TAG"
echo ""
log_info "Images pushed:"
for img in $IMAGES; do
    # Resolve environment variables in image name (same as tagging/pushing steps)
    RESOLVED_IMG=$(echo "$img" | envsubst 2>/dev/null || echo "$img")
    
    # If still contains ${}, try to resolve with defaults
    if [[ "$RESOLVED_IMG" =~ \$\{ ]]; then
        RESOLVED_IMG=$(echo "$RESOLVED_IMG" | sed -E 's/\$\{([^:}]+):-([^}]+)\}/\2/g' | sed -E 's/\$\{([^}]+)\}/\1/g')
    fi
    
    BASE_IMAGE="${RESOLVED_IMG%:*}"
    echo "  - ${BASE_IMAGE}:${FULL_VERSION}"
    echo "  - ${BASE_IMAGE}:${VERSION}"
    echo "  - ${BASE_IMAGE}:${CURRENT_COMMIT_SHORT}"
done
echo ""
