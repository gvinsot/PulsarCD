#!/bin/bash
#
# Run Tests
# =========
# Runs the "test" build target from docker-compose.swarm.yml
#
# Usage:
#   ./test.sh <folder> [branch] [commit]
#
# Arguments:
#   folder  - Repository folder name (e.g., "Art Retrainer") or full path
#   branch  - (Optional) Branch name to test from
#   commit  - (Optional) Specific commit hash to test from
#
# Examples:
#   ./test.sh "Art Retrainer"                     # Test current branch
#   ./test.sh "Art Retrainer" main                # Test from main branch
#   ./test.sh "Art Retrainer" develop abc123      # Test specific commit
#
# The script will:
#   1. Update repository (checkout branch/commit if specified, or pull latest)
#   2. Build the "test" service from devops/docker-compose.swarm.yml
#   3. Run the "test" service and capture output
#   4. Return exit code from the test run
#

set -e

# ============================================================================
# Configuration
# ============================================================================
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
    echo "Usage: $0 <folder> [branch] [commit]"
    echo ""
    echo "Arguments:"
    echo "  folder  - Repository folder name (sibling to this repo) or full path"
    echo "  branch  - (Optional) Branch name to test from"
    echo "  commit  - (Optional) Specific commit hash to test from"
    echo ""
    echo "Examples:"
    echo "  $0 \"Art Retrainer\"                     # Test current branch"
    echo "  $0 \"Art Retrainer\" main                # Test from main branch"
    echo "  $0 \"Art Retrainer\" develop abc123      # Test specific commit"
    exit 1
}

# Backup .env files before git operations
backup_env_files() {
    local repo_path="$1"
    ENV_BACKUP_DIR=$(mktemp -d)

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

        rm -rf "$ENV_BACKUP_DIR"
    fi
}

# ============================================================================
# Main Script
# ============================================================================

# Check arguments
if [ $# -lt 1 ]; then
    log_error "Missing required arguments"
    show_usage
fi

REPO_FOLDER="$1"
BRANCH="${2:-}"
COMMIT="${3:-}"

# Get the script's directory and the parent of the parent (where repositories are)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPOS_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"

# Resolve absolute path
if [[ "$REPO_FOLDER" = /* ]]; then
    REPO_PATH="$REPO_FOLDER"
elif [[ "$REPO_FOLDER" = ./* ]] || [[ "$REPO_FOLDER" = ../* ]]; then
    REPO_PATH="$(cd "$SCRIPT_DIR" && cd "$REPO_FOLDER" && pwd)"
else
    REPO_PATH="$REPOS_DIR/$REPO_FOLDER"
fi

# Define devops folder path
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
echo "  Run Tests"
echo "=============================================="
echo ""
log_info "Repository: $REPO_PATH"
log_info "DevOps folder: $DEVOPS_PATH"
[ -n "$BRANCH" ] && log_info "Branch: $BRANCH" || log_info "Branch: (current)"
[ -n "$COMMIT" ] && log_info "Commit: $COMMIT"
log_info "Compose file: $DEVOPS_FOLDER/$COMPOSE_FILE"
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
    git stash push -m "test-script-autostash-$(date +%Y%m%d%H%M%S)"
    STASHED=true
else
    STASHED=false
fi

# Fetch latest changes
log_info "Fetching latest changes from remote..."
git fetch --all --prune

if [ -n "$BRANCH" ]; then
    log_info "Checking out branch: $BRANCH"

    if git show-ref --verify --quiet "refs/heads/$BRANCH"; then
        git checkout "$BRANCH" || {
            log_error "Failed to checkout branch: $BRANCH"
            exit 1
        }
    elif git show-ref --verify --quiet "refs/remotes/origin/$BRANCH"; then
        log_info "Creating local tracking branch for origin/$BRANCH"
        git checkout -b "$BRANCH" "origin/$BRANCH" || {
            log_error "Failed to checkout remote branch: origin/$BRANCH"
            exit 1
        }
    elif git show-ref --verify --quiet "refs/tags/$BRANCH"; then
        log_info "Checking out tag: $BRANCH"
        git checkout "refs/tags/$BRANCH" || {
            log_error "Failed to checkout tag: $BRANCH"
            exit 1
        }
    else
        log_error "Branch or tag not found: $BRANCH (checked locally and on origin)"
        exit 1
    fi

    if [ -z "$COMMIT" ]; then
        log_info "Pulling latest changes..."
        git pull origin "$BRANCH" || log_warning "Could not pull (may be a local-only branch or a tag)"
    else
        log_info "Checking out commit: $COMMIT"
        git checkout "$COMMIT" || {
            log_error "Failed to checkout commit: $COMMIT"
            exit 1
        }
    fi
else
    if [ -z "$ORIGINAL_BRANCH" ] || [ "$ORIGINAL_BRANCH" = "HEAD" ]; then
        log_warning "Currently in detached HEAD state"
        if [ -n "$COMMIT" ]; then
            log_info "Checking out commit: $COMMIT"
            git checkout "$COMMIT" || {
                log_error "Failed to checkout commit: $COMMIT"
                exit 1
            }
        else
            log_warning "Staying in detached HEAD state"
        fi
        BRANCH="detached"
    else
        BRANCH="$ORIGINAL_BRANCH"
        log_info "Using current branch: $BRANCH"

        if [ -n "$COMMIT" ]; then
            log_info "Checking out commit: $COMMIT"
            git checkout "$COMMIT" || {
                log_error "Failed to checkout commit: $COMMIT"
                exit 1
            }
        else
            log_info "Pulling latest changes for branch: $BRANCH"
            git pull origin "$BRANCH" 2>/dev/null || log_warning "Could not pull (may be a local-only branch)"
        fi
    fi
fi

CURRENT_COMMIT=$(git rev-parse HEAD)
CURRENT_COMMIT_SHORT=$(git rev-parse --short HEAD)
log_success "Checked out: $CURRENT_COMMIT_SHORT"

# Restore .env files after git operations
restore_env_files "$REPO_PATH"

# ============================================================================
# Step 2: Validate devops folder and compose file
# ============================================================================
if [ ! -d "$DEVOPS_PATH" ]; then
    log_error "DevOps folder not found: $DEVOPS_PATH"
    exit 1
fi

if [ ! -f "$COMPOSE_PATH" ]; then
    log_error "Compose file not found: $COMPOSE_PATH"
    exit 1
fi

# ============================================================================
# Step 3: Check that the "test" service exists in the compose file
# ============================================================================
log_info "Checking for 'test' service in $COMPOSE_FILE..."

# Check if a "test" service is defined (indented with 2 spaces under services)
if ! grep -qE '^\s{2}test:\s*$' "$COMPOSE_PATH"; then
    log_warning "No 'test' service found in $COMPOSE_FILE"
    log_info "Skipping tests (no test service configured)"
    echo ""

    # Restore original state before exiting
    cd "$REPO_PATH"
    if [ -n "$ORIGINAL_BRANCH" ] && [ "$ORIGINAL_BRANCH" != "HEAD" ]; then
        git checkout "$ORIGINAL_BRANCH" 2>/dev/null || true
    fi
    if [ "$STASHED" = true ]; then
        git stash pop 2>/dev/null || true
    fi

    echo "=============================================="
    echo -e "  ${GREEN}Tests Skipped (no test service)${NC}"
    echo "=============================================="
    echo ""
    exit 0
fi

# ============================================================================
# Step 4: Load environment variables
# ============================================================================
if [ -f "$DEVOPS_PATH/.env" ]; then
    log_info "Loading environment variables from devops/.env..."
    set -a
    source "$DEVOPS_PATH/.env"
    set -a
elif [ -f "$REPO_PATH/.env" ]; then
    log_info "Loading environment variables from .env..."
    set -a
    source "$REPO_PATH/.env"
    set -a
fi

# Export common variables
export REGISTRY_URL="${REGISTRY_URL:-registry.methodinfo.fr}"
export DOCKER_REGISTRY_URL="${DOCKER_REGISTRY_URL:-$REGISTRY_URL}"
export REPO_NAME="${REPO_NAME:-$(basename "$REPO_PATH")}"

# ============================================================================
# Step 5: Build the test service
# ============================================================================
log_info "Building the 'test' service..."

cd "$DEVOPS_PATH"

# Clean up stale containers/networks from previous interrupted runs
# (prevents "container is not connected to the network" errors)
docker compose -f "$COMPOSE_FILE" down --remove-orphans 2>/dev/null || true

if ! docker compose -f "$COMPOSE_FILE" build test; then
    log_error "Test build failed!"

    # Restore original state
    cd "$REPO_PATH"
    if [ -n "$ORIGINAL_BRANCH" ] && [ "$ORIGINAL_BRANCH" != "HEAD" ]; then
        git checkout "$ORIGINAL_BRANCH" 2>/dev/null || true
    fi
    if [ "$STASHED" = true ]; then
        git stash pop 2>/dev/null || true
    fi

    exit 1
fi

log_success "Test service built successfully!"

# ============================================================================
# Step 6: Run the test service
# ============================================================================
log_info "Running the 'test' service..."
echo ""

TEST_EXIT_CODE=0
docker compose -f "$COMPOSE_FILE" run --rm test || TEST_EXIT_CODE=$?

# Clean up containers and networks after test run
docker compose -f "$COMPOSE_FILE" down --remove-orphans 2>/dev/null || true

echo ""

# ============================================================================
# Step 7: Restore original state
# ============================================================================
cd "$REPO_PATH"

if [ -n "$ORIGINAL_BRANCH" ] && [ "$ORIGINAL_BRANCH" != "HEAD" ]; then
    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "HEAD")
    if [ "$CURRENT_BRANCH" != "$ORIGINAL_BRANCH" ]; then
        log_info "Restoring original branch: $ORIGINAL_BRANCH"
        git checkout "$ORIGINAL_BRANCH" 2>/dev/null || log_warning "Could not restore original branch"
    fi
elif [ -n "$ORIGINAL_COMMIT" ] && [ "$ORIGINAL_BRANCH" = "HEAD" ]; then
    CURRENT_COMMIT_CHECK=$(git rev-parse HEAD 2>/dev/null || echo "")
    if [ "$CURRENT_COMMIT_CHECK" != "$ORIGINAL_COMMIT" ]; then
        log_info "Restoring original detached HEAD state: ${ORIGINAL_COMMIT:0:7}"
        git checkout "$ORIGINAL_COMMIT" 2>/dev/null || log_warning "Could not restore original commit"
    fi
fi

if [ "$STASHED" = true ]; then
    log_info "Restoring stashed changes..."
    git stash pop 2>/dev/null || log_warning "Could not restore stash"
fi

# ============================================================================
# Summary
# ============================================================================
echo ""
echo "=============================================="
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "  ${GREEN}Tests Passed!${NC}"
else
    echo -e "  ${RED}Tests Failed! (exit code: $TEST_EXIT_CODE)${NC}"
fi
echo "=============================================="
echo ""
log_info "Repository: $REPO_PATH"
log_info "Branch: $BRANCH"
log_info "Commit: $CURRENT_COMMIT_SHORT"
echo ""

exit $TEST_EXIT_CODE
