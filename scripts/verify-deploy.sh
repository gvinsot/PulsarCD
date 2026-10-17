#!/bin/bash
#
# Verify Deploy
# =============
# Post-deploy checks run by deploy-service.sh once "docker stack deploy" has
# returned. That command only submits the new service specs to the swarm: it
# succeeds before a single new task is running, and a crash-looping container
# is invisible to it. This script turns "submitted" into "actually running".
#
# Usage:
#   verify-deploy.sh converge <stack> [baseline_tasks_file] [deploy_start_epoch]
#   verify-deploy.sh health <url> [<url>...]
#
# converge
#   Waits until every service of <stack> has converged:
#     - replicated services: as many tasks running the service's current image
#       as requested replicas, and no other task still desired running
#       (replicas: 0 converges as soon as the old tasks are gone);
#     - global services: every task the scheduler wants running is running the
#       current image;
#     - job services (replicated-job / global-job): only checked for failures;
#     - one-shot replicated services (restart_policy condition none or
#       on-failure): tasks that completed on the current image count as done.
#   It fails as soon as:
#     - a task created by this deploy ends up Failed or Rejected (more than
#       DEPLOY_CONVERGE_MAX_FAILURES of them), which is how a crash loop shows;
#     - the swarm itself gave up on an update started by this deploy
#       (UpdateStatus paused / rollback_*, i.e. update_config.failure_action
#       fired);
#     - DEPLOY_CONVERGE_TIMEOUT elapses before convergence.
#   baseline_tasks_file lists the task IDs that existed before the deploy
#   (docker stack ps -q --no-trunc), so failures left over from earlier deploys
#   are not blamed on this one. deploy_start_epoch (seconds) tells a fresh swarm
#   update status from a stale one. Both are optional: without them every task
#   and every update status counts.
#
# health
#   Polls each URL until it answers HTTP 2xx/3xx, for at most
#   DEPLOY_HEALTHCHECK_TIMEOUT seconds overall.
#
# Tunables (environment, typically set in the project's devops/.env):
#   DEPLOY_CONVERGE_TIMEOUT       seconds to wait for convergence      (180)
#   DEPLOY_CONVERGE_INTERVAL      seconds between two polls            (5)
#   DEPLOY_CONVERGE_STABLE        seconds the stack must stay converged
#                                 before it is declared healthy        (10)
#   DEPLOY_CONVERGE_MAX_FAILURES  failed/rejected new tasks tolerated  (0)
#   DEPLOY_HEALTHCHECK_TIMEOUT    seconds to wait for a 2xx/3xx        (60)
#   DEPLOY_HEALTHCHECK_INTERVAL   seconds between two probes           (5)
#
# Exit codes:
#   0  verified
#   2  usage error
#   3  the stack did not converge (failed tasks, swarm rollback, timeout)
#   4  the health URL never answered 2xx/3xx
#

set -u

EXIT_USAGE=2
EXIT_NOT_CONVERGED=3
EXIT_UNHEALTHY=4

# Tolerance for clock differences between this host and the swarm manager that
# stamps UpdateStatus.StartedAt.
CLOCK_SKEW_SECONDS=10

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; }

# Read a non-negative integer tunable, falling back to its default when unset
# or malformed (a typo in .env must not turn into an instant timeout).
int_setting() {
    local name="$1" default="$2"
    local value="${!name:-}"
    if [[ "$value" =~ ^[0-9]+$ ]]; then
        echo "$value"
    else
        [ -n "$value" ] && log_warning "Ignoring invalid $name='$value' (using $default)" >&2
        echo "$default"
    fi
}

# Drop the "@sha256:..." pin docker adds to service images after a deploy with
# --with-registry-auth, so spec and task images compare on name:tag.
strip_digest() {
    echo "${1%%@sha256:*}"
}

# ----------------------------------------------------------------------------
# converge
# ----------------------------------------------------------------------------

# Inspect one service and set:
#   SVC_STATE  converged | pending | failed
#   SVC_DETAIL one-line human summary
check_service() {
    local svc="$1" baseline_file="$2" start_epoch="$3"
    local info mode spec_image restart_condition update_state update_started
    SVC_STATE="pending"
    SVC_DETAIL=""

    if ! info=$(docker service inspect "$svc" --format \
        '{{if .Spec.Mode.Global}}global{{else if .Spec.Mode.Replicated}}{{.Spec.Mode.Replicated.Replicas}}{{else}}job{{end}}|{{.Spec.TaskTemplate.ContainerSpec.Image}}|{{with .Spec.TaskTemplate.RestartPolicy}}{{.Condition}}{{end}}|{{with .UpdateStatus}}{{.State}}|{{with .StartedAt}}{{.Unix}}{{end}}{{end}}' \
        2>/dev/null); then
        SVC_DETAIL="cannot inspect service"
        return
    fi
    IFS='|' read -r mode spec_image restart_condition update_state update_started <<< "$info"
    spec_image=$(strip_digest "$spec_image")

    # A swarm update started by this deploy that the swarm paused or rolled
    # back means update_config.failure_action fired: the new tasks failed.
    local update_is_fresh=true
    if [ -n "$start_epoch" ] && [[ "${update_started:-}" =~ ^[0-9]+$ ]] \
        && [ "$update_started" -lt $((start_epoch - CLOCK_SKEW_SECONDS)) ]; then
        update_is_fresh=false
    fi
    if [ -n "${update_state:-}" ] && [ "$update_is_fresh" = true ]; then
        case "$update_state" in
            paused|rollback_started|rollback_paused|rollback_completed)
                SVC_STATE="failed"
                SVC_DETAIL="swarm update ${update_state} (update_config.failure_action fired)"
                return
                ;;
        esac
    fi

    local tasks
    if ! tasks=$(docker service ps "$svc" --no-trunc \
        --format '{{.ID}}|{{.Image}}|{{.DesiredState}}|{{.CurrentState}}|{{.Error}}' 2>/dev/null); then
        SVC_DETAIL="cannot list tasks"
        return
    fi

    local id image desired current error
    local running_new=0 desired_running=0 completed_new=0 failed=0 failure_detail=""
    while IFS='|' read -r id image desired current error; do
        [ -z "$id" ] && continue
        if [ "$desired" = "Running" ]; then
            desired_running=$((desired_running + 1))
            if [[ "$current" == Running* ]] && [ "$(strip_digest "$image")" = "$spec_image" ]; then
                running_new=$((running_new + 1))
            fi
        fi
        # A one-shot task (restart_policy none / on-failure) that exited 0
        # is done: swarm moves it to desired Shutdown / current Complete.
        if [[ "$current" == Complete* ]] && [ "$(strip_digest "$image")" = "$spec_image" ]; then
            completed_new=$((completed_new + 1))
        fi
        # Only tasks created by this deploy count as failures.
        if [[ "$current" == Failed* || "$current" == Rejected* ]]; then
            if [ -z "$baseline_file" ] || ! grep -qxF "$id" "$baseline_file" 2>/dev/null; then
                failed=$((failed + 1))
                [ -z "$failure_detail" ] && failure_detail="${current%% *}: ${error:-no error message}"
            fi
        fi
    done <<< "$tasks"

    if [ "$failed" -gt "$MAX_FAILED_TASKS" ]; then
        SVC_STATE="failed"
        SVC_DETAIL="${failed} new task(s) failed (${failure_detail})"
        return
    fi

    local expected
    case "$mode" in
        job)
            SVC_STATE="converged"
            SVC_DETAIL="job service, ${failed} failed task(s)"
            return
            ;;
        global)
            expected="$desired_running"
            ;;
        *)
            if [[ "$mode" =~ ^[0-9]+$ ]]; then
                expected="$mode"
            else
                SVC_DETAIL="unknown service mode '${mode}'"
                return
            fi
            ;;
    esac

    SVC_DETAIL="${running_new}/${expected} running ${spec_image##*/}"
    [ -n "${update_state:-}" ] && SVC_DETAIL="${SVC_DETAIL}, update ${update_state}"
    if [ "${update_state:-}" = "updating" ]; then
        return
    fi
    if [ "$running_new" -eq "$expected" ] && [ "$desired_running" -eq "$expected" ]; then
        SVC_STATE="converged"
        return
    fi
    # One-shot services (e.g. a migration with restart_policy condition: none
    # or on-failure) are never restarted once they exit 0, so their replicas
    # end up Complete instead of Running: count those as converged, as long as
    # no task is still pending or running an old image.
    case "${restart_condition:-any}" in
        none|on-failure)
            if [ "$desired_running" -eq "$running_new" ]                 && [ $((running_new + completed_new)) -ge "$expected" ]; then
                SVC_STATE="converged"
                SVC_DETAIL="${SVC_DETAIL}, ${completed_new} completed (restart_policy ${restart_condition})"
            fi
            ;;
    esac
}

converge() {
    local stack="${1:-}" baseline_file="${2:-}" start_epoch="${3:-}"
    if [ -z "$stack" ]; then
        log_error "Usage: $0 converge <stack> [baseline_tasks_file] [deploy_start_epoch]"
        return $EXIT_USAGE
    fi
    [[ "$start_epoch" =~ ^[0-9]+$ ]] || start_epoch=""

    local timeout interval stable
    timeout=$(int_setting DEPLOY_CONVERGE_TIMEOUT 180)
    interval=$(int_setting DEPLOY_CONVERGE_INTERVAL 5)
    stable=$(int_setting DEPLOY_CONVERGE_STABLE 10)
    MAX_FAILED_TASKS=$(int_setting DEPLOY_CONVERGE_MAX_FAILURES 0)

    local services
    services=$(docker stack services "$stack" --format '{{.Name}}' 2>/dev/null | sort)
    if [ -z "$services" ]; then
        log_error "Stack $stack has no services after deploy"
        return $EXIT_NOT_CONVERGED
    fi

    log_info "Waiting up to ${timeout}s for stack $stack to converge ($(echo "$services" | wc -l | tr -d ' ') service(s))..."

    local deadline=$((SECONDS + timeout))
    local converged_since="" last_report="" svc report all_converged
    local -a pending_lines failed_services

    while true; do
        all_converged=true
        report=""
        pending_lines=()
        failed_services=()
        while IFS= read -r svc; do
            [ -z "$svc" ] && continue
            check_service "$svc" "$baseline_file" "$start_epoch"
            report="${report}${svc}=${SVC_STATE}:${SVC_DETAIL};"
            case "$SVC_STATE" in
                failed)
                    all_converged=false
                    failed_services+=("$svc")
                    log_error "$svc: $SVC_DETAIL"
                    ;;
                pending)
                    all_converged=false
                    pending_lines+=("$svc: $SVC_DETAIL")
                    ;;
            esac
        done <<< "$services"

        if [ ${#failed_services[@]} -gt 0 ]; then
            for svc in "${failed_services[@]}"; do
                show_recent_logs "$svc"
            done
            return $EXIT_NOT_CONVERGED
        fi

        if [ "$all_converged" = true ]; then
            [ -z "$converged_since" ] && converged_since=$SECONDS
            if [ $((SECONDS - converged_since)) -ge "$stable" ]; then
                log_success "Stack $stack converged: all services running the deployed version"
                return 0
            fi
        else
            converged_since=""
            # Only print progress when something changed, to keep logs short.
            if [ "$report" != "$last_report" ]; then
                local line
                for line in "${pending_lines[@]}"; do
                    log_info "  waiting: $line"
                done
            fi
        fi
        last_report="$report"

        if [ "$SECONDS" -ge "$deadline" ]; then
            log_error "Stack $stack did not converge within ${timeout}s"
            for svc in "${pending_lines[@]}"; do
                log_error "  $svc"
                show_recent_logs "${svc%%:*}"
            done
            return $EXIT_NOT_CONVERGED
        fi
        sleep "$interval"
    done
}

# Print the tail of a service's logs so the failure is diagnosable from the
# deploy output alone (it is also what the LLM agent gets to investigate).
show_recent_logs() {
    local svc="$1" logs
    logs=$(docker service logs --tail 20 --no-task-ids "$svc" 2>&1 | tail -n 20)
    if [ -n "$logs" ]; then
        log_info "Last log lines of $svc:"
        printf '%s\n' "$logs" | sed 's/^/    /'
    fi
}

# ----------------------------------------------------------------------------
# health
# ----------------------------------------------------------------------------

health() {
    if [ $# -eq 0 ]; then
        log_error "Usage: $0 health <url> [<url>...]"
        return $EXIT_USAGE
    fi
    local timeout interval
    timeout=$(int_setting DEPLOY_HEALTHCHECK_TIMEOUT 60)
    interval=$(int_setting DEPLOY_HEALTHCHECK_INTERVAL 5)
    local deadline=$((SECONDS + timeout))
    local url status

    for url in "$@"; do
        log_info "Health check: waiting for $url to answer 2xx/3xx (up to ${timeout}s overall)..."
        while true; do
            status=$(curl -k -sS -o /dev/null \
                --connect-timeout 5 --max-time 10 \
                -w '%{http_code}' "$url" 2>/dev/null || true)
            if [[ "$status" =~ ^[23][0-9][0-9]$ ]]; then
                log_success "Health check passed: $url (HTTP $status)"
                break
            fi
            if [ "$SECONDS" -ge "$deadline" ]; then
                log_error "Health check failed: $url answered HTTP ${status:-none} (expected 2xx/3xx)"
                return $EXIT_UNHEALTHY
            fi
            sleep "$interval"
        done
    done
    return 0
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

MAX_FAILED_TASKS=0

case "${1:-}" in
    converge)
        shift
        converge "$@"
        ;;
    health)
        shift
        health "$@"
        ;;
    *)
        log_error "Usage: $0 {converge <stack> [baseline_tasks_file] [deploy_start_epoch] | health <url>...}"
        exit $EXIT_USAGE
        ;;
esac
