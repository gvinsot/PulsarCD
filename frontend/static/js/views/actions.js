/**
 * PulsarCD frontend: Background action tracking and the action logs modal.
 * Classic script sharing the page's globals; index.html sets the load order.
 */

// ============== Background Action Tracking ==============

const _activeActions = {}; // action_id -> { interval, toastEl, ... }

function trackBackgroundAction(actionId, actionType, repoName) {
    const tracker = { actionId, actionType, repoName, interval: null };
    _activeActions[actionId] = tracker;

    // Force immediate pipeline state refresh to update inline status
    scheduleStacksStateUpdate();

    // Poll for completion (no toast — status shown inline in pipeline flow)
    tracker.interval = setInterval(async () => {
        try {
            const status = await apiGet(`/stacks/actions/${actionId}/status`);
            if (!status) return;

            if (status.status !== 'running') {
                clearInterval(tracker.interval);
                delete _activeActions[actionId];

                if (currentActionLogsId === actionId) {
                    stopActionLogsStream();
                }

                // Refresh pipeline state to update step indicators
                scheduleStacksStateUpdate();
            }
        } catch (e) {
            console.error('Failed to poll action status:', e);
        }
    }, 2000);
}

function formatElapsed(seconds) {
    const m = Math.floor(seconds / 60);
    const s = Math.floor(seconds % 60);
    return m > 0 ? `${m}m ${s}s` : `${s}s`;
}

function showActionToast(actionId, actionType, repoName) {
    // Remove any existing toast for the same action
    const existing = document.getElementById(`action-toast-${actionId}`);
    if (existing) existing.remove();
    
    const toast = document.createElement('div');
    toast.id = `action-toast-${actionId}`;
    toast.className = 'action-toast';
    toast.innerHTML = `
        <div class="action-toast-content">
            <div class="action-toast-info">
                <span class="action-toast-spinner"></span>
                <span class="action-toast-text"><strong>${escapeHtml(actionType)}</strong> ${escapeHtml(repoName)}</span>
                <span class="action-toast-elapsed">0s</span>
            </div>
            <div class="action-toast-actions">
                <button class="btn btn-sm btn-ghost" data-click="openActionLogs" data-args="${uiArgs(actionId, actionType, repoName)}" title="View logs">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                        <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
                        <polyline points="14 2 14 8 20 8"/>
                        <line x1="16" y1="13" x2="8" y2="13"/>
                        <line x1="16" y1="17" x2="8" y2="17"/>
                    </svg>
                    Logs
                </button>
                <button class="btn btn-sm btn-danger" data-click="cancelAction" data-args="${uiArgs(actionId)}" title="Stop">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                        <rect x="6" y="6" width="12" height="12"/>
                    </svg>
                    Stop
                </button>
            </div>
        </div>
    `;
    
    // Add to toast container (create if needed)
    let container = document.getElementById('action-toast-container');
    if (!container) {
        container = document.createElement('div');
        container.id = 'action-toast-container';
        document.body.appendChild(container);
    }
    container.appendChild(toast);
    
    // Trigger entrance animation
    requestAnimationFrame(() => toast.classList.add('toast-visible'));
    
    return toast;
}

async function cancelAction(actionId) {
    try {
        const result = await apiPost(`/stacks/actions/${actionId}/cancel`);
        if (result && result.success) {
            showNotification('warning', 'Cancellation requested...');
        } else {
            showNotification('error', result?.message || 'Failed to cancel');
        }
    } catch (e) {
        showNotification('error', `Failed to cancel: ${e.message || 'Unknown error'}`);
    } finally {
        // Always refresh pipeline state — even on cancel failure, the action may
        // already be finished server-side, so the UI must catch up to truth.
        if (typeof updateStacksContainerStates === 'function') {
            updateStacksContainerStates();
        }
        if (typeof scheduleStacksStateUpdate === 'function') {
            scheduleStacksStateUpdate();
        }
    }
}

// ============== Action Logs Modal ==============

let currentActionLogsId = null;
let currentActionLogsType = null;
let currentActionLogsRepo = null;
let actionLogsPollTimer = null;
let actionLogsPollOffset = 0;
let actionLogsFirstRender = true;
let actionLogsEventSource = null;
let actionLogsGeneration = 0;
let testLogsViewer = null;

// Rendering is batched through requestAnimationFrame: a build can dump tens of
// thousands of lines at once, and appending them one by one — each followed by
// a scrollTop write, which forces a synchronous layout — freezes the main thread.
let actionLogsPendingLines = [];
let actionLogsFlushHandle = null;
const ACTION_LOGS_MAX_DOM_LINES = 5000;

function openActionLogs(actionId, actionType, repoName) {
    stopActionLogsPoll();
    if (testLogsViewer) { testLogsViewer.dispose(); testLogsViewer = null; }
    currentActionLogsId = actionId;
    currentActionLogsType = actionType;
    currentActionLogsRepo = repoName;

    const modal = document.getElementById('action-logs-modal');
    const title = document.getElementById('action-logs-title');
    const content = document.getElementById('action-logs-content');

    title.textContent = `${actionType}: ${repoName}`;
    content.innerHTML = '<div class="loading-placeholder">Loading logs...</div>';

    // Show Stop button if this action is currently active (running)
    const stopBtn = document.getElementById('action-logs-stop-btn');
    if (stopBtn) {
        stopBtn.style.display = _activeActions[actionId] ? '' : 'none';
    }

    modal.classList.add('active');

    if (/^test(?:s| logs)?$/i.test(actionType || '')) {
        testLogsViewer = new TestLogsViewer(actionId, repoName, content);
    }

    startActionLogsPoll(actionId);
}

function closeActionLogsModal() {
    stopActionLogsPoll();
    if (testLogsViewer) { testLogsViewer.dispose(); testLogsViewer = null; }
    document.getElementById('action-logs-modal').classList.remove('active');
    currentActionLogsId = null;
    currentActionLogsType = null;
    currentActionLogsRepo = null;
}

function analyzeActionLogs() {
    const content = document.getElementById('action-logs-content');
    const logLines = testLogsViewer ? testLogsViewer.lines.join('\n') : Array.from(content.querySelectorAll('.log-line'))
        .map(el => el.textContent)
        .join('\n');

    const actionType = currentActionLogsType || 'Action';
    const repoName = currentActionLogsRepo || 'unknown';

    // Truncate logs if too long (keep last 200 lines)
    const lines = logLines.split('\n');
    const truncated = lines.length > 200
        ? '... (truncated)\n' + lines.slice(-200).join('\n')
        : logLines;

    const taskDesc = [
        `Analyze the following ${actionType} logs for project '${repoName}'.`,
        `Identify any errors, warnings, or issues and suggest fixes.\n`,
        `Logs:\n\`\`\`\n${truncated}\n\`\`\``
    ].join('\n');

    document.getElementById('task-project').value = repoName;
    document.getElementById('task-source').value = `${actionType} logs`;
    document.getElementById('task-description').value = taskDesc;
    document.getElementById('task-submit-btn').disabled = false;
    document.getElementById('task-submit-btn').textContent = 'Send Task';

    document.getElementById('create-task-modal').classList.add('active');
}

async function cancelCurrentActionLogs() {
    if (!currentActionLogsId) return;
    await cancelAction(currentActionLogsId);
    const stopBtn = document.getElementById('action-logs-stop-btn');
    if (stopBtn) stopBtn.style.display = 'none';
}

function stopActionLogsPoll() {
    actionLogsGeneration++;
    if (actionLogsPollTimer) {
        clearTimeout(actionLogsPollTimer);
        actionLogsPollTimer = null;
    }
    if (actionLogsEventSource) {
        actionLogsEventSource.close();
        actionLogsEventSource = null;
    }
    if (actionLogsFlushHandle !== null) {
        cancelAnimationFrame(actionLogsFlushHandle);
        actionLogsFlushHandle = null;
    }
    actionLogsPendingLines = [];
}

function buildLogLine(line, extraClass) {
    const lineEl = document.createElement('div');
    lineEl.className = 'log-line';
    if (extraClass) {
        lineEl.classList.add(extraClass);
    } else if (/\b(error|ERROR|fatal|FATAL|failed|FAILED)\b/.test(line)) {
        lineEl.classList.add('log-error');
    }
    lineEl.textContent = line;
    return lineEl;
}

function appendLogLine(content, line) {
    content.appendChild(buildLogLine(line));
}

/**
 * Queue log lines for rendering. Pending lines are flushed once per animation
 * frame in a single DocumentFragment, so a backlog of thousands of lines costs
 * one layout instead of one per line.
 */
function queueActionLogLines(lines, extraClass) {
    if (!lines || lines.length === 0) return;
    if (testLogsViewer) {
        testLogsViewer.ingest(lines, extraClass);
        return;
    }
    for (const line of lines) {
        actionLogsPendingLines.push(extraClass ? { line, cls: extraClass } : line);
    }
    // rAF does not fire on a hidden tab: keep the queue bounded so a long build
    // running in the background cannot grow it without limit.
    if (actionLogsPendingLines.length > ACTION_LOGS_MAX_DOM_LINES) {
        actionLogsPendingLines = actionLogsPendingLines.slice(-ACTION_LOGS_MAX_DOM_LINES);
    }
    if (actionLogsFlushHandle === null) {
        actionLogsFlushHandle = requestAnimationFrame(flushActionLogLines);
    }
}

function flushActionLogLines() {
    actionLogsFlushHandle = null;
    const pending = actionLogsPendingLines;
    actionLogsPendingLines = [];
    if (pending.length === 0) return;

    const content = document.getElementById('action-logs-content');
    if (!content) return;

    const followTail = actionLogsFirstRender || content.scrollHeight - content.clientHeight - content.scrollTop < 40;

    if (actionLogsFirstRender) {
        content.innerHTML = '';
        actionLogsFirstRender = false;
    }

    // Only the tail is worth building: anything before it would be trimmed away
    // right after being inserted.
    const start = Math.max(0, pending.length - ACTION_LOGS_MAX_DOM_LINES);
    const fragment = document.createDocumentFragment();
    for (let i = start; i < pending.length; i++) {
        const item = pending[i];
        fragment.appendChild(
            typeof item === 'string' ? buildLogLine(item) : buildLogLine(item.line, item.cls)
        );
    }
    content.appendChild(fragment);

    // Cap the number of DOM nodes so long builds keep the modal responsive
    let excess = content.childElementCount - ACTION_LOGS_MAX_DOM_LINES;
    while (excess > 0 && content.firstChild) {
        content.removeChild(content.firstChild);
        excess--;
    }

    if (followTail) content.scrollTop = content.scrollHeight;
}

async function startActionLogsPoll(actionId) {
    stopActionLogsPoll();
    actionLogsPollOffset = 0;
    actionLogsFirstRender = true;

    // Try SSE streaming first for real-time logs
    const token = getAuthToken();
    const sseUrl = `${API_BASE}/stacks/actions/${actionId}/logs/stream?offset=0${token ? '&token=' + encodeURIComponent(token) : ''}`;
    const es = new EventSource(sseUrl);
    actionLogsEventSource = es;

    es.onmessage = function(event) {
        if (actionId !== currentActionLogsId || es !== actionLogsEventSource) { es.close(); return; }
        try {
            const data = JSON.parse(event.data);
            if (data.type === 'line') {
                queueActionLogLines([data.line]);
            } else if (data.type === 'lines') {
                queueActionLogLines(data.lines);
            } else if (data.type === 'done') {
                queueActionLogLines(
                    [`--- ${data.status.toUpperCase()} ---`],
                    data.status === 'completed' ? 'log-success' : 'log-error'
                );
                const _stopBtn = document.getElementById('action-logs-stop-btn');
                if (_stopBtn) _stopBtn.style.display = 'none';
                es.close();
                actionLogsEventSource = null;
            }
        } catch (e) { /* ignore parse errors */ }
    };

    es.onerror = function() {
        if (actionId !== currentActionLogsId || es !== actionLogsEventSource) { es.close(); return; }
        es.close();
        actionLogsEventSource = null;
        // Fallback to polling (action may no longer be in memory)
        startActionLogsFallbackPoll(actionId);
    };
}

async function startActionLogsFallbackPoll(actionId) {
    const generation = actionLogsGeneration;
    actionLogsPollOffset = 0;
    actionLogsFirstRender = true;
    if (testLogsViewer) testLogsViewer.reset();

    async function poll() {
        if (actionId !== currentActionLogsId || generation !== actionLogsGeneration) return;

        const content = document.getElementById('action-logs-content');

        try {
            const response = await fetch(`${API_BASE}/stacks/actions/${actionId}/logs?offset=${actionLogsPollOffset}`, { headers: authHeaders() });

            if (actionId !== currentActionLogsId || generation !== actionLogsGeneration) return;

            if (response.status === 401) {
                showLogin();
                return;
            }

            if (response.status === 404) {
                content.innerHTML = '<div class="log-line log-error">Action introuvable — le serveur a peut-être redémarré.</div>';
                if (testLogsViewer) testLogsViewer.ingest(['Action not found — the server may have restarted.'], 'log-error');
                actionLogsFirstRender = false;
                return;
            }

            if (!response.ok) {
                // Transient error — retry
                actionLogsPollTimer = setTimeout(poll, 2000);
                return;
            }

            const data = await response.json();
            if (actionId !== currentActionLogsId || generation !== actionLogsGeneration) return;

            if (data.lines && data.lines.length > 0) {
                queueActionLogLines(data.lines);
                actionLogsPollOffset += data.lines.length;
            }

            if (data.status !== 'running') {
                queueActionLogLines(
                    [`--- ${data.status.toUpperCase()} ---`],
                    data.status === 'completed' ? 'log-success' : 'log-error'
                );
                const _stopBtn2 = document.getElementById('action-logs-stop-btn');
                if (_stopBtn2) _stopBtn2.style.display = 'none';
                return; // done
            }
        } catch (e) {
            // Network error — retry
        }

        if (actionId !== currentActionLogsId || generation !== actionLogsGeneration) return;
        actionLogsPollTimer = setTimeout(poll, 1000);
    }

    poll();
}
