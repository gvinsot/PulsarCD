/**
 * PulsarCD frontend: Formatting and escaping helpers shared by every view.
 * Classic script sharing the page's globals; index.html sets the load order.
 */

function formatRelativeTime(isoString) {
    if (!isoString) return '';
    const date = new Date(isoString);
    const now = new Date();
    const diffMs = now - date;
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);
    
    if (diffMins < 1) return 'just now';
    if (diffMins < 60) return `${diffMins}m ago`;
    if (diffHours < 24) return `${diffHours}h ago`;
    if (diffDays < 30) return `${diffDays}d ago`;
    return date.toLocaleDateString();
}

// ============== Utilities ==============

function normalizeVersion(v) {
    if (!v) return '';
    return v.replace(/^v/, '');
}

/** Convert repo name to Docker stack name (mirrors deploy-service.sh get_stack_name). */
function repoToStackName(name) {
    return name.toLowerCase().replace(/[^a-z0-9]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '');
}

function formatImageName(image) {
    if (!image) return '';
    const escaped = escapeHtml(image);
    const shaIndex = escaped.indexOf('@sha256:');
    if (shaIndex === -1) return escaped;
    const name = escaped.substring(0, shaIndex);
    const sha = escaped.substring(shaIndex);
    return `${name}<span class="image-sha" title="${sha}">${sha}</span>`;
}

/**
 * Escape text for HTML, quotes included: the result goes into attribute
 * values as often as into element content, and an unescaped quote there ends
 * the attribute early.
 */
function escapeHtml(str) {
    if (!str) return '';
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

/** data-args value for a declarative handler (see UI_HANDLERS). */
function uiArgs(...args) {
    return escapeHtml(JSON.stringify(args));
}

function formatNumber(num) {
    if (num === undefined || num === null) return '-';
    return num.toLocaleString();
}

function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatMemory(mb) {
    if (mb === 0 || mb == null) return '';
    if (mb >= 1024) {
        return (mb / 1024).toFixed(1) + ' GB';
    }
    return Math.round(mb) + ' MB';
}

function formatTime(isoString) {
    if (!isoString) return '-';
    const date = new Date(isoString);
    return date.toLocaleTimeString('en-US', { 
        hour: '2-digit', 
        minute: '2-digit', 
        second: '2-digit',
        hour12: false 
    });
}

function formatDateTime(isoString) {
    if (!isoString) return '-';
    const date = new Date(isoString);
    return date.toLocaleString('en-US', {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
    });
}
