/**
 * PulsarCD Frontend Application
 * Professional Docker Log Analytics Dashboard
 */

// API Base URL
const API_BASE = '/api';

// State
let currentView = 'dashboard';

// Hash ↔ view mapping
const HASH_TO_VIEW = { dashboard: 'dashboard', stacks: 'stacks', computers: 'containers', logs: 'logs', usage: 'usage', security: 'security', terminal: 'terminal' };
const VIEW_TO_HASH = { dashboard: 'Dashboard', stacks: 'Stacks', containers: 'Computers', logs: 'Logs', usage: 'Usage', security: 'Security', terminal: 'Terminal' };

function getViewFromHash() {
    const h = location.hash.replace('#', '').toLowerCase();
    return HASH_TO_VIEW[h] || null;
}
let currentContainer = null;
let charts = {};
let logsPage = 0;
let logsPageSize = 100;
let totalLogs = 0;

// Before anything shows or hides these elements (see adoptStartHidden).
adoptStartHidden(document);
