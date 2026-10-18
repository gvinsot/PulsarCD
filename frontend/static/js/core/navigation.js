/**
 * PulsarCD frontend: Page initialization, mobile menu and view switching.
 * Classic script sharing the page's globals; index.html sets the load order.
 */

// ============== Initialization ==============

document.addEventListener('DOMContentLoaded', () => {
    initLoginForm();
    initNavigation();
    initModalTabs();
    window.addEventListener('hashchange', () => {
        const view = getViewFromHash();
        if (view && view !== currentView) switchView(view, true);
    });
    checkAuth();
});

// ============== Mobile Menu ==============

function toggleMobileMenu() {
    const dropdown = document.getElementById('mobile-nav-dropdown');
    const logoIcon = document.getElementById('logo-icon');
    dropdown.classList.toggle('open');
    if (logoIcon) logoIcon.classList.toggle('menu-open', dropdown.classList.contains('open'));
}

function closeMobileMenu() {
    const dropdown = document.getElementById('mobile-nav-dropdown');
    const logoIcon = document.getElementById('logo-icon');
    if (dropdown) dropdown.classList.remove('open');
    if (logoIcon) logoIcon.classList.remove('menu-open');
}

function initNavigation() {
    document.querySelectorAll('.nav-item').forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            const view = item.dataset.view;
            switchView(view);
            // Close mobile menu after navigation
            closeMobileMenu();
        });
    });
}

function switchView(view, skipHash) {
    currentView = view;
    
    // Update URL hash
    if (!skipHash && VIEW_TO_HASH[view]) {
        history.replaceState(null, '', '#' + VIEW_TO_HASH[view]);
    }
    
    // Update nav
    document.querySelectorAll('.nav-item').forEach(item => {
        item.classList.toggle('active', item.dataset.view === view);
    });
    
    // Update views
    document.querySelectorAll('.view').forEach(v => {
        v.classList.toggle('active', v.id === `${view}-view`);
    });
    
    // Stop stacks polling when leaving the view
    if (view !== 'stacks') {
        stopStacksPolling();
    }
    if (view !== 'security') {
        stopSecurityPolling();
    }
    if (view !== 'usage') {
        stopUsagePolling();
    }

    // Load view data
    switch (view) {
        case 'dashboard':
            loadDashboard();
            break;
        case 'containers':
            loadContainers();
            break;
        case 'logs':
            // Load last 100 logs by default on first visit
            if (totalLogs === 0) {
                loadDefaultLogs();
            }
            checkAIStatus();
            renderRecentQueries();
            break;
        case 'stacks':
            loadStacks();
            break;
        case 'usage':
            loadUsage();
            startUsagePolling();
            break;
        case 'security':
            loadSecurity();
            startSecurityPolling();
            break;
        case 'terminal':
            initTerminal();
            setTimeout(() => {
                if (terminalFitAddon) terminalFitAddon.fit();
                if (terminalInstance) terminalInstance.focus();
            }, 100);
            break;
    }
}
