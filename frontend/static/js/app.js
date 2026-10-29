/**
 * PulsarCD frontend: Global key bindings and the declarative UI handler registry. Loaded last:
 * UI_HANDLERS names functions from every other script.
 * Classic script sharing the page's globals; index.html sets the load order.
 */

// Close modal on escape key
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        closeHostInfoModal();
        closeModal();
    }
});

document.getElementById('host-info-modal').addEventListener('click', (e) => {
    if (e.target === e.currentTarget) closeHostInfoModal();
});
document.getElementById('host-info-modal').addEventListener('keydown', (e) => {
    // The close button is the dialog's only focusable control.
    if (e.key === 'Tab') {
        e.preventDefault();
        e.currentTarget.querySelector('.modal-close').focus();
    }
});

// Close modal on backdrop click
document.getElementById('container-modal').addEventListener('click', (e) => {
    if (e.target.classList.contains('modal')) {
        closeModal();
    }
});

document.getElementById('recurring-error-modal').addEventListener('click', (e) => {
    if (e.target === e.currentTarget) {
        closeRecurringErrorModal();
    }
});

// Enter key sends the agent chat message
document.getElementById('llm-test-input').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
        sendLLMTest();
    }
});

// Enter key to search with AI
document.getElementById('ai-query').addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
        aiSearchLogs();
    }
});

// Declarative event handlers. An element names its handler, and one listener
// per event type on the document runs it:
//
//   data-click / data-change / data-input / data-focus = "handlerName"
//   data-args = JSON array of arguments, built with uiArgs()
//   data-click-stop: the click does not reach handlers on enclosing elements,
//                    as event.stopPropagation() did in an inline handler
//
// A handler runs with `this` bound to the element that names it. Names resolve
// against UI_HANDLERS only, never against window, so markup that got past
// escaping still cannot call an arbitrary global.
const UI_HANDLERS = Object.freeze({
    addMCPServer, aiSearchLogs, analyzeActionLogs, applyLLMPreset, blockIp,
    blockIpFromForm, unblockIp, cancelAction, changeSecurityWindow, clearSecuritySlice,
    cancelCurrentActionLogs, cleanupRepoTags, clearLLMChat, clearRecentQueries,
    closeActionLogsModal, closeActivityDiff, closeActivityModal, closeAgentModal,
    closeBuildModal, closeCreateTaskModal, closeDeployModal, closeHostInfoModal, closeModal, closePipelineModal,
    closeRecurringErrorModal, closeSecurityIpModal, closeServiceDeployModal,
    closeServiceLogsModal, closeServiceStatusModal, closeSettingsModal, closeStackEnvModal,
    closeStackOutputModal, closeTestModal, containerAction, createUser, deleteRecentQuery,
    deleteUser, editStackEnv, executeGeneratedQuery, exportLogs, filterContainerEnv,
    filterContainerLogs, filterContainers, filterServiceLogs, hostAction, loadAgentHistory,
    loadContainerMetrics, loadSecurity, loadStackEnvVersion, loadUsage, logout, nextPage,
    onPipelineBranchChange,
    openActionLogs, openAgentModal, openContainer, openHostInfo,
    openCreateTaskModal, openSecurityIp, openServiceDeploy, openServiceLogs, openServiceStatus,
    openTransitionConfig, pipelineStepClick, prevPage, quickAction, reconnectTerminal,
    refreshContainerEnv, refreshContainerLogs, refreshContainers, refreshDashboard,
    refreshLogsSearch, refreshServiceLogs, refreshServiceStatus, refreshStacks,
    removeDeployedStack, removeMCPServer, removeSecurityRequestFilter, removeService,
    resetSecurityRequestFilters, saveSettings, saveSettingsFromAgent,
    saveStackEnv, saveTransitionConfig, searchHttpErrors, selectBuildTag, selectDeployTag,
    selectPipelineCommit, selectPipelineTag, selectServiceDeployTag, selectTestTag, selectUsageStack,
    sendLLMTest,
    setUserRole, showCommitDiff, showRecentQueries, showRecurringErrorDetail, showSecurityIpRequests,
    showSecurityRequests, showStackActivity,
    submitBuild, submitCreateTask, submitDeploy, submitPipeline, submitServiceDeploy,
    submitTest, switchAgentTab, switchSecurityTab, switchSettingsTab, switchStackEnvTab, testLLMConnection,
    testMCPServer,
    toggleBuildSource, toggleDeploySource, toggleMobileMenu, togglePasswordLogin,
    toggleServiceDeploySource, toggleServiceLogsAutoRefresh, toggleServiceStatusAutoRefresh,
    toggleStackEnvHistory, toggleStackExpand, toggleStackSubSection, toggleTaskPreview, toggleTestSource,
    toggleUserMenu, updateTaskPreview, useRecentQuery,

    // Handlers that use the element, or make more than one call
    closeGateDecisionModal() {
        document.getElementById('gate-decision-modal').classList.remove('active');
    },
    closeTransitionConfigModal() {
        document.getElementById('transition-config-modal').classList.remove('active');
    },
    dismissToast() {
        const toast = this.closest('.action-toast');
        toast.classList.add('toast-exit');
        setTimeout(() => toast.remove(), 300);
    },
    openSettingsFromUserMenu() {
        openSettingsModal();
        closeUserMenu();
    },
    refreshContainerStatsAndMetrics() {
        refreshContainerStats();
        loadContainerMetrics();
    },
    toggleComposeHeader() {
        toggleComposeGroup(this);
    },
    toggleContainerLogRow(index) {
        toggleContainerLogExpand(this, index);
    },
    toggleDiffFile() {
        this.parentElement.classList.toggle('collapsed');
    },
    toggleExpanded() {
        this.classList.toggle('expanded');
    },
    toggleHostHeader() {
        toggleHostGroup(this);
    },
    toggleLogRow(index) {
        toggleLogExpand(this, index);
    },
});

const UI_HANDLER_ATTRIBUTES = {
    click: 'data-click',
    change: 'data-change',
    input: 'data-input',
    focusin: 'data-focus',
};

function runUiHandler(el, name) {
    const handler = Object.hasOwn(UI_HANDLERS, name) ? UI_HANDLERS[name] : null;
    if (!handler) {
        console.error(`No UI handler named "${name}"`);
        return;
    }
    let args;
    try {
        args = JSON.parse(el.getAttribute('data-args') || '[]');
    } catch (e) {
        console.error(`Invalid data-args for "${name}":`, e);
        return;
    }
    try {
        handler.apply(el, args);
    } catch (e) {
        // As with inline handlers, one failing does not stop the enclosing ones.
        console.error(e);
    }
}

for (const [type, attribute] of Object.entries(UI_HANDLER_ATTRIBUTES)) {
    document.addEventListener(type, (event) => {
        // The path is fixed when the event is dispatched, as it was for inline
        // handlers: a handler that re-renders its own markup does not change
        // which enclosing handlers run next.
        for (const el of event.composedPath()) {
            if (!(el instanceof Element)) break;
            if (el.disabled) return;
            const name = el.getAttribute(attribute);
            if (name) runUiHandler(el, name);
            if (type === 'click' && el.hasAttribute('data-click-stop')) return;
        }
    });
}
