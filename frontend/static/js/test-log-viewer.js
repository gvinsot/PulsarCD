/* Structured test output and SwiftProof evidence. No report content is executable. */
class TestLogsViewer {
    constructor(actionId, repo, rawContent) {
        this.actionId = actionId;
        this.repo = repo;
        this.rawContent = rawContent;
        this.lines = [];
        this.offset = 0;
        this.view = 'results';
        this.groupBy = 'result';
        this.query = '';
        this.status = '';
        this.follow = true;
        this.rawStart = 0;
        this.selectedLine = -1;
        this.closed = false;
        this.revision = 0;
        this.abort = new AbortController();
        this.model = TestLogModel.parse([]);
        this.root = document.createElement('div');
        this.root.className = 'test-workspace';
        this.root.innerHTML = `
            <section class="test-proof" aria-label="SwiftProof review"><div data-slot="proof">Loading SwiftProof status…</div></section>
            <nav class="test-tabs" aria-label="Test log views">
                <button class="btn btn-sm" data-view="results" aria-pressed="true">Test explorer</button>
                <button class="btn btn-sm" data-view="raw" aria-pressed="false">Raw logs</button>
                <button class="btn btn-sm" data-view="report" aria-pressed="false" disabled>SwiftProof report</button>
                <span class="test-live" data-slot="live" role="status">Waiting for output…</span>
            </nav>
            <section data-pane="results">
                <div class="test-toolbar">
                    <form class="test-search" role="search" autocomplete="off">
                        <label>Search tests<input type="search" name="test-log-search" data-control="query" placeholder="Name, file, theme…" autocomplete="off" data-lpignore="true" data-1p-ignore></label>
                    </form>
                    <label>Result<select data-control="status"><option value="">All results</option><option value="failed">Failures & errors</option><option value="passed">Passed</option><option value="skipped">Skipped</option></select></label>
                    <label>Group by<select data-control="group"><option value="result">Result (ERROR / SUCCESS)</option><option value="type">Test type</option><option value="framework">Framework</option><option value="theme">Theme (automatic)</option><option value="file">File</option><option value="llm" disabled>Theme (LLM)</option></select></label>
                    <button class="btn btn-sm btn-secondary" data-action="themes" ${isAdmin() ? '' : 'hidden'}>Group with LLM</button>
                </div>
                <p class="test-note" data-slot="themes" role="status">Automatic themes use test names and file paths.</p>
                <div class="test-counts" data-slot="counts"></div>
                <div class="test-results" data-slot="results"></div>
            </section>
            <section data-pane="raw" hidden>
                <div class="test-toolbar"><button class="btn btn-sm btn-secondary" data-action="previous">Earlier lines</button><button class="btn btn-sm btn-secondary" data-action="next">Later lines</button><label class="test-follow"><input type="checkbox" data-control="follow" checked> Follow live output</label><span data-slot="range" class="test-note"></span></div>
                <div class="test-raw" data-slot="raw" tabindex="0" aria-label="Numbered test logs"></div>
            </section>
            <section data-pane="report" hidden><div data-slot="report"></div></section>`;
        rawContent.hidden = true;
        rawContent.before(this.root);
        document.getElementById('action-logs-modal').classList.add('test-logs-modal');
        // Keep this filter separate from the page's credential fields; Enter only filters locally.
        this.root.querySelector('form.test-search').addEventListener('submit', event => event.preventDefault());
        this.root.addEventListener('click', event => this.click(event));
        this.root.addEventListener('input', event => {
            if (event.target.dataset.control !== 'query') return;
            this.query = event.target.value.toLowerCase();
            this.renderResults();
        });
        this.root.addEventListener('change', event => {
            const { control } = event.target.dataset;
            if (control === 'status') this.status = event.target.value;
            if (control === 'group') this.groupBy = event.target.value;
            if (control === 'follow') {
                this.follow = event.target.checked;
                if (this.follow) { this.rawStart = Math.max(this.offset, this.offset + this.lines.length - 500); this.renderRaw(); }
            } else this.renderResults();
        });
        this.slot('raw').addEventListener('scroll', () => {
            const el = this.slot('raw');
            if (this.follow && el.scrollHeight - el.clientHeight - el.scrollTop > 40) {
                this.follow = false;
                this.root.querySelector('[data-control="follow"]').checked = false;
            }
        });
        this.renderResults();
        this.pollProof();
    }

    slot(name) { return this.root.querySelector(`[data-slot="${name}"]`); }
    escape(value) { return escapeHtml(String(value ?? '')); }
    badge(value) {
        const text = String(value || 'unknown').toLowerCase();
        const tone = /^(critical|high|fail|failed|error|blocked|reproduced)$/.test(text) ? 'danger' : /^(medium|warning|needs_review|unverified|inconclusive)$/.test(text) ? 'warning' : /^(pass|passed|success|approved)$/.test(text) ? 'success' : 'neutral';
        return `<span class="test-badge test-badge-${tone}">${this.escape(text.replaceAll('_', ' '))}</span>`;
    }

    dispose() {
        this.closed = true;
        this.abort.abort();
        clearTimeout(this.refreshTimer);
        clearTimeout(this.proofTimer);
        this.root.remove();
        this.rawContent.hidden = false;
        document.getElementById('action-logs-modal').classList.remove('test-logs-modal');
    }

    reset() {
        this.revision++;
        clearTimeout(this.refreshTimer);
        this.lines = [];
        this.offset = 0;
        this.done = false;
        this.selectedLine = -1;
        this.llmGroups = null;
        this.slot('themes').textContent = 'Automatic themes use test names and file paths.';
        this.root.querySelector('[data-action="themes"]').disabled = false;
        this.root.querySelector('[data-control="group"] option[value="llm"]').disabled = true;
        if (this.groupBy === 'llm') {
            this.groupBy = 'result';
            this.root.querySelector('[data-control="group"]').value = 'result';
        }
        this.refresh();
    }

    ingest(lines, statusClass) {
        // Preserve timestamps and service prefixes in Raw logs; only strip terminal escapes.
        for (const line of lines) this.lines.push(String(line).replace(/\x1b\][^\x07]*(?:\x07|\x1b\\)/g, '').replace(/\x1b\[[0-?]*[ -/]*[@-~]/g, ''));
        if (this.lines.length > 20000) {
            const dropped = this.lines.length - 20000;
            this.lines.splice(0, dropped);
            this.offset += dropped;
        }
        if (statusClass) this.done = true;
        if (!this.refreshTimer) this.refreshTimer = setTimeout(() => this.refresh(), 180);
    }

    refresh() {
        this.refreshTimer = null;
        if (this.closed) return;
        this.model = TestLogModel.parse(this.lines, { lineOffset: this.offset });
        this.slot('live').textContent = `${this.done ? 'Finished' : 'Live'} · ${this.offset + this.lines.length} lines${this.offset ? ' · last 20,000 retained' : ''}`;
        this.renderResults();
        if (this.view === 'raw') {
            if (this.follow) this.rawStart = Math.max(this.offset, this.offset + this.lines.length - 500);
            this.renderRaw();
        }
    }

    showView(view) {
        this.view = view;
        this.root.querySelectorAll('[data-pane]').forEach(el => { el.hidden = el.dataset.pane !== view; });
        this.root.querySelectorAll('[data-view]').forEach(el => el.setAttribute('aria-pressed', String(el.dataset.view === view)));
        if (view === 'raw') {
            if (this.follow) this.rawStart = Math.max(this.offset, this.offset + this.lines.length - 500);
            this.renderRaw();
        }
        if (view === 'report') this.renderReport();
    }

    click(event) {
        const button = event.target.closest('button');
        if (!button || !this.root.contains(button) || button.disabled) return;
        if (button.dataset.view) this.showView(button.dataset.view);
        if (button.dataset.line !== undefined) this.jumpToLine(Number(button.dataset.line));
        if (button.dataset.finding !== undefined) {
            this.selectedFinding = Number(button.dataset.finding);
            this.showView('report');
            this.slot('report').scrollIntoView({ block: 'nearest' });
        }
        if (button.dataset.reportLine !== undefined) {
            const target = this.root.querySelector(`[data-report-anchor="${Number(button.dataset.reportLine)}"]`);
            if (target) { target.scrollIntoView({ block: 'nearest' }); target.focus({ preventScroll: true }); }
        }
        const action = button.dataset.action;
        if (action === 'previous' || action === 'next') {
            this.follow = false;
            this.root.querySelector('[data-control="follow"]').checked = false;
            this.rawStart += action === 'previous' ? -500 : 500;
            this.renderRaw();
        }
        if (action === 'themes') this.groupWithLLM();
        if (action === 'download') this.downloadProof();
        if (action === 'more') { this.resultLimit = (this.resultLimit || 150) + 150; this.renderResults(); }
    }

    renderResults() {
        const model = this.model;
        const counts = model.counts;
        this.slot('counts').innerHTML = counts.total ? ['passed', 'failed', 'error', 'skipped'].map(status => `<span>${this.badge(status)} <strong>${Number(counts[status] || 0)}</strong></span>`).join('') + '<span class="test-note">Named tests parsed from retained output</span>' : '<span class="test-note">No individual test results emitted yet. Suite results and runner summaries are shown when available.</span>';
        const entries = model.entries.filter(entry => (!this.status || (this.status === 'failed' ? ['failed', 'error'].includes(entry.status) : entry.status === this.status)) && (!this.query || [entry.name, entry.file, entry.theme, entry.type, entry.framework].join(' ').toLowerCase().includes(this.query)));
        let groups;
        if (this.groupBy === 'llm' && this.llmGroups) {
            const byId = new Map(entries.map(entry => [entry.id, entry]));
            groups = this.llmGroups.map(group => ({ label: group.label, entries: group.entry_ids.map(id => byId.get(id)).filter(Boolean) }));
            const assigned = new Set(this.llmGroups.flatMap(group => group.entry_ids));
            groups.push({ label: 'Other / new tests', entries: entries.filter(entry => !assigned.has(entry.id)) });
            groups = groups.filter(group => group.entries.length);
        } else groups = TestLogModel.group(entries, this.groupBy);
        // Keep expanded groups and the reading position while live output arrives.
        const oldGroups = new Map(Array.from(this.slot('results').querySelectorAll('details[data-group]')).map(el => [el.dataset.group, el.open]));
        let remaining = this.resultLimit || 150;
        const rendered = groups.map(group => {
            const visible = group.entries.slice(0, Math.max(0, remaining));
            remaining -= visible.length;
            if (!visible.length) return '';
            const failures = group.entries.filter(entry => ['failed', 'error'].includes(entry.status)).length;
            const open = oldGroups.get(group.label) ?? true;
            return `<details class="test-group" data-group="${this.escape(group.label)}" ${open ? 'open' : ''}><summary><strong>${this.escape(group.label)}</strong><span>${group.entries.length} entries${failures ? ` · ${failures} failed` : ''}</span></summary>${visible.map(entry => {
                const line = entry.detailLine ?? entry.line;
                return `<button class="test-entry" data-line="${line}" title="Jump to log line ${line + 1}">${this.badge(entry.status)}<span class="test-entry-name">${this.escape(entry.name)}<small>${this.escape([entry.file, entry.framework, entry.kind === 'suite' ? 'suite' : '', entry.duration].filter(Boolean).join(' · '))}</small>${entry.diagnostic ? `<small>${this.escape(entry.diagnostic)}</small>` : ''}</span><span class="test-line-ref">L${line + 1} ↗</span></button>`;
            }).join('')}</details>`;
        }).join('');
        const sections = (model.sections || []).slice(0, 100);
        this.slot('results').innerHTML = rendered || `<p class="test-empty">${this.query || this.status ? 'No tests match these filters.' : 'Waiting for named test results. Use the log sections below or open Raw logs.'}</p>`;
        if (entries.length > (this.resultLimit || 150)) this.slot('results').insertAdjacentHTML('beforeend', '<button class="btn btn-sm btn-secondary" data-action="more">Show more tests</button>');
        if (!this.query && !this.status && sections.length) this.slot('results').insertAdjacentHTML('beforeend', `<details class="test-group" ${entries.length ? '' : 'open'}><summary>Log sections <span>${sections.length}</span></summary><div class="test-section-links">${sections.map(section => `<button class="btn btn-sm btn-secondary" data-line="${section.line}">${this.escape(section.title || section.name || section.label || 'Section')} · L${section.line + 1}</button>`).join('')}</div></details>`);
        if (!this.query && !this.status && model.summaries?.length) this.slot('results').insertAdjacentHTML('beforeend', `<details class="test-group" ${counts.total ? '' : 'open'}><summary>Runner summaries</summary><div class="test-section-links">${model.summaries.map(summary => `<button class="test-summary-line" data-line="${summary.line}">${this.escape(summary.text || summary.name || '')}</button>`).join('')}</div></details>`);
    }

    jumpToLine(line) {
        this.selectedLine = line;
        this.follow = false;
        this.root.querySelector('[data-control="follow"]').checked = false;
        this.rawStart = Math.max(this.offset, line - 15);
        this.showView('raw');
        const target = this.slot('raw').querySelector('.test-log-selected');
        if (target) { target.scrollIntoView({ block: 'center' }); target.focus({ preventScroll: true }); }
    }

    renderRaw() {
        const end = this.offset + this.lines.length;
        this.rawStart = Math.max(this.offset, Math.min(this.rawStart, Math.max(this.offset, end - 1)));
        const lines = this.lines.slice(this.rawStart - this.offset, this.rawStart - this.offset + 500);
        const pane = this.slot('raw');
        const oldScroll = pane.scrollTop;
        pane.innerHTML = lines.map((line, index) => {
            const number = this.rawStart + index;
            return `<div class="test-log-line ${number === this.selectedLine ? 'test-log-selected' : ''} ${/\b(FAILED|ERROR|Error|FAIL|fatal)\b/.test(line) ? 'test-log-error' : ''}" tabindex="-1"><span class="test-line-number" aria-hidden="true">${number + 1}</span><span>${this.escape(line) || ' '}</span></div>`;
        }).join('') || '<p class="test-empty">No log output yet.</p>';
        pane.scrollTop = this.follow ? pane.scrollHeight : oldScroll;
        this.slot('range').textContent = lines.length ? `Lines ${this.rawStart + 1}–${this.rawStart + lines.length} of ${end}` : '0 lines';
        this.root.querySelector('[data-action="previous"]').disabled = this.rawStart <= this.offset;
        this.root.querySelector('[data-action="next"]').disabled = this.rawStart + lines.length >= end;
    }

    async request(path, options = {}) {
        const response = await fetch(API_BASE + path, { ...options, headers: { ...authHeaders(), ...(options.headers || {}) }, signal: this.abort.signal });
        if (!response.ok) throw new Error(`Request failed (HTTP ${response.status})`);
        return response;
    }

    async groupWithLLM() {
        const revision = this.revision;
        const button = this.root.querySelector('[data-action="themes"]');
        const entries = this.model.entries.filter(entry => entry.kind === 'test').slice(0, 300);
        if (!entries.length) { this.slot('themes').textContent = 'No named tests to group yet.'; return; }
        button.disabled = true;
        this.slot('themes').textContent = 'Grouping test names and paths with the configured LLM…';
        try {
            const response = await this.request(`/stacks/actions/${encodeURIComponent(this.actionId)}/logs/themes`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ entries: entries.map(({ id, name, file, framework, type }) => ({ id, name, file, framework, type })) }) });
            const data = await response.json();
            if (this.closed || revision !== this.revision) return;
            this.llmGroups = data.groups;
            this.root.querySelector('[data-control="group"] option[value="llm"]').disabled = false;
            this.root.querySelector('[data-control="group"]').value = 'llm';
            this.groupBy = 'llm';
            this.slot('themes').textContent = `LLM themes · ${entries.length} tests submitted (maximum 300). New or unassigned tests appear in Other / new tests. Results are unchanged.`;
            this.renderResults();
        } catch (error) {
            if (!this.closed && revision === this.revision) this.slot('themes').textContent = `LLM grouping unavailable. Automatic themes remain available. ${error.message}`;
        } finally { if (!this.closed && revision === this.revision) button.disabled = false; }
    }

    proofURL(id) { return `/stacks/pipeline/${encodeURIComponent(this.repo)}/swiftproof/${encodeURIComponent(id)}/report`; }

    async pollProof() {
        try {
            const response = await this.request(`/stacks/pipeline/${encodeURIComponent(this.repo)}/transition/build_to_test`);
            const data = await response.json();
            if (this.closed) return;
            this.proofEnabled = !!data.config?.swiftproof_enabled;
            this.proofBlocking = data.config?.swiftproof_blocking !== false;
            this.proof = data.swiftproof || {};
            this.root.querySelector('.test-proof').hidden = !this.proofEnabled;
            if (this.proofEnabled && this.proof.id && this.proof.status !== 'running' && this.reportId !== this.proof.id) {
                // A new review must not keep the preceding review's findings on screen.
                this.report = null;
                this.reportId = null;
                this.selectedFinding = undefined;
                this.renderProof();
                const reportResponse = await this.request(this.proofURL(this.proof.id) + '?format=json');
                const report = await reportResponse.json();
                if (this.closed) return;
                this.report = report;
                this.reportId = this.proof.id;
            } else if (!this.proofEnabled || !this.proof.id || this.proof.id !== this.reportId) {
                this.report = null;
                this.reportId = null;
                this.selectedFinding = undefined;
            }
            this.proofError = '';
            this.renderProof();
        } catch (error) {
            if (!this.closed) { this.proofError = 'SwiftProof status or report unavailable. Retrying automatically…'; this.renderProof(); }
        } finally {
            if (!this.closed) this.proofTimer = setTimeout(() => this.pollProof(), 4000);
        }
    }

    renderProof() {
        const proof = this.proof || {};
        const toolVersion = this.report?.report?.tool_version || proof.tool_version;
        const findings = this.report?.findings || [];
        const signature = JSON.stringify([this.proofEnabled, this.proofBlocking, proof, this.reportId, this.proofError]);
        this.root.querySelector('[data-view="report"]').disabled = !this.report;
        if (signature === this.proofSignature) return;
        this.proofSignature = signature;
        this.slot('proof').innerHTML = `<div class="test-proof-heading"><div><strong>SwiftProof${toolVersion ? ' ' + this.escape(toolVersion) : ''}</strong> ${this.badge(proof.status || 'pending')} <span class="test-badge test-badge-neutral">${this.proofBlocking !== false ? 'Blocking' : 'Non-blocking'}</span><p class="test-note">Latest project review${proof.release ? ` · ${this.escape(proof.release)}` : ''}${proof.head ? ` · ${this.escape(proof.head.slice(0, 12))}` : ''}. Independent of the selected test run.</p></div>${this.report ? '<button class="btn btn-sm btn-secondary" data-action="download">Download evidence</button>' : ''}</div>
            <p class="test-note">${this.proofBlocking !== false ? 'A rejected or unavailable review fails the Test stage and stops the pipeline.' : 'This review is non-blocking: its verdict does not change the automated test result.'}</p>
            <p>${this.escape(proof.reason || 'The deployment review will appear here when SwiftProof runs.')}</p>
            ${this.proofError ? `<p class="test-warning" role="status">${this.escape(this.proofError)}</p>` : ''}
            ${findings.length ? `<p class="test-note">${findings.length} findings / review areas · Select a finding to inspect its evidence and source changes.</p><div class="test-risk-list">${findings.map((finding, index) => `<button class="test-risk" data-finding="${index}"><span>${this.badge(finding.severity)}${finding.status ? ' ' + this.badge(finding.status) : ''}</span><strong>${this.escape(finding.title)}</strong><small>${this.escape(finding.path || finding.kind)}${finding.line ? ':' + finding.line : ''} ↗</small></button>`).join('')}</div>` : this.report ? '<p class="test-note">No structured findings in this report. Consult the full report for coverage and limitations.</p>' : ''}
            ${this.report ? '<button class="btn btn-sm btn-secondary" data-view="report">Open full report</button>' : ''}`;
        if (this.view === 'report') this.renderReport();
    }

    renderReport() {
        if (!this.report) { this.slot('report').innerHTML = '<p class="test-empty">Waiting for a SwiftProof report.</p>'; return; }
        const report = this.report.report || {};
        const finding = this.report.findings?.[this.selectedFinding];
        let detail = '';
        if (finding) {
            const files = report.change?.files || [];
            const file = files.find(file => (finding.side === 'old' ? file.old_path || file.path : file.path) === finding.path);
            const source = (file?.hunks || []).flatMap(hunk => hunk.lines || []);
            const target = source.findIndex(line => Number(finding.side === 'old' ? line.old_line : line.new_line) === finding.line);
            const context = target < 0 ? source.slice(0, 80) : source.slice(Math.max(0, target - 12), target + 40);
            detail = `<article class="test-finding"><h3>${this.escape(finding.title)}</h3><p>${this.badge(finding.severity)} ${finding.status ? this.badge(finding.status) : ''} <span class="test-note">${this.escape(finding.path)}${finding.line ? ':' + finding.line : ''}</span></p>
                ${(finding.evidence || []).map(evidence => {
                    const checks = [['Baseline', evidence.base_check_id], ['Candidate', evidence.check_id]].map(([label, id]) => {
                        const check = (report.checks || []).find(check => id && check.id === id);
                        return check ? `<details class="test-group" open><summary>${label} check ${this.badge(check.status)}${check.duration_ms != null ? ' · ' + this.escape(check.duration_ms) + ' ms' : ''}</summary><pre>${this.escape((check.command || []).join(' '))}\n${this.escape(check.output || 'No captured output.')}${check.truncated ? '\n[Output truncated by SwiftProof]' : ''}</pre></details>` : '';
                    }).join('');
                    return `<details class="test-group" open><summary>${this.escape(evidence.description || evidence.id || 'Evidence')}</summary>${evidence.path ? `<p class="test-note">${this.escape(evidence.path)}</p>` : ''}${evidence.test_names?.length ? `<p class="test-note">Tests: ${this.escape(evidence.test_names.join(', '))}</p>` : ''}${evidence.output || evidence.status ? `<pre>${this.escape(evidence.output || evidence.status)}</pre>` : ''}${checks}</details>`;
                }).join('') || '<p class="test-note">No linked execution evidence for this finding.</p>'}
                ${context.length ? `<h4>Source changes · ${this.escape(finding.side || 'new')} side</h4><div class="test-source">${context.map(line => {
                    const number = finding.side === 'old' ? line.old_line : line.new_line;
                    const highlighted = finding.line > 0 && number >= finding.line && number <= (finding.end_line || finding.line);
                    return `<div class="test-log-line ${highlighted ? 'test-log-selected' : ''}"><span class="test-line-number">${this.escape(number || '·')}</span><span>${this.escape(line.kind)} ${this.escape(line.content)}</span></div>`;
                }).join('')}</div>` : '<p class="test-note">No source excerpt is available in the retained report. The location above identifies the review target.</p>'}</article>`;
        }
        const lines = String(this.report.markdown || '').split('\n');
        const headings = lines.map((text, index) => ({ text, index })).filter(item => /^#{1,6}\s/.test(item.text));
        this.slot('report').innerHTML = detail + `<div class="test-report-heading"><h3>Full confidence report</h3><button class="btn btn-sm btn-secondary" data-action="download">Download evidence</button></div><nav class="test-section-links" aria-label="Report sections">${headings.map(item => `<button class="btn btn-sm btn-secondary" data-report-line="${item.index}">${this.escape(item.text.replace(/^#+\s*/, ''))}</button>`).join('')}</nav><div class="test-markdown">${lines.map((line, index) => /^#{1,6}\s/.test(line) ? `<h4 tabindex="-1" data-report-anchor="${index}">${this.escape(line.replace(/^#+\s*/, ''))}</h4>` : `<div>${this.escape(line) || ' '}</div>`).join('')}</div>`;
    }

    async downloadProof() {
        if (!this.reportId) return;
        try {
            const response = await this.request(this.proofURL(this.reportId) + '?download=true');
            const url = URL.createObjectURL(await response.blob());
            const link = document.createElement('a');
            link.href = url;
            link.download = `swiftproof-${this.reportId.slice(0, 12)}.zip`;
            link.click();
            setTimeout(() => URL.revokeObjectURL(url), 1000);
        } catch (error) { if (!this.closed) showNotification('error', error.message); }
    }
}
