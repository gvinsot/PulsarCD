/* Structured test output. No log content is executable. */
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
            <nav class="test-tabs" aria-label="Test log views">
                <button class="btn btn-sm" data-view="results" aria-pressed="true">Test explorer</button>
                <button class="btn btn-sm" data-view="raw" aria-pressed="false">Raw logs</button>
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
            </section>`;
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
    }

    click(event) {
        const button = event.target.closest('button');
        if (!button || !this.root.contains(button) || button.disabled) return;
        if (button.dataset.view) this.showView(button.dataset.view);
        if (button.dataset.line !== undefined) this.jumpToLine(Number(button.dataset.line));
        const action = button.dataset.action;
        if (action === 'previous' || action === 'next') {
            this.follow = false;
            this.root.querySelector('[data-control="follow"]').checked = false;
            this.rawStart += action === 'previous' ? -500 : 500;
            this.renderRaw();
        }
        if (action === 'themes') this.groupWithLLM();
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
}
