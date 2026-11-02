(() => {
  'use strict';

  const $ = (id) => document.getElementById(id);
  const SESSION_KEY = 'dhara.playground.session';
  const POLL_MS = 1500;

  const state = {
    session: '',
    scope: 'all',
    status: '',
    snapshot: null,
    timer: null,
    inflight: false,
    errorShown: false,
    metricsLoaded: false,
    activeTaskId: '',
  };

  const phaseLabels = {
    idle: 'no crash demo running',
    starting: 'starting isolated worker',
    'enqueuing task': 'enqueuing crash task',
    'waiting for worker claim': 'waiting for the isolated worker to claim the task',
    'killing isolated worker process': 'killing the isolated worker process',
    'waiting for stale heartbeat reaper': 'heartbeat stopped — waiting for the reaper',
    'restarting isolated worker': 'restarting the isolated worker',
    'waiting for recovered task to complete': 'reaper recovered the task — waiting for completion',
    completed: 'recovered and completed',
    failed: 'demo failed',
  };

  function newSessionId() {
    if (window.crypto && typeof window.crypto.randomUUID === 'function') {
      return window.crypto.randomUUID();
    }
    const bytes = new Uint8Array(16);
    window.crypto.getRandomValues(bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
  }

  function loadSession() {
    let session = '';
    try { session = localStorage.getItem(SESSION_KEY) || ''; } catch (_) { /* storage unavailable */ }
    if (!session) {
      session = newSessionId();
      try { localStorage.setItem(SESSION_KEY, session); } catch (_) { /* ignore */ }
    }
    state.session = session;
    $('sessionLabel').textContent = `session ${session.slice(0, 8)}`;
  }

  async function api(path, options = {}) {
    const headers = new Headers(options.headers || {});
    headers.set('X-Playground-Session', state.session);
    if (options.body && !headers.has('Content-Type')) {
      headers.set('Content-Type', 'application/json');
    }
    const res = await fetch(path, { ...options, headers, cache: 'no-store' });
    const text = await res.text();
    let data = null;
    if (text) {
      try { data = JSON.parse(text); } catch (_) { data = { error: text }; }
    }
    if (!res.ok) {
      const error = new Error((data && data.error) || `HTTP ${res.status}`);
      error.status = res.status;
      error.data = data;
      throw error;
    }
    return data;
  }

  async function fetchText(path) {
    const res = await fetch(path, { cache: 'no-store' });
    if (!res.ok) throw new Error(`metrics request failed (HTTP ${res.status})`);
    return res.text();
  }

  function setLive(ok, text) {
    const dot = $('liveDot');
    dot.classList.toggle('ok', ok === true);
    dot.classList.toggle('err', ok === false);
    $('liveText').textContent = text || (ok ? 'live' : 'disconnected');
  }

  function showError(message) {
    $('errorText').textContent = message;
    $('errorBanner').hidden = false;
    state.errorShown = true;
  }

  function hideError() {
    if (!state.errorShown) return;
    $('errorBanner').hidden = true;
    state.errorShown = false;
  }

  function toast(message, isError = false) {
    const el = $('toast');
    el.textContent = message;
    el.classList.toggle('error', isError);
    el.hidden = false;
    clearTimeout(state.toastTimer);
    state.toastTimer = setTimeout(() => { el.hidden = true; }, 4200);
  }

  function schedule(delay = POLL_MS) {
    clearTimeout(state.timer);
    if (document.hidden) {
      setLive(true, 'paused');
      return;
    }
    state.timer = setTimeout(loadSnapshot, delay);
  }

  async function loadSnapshot() {
    if (state.inflight) return;
    state.inflight = true;
    try {
      const query = new URLSearchParams({ scope: state.scope, status: state.status, limit: '100' });
      const snap = await api(`/api/v1/playground/snapshot?${query}`);
      state.snapshot = snap;
      render();
      setLive(true, 'live');
      hideError();
    } catch (err) {
      setLive(false, err.status === 429 ? 'rate limited' : 'disconnected');
      showError(err.message || 'Unable to load playground state.');
    } finally {
      state.inflight = false;
      schedule();
    }
  }

  function loadNow() {
    clearTimeout(state.timer);
    loadSnapshot();
  }

  function render() {
    const snap = state.snapshot;
    if (!snap) return;
    const o = snap.overview;
    const w = snap.worker_pool;

    $('mPending').textContent = fmtInt(o.pending);
    $('mPendingSub').textContent = `${fmtInt(o.pending_ready)} ready · ${fmtInt(o.pending_delayed)} delayed`;
    $('mRunning').textContent = fmtInt(o.running);
    $('mRunningSub').textContent = `${fmtInt(o.workers_inflight)} handlers inflight`;
    $('mCompleted').textContent = fmtInt(o.completed);
    $('mCompletedSub').textContent = `+${fmtInt(o.completed_last_minute)} last min`;
    $('mDead').textContent = fmtInt(o.dead);
    $('mDeadSub').textContent = `+${fmtInt(o.dead_last_minute)} last min`;
    $('mCanceled').textContent = fmtInt(o.canceled);
    $('mWorkers').textContent = fmtInt(o.workers_active);
    $('mWorkersSub').textContent = `${fmtInt(o.workers_inflight)} inflight · ${fmtInt(w.configured)} configured`;
    $('mRate').textContent = fmtInt(o.completed_last_minute);
    $('mReaped').textContent = fmtInt(o.tasks_reaped_total);
    $('updatedAt').textContent = `updated ${formatClock(snap.generated_at)}`;

    $('wConfigured').textContent = fmtInt(w.configured);
    $('wActive').textContent = fmtInt(w.active);
    $('wInflight').textContent = fmtInt(w.inflight);
    $('wPrefix').textContent = w.prefix || '—';
    renderLeases(w.leases || []);

    renderTasks(snap.tasks || []);
    renderCrash(snap);
  }

  function renderLeases(leases) {
    const body = $('leaseRows');
    body.replaceChildren();
    if (!leases.length) {
      body.appendChild(emptyRow(4, 'No running task leases.'));
      return;
    }
    for (const lease of leases) {
      const tr = document.createElement('tr');
      tr.appendChild(cell(lease.worker_id, 'mono-dim'));
      tr.appendChild(cell(lease.task_type, 'task-type'));
      tr.appendChild(cell(`${lease.attempts}`, 'mono-dim'));
      tr.appendChild(timeCell(lease.locked_at, 'never'));
      body.appendChild(tr);
    }
  }

  function renderTasks(tasks) {
    const body = $('taskRows');
    body.replaceChildren();
    if (!tasks.length) {
      body.appendChild(emptyRow(8, state.status ? `No ${state.status.toLowerCase()} tasks in this view.` : 'No tasks yet. Start a scenario above.'));
    } else {
      for (const task of tasks) {
        const tr = document.createElement('tr');
        tr.dataset.taskId = task.id;
        tr.title = 'Open task detail';
        tr.appendChild(cell(task.id.slice(0, 8), 'task-id'));
        tr.appendChild(cell(task.type, 'task-type'));
        tr.appendChild(stateCell(task.status));
        tr.appendChild(cell(`${task.attempts}/${task.max_retries}`, 'mono-dim'));
        tr.appendChild(cell(task.locked_by || '—', 'mono-dim'));
        tr.appendChild(cell(nextRun(task), 'mono-dim'));
        tr.appendChild(cell(task.duration_ms != null ? formatDuration(task.duration_ms) : '—', 'mono-dim'));
        tr.appendChild(timeCell(task.created_at, '—'));
        tr.addEventListener('click', () => openDetail(task.id));
        body.appendChild(tr);
      }
    }
    const snap = state.snapshot;
    if (snap) {
      $('taskCount').textContent = `${tasks.length} shown · ${fmtInt(snap.overview.pending)} pending · ${fmtInt(snap.overview.dead)} dead`;
    }
  }

  function renderCrash(snap) {
    const scenario = $('crashScenario');
    scenario.hidden = !snap.crash_enabled;
    if (!snap.crash_enabled) return;

    const crash = snap.crash_demo;
    const box = $('crashStatus');
    box.replaceChildren();
    if (!crash || crash.phase === 'idle') {
      box.appendChild(hint('No crash demo running. The isolated worker is started on demand.'));
      $('crashBtn').disabled = false;
      return;
    }

    const phase = document.createElement('div');
    phase.className = 'crash-phase';
    phase.textContent = `phase: ${phaseLabels[crash.phase] || crash.phase}`;
    box.appendChild(phase);

    if (crash.task) {
      const info = document.createElement('div');
      info.className = 'crash-task';
      info.textContent = `${crash.task.type} · ${crash.task.status} · attempt ${crash.task.attempts}/${crash.task.max_retries}`;
      box.appendChild(info);
    }
    if (crash.error) {
      const err = document.createElement('div');
      err.className = 'crash-error';
      err.textContent = crash.error;
      box.appendChild(err);
    }
    box.appendChild(hint(`reaped tasks in this process: ${fmtInt(crash.reaped_total || 0)}`));

    const running = !['idle', 'completed', 'failed'].includes(crash.phase);
    $('crashBtn').disabled = running;
  }

  function hint(text) {
    const el = document.createElement('span');
    el.className = 'muted';
    el.textContent = text;
    return el;
  }

  function emptyRow(cols, text) {
    const tr = document.createElement('tr');
    const td = document.createElement('td');
    td.colSpan = cols;
    td.className = 'empty';
    td.textContent = text;
    tr.appendChild(td);
    return tr;
  }

  function cell(text, className) {
    const td = document.createElement('td');
    if (className) td.className = className;
    td.textContent = text;
    return td;
  }

  function timeCell(iso, fallback) {
    const td = document.createElement('td');
    td.className = 'mono-dim';
    if (!iso) {
      td.textContent = fallback;
      return td;
    }
    td.textContent = formatRelative(iso);
    td.title = formatAbsolute(iso);
    return td;
  }

  function stateCell(status) {
    const td = document.createElement('td');
    td.appendChild(stateBadge(status));
    return td;
  }

  function stateBadge(status) {
    const span = document.createElement('span');
    span.className = `state state-${String(status || '').toLowerCase()}`;
    span.textContent = status || 'UNKNOWN';
    return span;
  }

  function nextRun(task) {
    if (task.status !== 'PENDING') return '—';
    const runAt = parseDate(task.run_at);
    if (!runAt) return '—';
    const delta = runAt.getTime() - Date.now();
    if (delta <= 0) return 'ready';
    return `in ${formatDuration(delta)}`;
  }

  async function openDetail(id) {
    state.activeTaskId = id;
    $('drawer').hidden = false;
    $('drawerTitle').textContent = id;
    $('drawerBody').replaceChildren(hint('Loading task…'));
    try {
      const detail = await api(`/api/v1/playground/tasks/${encodeURIComponent(id)}`);
      renderDetail(detail);
    } catch (err) {
      $('drawerBody').replaceChildren(hint(err.message || 'Unable to load task.'));
    }
  }

  function renderDetail(detail) {
    const t = detail.task;
    const body = $('drawerBody');
    body.replaceChildren();

    const grid = document.createElement('div');
    grid.className = 'detail-grid';
    addField(grid, 'State', stateBadge(t.status));
    addField(grid, 'Type', t.type);
    addField(grid, 'Attempts', `${t.attempts} / ${t.max_retries}`);
    addField(grid, 'Priority', String(t.priority));
    addField(grid, 'Worker lease', t.locked_by || '—');
    addField(grid, 'Last heartbeat', t.locked_at ? formatRelative(t.locked_at) : '—');
    addField(grid, 'Duration', t.duration_ms != null ? formatDuration(t.duration_ms) : '—');
    addField(grid, 'Next run', nextRun(t));
    addField(grid, 'Run at', formatAbsolute(t.run_at));
    addField(grid, 'Started', t.started_at ? formatAbsolute(t.started_at) : '—');
    addField(grid, 'Completed', t.completed_at ? formatAbsolute(t.completed_at) : '—');
    addField(grid, 'Created', formatAbsolute(t.created_at));
    addField(grid, 'Updated', formatAbsolute(t.updated_at));
    addField(grid, 'Task ID', t.id, true);
    if (t.last_error) addField(grid, 'Last error', t.last_error, true);
    body.appendChild(grid);

    if (detail.owned && detail.payload != null) {
      const section = document.createElement('div');
      section.className = 'detail-section';
      const h = document.createElement('h3');
      h.textContent = 'Payload';
      const pre = document.createElement('pre');
      pre.textContent = JSON.stringify(detail.payload, null, 2);
      section.append(h, pre);
      body.appendChild(section);
    } else {
      body.appendChild(hint('Payload and logs are exposed only to the browser session that created this task.'));
    }

    if (detail.owned && detail.logs && detail.logs.length) {
      const section = document.createElement('div');
      section.className = 'detail-section';
      const h = document.createElement('h3');
      h.textContent = 'PostgreSQL task log';
      const list = document.createElement('ul');
      list.className = 'log-list';
      for (const log of detail.logs) {
        const li = document.createElement('li');
        li.className = `log-item ${String(log.status || '').toLowerCase()}`;
        const status = document.createElement('div');
        status.className = 'log-status';
        status.textContent = log.status;
        const message = document.createElement('div');
        message.className = 'log-message';
        message.textContent = log.message || '';
        const time = document.createElement('div');
        time.className = 'log-time';
        time.textContent = formatAbsolute(log.created_at);
        li.append(status, message, time);
        list.appendChild(li);
      }
      section.append(h, list);
      body.appendChild(section);
    }

    if (detail.owned) {
      const actions = document.createElement('div');
      actions.className = 'detail-actions';
      if (t.status === 'DEAD') {
        const retry = document.createElement('button');
        retry.className = 'button primary';
        retry.type = 'button';
        retry.textContent = 'Retry dead task';
        retry.addEventListener('click', () => mutateTask(t.id, 'retry', retry));
        actions.appendChild(retry);
      }
      if (t.status === 'PENDING') {
        const cancel = document.createElement('button');
        cancel.className = 'button';
        cancel.type = 'button';
        cancel.textContent = 'Cancel pending task';
        cancel.addEventListener('click', () => mutateTask(t.id, 'cancel', cancel));
        actions.appendChild(cancel);
      }
      const refresh = document.createElement('button');
      refresh.className = 'button small';
      refresh.type = 'button';
      refresh.textContent = 'Refresh';
      refresh.addEventListener('click', () => openDetail(t.id));
      actions.appendChild(refresh);
      if (actions.children.length) body.appendChild(actions);
    }
  }

  function addField(grid, label, value, wide = false) {
    const box = document.createElement('div');
    box.className = `detail-field${wide ? ' wide' : ''}`;
    const l = document.createElement('span');
    l.className = 'metric-label';
    l.textContent = label;
    const v = document.createElement('span');
    v.className = 'detail-value';
    if (value instanceof Node) v.appendChild(value);
    else v.textContent = value == null ? '—' : String(value);
    box.append(l, v);
    grid.appendChild(box);
  }

  async function mutateTask(id, action, button) {
    button.disabled = true;
    try {
      if (action === 'retry') {
        await api(`/api/v1/tasks/${encodeURIComponent(id)}/retry`, { method: 'POST' });
        toast('Task returned to the ready queue.');
      } else {
        const task = await api(`/api/v1/tasks/${encodeURIComponent(id)}`, {
          method: 'DELETE',
        });
        toast(`Task is now ${task.status}.`);
      }
      await openDetail(id);
      loadNow();
    } catch (err) {
      toast(err.message || 'Action failed.', true);
      button.disabled = false;
    }
  }

  async function runScenario(name, body, button) {
    const original = button.textContent;
    button.disabled = true;
    button.textContent = 'Enqueueing…';
    try {
      const result = await api(`/api/v1/playground/scenarios/${name}`, {
        method: 'POST',
        body: JSON.stringify(body),
      });
      toast(`${result.count} ${result.scenario} task${result.count === 1 ? '' : 's'} enqueued.`);
      loadNow();
    } catch (err) {
      toast(err.message || 'Could not enqueue tasks.', true);
    } finally {
      button.disabled = false;
      button.textContent = original;
    }
  }

  async function startCrashDemo() {
    const button = $('crashBtn');
    button.disabled = true;
    try {
      await api('/api/v1/playground/scenarios/crash', { method: 'POST', body: '{}' });
      toast('Crash demo started. Watch the isolated worker lose its lease.');
      loadNow();
    } catch (err) {
      toast(err.message || 'Could not start crash demo.', true);
      button.disabled = false;
    }
  }

  async function loadMetrics(force = false) {
    if (state.metricsLoaded && !force) return;
    $('clientMetrics').textContent = 'Loading…';
    $('workerMetrics').textContent = 'Loading…';
    try {
      const [client, worker] = await Promise.all([
        fetchText('/metrics'),
        fetchText('/worker/metrics'),
      ]);
      $('clientMetrics').textContent = client;
      $('workerMetrics').textContent = worker;
      state.metricsLoaded = true;
    } catch (err) {
      $('clientMetrics').textContent = err.message;
      $('workerMetrics').textContent = err.message;
    }
  }

  function fmtInt(value) {
    const n = Number(value || 0);
    return new Intl.NumberFormat().format(n);
  }

  function formatDuration(ms) {
    const n = Number(ms);
    if (!Number.isFinite(n)) return '—';
    if (n < 1000) return `${Math.max(0, Math.round(n))}ms`;
    if (n < 60_000) return `${(n / 1000).toFixed(n < 10_000 ? 1 : 0)}s`;
    if (n < 3_600_000) return `${Math.floor(n / 60_000)}m ${Math.round((n % 60_000) / 1000)}s`;
    return `${Math.floor(n / 3_600_000)}h ${Math.round((n % 3_600_000) / 60_000)}m`;
  }

  function formatRelative(iso) {
    const date = parseDate(iso);
    if (!date) return '—';
    const delta = Date.now() - date.getTime();
    if (delta < 0) return `in ${formatDuration(-delta)}`;
    if (delta < 1500) return 'just now';
    return `${formatDuration(delta)} ago`;
  }

  function formatAbsolute(iso) {
    const date = parseDate(iso);
    if (!date) return iso ? String(iso) : '—';
    return date.toLocaleString(undefined, { hour12: false });
  }

  function formatClock(iso) {
    const date = parseDate(iso);
    if (!date) return '—';
    return date.toLocaleTimeString(undefined, { hour12: false });
  }

  function parseDate(iso) {
    if (!iso) return null;
    // Go emits RFC3339 with up to nanosecond precision. Some browsers only
    // parse three fractional digits, so trim anything beyond milliseconds.
    const normalized = String(iso).replace(/(\.\d{3})\d+/, '$1');
    const date = new Date(normalized);
    return Number.isNaN(date.getTime()) ? null : date;
  }

  function bind() {
    $('errorClose').addEventListener('click', hideError);
    $('newSession').addEventListener('click', () => {
      const id = newSessionId();
      try { localStorage.setItem(SESSION_KEY, id); } catch (_) { /* ignore */ }
      state.session = id;
      $('sessionLabel').textContent = `session ${id.slice(0, 8)}`;
      state.snapshot = null;
      closeDrawer();
      loadNow();
      toast('New browser session generated.');
    });

    $('scopeAll').addEventListener('click', () => setScope('all'));
    $('scopeMine').addEventListener('click', () => setScope('mine'));

    document.querySelectorAll('.tab').forEach((tab) => {
      tab.addEventListener('click', () => {
        document.querySelectorAll('.tab').forEach((t) => t.classList.remove('active'));
        tab.classList.add('active');
        state.status = tab.dataset.status || '';
        loadNow();
      });
    });

    $('burstSize').addEventListener('input', (event) => {
      const n = Math.max(1, Math.min(50, Number(event.target.value) || 25));
      $('burstBtn').textContent = `Burst ${n} fast jobs`;
    });
    $('burstBtn').addEventListener('click', () => {
      const size = Math.max(1, Math.min(50, Number($('burstSize').value) || 25));
      runScenario('burst', { size }, $('burstBtn'));
    });
    $('flakyBtn').addEventListener('click', () => {
      const count = Math.max(1, Math.min(10, Number($('flakyCount').value) || 3));
      runScenario('flaky', { count, mode: $('flakyMode').value }, $('flakyBtn'));
    });
    $('slowBtn').addEventListener('click', () => {
      const count = Math.max(1, Math.min(3, Number($('slowCount').value) || 1));
      runScenario('slow', { count }, $('slowBtn'));
    });
    $('crashBtn').addEventListener('click', startCrashDemo);

    $('drawerClose').addEventListener('click', closeDrawer);
    document.querySelector('[data-close-drawer]').addEventListener('click', closeDrawer);
    document.addEventListener('keydown', (event) => {
      if (event.key === 'Escape') closeDrawer();
    });

    $('metricsDetails').addEventListener('toggle', () => {
      if ($('metricsDetails').open) loadMetrics();
    });
    $('metricsRefresh').addEventListener('click', () => loadMetrics(true));

    document.addEventListener('visibilitychange', () => {
      if (!document.hidden) loadNow();
      else schedule();
    });
  }

  function setScope(scope) {
    state.scope = scope;
    $('scopeAll').classList.toggle('active', scope === 'all');
    $('scopeMine').classList.toggle('active', scope === 'mine');
    loadNow();
  }

  function closeDrawer() {
    $('drawer').hidden = true;
    state.activeTaskId = '';
  }

  loadSession();
  bind();
  loadSnapshot();
})();
