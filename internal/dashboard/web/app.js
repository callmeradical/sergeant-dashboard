const workersNode = document.querySelector('#workers');
const scrim = document.querySelector('#scrim');
const drawer = document.querySelector('#drawer');
const drawerClose = document.querySelector('#drawer-close');
const drawerBody = document.querySelector('#drawer-body');
const drawerProject = document.querySelector('#drawer-project');
const drawerBadge = document.querySelector('#drawer-badge');
const mainEl = document.querySelector('main');

let state = { workers: [], warnings: [] };
let filter = 'all';
let activeCard = null;

const HEALTH_COLORS = { active:'var(--acid)', stale:'var(--amber)', orphaned:'var(--red)', complete:'var(--muted)', attention:'var(--red)', recycled:'var(--muted)' };
const ATTENTION_HEALTH = new Set(['stale', 'orphaned', 'attention']);

const text = (tag, value, className) => {
  const node = document.createElement(tag);
  node.textContent = value || '-';
  if (className) node.className = className;
  return node;
};

// ── Drawer ───────────────────────────────────────────────────────────────────

function openDrawer(worker, cardEl) {
  const statusColor = HEALTH_COLORS[worker.health] || 'var(--muted)';

  drawerProject.textContent = worker.project || '-';
  drawerBadge.textContent = worker.health || '-';
  drawerBadge.style.setProperty('--status', statusColor);
  drawerBadge.style.borderColor = statusColor;
  drawerBadge.style.color = statusColor;

  drawerBody.replaceChildren();
  buildDetail(drawerBody, worker);

  if (activeCard) activeCard.classList.remove('drawer-open');
  activeCard = cardEl;
  if (activeCard) activeCard.classList.add('drawer-open');

  drawer.classList.add('open');
  scrim.classList.add('open');
  if (mainEl) mainEl.inert = true;
  drawer.focus();
}

function closeDrawer() {
  const trigger = activeCard;
  drawer.classList.remove('open');
  scrim.classList.remove('open');
  if (mainEl) mainEl.inert = false;
  if (activeCard) { activeCard.classList.remove('drawer-open'); activeCard = null; }
  // Restore focus to the card that opened the drawer.
  if (trigger) trigger.focus();
}

// Focus trap: when the drawer is open, keep Tab cycling within it.
drawer.addEventListener('keydown', e => {
  if (!drawer.classList.contains('open') || e.key !== 'Tab') return;
  const focusable = [...drawer.querySelectorAll('a[href], button, [tabindex="0"]')];
  if (!focusable.length) return;
  const first = focusable[0];
  const last = focusable[focusable.length - 1];
  if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
  else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
});

drawerClose.addEventListener('click', closeDrawer);
scrim.addEventListener('click', closeDrawer);
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeDrawer(); });

// ── Render ───────────────────────────────────────────────────────────────────

// isAttention returns true for actionable workers: legacy attention states,
// orphaned records, and active workers in a waiting/blocked lifecycle state.
const isAttention = w => ATTENTION_HEALTH.has(w.health) || (w.health === 'active' && (w.status === 'needs_input' || w.status === 'blocked'));

function render() {
  const attention = state.workers.filter(isAttention);
  document.querySelector('#total').textContent = state.workers.length;
  document.querySelector('#active').textContent = state.workers.filter(w => w.health === 'active').length;
  document.querySelector('#attention').textContent = attention.length;
  document.querySelector('#updated').textContent = `Updated ${new Date(state.collectedAt).toLocaleString()}`;
  const warnings = document.querySelector('#warnings');
  warnings.hidden = !state.warnings.length;
  warnings.textContent = state.warnings.join('\n');
  workersNode.replaceChildren();
  const shown = state.workers.filter(w =>
    filter === 'all' ||
    (filter === 'attention' ? isAttention(w) : w.health === filter)
  );
  if (!shown.length) workersNode.append(text('p', 'No workers match this view.', 'empty'));
  shown.forEach(w => workersNode.append(workerCard(w)));
}

function workerCard(worker) {
  const card = document.createElement('article');
  card.className = 'worker';
  card.style.setProperty('--status', HEALTH_COLORS[worker.health] || 'var(--muted)');
  card.setAttribute('role', 'button');
  card.setAttribute('tabindex', '0');
  const label = [worker.project, worker.health, worker.status, worker.task].filter(Boolean).join(', ');
  card.setAttribute('aria-label', `Open details: ${label}`);

  const head = document.createElement('div');
  head.className = 'worker-head';
  head.append(text('h3', worker.project), text('span', worker.health, 'badge'));
  card.append(head);

  if (worker.task) {
    const sub = document.createElement('div');
    sub.className = 'worker-sub';
    sub.title = worker.task;
    sub.textContent = worker.task;
    card.append(sub);
  }

  const line = document.createElement('div');
  line.className = 'worker-line';
  const branch = worker.branch ? worker.branch.replace(/^refs\/heads\//, '') : null;
  const parts = [
    worker.status ? `<strong>${worker.status}</strong>` : null,
    branch ? `<strong>${branch.length > 28 ? branch.slice(0, 28) + '\u2026' : branch}</strong>` : null
  ].filter(Boolean);
  line.innerHTML = parts.join(' <span style="color:var(--muted)">\u00b7</span> ');
  card.append(line);

  const activeSignals = [
    ['message', worker.message?.present],
    ['graphify', worker.graphify?.present],
    ['no-mistakes', worker.noMistakes?.available],
    ['oc-inject', worker.ocInject?.responsePending || worker.ocInject?.responseAcked],
  ].filter(([, on]) => on).map(([label]) => label);

  if (activeSignals.length) {
    const signals = document.createElement('div');
    signals.className = 'signals';
    activeSignals.forEach(label => signals.append(text('span', label, 'signal')));
    card.append(signals);
  }

  const openThis = () => openDrawer(worker, card);
  card.addEventListener('click', openThis);
  card.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); openThis(); } });

  // DOM-only metadata for text search: hidden from assistive tech, visible to textContent queries
  const accessible = document.createElement('span');
  accessible.className = 'sr-only';
  accessible.setAttribute('aria-hidden', 'true');
  const metaParts = [
    worker.tdTask,
    worker.worktree,
    worker.message?.summary,
    worker.diagnostic?.summary,
    worker.log?.summary,
    worker.handoff?.summary,
    worker.pullRequest?.status,
    ...(worker.pullRequest?.checks || []).map(c => `${c.name || 'check'}: ${c.conclusion || c.state || c.status || 'pending'}`),
    worker.noMistakes?.summary || worker.noMistakes?.status,
    worker.graphify?.summary || worker.graphify?.status,
    worker.ocInject?.responsePending ? 'response pending' : null,
    worker.ocInject?.responseAcked ? 'response acked' : null,
  ].filter(Boolean);
  if (metaParts.length) accessible.textContent = metaParts.join(' ');
  for (const url of [worker.pullRequest?.url, ...(worker.pullRequest?.comments || []).map(c => c.url)].filter(Boolean)) {
    const a = document.createElement('a');
    a.href = url; a.rel = 'noreferrer'; a.tabIndex = -1; a.setAttribute('aria-hidden', 'true');
    accessible.append(a);
  }
  card.append(accessible);

  return card;
}

function buildDetail(container, worker) {
  // ── Meta grid ──────────────────────────────────────────────────────────────
  const meta = document.createElement('dl');
  meta.className = 'meta';
  const row = (label, value) => {
    if (!value && value !== 0) return;
    meta.append(text('dt', label), text('dd', value));
  };
  row('Repo', worker.repository);
  row('Agent', worker.agent);
  row('Worktree', worker.worktree);
  if (worker.pullRequest?.url) {
    const dd = document.createElement('dd');
    const a = document.createElement('a');
    a.href = worker.pullRequest.url; a.rel = 'noreferrer';
    a.textContent = worker.pullRequest.state || 'View PR';
    dd.append(a);
    if (worker.pullRequest?.status) {
      dd.append(document.createTextNode(' (' + worker.pullRequest.status + ')'));
    }
    meta.append(text('dt', 'PR'), dd);
  } else if (worker.pullRequest?.status) {
    row('PR', worker.pullRequest.status);
  }
  if (worker.pullRequest?.checks?.length) {
    row('Checks', worker.pullRequest.checks.map(c => `${c.name || 'check'}: ${c.conclusion || c.state || c.status || 'pending'}`).join(', '));
  }
  if (worker.pullRequest?.comments?.length) {
    worker.pullRequest.comments.forEach(c => {
      const label = `${c.author || ''}: ${c.body || ''}`.trim() || 'View comment';
      if (c.url) {
        const dd = document.createElement('dd');
        const a = document.createElement('a');
        a.href = c.url; a.rel = 'noreferrer'; a.textContent = label;
        dd.append(a); meta.append(text('dt', 'Comment'), dd);
      } else {
        row('Comment', label);
      }
    });
  }
  [['Message', worker.message], ['Diag', worker.diagnostic], ['Log', worker.log], ['Handoff', worker.handoff]].forEach(([label, file]) => {
    if (file?.present) row(label, file.summary || 'present');
  });
  if (worker.noMistakes?.available || worker.noMistakes?.status) {
    row('no-mistakes', worker.noMistakes?.summary || worker.noMistakes?.status || (worker.noMistakes?.phase ? `${worker.noMistakes.phase} phase` : 'detected'));
  }
  if (worker.graphify?.present || worker.graphify?.status) {
    row('Graphify', worker.graphify?.summary || worker.graphify?.status || 'available');
  }
  if (worker.ocInject?.responsePending) {
    const at = worker.ocInject.responsePendingAt ? ` since ${new Date(worker.ocInject.responsePendingAt).toLocaleString()}` : '';
    row('oc-inject', `response pending${at}`);
  } else if (worker.ocInject?.responseAcked) {
    const at = worker.ocInject.responseAckedAt ? ` ${new Date(worker.ocInject.responseAckedAt).toLocaleString()}` : '';
    row('oc-inject', `acked${at}`);
  }
  container.append(meta);

  // ── Message block ──────────────────────────────────────────────────────────
  if (worker.message?.present && worker.message?.summary) {
    const msg = document.createElement('div');
    msg.className = 'message';
    msg.textContent = worker.message.summary;
    container.append(msg);
  }

  // ── Tech Debt section ──────────────────────────────────────────────────────
  const hasTD = worker.tdTask || worker.td?.present;
  if (hasTD) {
    const section = document.createElement('div');
    section.className = 'td-section';

    const hdr = document.createElement('div');
    hdr.className = 'td-section-header';
    hdr.textContent = 'Tech Debt';
    section.append(hdr);

    const card = document.createElement('div');
    card.className = 'td-card';

    if (worker.tdTask) {
      const chip = document.createElement('code');
      chip.className = 'td-chip';
      chip.textContent = worker.tdTask;
      card.append(chip);
    }

    if (worker.td?.present && worker.td?.summary) {
      const desc = document.createElement('p');
      desc.className = 'td-desc';
      desc.textContent = worker.td.summary;
      card.append(desc);
    }

    if (worker.td?.status && worker.td.status !== 'available') {
      const pill = document.createElement('span');
      pill.className = 'td-pill';
      pill.textContent = worker.td.status;
      card.append(pill);
    }

    section.append(card);
    container.append(section);
  }
}

// ── Filters ──────────────────────────────────────────────────────────────────

document.querySelectorAll('[data-filter]').forEach(btn => btn.addEventListener('click', () => {
  filter = btn.dataset.filter;
  document.querySelectorAll('[data-filter]').forEach(b => b.classList.toggle('selected', b === btn));
  closeDrawer();
  render();
}));

// ── Data fetch + 5s poll ─────────────────────────────────────────────────────

function fetchState() {
  fetch('api/state', { cache:'no-store' })
    .then(r => { if (!r.ok) throw new Error('unavailable'); return r.json(); })
    .then(data => { state = data; render(); })
    .catch(() => {
      if (!state.workers.length) {
        workersNode.replaceChildren(text('p', 'Fleet state is temporarily unavailable.', 'empty'));
      }
    });
}

fetchState();
setInterval(fetchState, 5000);
