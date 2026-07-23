const workersNode = document.querySelector('#workers');
const scrim = document.querySelector('#scrim');
const drawer = document.querySelector('#drawer');
const drawerClose = document.querySelector('#drawer-close');
const drawerBody = document.querySelector('#drawer-body');
const drawerProject = document.querySelector('#drawer-project');
const drawerBadge = document.querySelector('#drawer-badge');

let state = { workers: [], warnings: [] };
let filter = 'all';
let activeCard = null;

const text = (tag, value, className) => {
  const node = document.createElement(tag);
  node.textContent = value || '-';
  if (className) node.className = className;
  return node;
};

// ── Drawer ───────────────────────────────────────────────────────────────────

function openDrawer(worker, cardEl) {
  const colors = { active:'var(--acid)', stale:'var(--amber)', orphaned:'var(--red)', complete:'var(--muted)' };
  const statusColor = colors[worker.health] || 'var(--muted)';

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
  drawer.focus();
}

function closeDrawer() {
  drawer.classList.remove('open');
  scrim.classList.remove('open');
  if (activeCard) { activeCard.classList.remove('drawer-open'); activeCard = null; }
}

drawerClose.addEventListener('click', closeDrawer);
scrim.addEventListener('click', closeDrawer);
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeDrawer(); });

// ── Render ───────────────────────────────────────────────────────────────────

function render() {
  const attention = state.workers.filter(w => ['stale','orphaned'].includes(w.health));
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
    (filter === 'attention' ? ['stale','orphaned'].includes(w.health) : w.health === filter)
  );
  if (!shown.length) workersNode.append(text('p', 'No workers match this view.', 'empty'));
  shown.forEach(w => workersNode.append(workerCard(w)));
}

function workerCard(worker) {
  const colors = { active:'var(--acid)', stale:'var(--amber)', orphaned:'var(--red)', complete:'var(--muted)' };
  const card = document.createElement('article');
  card.className = 'worker';
  card.style.setProperty('--status', colors[worker.health] || 'var(--muted)');
  card.setAttribute('role', 'button');
  card.setAttribute('tabindex', '0');

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
    dd.append(a); meta.append(text('dt', 'PR'), dd);
  } else if (worker.pullRequest?.status) {
    row('PR', worker.pullRequest.status);
  }
  if (worker.pullRequest?.checks?.length) {
    row('Checks', worker.pullRequest.checks.map(c => `${c.name || 'check'}: ${c.conclusion || c.state || c.status || 'pending'}`).join(', '));
  }
  if (worker.pullRequest?.comments?.length) {
    worker.pullRequest.comments.forEach(c => row('Comment', `${c.author || ''}: ${c.body || ''}`.trim()));
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
    row('oc-inject', `pending${at}`);
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
