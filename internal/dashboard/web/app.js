const workersNode = document.querySelector('#workers');
const searchNode = document.querySelector('#search');
const drawer = document.querySelector('#drawer');
const drawerContent = document.querySelector('#drawer-content');
const backdrop = document.querySelector('#drawer-backdrop');
const closeButton = document.querySelector('#drawer-close');
const drawerTitle = document.querySelector('#drawer-title');
const drawerIdentity = document.querySelector('#drawer-identity');
const pageRegions = document.querySelectorAll('body > header, body > main, body > footer');
let state = { workers: [], warnings: [] };
let filter = 'all';
let query = '';
let activeCard = null;
let detailController = null;

const text = (tag, value, className) => {
  const node = document.createElement(tag);
  node.textContent = value || '-';
  if (className) node.className = className;
  return node;
};

const isAttention = worker => ['stale', 'orphaned', 'unknown', 'attention'].includes(worker.health) || ['blocked', 'needs_input', 'failed'].some(status => (worker.status || '').startsWith(status));

function signal(worker) {
  const status = worker.status || '';
  if (status.startsWith('failed')) return ['failed', 'Failure requires review'];
  if (status.startsWith('blocked')) return ['blocked', 'Blocked'];
  if (status.startsWith('needs_input')) return ['needs-input', 'Input required'];
  if (worker.health === 'orphaned') return ['orphaned', 'Worker orphaned'];
  if (worker.health === 'attention') return ['stale', 'Attention required'];
  if (worker.health === 'stale') return ['stale', 'Telemetry stale'];
  if (worker.health === 'unknown') return ['stale', 'State unknown'];
  if (worker.health === 'recycled') return ['done', ''];
  if (worker.health === 'active') return ['active', ''];
  return ['done', ''];
}

function searchable(worker) {
  return [worker.project, worker.repository, worker.task, worker.title, worker.status, worker.health, worker.agent, worker.tdTask, worker.pullRequestState, isAttention(worker) ? 'attention' : ''].join(' ').toLowerCase();
}

function age(value) {
  if (!value || value.startsWith('0001-')) return '-';
  const seconds = Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 1000));
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`;
  return `${Math.floor(seconds / 86400)}d`;
}

function render() {
  const attention = state.workers.filter(isAttention);
  document.querySelector('#total').textContent = state.workers.length;
  document.querySelector('#active').textContent = state.workers.filter(worker => worker.health === 'active').length;
  document.querySelector('#attention').textContent = attention.length;
  document.querySelector('#updated').textContent = state.collectedAt ? `Updated ${new Date(state.collectedAt).toLocaleString()}` : 'Update unavailable';
  const warnings = document.querySelector('#warnings');
  warnings.hidden = !state.warnings.length;
  warnings.replaceChildren();
  warnings.classList.toggle('severity-error', state.highestSeverity === 'error');
  if (state.warnings.length) {
    const labels = state.warnings.slice(0, 2).map(group => group.label);
    if (state.warnings.length > 2) labels.push(`+${state.warnings.length - 2} more categories`);
    const summary = document.createElement('span');
    summary.className = 'warning-summary';
    summary.append(text('strong', state.highestSeverity || 'warning', 'severity-label'), document.createTextNode(` · ${labels.join(' · ')}`));
    warnings.append(summary);
    const details = text('button', 'View details');
    details.type = 'button';
    details.setAttribute('aria-controls', 'drawer');
    details.setAttribute('aria-expanded', 'false');
    details.addEventListener('click', () => openDiagnostics(details));
    warnings.append(details);
  }

  const tokens = query.toLowerCase().trim().split(/\s+/).filter(Boolean);
  const shown = state.workers.filter(worker => {
    const filterMatch = filter === 'all' || (filter === 'attention' ? isAttention(worker) : worker.health === filter);
    const haystack = searchable(worker);
    return filterMatch && tokens.every(token => haystack.includes(token));
  });
  workersNode.replaceChildren();
  if (!shown.length) workersNode.append(text('p', query ? 'No worker summaries match this search.' : 'No workers match this view.', 'empty'));
  shown.forEach(worker => workersNode.append(workerCard(worker)));
}

function workerCard(worker) {
  const card = document.createElement('article');
  card.className = 'worker';
  card.tabIndex = 0;
  card.setAttribute('role', 'button');
  card.setAttribute('aria-controls', 'drawer');
  card.setAttribute('aria-expanded', 'false');
  const [signalName, attention] = signal(worker);
  const metadata = [['Agent', worker.agent], ['Updated', age(worker.updatedAt)], ['PR', worker.pullRequestState]];
  const metadataLabel = metadata.map(([label, value]) => `${label} ${value || 'unavailable'}`).join(', ');
  card.setAttribute('aria-label', `Open ${worker.project} ${worker.repository} ${worker.task} ${worker.title || ''}, ${worker.status || worker.health}, ${metadataLabel}${attention ? `, ${attention}` : ''}`);
  card.style.setProperty('--signal', `var(--${signalName})`);
  const head = document.createElement('div');
  head.className = 'worker-head';
  const title = document.createElement('div');
  title.append(text('p', `${worker.project} / ${worker.repository}`, 'worker-path'), text('h3', worker.title || worker.task), text('div', worker.task, 'task'));
  head.append(title, text('span', worker.status || worker.health, 'badge'));
  card.append(head);
  const meta = document.createElement('dl');
  meta.className = 'summary-meta';
  metadata.forEach(([label, value]) => meta.append(text('dt', label), text('dd', value)));
  card.append(meta);
  if (attention) card.append(text('p', attention, 'attention-note'));
  const open = () => openDetail(worker, card);
  card.addEventListener('click', open);
  card.addEventListener('keydown', event => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      open();
    }
  });
  return card;
}

function addRow(list, label, value) {
  if (value === undefined || value === null || value === '') return;
  list.append(text('dt', label), text('dd', value));
}

function detailSection(title, className = '') {
  const section = document.createElement('section');
  section.className = `detail-section ${className}`.trim();
  section.append(text('h3', title));
  const rows = document.createElement('dl');
  rows.className = 'detail-meta';
  section.append(rows);
  return [section, rows];
}

function detailView(worker) {
  const fragment = document.createDocumentFragment();
  const [overview, overviewRows] = detailSection('Overview');
  [['Task', worker.task], ['Status', worker.status], ['Health', worker.health], ['Agent', worker.agent], ['td', worker.tdTask], ['Description / acceptance / reviews', worker.td?.summary]].forEach(([label, value]) => addRow(overviewRows, label, value));
  fragment.append(overview);

  const [attention, attentionRows] = detailSection('Attention', 'attention-callout');
  addRow(attentionRows, 'Current signal', signal(worker)[1] || 'No immediate attention required');
  if (worker.message?.present || worker.message?.status) addRow(attentionRows, 'Message', worker.message.summary || worker.message.status || 'present');
  if (worker.diagnostic?.present || worker.diagnostic?.status) addRow(attentionRows, 'Diagnostic', worker.diagnostic.summary || worker.diagnostic.status || 'present');
  fragment.append(attention);

  const [timeline, timelineRows] = detailSection('Timeline', 'timeline');
  [['Log', worker.log], ['Handoff', worker.handoff]].forEach(([label, file]) => {
    if (file?.present || file?.status) addRow(timelineRows, label, file.summary || file.status || 'present');
  });
  if (worker.pullRequest?.comments?.length) addRow(timelineRows, 'Comments', worker.pullRequest.comments.map(comment => `${comment.author || 'comment'}: ${comment.body || '-'}`).join('\n'));
  fragment.append(timeline);

  const [delivery, deliveryRows] = detailSection('Delivery');
  if (worker.pullRequest?.url) {
    const value = document.createElement('dd');
    const link = document.createElement('a');
    link.href = worker.pullRequest.url;
    link.rel = 'noreferrer';
    link.textContent = worker.pullRequest.state || 'View pull request';
    value.append(link);
    deliveryRows.append(text('dt', 'Pull request'), value);
  }
  addRow(deliveryRows, 'PR source', worker.pullRequest?.status);
  if (worker.pullRequest?.checks?.length) addRow(deliveryRows, 'Checks', worker.pullRequest.checks.map(check => `${check.name || check.context || 'check'}${check.context && check.name ? ` (${check.context})` : ''}: ${check.conclusion || check.state || check.status || 'pending'}`).join(', '));
  if (worker.noMistakes?.available || worker.noMistakes?.status) addRow(deliveryRows, 'no-mistakes', worker.noMistakes.summary || worker.noMistakes.status || worker.noMistakes.phase);
  if (worker.graphify?.present || worker.graphify?.status) addRow(deliveryRows, 'Graphify', worker.graphify.summary || worker.graphify.status || 'present');
  if (worker.ocInject?.responsePending) addRow(deliveryRows, 'oc-inject', 'response pending');
  if (worker.ocInject?.responseAcked) addRow(deliveryRows, 'oc-inject', 'response acknowledged');
  fragment.append(delivery);

  const [files, fileRows] = detailSection('Files');
  [['Branch', worker.branch], ['Worktree', worker.worktree]].forEach(([label, value]) => {
    if (!value) return;
    const block = text('code', value, 'path-block');
    fileRows.append(text('dt', label));
    const cell = document.createElement('dd');
    cell.append(block);
    fileRows.append(cell);
  });
  [['Message', worker.message], ['Diagnostic', worker.diagnostic], ['Worker log', worker.log], ['Handoff', worker.handoff]].forEach(([label, file]) => {
    addRow(fileRows, label, file?.status || (file?.present ? 'available' : 'missing'));
  });
  fragment.append(files);
  return fragment;
}

function diagnosticsView(diagnostics) {
  const fragment = document.createDocumentFragment();
  diagnostics.warnings.forEach(group => {
    const section = document.createElement('section');
    section.className = `detail-section diagnostic-group severity-${group.severity}`;
    const title = group.label.replace(/^\d+\s+/, '');
    section.append(text('h3', `${group.severity}: ${title.charAt(0).toUpperCase() + title.slice(1)}`));
    const list = document.createElement('ul');
    (group.items || []).forEach(item => list.append(text('li', item)));
    if (group.truncated) list.append(text('li', 'Additional diagnostics omitted at the safety limit.'));
    section.append(list);
    fragment.append(section);
  });
  return fragment;
}

function beginInspection(trigger, identity, title) {
  if (detailController) detailController.abort();
  activeCard?.setAttribute('aria-expanded', 'false');
  const controller = new AbortController();
  detailController = controller;
  activeCard = trigger;
  trigger.setAttribute('aria-expanded', 'true');
  drawerIdentity.textContent = identity;
  drawerTitle.textContent = title;
  drawerContent.replaceChildren(text('p', 'Loading operational context...', 'empty'));
  backdrop.hidden = false;
  drawer.hidden = false;
  document.body.classList.add('drawer-open');
  pageRegions.forEach(region => { region.inert = true; });
  closeButton.focus();
  return controller;
}

async function openDetail(worker, card) {
  const controller = beginInspection(card, `${worker.project} / ${worker.repository} / ${worker.task} / ${worker.status || worker.health}`, worker.title || worker.task);
  try {
    const response = await fetch(`api/workers/${encodeURIComponent(worker.task)}/${encodeURIComponent(worker.detailKey || worker.repository)}`, { cache: 'no-store', signal: controller.signal });
    if (!response.ok) throw new Error('detail unavailable');
    const detail = await response.json();
    if (controller !== detailController) return;
    drawerContent.replaceChildren(detailView(detail));
  } catch (error) {
    if (controller === detailController && error.name !== 'AbortError') drawerContent.replaceChildren(text('p', 'Worker detail is temporarily unavailable.', 'empty'));
  }
}

async function openDiagnostics(button) {
  const controller = beginInspection(button, 'FLEET DIAGNOSTICS', 'System diagnostics');
  try {
    const response = await fetch('api/diagnostics', { cache: 'no-store', signal: controller.signal });
    if (!response.ok) throw new Error('diagnostics unavailable');
    const diagnostics = await response.json();
    if (controller !== detailController) return;
    drawerContent.replaceChildren(diagnosticsView(diagnostics));
  } catch (error) {
    if (controller === detailController && error.name !== 'AbortError') drawerContent.replaceChildren(text('p', 'Fleet diagnostics are temporarily unavailable.', 'empty'));
  }
}

function closeDetail() {
  if (drawer.hidden) return;
  if (detailController) detailController.abort();
  detailController = null;
  drawer.hidden = true;
  backdrop.hidden = true;
  drawerContent.replaceChildren();
  document.body.classList.remove('drawer-open');
  pageRegions.forEach(region => { region.inert = false; });
  const restore = activeCard;
  activeCard = null;
  restore?.setAttribute('aria-expanded', 'false');
  drawerIdentity.textContent = 'WORKER DETAIL';
  drawerTitle.textContent = 'Operational context';
  if (restore?.isConnected) restore.focus();
}

drawer.addEventListener('keydown', event => {
  if (event.key === 'Escape') {
    event.preventDefault();
    closeDetail();
    return;
  }
  if (event.key !== 'Tab') return;
  const focusable = [...drawer.querySelectorAll('button, a[href], [tabindex]:not([tabindex="-1"])')];
  if (!focusable.length) return;
  const first = focusable[0];
  const last = focusable.at(-1);
  if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
  if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
});
closeButton.addEventListener('click', closeDetail);
backdrop.addEventListener('click', closeDetail);

document.querySelectorAll('[data-filter]').forEach(button => button.addEventListener('click', () => {
  filter = button.dataset.filter;
  document.querySelectorAll('[data-filter]').forEach(item => {
    item.classList.toggle('selected', item === button);
    item.setAttribute('aria-pressed', String(item === button));
  });
  render();
}));
searchNode.addEventListener('input', () => { query = searchNode.value; render(); });
searchNode.addEventListener('keydown', event => {
  if (event.key === 'Escape') {
    searchNode.value = '';
    query = '';
    render();
  }
});

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
