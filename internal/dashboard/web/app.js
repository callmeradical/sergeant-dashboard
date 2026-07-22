const workersNode = document.querySelector('#workers');
let state = { workers: [], warnings: [] };
let filter = 'all';

const text = (tag, value, className) => {
  const node = document.createElement(tag);
  node.textContent = value || '-';
  if (className) node.className = className;
  return node;
};

const tdTaskLink = value => {
  if (!/^td-[A-Za-z0-9]+$/.test(value || '')) return text('dd', value);
  const node = document.createElement('dd');
  const link = document.createElement('a');
  link.href = `http://127.0.0.1:8991/api/issues/${encodeURIComponent(value)}`;
  link.rel = 'noreferrer';
  link.textContent = value;
  node.append(link);
  return node;
};

function render() {
  const attention = state.workers.filter(worker => ['stale', 'orphaned'].includes(worker.health));
  document.querySelector('#total').textContent = state.workers.length;
  document.querySelector('#active').textContent = state.workers.filter(worker => worker.health === 'active').length;
  document.querySelector('#attention').textContent = attention.length;
  document.querySelector('#updated').textContent = `Updated ${new Date(state.collectedAt).toLocaleString()}`;
  const warnings = document.querySelector('#warnings');
  warnings.hidden = !state.warnings.length;
  warnings.textContent = state.warnings.length ? `${state.warnings.length} source warning${state.warnings.length === 1 ? '' : 's'}` : '';

  workersNode.replaceChildren();
  const shown = state.workers.filter(worker => filter === 'all' || (filter === 'attention' ? ['stale', 'orphaned'].includes(worker.health) : worker.health === filter));
  if (!shown.length) workersNode.append(text('p', 'No workers match this view.', 'empty'));
  shown.forEach(worker => workersNode.append(workerCard(worker)));
}

function workerCard(worker) {
  const card = document.createElement('article');
  card.className = 'worker';
  const colors = { active: 'var(--acid)', stale: 'var(--amber)', orphaned: 'var(--red)', complete: 'var(--muted)' };
  card.style.setProperty('--status', colors[worker.health] || 'var(--muted)');
  const head = document.createElement('header');
  const title = document.createElement('div');
  title.append(text('h3', worker.project), text('div', worker.task, 'task'));
  head.append(title, text('span', worker.health, 'badge'));
  card.append(head);
  const meta = document.createElement('dl');
  meta.className = 'meta';
  [['Status', worker.status], ['Agent', worker.agent]].forEach(([label, value]) => {
    meta.append(text('dt', label), text('dd', value));
  });
  meta.append(text('dt', 'td'), tdTaskLink(worker.tdTask));
  if (worker.pullRequest?.url) {
    meta.append(text('dt', 'Pull request'));
    const value = document.createElement('dd');
    const link = document.createElement('a');
    link.href = worker.pullRequest.url;
    link.rel = 'noreferrer';
    link.textContent = worker.pullRequest.state || 'View PR';
    value.append(link); meta.append(value);
  }
  if (worker.pullRequest?.checks?.length) {
    const checks = worker.pullRequest.checks.map(check => `${check.name || 'check'}: ${check.conclusion || check.state || check.status || 'pending'}`);
    meta.append(text('dt', 'Checks'), text('dd', checks.join(', ')));
  }
  if (worker.message?.present) {
    meta.append(text('dt', 'Message'), text('dd', worker.message.updatedAt ? `updated ${new Date(worker.message.updatedAt).toLocaleString()}` : 'present'));
  }
  if (worker.noMistakes?.available) {
    meta.append(text('dt', 'no-mistakes'), text('dd', worker.noMistakes?.phase ? `${worker.noMistakes.phase} phase` : 'run detected'));
  }
  if (worker.graphify?.present) {
    const updated = worker.graphify?.updatedAt ? `, ${new Date(worker.graphify.updatedAt).toLocaleString()}` : '';
    meta.append(text('dt', 'Graphify'), text('dd', `${worker.graphify?.status || 'available'}${updated}`));
  }
  card.append(meta);
  const signals = document.createElement('div');
  signals.className = 'signals';
  [['message', worker.message?.present], ['graphify', worker.graphify?.present], ['no-mistakes', worker.noMistakes?.available], ['oc-inject', worker.ocInject?.responsePending || worker.ocInject?.responseAcked]].forEach(([label, on]) => {
    signals.append(text('span', label, `signal ${on ? 'on' : ''}`));
  });
  card.append(signals);
  return card;
}

document.querySelectorAll('[data-filter]').forEach(button => button.addEventListener('click', () => {
  filter = button.dataset.filter;
  document.querySelectorAll('[data-filter]').forEach(item => item.classList.toggle('selected', item === button));
  render();
}));

fetch('api/state', { cache: 'no-store' })
  .then(response => { if (!response.ok) throw new Error('state unavailable'); return response.json(); })
  .then(data => { state = data; render(); })
  .catch(() => { workersNode.replaceChildren(text('p', 'Fleet state is temporarily unavailable.', 'empty')); });
