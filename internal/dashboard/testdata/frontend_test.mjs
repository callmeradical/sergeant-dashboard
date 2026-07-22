import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';

const application = await readFile(new URL('../web/app.js', import.meta.url), 'utf8');
const stylesheet = await readFile(new URL('../web/app.css', import.meta.url), 'utf8');
const documentSource = await readFile(new URL('../web/index.html', import.meta.url), 'utf8');

class Element {
  constructor(tag, id = '') {
    this.tagName = tag.toUpperCase();
    this.id = id;
    this.children = [];
    this.className = '';
    this.dataset = {};
    this.hidden = false;
    this.href = '';
    this.rel = '';
    this.listeners = {};
    this.style = { setProperty: (name, value) => { this.style[name] = value; } };
    this.classList = {
      toggle: (name, enabled) => {
        const names = new Set(this.className.split(/\s+/).filter(Boolean));
        enabled ? names.add(name) : names.delete(name);
        this.className = [...names].join(' ');
      },
    };
  }

  set textContent(value) {
    this.text = String(value);
    this.children = [];
  }

  get textContent() {
    return [this.text || '', ...this.children.map(child => child.textContent)].join('');
  }

  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children = [...children]; }
  addEventListener(name, listener) { this.listeners[name] = listener; }
}

const descendants = node => [node, ...node.children.flatMap(descendants)];

async function run(fetchResult) {
  const ids = Object.fromEntries(['workers', 'total', 'active', 'attention', 'updated', 'warnings'].map(id => [id, new Element('div', id)]));
  const filters = ['all', 'active', 'attention'].map(value => {
    const button = new Element('button');
    button.dataset.filter = value;
    if (value === 'all') button.className = 'selected';
    return button;
  });
  const document = {
    createElement: tag => new Element(tag),
    querySelector: selector => ids[selector.slice(1)],
    querySelectorAll: selector => selector === '[data-filter]' ? filters : [],
  };
  const context = vm.createContext({
    document,
    fetch: () => Promise.resolve({ ok: true, json: async () => fetchResult }),
    console,
    Date,
    Promise,
    setTimeout,
  });
  if (fetchResult instanceof Error) {
    context.fetch = () => Promise.resolve({ ok: true, json: async () => { throw fetchResult; } });
  }
  vm.runInContext(application, context, { filename: 'app.js' });
  await new Promise(resolve => setTimeout(resolve, 0));
  return { ids, filters };
}

const state = {
  collectedAt: '2026-07-21T12:00:00Z',
  warnings: ['source warning'],
  workers: [
    {
      task: 'task-safe', project: 'project-safe', status: 'in_progress', health: 'active', agent: 'opencode', tdTask: 'td-123',
      branch: 'secret-branch', worktree: '/secret/worktree', message: { present: true, summary: 'secret message body' },
      pullRequest: { url: 'https://github.com/acme/widget/pull/7', state: 'OPEN', checks: [{ name: 'check 1', conclusion: 'SUCCESS' }] },
      noMistakes: { available: true, phase: 'review' }, graphify: { present: true, status: 'ready' }, ocInject: { responsePending: true },
    },
    { task: 'task-stale', project: 'project-stale', status: 'blocked', health: 'stale', tdTask: 'not-a-task', message: {}, pullRequest: { checks: [] } },
  ],
};

const rendered = await run(state);
assert.equal(rendered.ids.total.textContent, '2');
assert.equal(rendered.ids.active.textContent, '1');
assert.equal(rendered.ids.attention.textContent, '1');
assert.equal(rendered.ids.warnings.textContent, '1 source warning');
let nodes = descendants(rendered.ids.workers);
assert.equal(nodes.filter(node => node.tagName === 'ARTICLE').length, 2);
const text = rendered.ids.workers.textContent;
for (const expected of ['in_progress', 'opencode', 'check 1: SUCCESS', 'review phase', 'ready', 'message', 'graphify', 'no-mistakes', 'oc-inject']) assert.match(text, new RegExp(expected));
for (const forbidden of ['secret-branch', '/secret/worktree', 'secret message body']) assert.doesNotMatch(text, new RegExp(forbidden));
const links = nodes.filter(node => node.tagName === 'A').map(node => node.href);
assert.deepEqual(links, ['http://127.0.0.1:8991/api/issues/td-123', 'https://github.com/acme/widget/pull/7']);

rendered.filters.find(button => button.dataset.filter === 'attention').listeners.click();
nodes = descendants(rendered.ids.workers);
assert.equal(nodes.filter(node => node.tagName === 'ARTICLE').length, 1);
assert.match(rendered.ids.workers.textContent, /project-stale/);
assert.doesNotMatch(rendered.ids.workers.textContent, /project-safe/);

const empty = await run({ collectedAt: '2026-07-21T12:00:00Z', workers: [], warnings: [] });
assert.match(empty.ids.workers.textContent, /No workers match this view/);
const malformed = await run(new Error('malformed state'));
assert.match(malformed.ids.workers.textContent, /Fleet state is temporarily unavailable/);

assert.match(documentSource, /name="viewport" content="width=device-width, initial-scale=1"/);
assert.match(documentSource, /<script src="app\.js"><\/script>/);
assert.doesNotMatch(documentSource, /<script(?![^>]*\bsrc=)[^>]*>/);
assert.match(stylesheet, /@media \(max-width:700px\)/);
assert.match(stylesheet, /\.toolbar \{[^}]*flex-direction:column/);
