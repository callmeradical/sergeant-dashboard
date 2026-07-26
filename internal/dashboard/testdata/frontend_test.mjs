import assert from 'node:assert/strict';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { request } from 'node:http';

const browser = process.env.CHROME_BIN;
const fixtures = JSON.parse(Buffer.from(process.env.FRONTEND_FIXTURES, 'base64'));

const delay = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));

async function freePort() {
  const server = createServer();
  await new Promise((resolve, reject) => server.listen(0, '127.0.0.1', error => error ? reject(error) : resolve()));
  const port = server.address().port;
  await new Promise(resolve => server.close(resolve));
  return port;
}

function requestJSON(port, path, method = 'GET') {
  return new Promise((resolve, reject) => {
    const operation = request({ hostname: '127.0.0.1', port, path, method, timeout: 200 }, response => {
      let body = '';
      response.setEncoding('utf8');
      response.on('data', chunk => { body += chunk; });
      response.on('end', () => resolve(JSON.parse(body)));
    });
    operation.on('timeout', () => operation.destroy(new Error('request timed out')));
    operation.on('error', reject);
    operation.end();
  });
}

async function openPage(target, viewport) {
  const profile = await mkdtemp(join(tmpdir(), 'sergeant-dashboard-chrome-'));
  const port = await freePort();
  const chrome = spawn(browser, [
    '--headless=new', '--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage', '--remote-allow-origins=*', `--remote-debugging-port=${port}`,
    `--user-data-dir=${profile}`, `--window-size=${viewport}`, 'about:blank',
  ], { stdio: 'ignore', detached: true });
  const cleanup = async () => {
    try { process.kill(-chrome.pid, 'SIGKILL'); } catch {}
    await Promise.race([new Promise(resolve => chrome.once('exit', resolve)), delay(500)]);
    for (let attempt = 0; attempt < 10; attempt++) {
      try {
        await rm(profile, { recursive: true, force: true });
        break;
      } catch (error) {
        if (attempt === 9) throw error;
        await delay(50);
      }
    }
  };
  try {
    for (let attempt = 0; attempt < 100; attempt++) {
      try {
        await requestJSON(port, '/json/version');
        break;
      } catch {}
      await delay(50);
      if (attempt === 99) throw new Error('timed out waiting for Chrome debugging endpoint');
    }
    const targetInfo = await requestJSON(port, `/json/new?${encodeURIComponent(target)}`, 'PUT');
    const socket = new WebSocket(targetInfo.webSocketDebuggerUrl);
    await new Promise((resolve, reject) => {
      socket.addEventListener('open', resolve, { once: true });
      socket.addEventListener('error', reject, { once: true });
    });
    socket.binaryType = 'arraybuffer';
    let id = 0;
    const pending = new Map();
    socket.addEventListener('message', event => {
	  const message = JSON.parse(typeof event.data === 'string' ? event.data : Buffer.from(event.data).toString());
      if (!message.id) return;
      const waiter = pending.get(message.id);
      pending.delete(message.id);
      if (!waiter) return;
      if (message.error) waiter.reject(new Error(message.error.message));
      else waiter.resolve(message.result);
    });
    const command = (method, params = {}) => new Promise((resolve, reject) => {
      const commandID = ++id;
      pending.set(commandID, { resolve, reject });
      socket.send(JSON.stringify({ id: commandID, method, params }));
    });
    await command('Page.enable');
    await command('Runtime.enable');
    await command('Emulation.setDeviceMetricsOverride', { width: Number(viewport.split(',')[0]), height: Number(viewport.split(',')[1]), deviceScaleFactor: 1, mobile: false });

    const prefix = `/${target}`;
    const html = Buffer.from(fixtures[`${prefix}/sergeant/`].body, 'base64').toString();
    const css = Buffer.from(fixtures[`${prefix}/sergeant/app.css`].body, 'base64').toString();
    const script = Buffer.from(fixtures[`${prefix}/sergeant/app.js`].body, 'base64').toString();
    const api = Buffer.from(fixtures[`${prefix}/sergeant/api/state`].body, 'base64').toString();
    const policy = fixtures[`${prefix}/sergeant/`].headers['Content-Security-Policy'][0];
    const securedHTML = html
      .replace('<head>', `<head><meta http-equiv="Content-Security-Policy" content="${policy.replaceAll('"', '&quot;')}"><script>window.inlineCSPProbe=true<\/script>`);
    const fixtureDirectory = join(profile, 'fixtures', target);
    await mkdir(fixtureDirectory, { recursive: true });
    await Promise.all([
      writeFile(join(fixtureDirectory, 'index.html'), securedHTML),
      writeFile(join(fixtureDirectory, 'app.css'), css),
      writeFile(join(fixtureDirectory, 'app.js'), script),
    ]);
    const fetchSetup = target === 'malformed'
      ? `window.fetch = () => Promise.resolve({ ok: true, json: () => Promise.reject(new Error('malformed state')) })`
      : `window.fetch = () => Promise.resolve({ ok: true, json: () => Promise.resolve(${api}) })`;
    await command('Page.addScriptToEvaluateOnNewDocument', { source: fetchSetup });
    await command('Page.navigate', { url: pathToFileURL(join(fixtureDirectory, 'index.html')).href });
    for (let attempt = 0; attempt < 100; attempt++) {
      const result = await command('Runtime.evaluate', { expression: `document.readyState === 'complete' && !document.querySelector('#workers')?.textContent.includes('Loading fleet state')`, returnByValue: true });
      if (result.result.value) break;
      await delay(50);
      if (attempt === 99) throw new Error(`page did not render: ${target}`);
    }
    const cspResult = await command('Runtime.evaluate', { expression: 'window.inlineCSPProbe === undefined', returnByValue: true });
    assert.equal(cspResult.result.value, true, 'handler CSP allowed inline script execution');
    const evaluate = async expression => {
      const result = await command('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true });
      if (result.exceptionDetails) throw new Error(result.exceptionDetails.text);
      return result.result.value;
    };
    return { evaluate, close: async () => { socket.close(); await cleanup(); } };
  } catch (error) {
    await cleanup();
    throw error;
  }
}

async function inspect(target, viewport) {
  const page = await openPage(target, viewport);
  try {
    return await page.evaluate(`(() => ({
      text: document.querySelector('#workers').textContent,
      cards: document.querySelectorAll('.worker').length,
      total: document.querySelector('#total').textContent,
      active: document.querySelector('#active').textContent,
      attention: document.querySelector('#attention').textContent,
      warnings: document.querySelector('#warnings').textContent,
      toolbarDirection: getComputedStyle(document.querySelector('.toolbar')).flexDirection,
      viewportWidth: innerWidth,
      documentWidth: document.documentElement.scrollWidth,
    }))()`);
  } finally {
    await page.close();
  }
}

for (const viewport of ['1280,900', '390,844']) {
  const rendered = await inspect('valid', viewport);
  assert.equal(rendered.cards, 2);
  assert.equal(rendered.total, '2');
  assert.equal(rendered.active, '1');
  assert.equal(rendered.attention, '1');
  assert.equal(rendered.warnings, 'source delayed token=[REDACTED]');
  // Items visible in worker cards without opening the drawer.
  for (const expected of ['raw-private-task', 'raw-private-project', 'secret-branch', 'message', 'graphify', 'no-mistakes', 'oc-inject']) assert.match(rendered.text, new RegExp(expected));
  for (const forbidden of ['token=secret']) assert.doesNotMatch(rendered.text, new RegExp(forbidden));
  assert.ok(rendered.documentWidth <= rendered.viewportWidth, `horizontal overflow at ${viewport}`);
  assert.equal(rendered.toolbarDirection, viewport.startsWith('390') ? 'column' : 'row');
}

// Verify drawer content by clicking worker cards.
const drawerPage = await openPage('valid', '1280,900');
try {
  // Click first card (active worker) and verify drawer detail.
  await drawerPage.evaluate(`document.querySelectorAll('.worker')[0].click()`);
  await delay(100);
  const firstDrawer = await drawerPage.evaluate(`document.querySelector('#drawer-body').textContent`);
  for (const expected of ['/secret/worktree', 'td-123', 'approval needed', 'worker recovered', 'tests passed', 'remaining: open PR', 'private check name: SUCCESS', 'available truncated', 'review passed', 'Collector connects fleet state', 'response pending']) {
    assert.match(firstDrawer, new RegExp(expected), `first drawer missing: ${expected}`);
  }
  // Verify PR and comment links are rendered in the drawer.
  const drawerLinks = await drawerPage.evaluate(`[...document.querySelectorAll('#drawer-body a')].map(a => a.href)`);
  assert.deepEqual(drawerLinks, ['https://github.com/acme/widget/pull/7', 'https://github.com/acme/widget/pull/7#issuecomment-1']);
  // Click second card (orphaned worker) and verify its drawer detail.
  await drawerPage.evaluate(`document.querySelectorAll('.worker')[1].click()`);
  await delay(100);
  const secondDrawer = await drawerPage.evaluate(`document.querySelector('#drawer-body').textContent`);
  for (const expected of ['unavailable', 'missing']) {
    assert.match(secondDrawer, new RegExp(expected), `second drawer missing: ${expected}`);
  }
} finally {
  await drawerPage.close();
}

const filteredPage = await openPage('valid', '1280,900');
try {
  const filtered = await filteredPage.evaluate(`(() => {
    document.querySelector('[data-filter="attention"]').click();
    return { cards: document.querySelectorAll('.worker').length, text: document.querySelector('#workers').textContent };
  })()`);
  assert.equal(filtered.cards, 1);
  assert.match(filtered.text, /orphaned/);
  assert.doesNotMatch(filtered.text, /in_progress/);
} finally {
  await filteredPage.close();
}

assert.match((await inspect('empty', '390,844')).text, /No workers match this view/);
assert.match((await inspect('malformed', '390,844')).text, /Fleet state is temporarily unavailable/);
