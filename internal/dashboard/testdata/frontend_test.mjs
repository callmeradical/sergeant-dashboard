import assert from 'node:assert/strict';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { request } from 'node:http';

const browser = process.env.CHROME_BIN;
const fixtures = JSON.parse(await readFile(process.env.FRONTEND_FIXTURES_FILE, 'utf8'));

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
    await command('Accessibility.enable');
    await command('Emulation.setDeviceMetricsOverride', { width: Number(viewport.split(',')[0]), height: Number(viewport.split(',')[1]), deviceScaleFactor: 1, mobile: false });

    const prefix = `/${target}`;
    const html = Buffer.from(fixtures[`${prefix}/sergeant/`].body, 'base64').toString();
    const css = Buffer.from(fixtures[`${prefix}/sergeant/app.css`].body, 'base64').toString();
    const script = Buffer.from(fixtures[`${prefix}/sergeant/app.js`].body, 'base64').toString();
    const api = Buffer.from(fixtures[`${prefix}/sergeant/api/state`].body, 'base64').toString();
    const detailFixture = fixtures[`${prefix}/sergeant/api/workers/raw-private-task/dashboard`];
    const detail = detailFixture?.status === 200 ? Buffer.from(detailFixture.body, 'base64').toString() : '{}';
    const diagnosticsFixture = fixtures[`${prefix}/sergeant/api/diagnostics`];
    const diagnostics = diagnosticsFixture?.status === 200 ? Buffer.from(diagnosticsFixture.body, 'base64').toString() : '{}';
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
      ? `window.fetches=[]; window.fetch = url => { window.fetches.push(String(url)); return Promise.resolve({ ok: true, json: () => Promise.reject(new Error('malformed state')) }); }`
      : `window.fetches=[]; window.fetch = url => { window.fetches.push(String(url)); const path = String(url); const body = path.includes('api/workers/') ? ${detail} : path.includes('api/diagnostics') ? ${diagnostics} : ${api}; return Promise.resolve({ ok: true, json: () => Promise.resolve(body) }); }`;
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
    const accessibilityTree = async () => (await command('Accessibility.getFullAXTree')).nodes;
    return { evaluate, accessibilityTree, close: async () => { socket.close(); await cleanup(); } };
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
      links: [...document.querySelectorAll('#workers a')].map(link => link.href),
      fetches: window.fetches,
      cardHeights: [...document.querySelectorAll('.worker')].map(card => card.getBoundingClientRect().height),
      cardStyles: [...document.querySelectorAll('.worker')].map(card => ({
        radius: getComputedStyle(card).borderRadius,
        backgroundImage: getComputedStyle(card).backgroundImage,
        titleSize: getComputedStyle(card.querySelector('h3')).fontSize,
        titleClamp: getComputedStyle(card.querySelector('h3')).webkitLineClamp,
      })),
      palette: {
        canvas: getComputedStyle(document.documentElement).getPropertyValue('--canvas').trim(),
        panel: getComputedStyle(document.documentElement).getPropertyValue('--panel').trim(),
        raised: getComputedStyle(document.documentElement).getPropertyValue('--raised').trim(),
        border: getComputedStyle(document.documentElement).getPropertyValue('--border').trim(),
        text: getComputedStyle(document.documentElement).getPropertyValue('--text').trim(),
        muted: getComputedStyle(document.documentElement).getPropertyValue('--muted').trim(),
      },
      cardLabels: [...document.querySelectorAll('.worker')].map(card => card.getAttribute('aria-label')),
      cardMetadata: [...document.querySelectorAll('.worker')].map(card => [...card.querySelectorAll('.summary-meta dt')].map((term, index) => term.textContent + ' ' + card.querySelectorAll('.summary-meta dd')[index].textContent)),
      warningRole: document.querySelector('#warnings').getAttribute('role'),
      warningButton: document.querySelector('#warnings button')?.textContent || '',
      warningHeight: document.querySelector('#warnings').getBoundingClientRect().height,
      warningClass: document.querySelector('#warnings').className,
      searchBorder: getComputedStyle(document.querySelector('#search')).borderColor,
      toolbarDirection: getComputedStyle(document.querySelector('.toolbar')).flexDirection,
      viewportWidth: innerWidth,
      documentWidth: document.documentElement.scrollWidth,
    }))()`);
  } finally {
    await page.close();
  }
}

for (const viewport of ['1280,900', '390,844', '390,320']) {
  const rendered = await inspect('valid', viewport);
  assert.equal(rendered.cards, 2);
  assert.equal(rendered.total, '2');
  assert.equal(rendered.active, '1');
  assert.equal(rendered.attention, '1');
  assert.match(rendered.warnings, /1 other fleet diagnostic/);
  assert.match(rendered.warnings, /1 probe failure/);
  assert.doesNotMatch(rendered.warnings, /source delayed|token=/);
  for (const expected of ['raw-private-task', 'operator-suite', 'dashboard', 'Repair dashboard', 'in_progress', 'opencode']) assert.match(rendered.text, new RegExp(expected));
  for (const forbidden of ['secret-branch', '/secret/worktree', 'approval needed', 'worker recovered', 'tests passed', 'remaining: open PR', 'private check name', 'review passed', 'Collector connects fleet state', 'token=secret']) assert.doesNotMatch(rendered.text, new RegExp(forbidden));
  assert.deepEqual(rendered.links, []);
  assert.deepEqual(rendered.fetches, ['api/state']);
  assert.ok(rendered.cardHeights.every(height => height === 208), `cards are not fixed at 208px at ${viewport}: ${rendered.cardHeights}`);
  assert.ok(rendered.cardStyles.every(style => style.radius === '6px' && style.backgroundImage === 'none' && style.titleSize === '16px' && style.titleClamp === '2'), `card anatomy mismatch at ${viewport}: ${JSON.stringify(rendered.cardStyles)}`);
  assert.deepEqual(rendered.palette, { canvas: '#0B0E11', panel: '#11161B', raised: '#171D23', border: '#29323A', text: '#E7ECEF', muted: '#8C98A3' });
  assert.match(rendered.cardLabels[0], /operator-suite.*dashboard.*raw-private-task.*Repair dashboard.*in_progress/i);
  rendered.cardMetadata[0].forEach(value => assert.match(rendered.cardLabels[0], new RegExp(value, 'i')));
  assert.equal(rendered.warningRole, 'status');
  assert.equal(rendered.warningButton, 'View details');
  assert.match(rendered.warnings, /error/i);
  assert.match(rendered.warningClass, /severity-error/);
  assert.ok(rendered.warningHeight <= 72, `warning summary exceeded two lines: ${rendered.warningHeight}`);
  assert.equal(rendered.searchBorder, 'rgb(92, 103, 113)');
  assert.ok(rendered.documentWidth <= rendered.viewportWidth, `horizontal overflow at ${viewport}`);
  assert.equal(rendered.toolbarDirection, viewport.startsWith('390') ? 'column' : 'row');
}

const filteredPage = await openPage('valid', '1280,900');
try {
  const filtered = await filteredPage.evaluate(`(() => {
    document.querySelector('[data-filter="attention"]').click();
    return { cards: document.querySelectorAll('.worker').length, text: document.querySelector('#workers').textContent };
  })()`);
  assert.equal(filtered.cards, 1);
  assert.match(filtered.text, /stale/);
  assert.doesNotMatch(filtered.text, /in_progress/);
} finally {
  await filteredPage.close();
}

const searchPage = await openPage('valid', '1280,900');
try {
  const searched = await searchPage.evaluate(`(() => {
    const input = document.querySelector('#search');
    input.value = 'operator dashboard';
    input.dispatchEvent(new Event('input', { bubbles: true }));
    return { cards: document.querySelectorAll('.worker').length, text: document.querySelector('#workers').textContent, fetches: window.fetches };
  })()`);
  assert.equal(searched.cards, 1);
  assert.match(searched.text, /Repair dashboard/);
  assert.deepEqual(searched.fetches, ['api/state']);
  const hiddenDetailSearch = await searchPage.evaluate(`(() => { const input = document.querySelector('#search'); input.value = 'secret-branch'; input.dispatchEvent(new Event('input', { bubbles: true })); return document.querySelectorAll('.worker').length; })()`);
  assert.equal(hiddenDetailSearch, 0, 'drawer-only detail was indexed by summary search');
  const prSearch = await searchPage.evaluate(`(() => { const input = document.querySelector('#search'); input.value = 'open'; input.dispatchEvent(new Event('input', { bubbles: true })); return { cards: document.querySelectorAll('.worker').length, fetches: window.fetches }; })()`);
  assert.equal(prSearch.cards, 1);
  assert.deepEqual(prSearch.fetches, ['api/state']);
  const cleared = await searchPage.evaluate(`(() => { document.querySelector('#search').dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true })); return document.querySelector('#search').value; })()`);
  assert.equal(cleared, '');
} finally {
  await searchPage.close();
}

for (const viewport of ['1280,900', '390,844', '390,320']) {
  const drawerPage = await openPage('valid', viewport);
  try {
    const opened = await drawerPage.evaluate(`(async () => {
      const card = document.querySelector('.worker');
      card.focus();
      card.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }));
      for (let attempt = 0; attempt < 50 && document.querySelector('#drawer-content').textContent.includes('Loading'); attempt++) await new Promise(resolve => setTimeout(resolve, 10));
      return {
        hidden: document.querySelector('#drawer').hidden,
        text: document.querySelector('#drawer').textContent,
        sections: [...document.querySelectorAll('#drawer-content section h3')].map(node => node.textContent),
        fetches: window.fetches,
        focusInside: document.querySelector('#drawer').contains(document.activeElement),
        width: document.querySelector('#drawer').getBoundingClientRect().width,
        mainInert: document.querySelector('main').inert,
        expanded: card.getAttribute('aria-expanded'),
        selected: card.getAttribute('aria-selected'),
        dialogName: document.querySelector('#drawer-title').textContent,
        dialogIdentity: document.querySelector('#drawer-identity').textContent,
        headerPosition: getComputedStyle(document.querySelector('.drawer-head')).position,
        liveRegion: document.querySelector('#drawer-content').getAttribute('aria-live'),
      };
    })()`);
    assert.equal(opened.hidden, false);
    for (const expected of ['secret-branch', '/secret/worktree', 'approval needed', 'worker recovered', 'tests passed', 'remaining: open PR', 'private check name', 'review passed', 'Collector connects fleet state', 'legacy']) assert.match(opened.text, new RegExp(expected, 'i'));
    assert.deepEqual(opened.fetches, ['api/state', 'api/workers/raw-private-task/dashboard']);
    assert.equal(opened.focusInside, true);
    assert.deepEqual(opened.sections, ['Overview', 'Attention', 'Timeline', 'Delivery', 'Files']);
    assert.equal(opened.mainInert, true);
    assert.equal(opened.expanded, 'true');
    assert.equal(opened.selected, null);
    assert.match(opened.dialogName, /Repair dashboard/);
    assert.match(opened.dialogIdentity, /operator-suite.*dashboard.*raw-private-task.*in_progress/);
    assert.equal(opened.headerPosition, 'sticky');
    assert.equal(opened.liveRegion, 'polite');
    const accessibilityTree = await drawerPage.accessibilityTree();
    const dialogNode = accessibilityTree.find(node => node.role?.value === 'dialog');
    assert.match(dialogNode?.name?.value || '', /operator-suite.*dashboard.*raw-private-task.*in_progress.*Repair dashboard/i);
    if (viewport.startsWith('390')) assert.ok(opened.width >= 389, `mobile sheet width = ${opened.width}`);
    else assert.equal(opened.width, 560);
    if (viewport === '390,320') {
      const reachable = await drawerPage.evaluate(`(() => {
        const drawer = document.querySelector('#drawer');
        drawer.scrollTop = drawer.scrollHeight;
        const footer = document.querySelector('.drawer-footer').getBoundingClientRect();
        const close = document.querySelector('#drawer-close').getBoundingClientRect();
        return { footerVisible: footer.top < innerHeight && footer.bottom > 0, closeVisible: close.top >= 0 && close.bottom <= innerHeight };
      })()`);
      assert.deepEqual(reachable, { footerVisible: true, closeVisible: true });
    }
    const closed = await drawerPage.evaluate(`(() => { document.querySelector('#drawer').dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true })); return { hidden: document.querySelector('#drawer').hidden, focusedCard: document.activeElement.classList.contains('worker'), detailText: document.querySelector('#drawer-content').textContent }; })()`);
    assert.equal(closed.hidden, true);
    assert.equal(closed.focusedCard, true);
    assert.equal(closed.detailText, '');
  } finally {
    await drawerPage.close();
  }
}

const textZoomPage = await openPage('valid', '390,844');
try {
  const zoomed = await textZoomPage.evaluate(`(async () => {
    document.querySelector('.worker').click();
    for (let attempt = 0; attempt < 50 && document.querySelector('#drawer-content').textContent.includes('Loading'); attempt++) await new Promise(resolve => setTimeout(resolve, 10));
    const nodes = [...document.querySelectorAll('body *')].filter(node => [...node.childNodes].some(child => child.nodeType === Node.TEXT_NODE && child.textContent.trim()));
    const sizes = nodes.map(node => [node, parseFloat(getComputedStyle(node).fontSize)]);
    sizes.forEach(([node, size]) => { node.style.fontSize = String(size * 2) + 'px'; });
    return {
      zoomedNodes: nodes.length,
      documentWidth: document.documentElement.scrollWidth,
      viewportWidth: innerWidth,
      cards: [...document.querySelectorAll('.worker')].map(card => ({ client: card.clientHeight, scroll: card.scrollHeight })),
      drawer: { client: document.querySelector('#drawer').clientHeight, scroll: document.querySelector('#drawer').scrollHeight },
    };
  })()`);
  assert.ok(zoomed.zoomedNodes > 30, `full-page text resize covered only ${zoomed.zoomedNodes} nodes`);
  assert.ok(zoomed.documentWidth <= zoomed.viewportWidth, `200% text caused horizontal overflow: ${JSON.stringify(zoomed)}`);
  assert.ok(zoomed.cards.every(card => card.scroll <= card.client && card.client > 208), `200% text clipped card content: ${JSON.stringify(zoomed.cards)}`);
  assert.ok(zoomed.drawer.scroll >= zoomed.drawer.client, `drawer did not retain a single scroll region: ${JSON.stringify(zoomed.drawer)}`);
} finally {
  await textZoomPage.close();
}

const reflowPage = await openPage('valid', '195,422');
try {
  const reflow = await reflowPage.evaluate(`(async () => {
    document.querySelector('#total').textContent = '1234';
    document.querySelector('#active').textContent = '987';
    document.querySelector('#attention').textContent = '4321';
    document.querySelector('.worker').click();
    for (let attempt = 0; attempt < 50 && document.querySelector('#drawer-content').textContent.includes('Loading'); attempt++) await new Promise(resolve => setTimeout(resolve, 10));
    return {
      documentWidth: document.documentElement.scrollWidth,
      viewportWidth: innerWidth,
      clipped: [...document.querySelectorAll('.worker-path,.worker h3,.worker .task,.summary-meta dd,.attention-note')].filter(node => node.scrollWidth > node.clientWidth || node.scrollHeight > node.clientHeight).map(node => node.textContent),
      metricBoxes: [...document.querySelectorAll('.overview strong')].map(node => ({ text: node.textContent, clientWidth: node.clientWidth, scrollWidth: node.scrollWidth, clientHeight: node.clientHeight, scrollHeight: node.scrollHeight })),
      drawerHeaderWidth: document.querySelector('.drawer-head').scrollWidth,
      drawerWidth: document.querySelector('#drawer').clientWidth,
      drawerFooterWidth: document.querySelector('.drawer-footer').scrollWidth,
    };
  })()`);
  assert.ok(reflow.documentWidth <= reflow.viewportWidth, `200% reflow overflow: ${JSON.stringify(reflow)}`);
  assert.ok(reflow.drawerHeaderWidth <= reflow.drawerWidth, `drawer header overflow: ${JSON.stringify(reflow)}`);
  assert.ok(reflow.drawerFooterWidth <= reflow.drawerWidth, `drawer footer overflow: ${JSON.stringify(reflow)}`);
  assert.deepEqual(reflow.clipped, []);
  assert.ok(reflow.metricBoxes.every(metric => metric.scrollWidth <= metric.clientWidth && metric.scrollHeight <= metric.clientHeight), `metrics clipped: ${JSON.stringify(reflow.metricBoxes)}`);
} finally {
  await reflowPage.close();
}

const interactionPage = await openPage('valid', '1280,900');
try {
  const interactions = await interactionPage.evaluate(`(async () => {
    const waitForDetail = async () => {
      for (let attempt = 0; attempt < 50 && document.querySelector('#drawer-content').textContent.includes('Loading'); attempt++) await new Promise(resolve => setTimeout(resolve, 10));
    };
    const card = document.querySelector('.worker');
    card.dispatchEvent(new KeyboardEvent('keydown', { key: ' ', bubbles: true }));
    await waitForDetail();
    document.querySelector('#drawer-close').click();
    const spaceClosed = document.querySelector('#drawer').hidden && document.activeElement === card;
    card.click();
    await waitForDetail();
    document.querySelector('#drawer-backdrop').click();
    const backdropClosed = document.querySelector('#drawer').hidden && document.activeElement === card;
    card.click();
    await waitForDetail();
    const close = document.querySelector('#drawer-close');
    close.focus();
    close.dispatchEvent(new KeyboardEvent('keydown', { key: 'Tab', shiftKey: true, bubbles: true, cancelable: true }));
    const shiftTabWrapped = document.activeElement.tagName === 'A';
    document.activeElement.dispatchEvent(new KeyboardEvent('keydown', { key: 'Tab', bubbles: true, cancelable: true }));
    const tabWrapped = document.activeElement === close;
    return { spaceClosed, backdropClosed, shiftTabWrapped, tabWrapped };
  })()`);
  assert.deepEqual(interactions, { spaceClosed: true, backdropClosed: true, shiftTabWrapped: true, tabWrapped: true });
} finally {
  await interactionPage.close();
}

const diagnosticsPage = await openPage('valid', '390,844');
try {
  const diagnostics = await diagnosticsPage.evaluate(`(async () => {
    document.querySelector('#warnings button').click();
    for (let attempt = 0; attempt < 50 && document.querySelector('#drawer-content').textContent.includes('Loading'); attempt++) await new Promise(resolve => setTimeout(resolve, 10));
    const opened = {
      title: document.querySelector('#drawer-title').textContent,
      text: document.querySelector('#drawer-content').textContent,
      fetches: window.fetches,
      focusInside: document.querySelector('#drawer').contains(document.activeElement),
      closeName: document.querySelector('#drawer-close').getAttribute('aria-label'),
    };
    return opened;
  })()`);
  assert.match(diagnostics.title, /System diagnostics/);
  assert.match(diagnostics.text, /Other fleet diagnostics.*source delayed.*token=\[REDACTED\]/is);
  assert.deepEqual(diagnostics.fetches, ['api/state', 'api/diagnostics']);
  assert.equal(diagnostics.focusInside, true);
  assert.equal(diagnostics.closeName, 'Close dialog');
  const accessibilityTree = await diagnosticsPage.accessibilityTree();
  const dialogNode = accessibilityTree.find(node => node.role?.value === 'dialog');
  assert.match(dialogNode?.name?.value || '', /FLEET DIAGNOSTICS.*System diagnostics/i);
  assert.match(diagnostics.text, /error/i);
  const released = await diagnosticsPage.evaluate(`(() => { document.querySelector('#drawer-close').click(); return document.querySelector('#drawer-content').textContent === ''; })()`);
  assert.equal(released, true);
} finally {
  await diagnosticsPage.close();
}

const staleRequestPage = await openPage('valid', '1280,900');
try {
  const staleRequest = await staleRequestPage.evaluate(`(async () => {
    let resolveFirst;
    let firstSignal;
    let secondSignal;
    const firstDetail = new Promise(resolve => { resolveFirst = resolve; });
    window.fetch = (url, options = {}) => {
      if (String(url).includes('raw-private-task')) { firstSignal = options.signal; return firstDetail; }
      secondSignal = options.signal;
      return Promise.resolve({ ok: true, json: () => Promise.resolve({ task: 'stale-task', project: 'operator-suite', repository: 'worker', title: 'Investigate queue', status: 'blocked', health: 'stale', message: {}, diagnostic: {}, log: {}, handoff: {}, td: {}, graphify: {}, pullRequest: { checks: [], comments: [] }, noMistakes: {}, ocInject: {} }) });
    };
    const cards = document.querySelectorAll('.worker');
    cards[0].click();
    cards[1].click();
    for (let attempt = 0; attempt < 50 && document.querySelector('#drawer-content').textContent.includes('Loading'); attempt++) await new Promise(resolve => setTimeout(resolve, 10));
    resolveFirst({ ok: true, json: () => Promise.resolve({ task: 'raw-private-task', title: 'Wrong stale detail', message: {}, diagnostic: {}, log: {}, handoff: {}, td: {}, graphify: {}, pullRequest: { checks: [], comments: [] }, noMistakes: {}, ocInject: {} }) });
    await new Promise(resolve => setTimeout(resolve, 10));
    const result = { title: document.querySelector('#drawer-title').textContent, text: document.querySelector('#drawer-content').textContent, firstExpanded: cards[0].getAttribute('aria-expanded'), secondExpanded: cards[1].getAttribute('aria-expanded'), firstAborted: firstSignal.aborted };
    document.querySelector('#drawer-close').click();
    result.secondAborted = secondSignal.aborted;
    return result;
  })()`);
  assert.match(staleRequest.title, /Investigate queue/);
  assert.doesNotMatch(staleRequest.text, /Wrong stale detail/);
  assert.equal(staleRequest.firstExpanded, 'false');
  assert.equal(staleRequest.secondExpanded, 'true');
  assert.equal(staleRequest.firstAborted, true);
  assert.equal(staleRequest.secondAborted, true);
} finally {
  await staleRequestPage.close();
}

assert.match((await inspect('empty', '390,844')).text, /No workers match this view/);
assert.match((await inspect('malformed', '390,844')).text, /Fleet state is temporarily unavailable/);
