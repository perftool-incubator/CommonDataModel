'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const http = require('node:http');
const { once } = require('node:events');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const path = require('node:path');

const execFileAsync = promisify(execFile);
const cli = path.resolve(__dirname, '../get-metric-data.js');

function fixture() {
  const points = (first, second) => [
    { begin: 0, end: 1000, value: first },
    { begin: 1001, end: 2000, value: second }
  ];
  return {
    values: { '<worker-10>': points(20, 100), '<worker-2>': points(50, 30), '<worker-1>': points(30, 70) },
    usedBreakouts: ['host'],
    remainingBreakouts: ['cpu']
  };
}

async function api(t, response = fixture(), status = 200) {
  const requests = [];
  const server = http.createServer(async (req, res) => {
    let body = '';
    for await (const chunk of req) body += chunk;
    requests.push({ method: req.method, path: req.url, body: JSON.parse(body) });
    res.writeHead(status, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(response));
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const url = 'http://127.0.0.1:' + server.address().port + '/api/v1/metric-data';
  return {
    requests,
    run: async (...args) => {
      const result = await execFileAsync(
        process.execPath,
        [cli, '--server-url', url, '--run', 'fixture', '--source', 'sar', '--type', 'busy', ...args],
        { timeout: 10000 }
      );
      assert.equal(result.stderr, '');
      return result.stdout.replaceAll(url, '<URL>');
    }
  };
}

function jsonOutput(output) {
  return JSON.parse(output.slice(output.indexOf('{')));
}

// Snapshots captured from get-metric-data.js on master before the extraction.
test('CLI default table preserves existing spacing, timestamps, rounding, and natural label order', async (t) => {
  const client = await api(t);
  assert.equal(
    await client.run(),
    '\nMetric data retrieved from API server: <URL>\n\nAvailable breakouts:  cpu\n\n' +
      '                       01-01-1970 01-01-1970\n' +
      ' source type      host   00:00:01   00:00:02\n' +
      '--------------------------------------------\n' +
      '    sar busy  worker-1      30.00      70.00\n' +
      '    sar busy  worker-2      50.00      30.00\n' +
      '    sar busy worker-10      20.00     100.00\n'
  );
});

test('CLI epoch table preserves one header row and all resolution values', async (t) => {
  const client = await api(t);
  assert.equal(
    await client.run('--date-format', 'epoch_ms'),
    '\nMetric data retrieved from API server: <URL>\n\nAvailable breakouts:  cpu\n\n' +
      ' source type      host  1000   2000\n' +
      '-----------------------------------\n' +
      '    sar busy  worker-1 30.00  70.00\n' +
      '    sar busy  worker-2 50.00  30.00\n' +
      '    sar busy worker-10 20.00 100.00\n'
  );
});

test('CLI JSON preserves the raw response shape and metadata without presentation options', async (t) => {
  const client = await api(t);
  assert.deepEqual(jsonOutput(await client.run('--output-format', 'json')), fixture());
});

test('CLI transforms locally and sends only raw query parameters to the API', async (t) => {
  const client = await api(t);
  const output = jsonOutput(
    await client.run(
      '--sort',
      'value',
      '--sort-order',
      'desc',
      '--top',
      '1',
      '--filter',
      'le:30',
      '--percent-of-total',
      '--output-format',
      'json',
      '--resolution',
      '2',
      '--breakout',
      'host',
      '--aggregation',
      'avg',
      '--distribution-stats',
      'min,p95',
      '--allow-incompatible-aggregation'
    )
  );
  assert.deepEqual(Object.keys(output.values), ['<worker-1>']);
  assert.deepEqual(output.values['<worker-1>'], [
    { begin: 0, end: 1000, value: 30, percentOfTotal: 30 },
    { begin: 1001, end: 2000, value: 70, percentOfTotal: 35 }
  ]);
  assert.deepEqual(client.requests, [
    {
      method: 'POST',
      path: '/api/v1/metric-data',
      body: {
        run: 'fixture',
        source: 'sar',
        type: 'busy',
        resolution: '2',
        breakout: ['host'],
        aggregation: 'avg',
        'distribution-stats': ['min', 'p95'],
        'allow-incompatible-aggregation': true
      }
    }
  ]);
});

test('CLI table, CSV, and JSON share sorting, filtering, top-N, and per-window percentages', async (t) => {
  const client = await api(t);
  const args = ['--sort', 'value', '--sort-order', 'desc', '--top', '1', '--filter', 'ge:30', '--percent-of-total'];
  const table = await client.run(...args, '--date-format', 'epoch_ms');
  assert.match(table, /1000 % of total\s+2000\s+2000 % of total/);
  assert.match(table, /worker-2\s+50\.00\s+50\.00\s+30\.00\s+15\.00/);
  assert.doesNotMatch(table, /worker-1|worker-10/);
  const csv = await client.run(...args, '--output-format', 'csv', '--date-format', 'epoch_ms');
  assert.ok(
    csv.endsWith(
      'source,type,host,1000,1000 % of total,2000,2000 % of total\nsar,busy,worker-2,50.00,50.00,30.00,15.00\n'
    )
  );
  const json = jsonOutput(await client.run(...args, '--output-format', 'json'));
  assert.deepEqual(Object.keys(json.values), ['<worker-2>']);
  assert.deepEqual(
    json.values['<worker-2>'].map((point) => point.percentOfTotal),
    [50, 15]
  );
});

test('CLI keeps presentation controls for values, headers, precision, and timestamps', async (t) => {
  const client = await api(t);
  const values = await client.run('--output-content', 'values', '--horizontal-break', 'no', '--decimal-places', '1');
  assert.match(values, /worker-1\s+30\.0\s+70\.0/);
  assert.doesNotMatch(values, /source type|---/);
  const headers = await client.run('--output-content', 'headers', '--timestamp-rows', '1');
  assert.match(headers, /01-01-1970\/00:00:01/);
  assert.doesNotMatch(headers, /worker-1|---/);
});

test('CLI filters distribution statistics consistently with displayed metric labels', async (t) => {
  const response = fixture();
  response.distributionStats = {
    '<worker-1>': { mean: 50, p95: 70 },
    '<worker-2>': { mean: 40, p95: 50 },
    '<worker-10>': { mean: 60, p95: 100 }
  };
  const client = await api(t, response);
  const output = await client.run('--top', '1');
  assert.ok(output.endsWith('\nDistribution statistics (native timeline):\n  <worker-1>: mean=50.00 p95=70.00\n'));
  assert.doesNotMatch(output, /worker-2|worker-10/);
  const headers = await client.run('--output-content', 'headers');
  assert.doesNotMatch(headers, /Distribution statistics/);
});

test('CLI handles a filter that hides every row and a top limit of zero', async (t) => {
  const client = await api(t);
  for (const args of [
    ['--filter', 'gt:1000'],
    ['--top', '0']
  ]) {
    const output = jsonOutput(await client.run(...args, '--output-format', 'json'));
    assert.deepEqual(output.values, {});
  }
});

test('CLI reports invalid transform options and API failures', async (t) => {
  const client = await api(t);
  await assert.rejects(client.run('--top', '-1'), (error) => {
    assert.equal(error.code, 1);
    assert.match(error.stdout, /Error transforming metric data: top must/);
    return true;
  });
  const failed = await api(t, { error: 'query unavailable' }, 503);
  await assert.rejects(failed.run(), (error) => {
    assert.equal(error.code, 1);
    assert.match(error.stdout, /Error fetching metric data: query unavailable/);
    return true;
  });
  const empty = await api(t, { values: {} });
  await assert.rejects(empty.run(), (error) => {
    assert.equal(error.code, 1);
    assert.match(error.stdout, /There were no metrics found/);
    return true;
  });
});
