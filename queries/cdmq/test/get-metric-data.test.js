'use strict';

const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const http = require('node:http');
const path = require('node:path');
const test = require('node:test');

const script = path.join(__dirname, '..', 'get-metric-data.js');

function runMetricQuery(serverUrl) {
  return new Promise((resolve, reject) => {
    const child = spawn(
      process.execPath,
      [
        script,
        '--server-url',
        serverUrl,
        '--period',
        'period-1',
        '--source',
        'test',
        '--type',
        'cycles',
        '--breakout',
        'hostname,function',
        '--resolution',
        '2',
        '--output-format',
        'table',
        '--decimal-places',
        '0',
        '--percent-of-total',
        '--percent-decimal-places',
        '2'
      ],
      { cwd: path.join(__dirname, '..') }
    );
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => {
      stdout += chunk;
    });
    child.stderr.on('data', (chunk) => {
      stderr += chunk;
    });
    child.on('error', reject);
    child.on('exit', (code) => resolve({ code, stdout, stderr }));
  });
}

test('table renders percent-of-total columns for multiple time windows', async () => {
  const metricData = {
    label: '<hostname>-<function>',
    usedBreakouts: ['hostname', 'function'],
    remainingBreakouts: 'cpu',
    values: {
      '<host1>-<alpha>': [
        { begin: 1000, end: 1999, value: 75 },
        { begin: 2000, end: 2999, value: 30 }
      ],
      '<host1>-<beta>': [
        { begin: 1000, end: 1999, value: 25 },
        { begin: 2000, end: 2999, value: 20 }
      ]
    }
  };

  const server = http.createServer((request, response) => {
    response.setHeader('Content-Type', 'application/json');
    response.setHeader('Connection', 'close');
    response.end(JSON.stringify(metricData));
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  try {
    const result = await runMetricQuery(`http://127.0.0.1:${server.address().port}/api/v1/metric-data`);

    assert.equal(result.code, 0, result.stderr);
    assert.equal((result.stdout.match(/percent-of-total/g) || []).length, 2);
    assert.match(result.stdout, /75\s+75\.00\s+30\s+60\.00/);
    assert.match(result.stdout, /25\s+25\.00\s+20\s+40\.00/);
  } finally {
    if (typeof server.closeAllConnections === 'function') {
      server.closeAllConnections();
    }
    await new Promise((resolve) => server.close(resolve));
  }
});
