'use strict';

const assert = require('node:assert/strict');
const http = require('node:http');
const test = require('node:test');

const cdm = require('../cdm');

function listen(server) {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => resolve(server.address()));
  });
}

function close(server) {
  return new Promise((resolve, reject) => {
    server.close((error) => {
      if (error) reject(error);
      else resolve();
    });
  });
}

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

test('getInstancesInfo waits for OpenSearch discovery to finish', async () => {
  const server = http.createServer((request, response) => {
    assert.equal(request.url, '/_cat/indices?format=json');
    setTimeout(() => {
      response.setHeader('Content-Type', 'application/json');
      response.end(JSON.stringify([{ index: 'cdm-v9dev-run@2026.01' }]));
    }, 25);
  });
  const address = await listen(server);
  const instance = { host: `127.0.0.1:${address.port}`, header: {} };

  try {
    const discovery = cdm.getInstancesInfo([instance]);
    await delay(5);

    assert.equal(instance.online, true);
    assert.equal(instance.indices, undefined);

    await discovery;

    assert.equal(instance.online, true);
    assert.deepEqual(instance.indices, { v9dev: ['cdm-v9dev-run@2026.01'] });
    assert.equal(instance.ver, 'v9dev');
  } finally {
    await close(server);
  }
});

test('getInstancesInfo marks an OpenSearch error response offline', async () => {
  const server = http.createServer((request, response) => {
    response.statusCode = 503;
    response.end('OpenSearch unavailable');
  });
  const address = await listen(server);
  const instance = { host: `127.0.0.1:${address.port}`, header: {} };

  try {
    await cdm.getInstancesInfo([instance]);

    assert.equal(instance.online, false);
    assert.equal(instance.indices, undefined);
    assert.equal(instance.ver, undefined);
  } finally {
    await close(server);
  }
});

test('getInstancesInfo supports overlapping discovery calls', async () => {
  const originalFetch = globalThis.fetch;
  const pendingRequests = [];

  globalThis.fetch = (url) =>
    new Promise((resolve) => {
      pendingRequests.push({ url: String(url), resolve });
    });

  const firstInstances = [
    { host: 'first-a', header: {} },
    { host: 'first-b', header: {} }
  ];
  const secondInstances = [
    { host: 'second-a', header: {} },
    { host: 'second-b', header: {} }
  ];
  const respond = (request, index) => {
    request.resolve({
      status: 200,
      json: async () => [{ index }]
    });
  };

  try {
    const firstDiscovery = cdm.getInstancesInfo(firstInstances);
    const secondDiscovery = cdm.getInstancesInfo(secondInstances);

    assert.equal(pendingRequests.length, 2);
    respond(pendingRequests[1], 'cdm-v9dev-second-a@2026.01');
    await delay(0);
    assert.equal(pendingRequests.length, 3);

    respond(pendingRequests[0], 'cdm-v9dev-first-a@2026.01');
    await delay(0);
    assert.equal(pendingRequests.length, 4);
    respond(pendingRequests[2], 'cdm-v9dev-second-b@2026.01');
    respond(pendingRequests[3], 'cdm-v9dev-first-b@2026.01');

    await Promise.all([firstDiscovery, secondDiscovery]);

    assert.deepEqual(firstInstances[0].indices, { v9dev: ['cdm-v9dev-first-a@2026.01'] });
    assert.deepEqual(firstInstances[1].indices, { v9dev: ['cdm-v9dev-first-b@2026.01'] });
    assert.deepEqual(secondInstances[0].indices, { v9dev: ['cdm-v9dev-second-a@2026.01'] });
    assert.deepEqual(secondInstances[1].indices, { v9dev: ['cdm-v9dev-second-b@2026.01'] });
  } finally {
    globalThis.fetch = originalFetch;
  }
});
