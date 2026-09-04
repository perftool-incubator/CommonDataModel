'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');

const baseUrl = process.env.CDM_TEST_SERVER_URL || 'http://localhost:3000';

async function postMetricData(body) {
  const response = await fetch(baseUrl + '/api/v1/metric-data', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  return { status: response.status, body: await response.json() };
}

test('CDM server health endpoint responds', async () => {
  const response = await fetch(baseUrl + '/health');
  assert.equal(response.status, 200);
  assert.equal((await response.json()).status, 'OK');
});

test('metric-data rejects invalid distribution statistics', async () => {
  const result = await postMetricData({ 'distribution-stats': ['p101'] });
  assert.equal(result.status, 400);
  assert.equal(result.body.code, 'INVALID_DISTRIBUTION_STATS');
});

const requiredQueryVariables = ['CDM_TEST_RUN', 'CDM_TEST_SOURCE', 'CDM_TEST_TYPE', 'CDM_TEST_BEGIN', 'CDM_TEST_END'];
const hasQueryFixture = requiredQueryVariables.every((name) => process.env[name]);

test('metric-data statistics are independent of output resolution', { skip: !hasQueryFixture }, async () => {
  const common = {
    run: process.env.CDM_TEST_RUN,
    source: process.env.CDM_TEST_SOURCE,
    type: process.env.CDM_TEST_TYPE,
    begin: Number(process.env.CDM_TEST_BEGIN),
    end: Number(process.env.CDM_TEST_END),
    'distribution-stats': ['min', 'max', 'mean', 'median', 'stddev', 'p95']
  };
  const resolutionOne = await postMetricData({ ...common, resolution: 1 });
  const resolutionTen = await postMetricData({ ...common, resolution: 10 });
  assert.equal(resolutionOne.status, 200);
  assert.equal(resolutionTen.status, 200);
  assert.deepEqual(resolutionOne.body.distributionStats, resolutionTen.body.distributionStats);
});

test('metric-data accepts all aggregation modes', { skip: !hasQueryFixture }, async () => {
  const common = {
    run: process.env.CDM_TEST_RUN,
    source: process.env.CDM_TEST_SOURCE,
    type: process.env.CDM_TEST_TYPE,
    begin: Number(process.env.CDM_TEST_BEGIN),
    end: Number(process.env.CDM_TEST_END),
    resolution: 1,
    'distribution-stats': ['min', 'max', 'mean', 'stddev']
  };
  for (const aggregation of ['sum', 'avg', 'min', 'max']) {
    const result = await postMetricData({ ...common, aggregation });
    assert.equal(result.status, 200, aggregation + ' aggregation failed: ' + JSON.stringify(result.body));
    assert.ok(result.body.distributionStats);
  }
});
