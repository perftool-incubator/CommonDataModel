'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { getNativeMetricStats } = require('../cdm');

function response(body) {
  return { ok: true, status: 200, json: async () => body };
}

test('streams PIT pages and closes the PIT', async () => {
  const requests = [];
  const bodies = [
    { pit_id: 'pit-1' },
    {
      hits: {
        total: { value: 2, relation: 'eq' },
        hits: [
          {
            sort: [0, 4, 'a'],
            fields: {
              'metric_desc.metric_desc-uuid': ['a'],
              'metric_data.begin': [0],
              'metric_data.end': [4],
              'metric_data.value': [10]
            }
          },
          {
            sort: [0, 4, 'b'],
            fields: {
              'metric_desc.metric_desc-uuid': ['b'],
              'metric_data.begin': [0],
              'metric_data.end': [4],
              'metric_data.value': [2]
            }
          }
        ]
      }
    },
    { hits: { total: { value: 0, relation: 'eq' }, hits: [] } },
    { succeeded: true }
  ];
  const fetchImpl = async (url, request) => {
    requests.push({ method: request.method, url, request: JSON.parse(request.body || '{}') });
    return response(bodies.shift());
  };

  const stats = await getNativeMetricStats(
    { host: 'opensearch.example', header: { Authorization: 'Basic test' } },
    ['a', 'b'],
    0,
    4,
    'avg',
    ['mean', 'stddev'],
    '@2026.09',
    { fetch: fetchImpl, indexName: 'cdm-v10dev-metric_data@2026.09', pageSize: 2 }
  );

  assert.deepEqual(stats, { mean: 6, stddev: 0 });
  assert.match(requests[0].url, /point_in_time/);
  assert.equal(requests[1].request.pit.id, 'pit-1');
  assert.deepEqual(requests[1].request.search_after, undefined);
  assert.deepEqual(requests[2].request.search_after, [0, 4, 'b']);
  assert.equal(requests.at(-1).method, 'DELETE');
});

test('closes the PIT when the document limit is exceeded', async () => {
  const methods = [];
  const fetchImpl = async (url, request) => {
    methods.push(request.method);
    return response(
      methods.length === 1 ? { pit_id: 'pit-2' } : { hits: { total: { value: 2, relation: 'eq' }, hits: [] } }
    );
  };

  await assert.rejects(
    getNativeMetricStats({ host: 'opensearch.example' }, ['a'], 0, 4, 'sum', ['mean'], '@2026.09', {
      fetch: fetchImpl,
      indexName: 'metric_data',
      maxDocuments: 1
    }),
    { code: 'NATIVE_STATS_LIMIT' }
  );
  assert.deepEqual(methods, ['POST', 'POST', 'DELETE']);
});

test('rejects invalid statistics before opening a PIT', async () => {
  let requestCount = 0;
  const fetchImpl = async () => {
    requestCount++;
    return response({ pit_id: 'unexpected' });
  };

  await assert.rejects(
    getNativeMetricStats({ host: 'opensearch.example' }, ['a'], 0, 4, 'sum', ['p101'], '@2026.09', {
      fetch: fetchImpl,
      indexName: 'metric_data'
    }),
    /invalid distribution statistic/
  );
  assert.equal(requestCount, 0);
});

test('reports unsupported PIT search clearly', async () => {
  const fetchImpl = async () => ({
    ok: false,
    status: 404,
    json: async () => ({ error: 'missing endpoint' })
  });

  await assert.rejects(
    getNativeMetricStats({ host: 'opensearch.example' }, ['a'], 0, 4, 'sum', ['mean'], '@2026.09', {
      fetch: fetchImpl,
      indexName: 'metric_data'
    }),
    (error) => error.code === 'NATIVE_STATS_PIT_UNSUPPORTED' && /Point-in-Time/.test(error.message)
  );
});

test('rejects invalid configured resource limits', async () => {
  const previous = process.env.CDM_NATIVE_STATS_MAX_DOCUMENTS;
  process.env.CDM_NATIVE_STATS_MAX_DOCUMENTS = 'not-a-number';
  try {
    await assert.rejects(
      getNativeMetricStats({ host: 'opensearch.example' }, ['a'], 0, 4, 'sum', ['mean'], '@2026.09', {
        fetch: async () => response({ pit_id: 'unexpected' }),
        indexName: 'metric_data'
      }),
      { code: 'NATIVE_STATS_CONFIG' }
    );
  } finally {
    if (typeof previous === 'undefined') delete process.env.CDM_NATIVE_STATS_MAX_DOCUMENTS;
    else process.env.CDM_NATIVE_STATS_MAX_DOCUMENTS = previous;
  }
});
