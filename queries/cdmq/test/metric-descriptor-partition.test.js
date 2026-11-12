'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const cdm = require('../cdm');

const source = 'perf-record';
const type = 'cycles';
const hostField = 'metric_desc.names.hostname';
const cmdField = 'metric_desc.names.cmd';

function rowsForPartitions() {
  return [
    {
      'run.run-uuid': 'run-1',
      'metric_desc.source': source,
      'metric_desc.type': type,
      'period.period-uuid': 'p1',
      [hostField]: 'h1',
      [cmdField]: 'f1'
    },
    {
      'run.run-uuid': 'run-1',
      'metric_desc.source': source,
      'metric_desc.type': type,
      'period.period-uuid': 'p1',
      [hostField]: 'h1',
      [cmdField]: 'f2'
    },
    {
      'run.run-uuid': 'run-1',
      'metric_desc.source': source,
      'metric_desc.type': type,
      'period.period-uuid': 'p1',
      [hostField]: 'h2',
      [cmdField]: 'f3'
    },
    {
      'run.run-uuid': 'run-1',
      'metric_desc.source': source,
      'metric_desc.type': type,
      'period.period-uuid': 'p1',
      [hostField]: 'h2',
      [cmdField]: 'f4'
    }
  ];
}

function queryFilters(query) {
  return query.query && query.query.bool ? query.query.bool.filter || [] : [];
}

function rowMatchesFilter(row, filter) {
  if (filter.term) {
    const field = Object.keys(filter.term)[0];
    return row[field] === filter.term[field];
  }
  if (filter.terms) {
    const field = Object.keys(filter.terms)[0];
    return filter.terms[field].includes(row[field]);
  }
  if (filter.regexp) {
    const field = Object.keys(filter.regexp)[0];
    return new RegExp(filter.regexp[field]).test(row[field]);
  }
  if (filter.bool && Array.isArray(filter.bool.should)) {
    return filter.bool.should.some((condition) => {
      if (condition.term) {
        const field = Object.keys(condition.term)[0];
        return row[field] === condition.term[field];
      }
      if (condition.bool && Array.isArray(condition.bool.must_not)) {
        return condition.bool.must_not.every((clause) => row[clause.exists.field] === undefined);
      }
      return false;
    });
  }
  return true;
}

function matchingRows(rows, query) {
  const filters = queryFilters(query);
  return rows.filter((row) => filters.every((filter) => rowMatchesFilter(row, filter)));
}

function constraintsFromQuery(query) {
  const constraints = {};
  queryFilters(query).forEach((filter) => {
    if (filter.term && filter.term[hostField] !== undefined) constraints.hostname = filter.term[hostField];
    if (filter.term && filter.term[cmdField] !== undefined) constraints.cmd = filter.term[cmdField];
  });
  return constraints;
}

function nestedFields(query) {
  const fields = [];
  function visit(aggregation) {
    if (!aggregation || typeof aggregation !== 'object') return;
    Object.keys(aggregation).forEach((field) => {
      if (field.startsWith('metric_desc.names.')) fields.push(field);
      if (aggregation[field] && aggregation[field].aggs) visit(aggregation[field].aggs);
    });
  }
  visit(query.aggs);
  return fields;
}

function nestedAggregation(fields, rows) {
  if (fields.length === 0) return { source: { buckets: [] } };
  const root = new Map();
  rows.forEach((row) => {
    let children = root;
    fields.forEach((field) => {
      if (!children.has(row[field])) children.set(row[field], new Map());
      children = children.get(row[field]);
    });
  });

  function bucketList(children, depth) {
    return Array.from(children.entries()).map(([key, nestedChildren]) => {
      const bucket = { key: key };
      if (depth + 1 < fields.length) bucket[fields[depth + 1]] = { buckets: bucketList(nestedChildren, depth + 1) };
      return bucket;
    });
  }

  return { [fields[0]]: { buckets: bucketList(root, 0) } };
}

function descriptorError(errorType, reason) {
  return {
    error: { type: errorType, reason: reason },
    status: 400
  };
}

function fakeOpenSearch(rows, overrides = {}) {
  const records = [];
  let metricId = 0;
  const tooMany =
    overrides.tooMany ||
    ((query) => {
      const constraints = constraintsFromQuery(query);
      return constraints.hostname === undefined || (constraints.hostname === 'h2' && constraints.cmd === undefined);
    });

  return {
    records: records,
    request: async (instance, docType, action, jsonArr) => {
      const queries = [];
      for (let index = 1; index < jsonArr.length; index += 2) queries.push(JSON.parse(jsonArr[index]));
      records.push(queries);

      return queries.map((query) => {
        if (query._source === 'metric_desc.metric_desc-uuid') {
          metricId++;
          return {
            hits: {
              total: { value: 1 },
              hits: [{ _source: { metric_desc: { 'metric_desc-uuid': 'metric-' + metricId } } }]
            }
          };
        }
        if (query.aggs && query.aggs.source && query.aggs.source.terms) {
          const field = query.aggs.source.terms.field;
          const values = [];
          matchingRows(rows, query).forEach((row) => {
            if (!values.includes(row[field])) values.push(row[field]);
          });
          return {
            aggregations: {
              source: {
                sum_other_doc_count: 0,
                buckets: values.map((value) => ({ key: value, doc_count: 1 }))
              }
            }
          };
        }
        if (overrides.error) return overrides.error(query);
        if (tooMany(query)) {
          return descriptorError(
            'too_many_buckets_exception',
            'Trying to create too many buckets. Must be less than or equal to: [65535] but was [65536].'
          );
        }
        const fields = nestedFields(query);
        return { aggregations: nestedAggregation(fields, matchingRows(rows, query)) };
      });
    }
  };
}

function querySet(breakout, extra = {}) {
  return {
    run: 'run-1',
    period: 'p1',
    source: source,
    type: type,
    breakout: breakout,
    ...extra
  };
}

function descriptorQueries(fake) {
  return fake.records.flat().filter((query) => query.aggs && query.aggs['metric_desc.source']);
}

function metricIdQueries(fake) {
  return fake.records.flat().filter((query) => query._source === 'metric_desc.metric_desc-uuid');
}

test('partitions bucket-limited breakouts, recursively splits failures, and preserves every label', async () => {
  const rows = rowsForPartitions();
  const fake = fakeOpenSearch(rows);
  const result = await cdm.getMetricGroupsFromBreakouts(
    { host: 'opensearch.example' },
    [querySet([hostField.replace('metric_desc.names.', ''), 'cmd'])],
    '@2026.11',
    { request: fake.request, maxQueries: 20, maxDepth: 4 }
  );

  assert.equal(result['ret-code'], 0, JSON.stringify(result));
  const metricIds = result['metric-id-sets'][0];
  assert.deepEqual(Object.keys(metricIds).sort(), ['<h1>-<f1>', '<h1>-<f2>', '<h2>-<f3>', '<h2>-<f4>']);
  Object.values(metricIds).forEach((ids) => assert.equal(ids.length, 1));
  assert.equal(descriptorQueries(fake).length, 5);
  assert.ok(
    descriptorQueries(fake).some((query) => {
      const constraints = constraintsFromQuery(query);
      return constraints.hostname === 'h2' && constraints.cmd === 'f3';
    })
  );
  metricIdQueries(fake).forEach((query) => assert.equal(matchingRows(rows, query).length, 1));
});

test('preserves value and regex filters and injects aggregate labels in original order', async () => {
  const rows = rowsForPartitions()
    .filter((row) => row[hostField] === 'h2')
    .map((row) => ({ ...row, 'metric_desc.names.cpu': 'c0' }));
  const fake = fakeOpenSearch(rows);
  const breakout = [
    { name: 'hostname', values: ['h2'] },
    { name: 'cmd', regex: 'f.*' },
    { name: 'cpu', regex: 'c.*', aggregate: true }
  ];
  const result = await cdm.getMetricGroupsFromBreakouts(
    { host: 'opensearch.example' },
    [querySet(breakout)],
    '@2026.11',
    { request: fake.request, maxQueries: 20, maxDepth: 4 }
  );

  assert.equal(result['ret-code'], 0, JSON.stringify(result));
  assert.deepEqual(Object.keys(result['metric-id-sets'][0]).sort(), ['<h2>-<f3>-</c.*/>', '<h2>-<f4>-</c.*/>']);
  assert.ok(
    fake.records
      .flat()
      .some((query) => queryFilters(query).some((filter) => filter.term && filter.term[hostField] === 'h2'))
  );
  assert.ok(
    fake.records
      .flat()
      .some((query) => queryFilters(query).some((filter) => filter.regexp && filter.regexp[cmdField] === 'f.*'))
  );
  metricIdQueries(fake).forEach((query) => {
    assert.ok(queryFilters(query).some((filter) => filter.regexp && filter.regexp['metric_desc.names.cpu'] === 'c.*'));
  });
});

test('returns an actionable limit error when the partition query budget is exhausted', async () => {
  const fake = fakeOpenSearch(rowsForPartitions());
  const result = await cdm.getMetricGroupsFromBreakouts(
    { host: 'opensearch.example' },
    [querySet(['hostname', 'cmd'])],
    '@2026.11',
    { request: fake.request, maxQueries: 3, maxDepth: 4 }
  );

  assert.equal(result['ret-code'], 4);
  assert.equal(result.code, 'METRIC_DESCRIPTOR_LIMIT');
  assert.equal(result.details.causeType, 'too_many_buckets_exception');
  assert.match(result.recoveryHint, /CDM_METRIC_DESCRIPTOR_MAX_QUERIES/);
  assert.doesNotMatch(result['ret-msg'], /reading 'key'/);
  assert.equal(fake.records.flat().length, 3);
});

test('bounds recursive descriptor splitting', async () => {
  const fake = fakeOpenSearch(rowsForPartitions(), { tooMany: () => true });
  const result = await cdm.getMetricGroupsFromBreakouts(
    { host: 'opensearch.example' },
    [querySet(['hostname', 'cmd'])],
    '@2026.11',
    { request: fake.request, maxQueries: 20, maxDepth: 1 }
  );

  assert.equal(result.code, 'METRIC_DESCRIPTOR_LIMIT');
  assert.equal(result.details.type, 'descriptor_partition_depth_exhausted');
  assert.match(result['ret-msg'], /too_many_buckets_exception/);
});

test('handles multiple requested sets independently and within one shared budget', async () => {
  const fake = fakeOpenSearch(rowsForPartitions());
  const result = await cdm.getMetricGroupsFromBreakouts(
    { host: 'opensearch.example' },
    [querySet(['hostname', 'cmd']), querySet(['hostname', 'cmd'])],
    '@2026.11',
    { request: fake.request, maxQueries: 30, maxDepth: 4 }
  );

  assert.equal(result['ret-code'], 0, JSON.stringify(result));
  assert.equal(result['metric-id-sets'].length, 2);
  assert.deepEqual(Object.keys(result['metric-id-sets'][0]).sort(), [
    '<h1>-<f1>',
    '<h1>-<f2>',
    '<h2>-<f3>',
    '<h2>-<f4>'
  ]);
  assert.deepEqual(Object.keys(result['metric-id-sets'][1]).sort(), Object.keys(result['metric-id-sets'][0]).sort());
  assert.equal(fake.records[0].length, 2);
});

test('propagates non-bucket OpenSearch errors with type and reason', async () => {
  const fake = fakeOpenSearch(rowsForPartitions(), {
    error: () => descriptorError('illegal_argument_exception', 'unknown aggregation field')
  });
  const result = await cdm.getMetricGroupsFromBreakouts(
    { host: 'opensearch.example' },
    [querySet(['hostname', 'cmd'])],
    '@2026.11',
    { request: fake.request }
  );

  assert.equal(result.code, 'OPENSEARCH_QUERY_FAILED');
  assert.equal(result.details.type, 'illegal_argument_exception');
  assert.match(result['ret-msg'], /unknown aggregation field/);
});

test('reports a malformed descriptor response without a reading-key exception', async () => {
  const fake = fakeOpenSearch(rowsForPartitions(), { error: () => ({ status: 200 }) });
  const result = await cdm.getMetricGroupsFromBreakouts(
    { host: 'opensearch.example' },
    [querySet(['hostname', 'cmd'])],
    '@2026.11',
    { request: fake.request }
  );

  assert.equal(result.code, 'METRIC_DESCRIPTOR_INVALID_RESPONSE');
  assert.equal(result.details.type, 'missing_aggregations');
  assert.doesNotMatch(result['ret-msg'], /reading 'key'/);
});

test('returns a structured error instead of dereferencing a failed transport result', async () => {
  const originalFetch = global.fetch;
  global.fetch = async () => ({ ok: false, status: 503, json: async () => ({}) });
  try {
    const responses = await cdm.esJsonArrRequest(
      {
        host: 'opensearch.example',
        ver: 'v10dev',
        indices: { v10dev: ['cdm-v10dev-metric_desc@*'] }
      },
      'metric_desc',
      '/_msearch',
      ['{}', '{"query":{"match_all":{}}}'],
      '@*'
    );
    assert.equal(responses.length, 1);
    assert.equal(responses[0].error.type, 'cdm_transport_error');
    assert.equal(responses[0].status, 502);
  } finally {
    global.fetch = originalFetch;
  }
});
