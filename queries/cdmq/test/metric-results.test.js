'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { transformMetricData } = require('../metric-results');
const { renderMetricData } = require('../metric-result-output');

function fixture() {
  const points = (first, second) => [
    { begin: 0, end: 999, value: first },
    { begin: 1000, end: 1999, value: second }
  ];
  return {
    values: { '<host-10>': points(20, 100), '<host-2>': points(50, 30), '<host-1>': points(30, 70) },
    usedBreakouts: ['host'],
    remainingBreakouts: ['cpu'],
    valueSeriesLabelDecoder: '<host>',
    distributionStats: { '<host-10>': { mean: 60 }, '<host-2>': { mean: 40 }, '<host-1>': { mean: 50 } }
  };
}

const labels = (result) => result.rows.map((row) => row.label);

test('label sorting is natural, case-insensitive, and supports both directions', () => {
  assert.deepEqual(labels(transformMetricData(fixture())), ['<host-1>', '<host-2>', '<host-10>']);
  assert.deepEqual(labels(transformMetricData(fixture(), { sortOrder: 'desc' })), [
    '<host-10>',
    '<host-2>',
    '<host-1>'
  ]);
  const data = { values: { '<cpu-10>': [], '<CPU-2>': [], '<Cpu-1>': [] } };
  assert.deepEqual(labels(transformMetricData(data)), ['<Cpu-1>', '<CPU-2>', '<cpu-10>']);
});

test('value sorting uses raw first-window values and retains subsequent windows', () => {
  const ascending = transformMetricData(fixture(), { sort: 'value' });
  assert.deepEqual(labels(ascending), ['<host-10>', '<host-1>', '<host-2>']);
  const descending = transformMetricData(fixture(), { sort: 'value', sortOrder: 'desc' });
  assert.deepEqual(labels(descending), ['<host-2>', '<host-1>', '<host-10>']);
  assert.deepEqual(descending.rows[0].values, fixture().values['<host-2>']);
});

test('value ties use ascending natural label order', () => {
  const data = fixture();
  data.values['<host-10>'][0].value = 50;
  assert.deepEqual(labels(transformMetricData(data, { sort: 'value', sortOrder: 'desc', top: 2 })), [
    '<host-2>',
    '<host-10>'
  ]);
});

test('top-N follows sorting, including zero and limits larger than the input', () => {
  assert.deepEqual(labels(transformMetricData(fixture(), { sort: 'value', top: 1 })), ['<host-10>']);
  assert.deepEqual(transformMetricData(fixture(), { top: 0 }).data.values, {});
  assert.equal(transformMetricData(fixture(), { top: 10 }).rows.length, 3);
});

for (const [filter, expected] of [
  ['gt:30', ['<host-2>']],
  ['ge:30', ['<host-1>', '<host-2>']],
  ['lt:30', ['<host-10>']],
  ['le:30', ['<host-1>', '<host-10>']]
]) {
  test('output-only filter ' + filter + ' uses first-window raw values', () => {
    const result = transformMetricData(fixture(), { filter });
    assert.deepEqual(labels(result), expected);
    for (const row of result.rows) assert.equal(row.values.length, 2);
  });
}

test('percentages use all labels in each window before filtering and top-N', () => {
  const result = transformMetricData(fixture(), {
    sort: 'value',
    sortOrder: 'desc',
    filter: 'le:30',
    top: 1,
    percentOfTotal: true
  });
  assert.deepEqual(result.totals, [
    { begin: 0, end: 999, value: 100 },
    { begin: 1000, end: 1999, value: 200 }
  ]);
  assert.deepEqual(labels(result), ['<host-1>']);
  assert.deepEqual(result.rows[0].values, [
    { begin: 0, end: 999, value: 30, percentOfTotal: 30 },
    { begin: 1000, end: 1999, value: 70, percentOfTotal: 35 }
  ]);
  assert.deepEqual(result.data.distributionStats, { '<host-1>': { mean: 50 } });
  assert.equal(result.data.valueSeriesLabelDecoder, '<host>');
});

test('filtering precedes top-N and comparisons never use rounded display values or percentages', () => {
  const result = transformMetricData(fixture(), {
    sort: 'value',
    sortOrder: 'desc',
    filter: 'le:30',
    top: 2,
    percentOfTotal: true
  });
  assert.deepEqual(labels(result), ['<host-1>', '<host-10>']);
  const data = { values: { a: [{ begin: 0, end: 1, value: 0.004 }] } };
  assert.equal(transformMetricData(data, { filter: 'gt:0.003', percentOfTotal: true }).rows.length, 1);
});

test('totals align by begin/end instead of array offset for sparse series', () => {
  const data = fixture();
  data.values['<host-10>'].shift();
  const result = transformMetricData(data, { percentOfTotal: true });
  assert.deepEqual(result.rows[2].values, [{ begin: 1000, end: 1999, value: 100, percentOfTotal: 50 }]);
  assert.match(
    renderMetricData(result, { outputFormat: 'csv', dateFormat: 'epoch_ms', source: 'sar', type: 'busy' }),
    /sar,busy,host-10,,,100.00,50.00/
  );
});

test('zero totals yield null percentages and render as empty cells', () => {
  const result = transformMetricData({ values: { '': [{ begin: 0, end: 1, value: 0 }] } }, { percentOfTotal: true });
  assert.deepEqual(result.rows[0].breakoutValues, []);
  assert.equal(result.rows[0].values[0].percentOfTotal, null);
  assert.match(renderMetricData(result, { outputFormat: 'json' }), /"percentOfTotal": null/);
  assert.equal(
    renderMetricData(result, { outputFormat: 'csv', dateFormat: 'epoch_ms' }),
    'source,type,1,1 % of total\n,,0.00,'
  );
});

test('percentage headers retain every requested window even when no rows remain', () => {
  const result = transformMetricData(fixture(), { percentOfTotal: true, top: 0 });
  assert.equal(
    renderMetricData(result, { outputFormat: 'csv', dateFormat: 'epoch_ms' }),
    'source,type,host,999,999 % of total,1999,1999 % of total'
  );
  assert.doesNotMatch(renderMetricData(result), /Distribution statistics/);
});

test('negative values and numeric strings preserve their raw values', () => {
  const result = transformMetricData(
    { values: { a: [{ begin: 0, end: 1, value: '-2' }], b: [{ begin: 0, end: 1, value: 6 }] } },
    { percentOfTotal: true, filter: 'ge:-2e0', sort: 'value' }
  );
  assert.equal(result.rows[0].values[0].value, '-2');
  assert.equal(result.rows[0].values[0].percentOfTotal, -50);
  assert.equal(result.rows[1].values[0].percentOfTotal, 150);
});

test('empty results, absent breakouts, empty labels, and empty series are valid', () => {
  const empty = transformMetricData({ values: {} });
  assert.deepEqual(empty.rows, []);
  assert.equal(renderMetricData(empty, { outputFormat: 'csv', dateFormat: 'epoch_ms' }), 'source,type');
  const data = { values: { '': [{ begin: 0, end: 1, value: 2 }], empty: [] } };
  assert.deepEqual(labels(transformMetricData(data, { sort: 'value', sortOrder: 'desc' })), ['', 'empty']);
  assert.deepEqual(labels(transformMetricData(data, { filter: 'gt:1' })), ['']);
});

test('structured rows preserve sort order for integer-like keys', () => {
  const result = transformMetricData({ values: { 1: [], 2: [], 10: [] } }, { sortOrder: 'desc' });
  assert.deepEqual(labels(result), ['10', '2', '1']);
});

test('transforming and rendering never mutate or alias the raw response', () => {
  const data = fixture();
  const original = structuredClone(data);
  function freeze(value) {
    if (value && typeof value === 'object') {
      Object.values(value).forEach(freeze);
      Object.freeze(value);
    }
  }
  freeze(data);
  const result = transformMetricData(data, { percentOfTotal: true, top: 1 });
  const beforeRender = structuredClone(result);
  for (const outputFormat of ['json', 'csv', 'table']) renderMetricData(result, { outputFormat });
  assert.deepEqual(result, beforeRender);
  result.rows[0].values[0].value = 999;
  result.data.usedBreakouts.push('new');
  result.data.distributionStats['<host-1>'].mean = 999;
  assert.deepEqual(data, original);
});

test('CSV escapes breakout commas, quotes, and newlines, preserving all windows', () => {
  const data = fixture();
  data.values = { '<a,"b\nc>': data.values['<host-1>'] };
  const output = renderMetricData(transformMetricData(data), {
    source: 'sar',
    type: 'busy',
    outputFormat: 'csv',
    dateFormat: 'epoch_ms',
    decimalPlaces: 1
  });
  assert.equal(output, 'source,type,host,999,1999\nsar,busy,"a,""b\nc",30.0,70.0');
});

test('table and CSV honor headers/values and timestamp formatting', () => {
  const result = transformMetricData(fixture(), { percentOfTotal: true, top: 1 });
  const options = { source: 'sar', type: 'busy', decimalPlaces: 3, timestampRows: 1 };
  const table = renderMetricData(result, options);
  assert.match(table, /01-01-1970\/00:00:00 % of total/);
  assert.match(table, /30\.000\s+30\.000\s+70\.000\s+35\.000/);
  const headers = renderMetricData(result, { ...options, outputFormat: 'csv', outputContent: 'headers' });
  assert.equal(headers.split('\n').length, 1);
  assert.doesNotMatch(headers, /Distribution statistics/);
  const values = renderMetricData(result, { ...options, outputFormat: 'csv', outputContent: 'values' });
  assert.equal(values, 'sar,busy,host-1,30.000,30.000,70.000,35.000');
});

test('invalid presentation options and malformed responses fail clearly', () => {
  for (const [options, message] of [
    [{ sort: 'mean' }, /sort must/],
    [{ sortOrder: 'down' }, /sortOrder must/],
    [{ top: -1 }, /top must/],
    [{ top: 1.5 }, /top must/],
    [{ top: NaN }, /top must/],
    [{ top: Infinity }, /top must/],
    [{ top: '2' }, /top must/],
    [{ percentOfTotal: 'yes' }, /percentOfTotal must/],
    [{ filter: 'eq:2' }, /filter must/],
    [{ filter: 'gt:2junk' }, /filter must/],
    [{ filter: 'lt:Infinity' }, /filter must/]
  ]) {
    assert.throws(() => transformMetricData(fixture(), options), message);
  }
  for (const response of [null, {}, { values: [] }]) {
    assert.throws(() => transformMetricData(response), /values object/);
  }
  assert.throws(() => transformMetricData({ values: { a: {} } }), /arrays of time windows/);
  assert.throws(() => transformMetricData({ values: { a: [{ value: null }] } }), /finite numbers/);
  const result = transformMetricData(fixture());
  assert.throws(() => renderMetricData(result, { outputFormat: 'xml' }), /outputFormat must/);
  assert.throws(() => renderMetricData(result, { decimalPlaces: 101 }), /decimalPlaces must/);
  assert.throws(() => renderMetricData(result, { timestampRows: 3 }), /timestampRows must/);
});

test('representative large input retains all windows and full-scope totals after top-N', () => {
  const values = {};
  for (let metric = 0; metric < 3000; metric++) {
    values['<cpu-' + metric + '>'] = Array.from({ length: 4 }, (_, window) => ({
      begin: window * 1000,
      end: window * 1000 + 999,
      value: metric + window
    }));
  }
  const result = transformMetricData({ values }, { sort: 'value', sortOrder: 'desc', top: 5, percentOfTotal: true });
  assert.equal(result.rows.length, 5);
  assert.equal(result.rows[0].label, '<cpu-2999>');
  assert.equal(result.rows[0].values.length, 4);
  assert.deepEqual(
    result.totals.map((point) => point.value),
    [4498500, 4501500, 4504500, 4507500]
  );
  assert.equal(result.rows[0].values[3].percentOfTotal, (3002 / 4507500) * 100);
});
