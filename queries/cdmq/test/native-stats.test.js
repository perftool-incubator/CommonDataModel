'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  calculateDistributionStats,
  reconstructTimeline,
  resampleTimeline,
  validateRequestedStats
} = require('../native-stats');

test('reconstructs aligned metric intervals and calculates weighted statistics', () => {
  const timeline = reconstructTimeline(
    {
      a: [
        { begin: 0, end: 4, value: 10 },
        { begin: 5, end: 9, value: 20 }
      ],
      b: [
        { begin: 0, end: 4, value: 2 },
        { begin: 5, end: 9, value: 4 }
      ]
    },
    0,
    9,
    'avg'
  );

  assert.deepEqual(timeline, [
    { begin: 0, end: 4, duration: 5, value: 6 },
    { begin: 5, end: 9, duration: 5, value: 12 }
  ]);
  assert.deepEqual(calculateDistributionStats(timeline, ['min', 'max', 'mean', 'median', 'stddev', 'p95']), {
    min: 6,
    max: 12,
    mean: 9,
    median: 6,
    stddev: 3,
    p95: 12
  });
});

test('handles misaligned intervals and clips to the query range', () => {
  const timeline = reconstructTimeline(
    {
      a: [
        { begin: -2, end: 2, value: 1 },
        { begin: 3, end: 8, value: 3 }
      ],
      b: [
        { begin: 0, end: 4, value: 10 },
        { begin: 5, end: 8, value: 20 }
      ]
    },
    0,
    8,
    'sum'
  );

  assert.deepEqual(timeline, [
    { begin: 0, end: 2, duration: 3, value: 11 },
    { begin: 3, end: 4, duration: 2, value: 13 },
    { begin: 5, end: 8, duration: 4, value: 23 }
  ]);
  assert.equal(calculateDistributionStats(timeline, ['mean']).mean, (3 * 11 + 2 * 13 + 4 * 23) / 9);
});

test('rejects gaps and overlaps', () => {
  assert.throws(
    () =>
      reconstructTimeline(
        {
          a: [
            { begin: 0, end: 1, value: 1 },
            { begin: 3, end: 4, value: 1 }
          ]
        },
        0,
        4
      ),
    (error) => error.code === 'NATIVE_STATS_DATA_QUALITY' && /has a gap/.test(error.message)
  );
  assert.throws(
    () =>
      reconstructTimeline(
        {
          a: [
            { begin: 0, end: 2, value: 1 },
            { begin: 2, end: 4, value: 1 }
          ]
        },
        0,
        4
      ),
    (error) => error.code === 'NATIVE_STATS_DATA_QUALITY' && /overlapping intervals/.test(error.message)
  );
});

test('returns zero standard deviation for a single interval', () => {
  const timeline = reconstructTimeline({ a: [{ begin: 0, end: 99, value: 7 }] }, 0, 99);
  assert.deepEqual(calculateDistributionStats(timeline, ['stddev', 'median']), { stddev: 0, median: 7 });
});

test('supports pointwise min and max aggregation', () => {
  const documents = {
    a: [{ begin: 0, end: 9, value: 10 }],
    b: [{ begin: 0, end: 9, value: 3 }]
  };
  assert.equal(reconstructTimeline(documents, 0, 9, 'min')[0].value, 3);
  assert.equal(reconstructTimeline(documents, 0, 9, 'max')[0].value, 10);
});

test('supports all aggregation modes across multiple metric IDs', () => {
  const documents = {
    a: [
      { begin: 0, end: 4, value: 10 },
      { begin: 5, end: 9, value: 20 }
    ],
    b: [
      { begin: 0, end: 4, value: 2 },
      { begin: 5, end: 9, value: 4 }
    ]
  };
  assert.deepEqual(
    reconstructTimeline(documents, 0, 9, 'sum').map((interval) => interval.value),
    [12, 24]
  );
  assert.deepEqual(
    reconstructTimeline(documents, 0, 9, 'avg').map((interval) => interval.value),
    [6, 12]
  );
  assert.deepEqual(
    reconstructTimeline(documents, 0, 9, 'min').map((interval) => interval.value),
    [2, 4]
  );
  assert.deepEqual(
    reconstructTimeline(documents, 0, 9, 'max').map((interval) => interval.value),
    [10, 20]
  );
});

test('uses duration for weighted percentiles', () => {
  const intervals = [
    { begin: 0, end: 0, duration: 1, value: 1 },
    { begin: 1, end: 9, duration: 9, value: 10 }
  ];
  assert.deepEqual(calculateDistributionStats(intervals, ['mean', 'median', 'p10']), {
    mean: 9.1,
    median: 10,
    p10: 1
  });
});

test('validates requested statistic names and percentile ranges', () => {
  assert.deepEqual(validateRequestedStats(['mean', 'p95']), ['mean', 'p95']);
  assert.throws(() => validateRequestedStats([]), /at least one/);
  assert.throws(() => validateRequestedStats(['p101']), /invalid distribution statistic/);
  assert.throws(() => validateRequestedStats(['variance']), /invalid distribution statistic/);
});

test('resamples the native timeline for values and preserves aggregation semantics', () => {
  const intervals = [
    { begin: 0, end: 4, duration: 5, value: 10 },
    { begin: 5, end: 9, duration: 5, value: 20 }
  ];
  assert.deepEqual(resampleTimeline(intervals, 0, 9, 2, 'sum'), [
    { begin: 0, end: 4, value: 10 },
    { begin: 5, end: 9, value: 20 }
  ]);
  assert.deepEqual(resampleTimeline(intervals, 0, 9, 2, 'min'), [
    { begin: 0, end: 4, value: 10 },
    { begin: 5, end: 9, value: 20 }
  ]);

  const misaligned = [
    { begin: 0, end: 2, duration: 3, value: 10 },
    { begin: 3, end: 9, duration: 7, value: 20 }
  ];
  assert.deepEqual(resampleTimeline(misaligned, 0, 9, 2, 'avg'), [
    { begin: 0, end: 4, value: 14 },
    { begin: 5, end: 9, value: 20 }
  ]);
});
