'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { periodScopedMetricDescriptorFilter } = require('../metric-desc-selection');

function matchesPeriodFilter(filter, descriptor) {
  if (filter === null) return true;
  const periodId = descriptor.period && descriptor.period['period-uuid'];
  return filter.bool.should.some((condition) => {
    if (condition.term) {
      return periodId === condition.term['period.period-uuid'];
    }
    return periodId === undefined || periodId === null;
  });
}

test('period-scoped filter includes the requested period and periodless descriptors', () => {
  assert.deepEqual(periodScopedMetricDescriptorFilter('period-1'), {
    bool: {
      should: [
        { term: { 'period.period-uuid': 'period-1' } },
        { bool: { must_not: [{ exists: { field: 'period.period-uuid' } }] } }
      ],
      minimum_should_match: 1
    }
  });
});

test('sequential benchmark samples do not dilute a period-scoped average', () => {
  const descriptors = [
    { id: 'p1', period: { 'period-uuid': 'p1' }, value: 11 },
    { id: 'p2', period: { 'period-uuid': 'p2' }, value: 22 },
    { id: 'p3', period: { 'period-uuid': 'p3' }, value: 33 },
    { id: 'p4', period: { 'period-uuid': 'p4' }, value: 44 }
  ];
  const filter = periodScopedMetricDescriptorFilter('p1');
  const selected = descriptors.filter((descriptor) => matchesPeriodFilter(filter, descriptor));

  assert.deepEqual(
    selected.map((descriptor) => descriptor.id),
    ['p1']
  );
  assert.equal(selected.reduce((total, descriptor) => total + descriptor.value, 0) / selected.length, 11);
});

test('keeps concurrent series from the requested period in the average', () => {
  const descriptors = [
    { id: 'p1-a', period: { 'period-uuid': 'p1' }, value: 10 },
    { id: 'p1-b', period: { 'period-uuid': 'p1' }, value: 12 },
    { id: 'p2-a', period: { 'period-uuid': 'p2' }, value: 30 }
  ];
  const selected = descriptors.filter((descriptor) =>
    matchesPeriodFilter(periodScopedMetricDescriptorFilter('p1'), descriptor)
  );

  assert.deepEqual(
    selected.map((descriptor) => descriptor.id),
    ['p1-a', 'p1-b']
  );
  assert.equal(selected.reduce((total, descriptor) => total + descriptor.value, 0) / selected.length, 11);
});

test('keeps periodless continuous descriptors in a mixed metric family', () => {
  const descriptors = [
    { id: 'benchmark-p1', period: { 'period-uuid': 'p1' } },
    { id: 'benchmark-p2', period: { 'period-uuid': 'p2' } },
    { id: 'continuous-tool' }
  ];
  const selected = descriptors.filter((descriptor) =>
    matchesPeriodFilter(periodScopedMetricDescriptorFilter('p1'), descriptor)
  );

  assert.deepEqual(
    selected.map((descriptor) => descriptor.id),
    ['benchmark-p1', 'continuous-tool']
  );
});

test('descriptor selection without a period remains run-wide', () => {
  const descriptors = [
    { id: 'p1', period: { 'period-uuid': 'p1' } },
    { id: 'p2', period: { 'period-uuid': 'p2' } },
    { id: 'continuous-tool' }
  ];
  const filter = periodScopedMetricDescriptorFilter(undefined);

  assert.equal(filter, null);
  assert.deepEqual(
    descriptors.filter((descriptor) => matchesPeriodFilter(filter, descriptor)),
    descriptors
  );
  assert.equal(periodScopedMetricDescriptorFilter(null), null);
});
