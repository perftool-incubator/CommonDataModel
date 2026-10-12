'use strict';

const periodField = 'period.period-uuid';

// Keep continuous descriptors queryable by time range while excluding metrics
// attached to other benchmark periods in the same run.
function periodScopedMetricDescriptorFilter(periodId) {
  if (periodId === undefined || periodId === null) return null;

  return {
    bool: {
      should: [{ term: { [periodField]: periodId } }, { bool: { must_not: [{ exists: { field: periodField } }] } }],
      minimum_should_match: 1
    }
  };
}

module.exports = { periodScopedMetricDescriptorFilter };
