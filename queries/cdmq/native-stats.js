'use strict';

const VALID_AGGREGATIONS = new Set(['sum', 'avg', 'min', 'max']);

function fail(message, code = 'NATIVE_STATS_DATA_QUALITY') {
  const error = new Error('Invalid metric timeline: ' + message);
  if (code) error.code = code;
  throw error;
}

function normalizeDocuments(documents, metricId, begin, end) {
  if (!Array.isArray(documents) || documents.length === 0) {
    fail('metric ID ' + metricId + ' has no documents');
  }

  const clipped = documents
    .map((document) => {
      if (!Number.isFinite(document.begin) || !Number.isFinite(document.end) || !Number.isFinite(document.value)) {
        fail('metric ID ' + metricId + ' contains a non-numeric document');
      }
      if (document.begin > document.end) {
        fail('metric ID ' + metricId + ' contains a reversed interval');
      }
      const clippedBegin = Math.max(begin, document.begin);
      const clippedEnd = Math.min(end, document.end);
      return clippedBegin <= clippedEnd ? { begin: clippedBegin, end: clippedEnd, value: document.value } : null;
    })
    .filter(Boolean)
    .sort((left, right) => left.begin - right.begin || left.end - right.end);

  if (clipped.length === 0) {
    fail('metric ID ' + metricId + ' does not cover the query range');
  }

  let nextBegin = begin;
  clipped.forEach((document) => {
    if (document.begin > nextBegin) {
      fail('metric ID ' + metricId + ' has a gap');
    }
    if (document.begin < nextBegin) {
      fail('metric ID ' + metricId + ' has overlapping intervals');
    }
    nextBegin = document.end + 1;
  });
  if (nextBegin <= end) {
    fail('metric ID ' + metricId + ' has a gap');
  }

  return clipped;
}

function aggregateValues(values, aggregation, metricCount) {
  if (aggregation === 'sum') return values.reduce((sum, value) => sum + value, 0);
  if (aggregation === 'avg') return values.reduce((sum, value) => sum + value, 0) / metricCount;
  if (aggregation === 'min') return Math.min(...values);
  return Math.max(...values);
}

function validateRequestedStats(requestedStats) {
  if (!Array.isArray(requestedStats) || requestedStats.length === 0) {
    fail('at least one distribution statistic must be requested');
  }
  requestedStats.forEach((stat) => {
    const percentile = typeof stat === 'string' && /^p(\d{1,3})$/.exec(stat);
    if (!['min', 'max', 'mean', 'median', 'stddev'].includes(stat) && (!percentile || Number(percentile[1]) > 100)) {
      fail('invalid distribution statistic ' + stat);
    }
  });
  return requestedStats;
}

/**
 * Reconstruct the piecewise-constant aggregate timeline for selected metric IDs.
 * Input documents use CDM's inclusive millisecond intervals.
 */
function reconstructTimeline(documentsById, begin, end, aggregation = 'sum', options = {}) {
  if (!Number.isInteger(begin) || !Number.isInteger(end) || begin > end) {
    fail('invalid query range');
  }
  if (!VALID_AGGREGATIONS.has(aggregation)) {
    fail('unsupported aggregation ' + aggregation);
  }

  const metricIds = Object.keys(documentsById);
  if (metricIds.length === 0) fail('no metric IDs were selected');
  const maxIntervals = options.maxIntervals || Infinity;

  const events = new Map();
  const addEvent = (time, type, metricId, value) => {
    if (!events.has(time)) events.set(time, { end: [], start: [] });
    events.get(time)[type].push({ metricId, value });
  };

  metricIds.forEach((metricId) => {
    normalizeDocuments(documentsById[metricId], metricId, begin, end).forEach((document) => {
      addEvent(document.begin, 'start', metricId, document.value);
      addEvent(document.end + 1, 'end', metricId, document.value);
    });
  });

  const active = new Map();
  const boundaries = [...events.keys()].sort((left, right) => left - right);
  const timeline = [];
  let previous = begin;

  boundaries.forEach((boundary) => {
    if (boundary > end + 1) return;
    if (boundary > previous) {
      if (active.size !== metricIds.length) {
        fail('aggregate timeline has a missing metric value');
      }
      if (timeline.length >= maxIntervals) {
        fail('native interval limit exceeded', 'NATIVE_STATS_LIMIT');
      }
      const values = [...active.values()];
      timeline.push({
        begin: previous,
        end: boundary - 1,
        duration: boundary - previous,
        value: aggregateValues(values, aggregation, metricIds.length)
      });
    }

    const event = events.get(boundary);
    event.end.forEach(({ metricId }) => active.delete(metricId));
    event.start.forEach(({ metricId, value }) => active.set(metricId, value));
    previous = boundary;
  });

  if (previous <= end) fail('aggregate timeline does not cover the query range');
  return timeline;
}

function percentile(intervals, quantile, totalDuration) {
  const target = quantile * totalDuration;
  let cumulative = 0;
  const ordered = [...intervals].sort((left, right) => left.value - right.value);
  for (const interval of ordered) {
    cumulative += interval.duration;
    if (cumulative >= target) return interval.value;
  }
  return ordered[ordered.length - 1].value;
}

/** Calculate requested duration-weighted statistics over a reconstructed timeline. */
function calculateDistributionStats(intervals, requestedStats) {
  if (!Array.isArray(intervals) || intervals.length === 0) fail('cannot calculate statistics for an empty timeline');
  validateRequestedStats(requestedStats);

  let totalDuration = 0;
  let mean = 0;
  let m2 = 0;
  let minimum = Infinity;
  let maximum = -Infinity;
  intervals.forEach((interval) => {
    if (!Number.isFinite(interval.value) || !Number.isFinite(interval.duration) || interval.duration <= 0) {
      fail('timeline contains an invalid interval');
    }
    const weight = interval.duration;
    const newTotal = totalDuration + weight;
    const delta = interval.value - mean;
    mean += (weight / newTotal) * delta;
    m2 += weight * delta * (interval.value - mean);
    totalDuration = newTotal;
    minimum = Math.min(minimum, interval.value);
    maximum = Math.max(maximum, interval.value);
  });

  const result = {};
  const standardDeviation = Math.sqrt(Math.max(0, m2 / totalDuration));
  requestedStats.forEach((stat) => {
    if (stat === 'min') result.min = minimum;
    else if (stat === 'max') result.max = maximum;
    else if (stat === 'mean') result.mean = mean;
    else if (stat === 'stddev') result.stddev = standardDeviation;
    else if (stat === 'median') result.median = percentile(intervals, 0.5, totalDuration);
    else {
      const match = /^p(\d{1,3})$/.exec(stat);
      if (!match || Number(match[1]) > 100) fail('invalid statistic ' + stat);
      result[stat] = percentile(intervals, Number(match[1]) / 100, totalDuration);
    }
  });
  return result;
}

function resampleTimeline(intervals, begin, end, resolution, aggregation) {
  if (!Number.isInteger(begin) || !Number.isInteger(end) || begin > end) fail('invalid query range');
  if (!Number.isInteger(resolution) || resolution <= 0) fail('resolution must be a positive integer');
  if (!VALID_AGGREGATIONS.has(aggregation)) fail('unsupported aggregation ' + aggregation);

  const windowDuration = Math.floor((end - begin) / resolution);
  if (windowDuration <= 0) fail('resolution is greater than the query duration');

  const values = [];
  let intervalIndex = 0;
  let windowBegin = begin;
  let windowEnd = begin + windowDuration;
  while (windowBegin <= end) {
    if (windowEnd > end) windowEnd = end;
    let weightedValue = 0;
    let totalWeight = 0;
    let extreme = aggregation === 'min' ? Infinity : -Infinity;
    while (intervalIndex < intervals.length && intervals[intervalIndex].end < windowBegin) intervalIndex++;
    for (let index = intervalIndex; index < intervals.length && intervals[index].begin <= windowEnd; index++) {
      const interval = intervals[index];
      const overlapBegin = Math.max(windowBegin, interval.begin);
      const overlapEnd = Math.min(windowEnd, interval.end);
      if (overlapBegin > overlapEnd) continue;
      const duration = overlapEnd - overlapBegin + 1;
      if (aggregation === 'min') extreme = Math.min(extreme, interval.value);
      else if (aggregation === 'max') extreme = Math.max(extreme, interval.value);
      else {
        weightedValue += interval.value * duration;
        totalWeight += duration;
      }
    }
    const value = aggregation === 'min' || aggregation === 'max' ? extreme : weightedValue / totalWeight;
    if (!Number.isFinite(value)) fail('native timeline does not cover a resolution window');
    values.push({ begin: windowBegin, end: windowEnd, value: value });
    windowBegin = windowEnd + 1;
    windowEnd += windowDuration + 1;
  }
  return values;
}

module.exports = {
  calculateDistributionStats,
  reconstructTimeline,
  resampleTimeline,
  validateRequestedStats
};
