'use strict';

// Client-side presentation only: this module neither queries CDM nor aggregates
// metric documents. Always pass the complete raw response for correct totals.
const labelCollator = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });

function compareLabels(a, b) {
  return labelCollator.compare(a, b);
}

function breakoutValues(label) {
  const parts = label.replace(/^</, '').replace(/>$/, '').split('>-<');
  return parts.length === 1 && parts[0] === '' ? [] : parts;
}

function parseValueFilter(filter) {
  if (filter == null) return () => true;
  const match = /^(gt|ge|lt|le):([-+]?(?:\d+\.?\d*|\.\d+)(?:e[-+]?\d+)?)$/i.exec(filter);
  if (!match || !Number.isFinite(Number(match[2]))) {
    throw new Error('filter must be gt:value, ge:value, lt:value, or le:value with a finite number');
  }
  const threshold = Number(match[2]);
  return (value) => {
    switch (match[1].toLowerCase()) {
      case 'gt':
        return value > threshold;
      case 'ge':
        return value >= threshold;
      case 'lt':
        return value < threshold;
      case 'le':
        return value <= threshold;
    }
  };
}

/**
 * Transform a raw POST /api/v1/metric-data response without mutating it.
 *
 * Options: sort ('label'|'value'), sortOrder ('asc'|'desc'), top (integer >= 0),
 * percentOfTotal (boolean), filter ('gt|ge|lt|le:number'). Value sorting and
 * filtering use the first window's raw value. Filtering precedes top-N.
 *
 * Returns { data, rows, totals, percentOfTotal }. data retains the response shape and metadata;
 * rows is the canonical display order (including for numeric-looking labels).
 * Each row has { label, breakoutValues, values }, retaining every time window.
 * Percentages are added as percentOfTotal on each point, using all input labels
 * in that same begin/end window. Zero totals produce null percentages.
 */
function transformMetricData(response, options = {}) {
  const { sort = 'label', sortOrder = 'asc', top, percentOfTotal = false, filter } = options;
  if (!['label', 'value'].includes(sort)) throw new Error('sort must be label or value');
  if (!['asc', 'desc'].includes(sortOrder)) throw new Error('sortOrder must be asc or desc');
  if (top !== undefined && (!Number.isSafeInteger(top) || top < 0)) {
    throw new Error('top must be a non-negative safe integer');
  }
  if (typeof percentOfTotal !== 'boolean') throw new Error('percentOfTotal must be a boolean');
  const passesFilter = parseValueFilter(filter);
  if (!response || !response.values || typeof response.values !== 'object' || Array.isArray(response.values)) {
    throw new Error('metric response must contain a values object');
  }

  const data = structuredClone(response);
  const totalsByWindow = new Map();
  const windowKey = (point) => JSON.stringify([point.begin, point.end]);
  let rows = Object.entries(data.values).map(([label, values]) => {
    if (!Array.isArray(values)) throw new Error('metric values must be arrays of time windows');
    for (const point of values) {
      if (!point || !Number.isFinite(Number(point.value)) || point.value == null || point.value === '') {
        throw new Error('metric window values must be finite numbers');
      }
      const key = windowKey(point);
      if (!totalsByWindow.has(key)) totalsByWindow.set(key, { begin: point.begin, end: point.end, value: 0 });
      totalsByWindow.get(key).value += Number(point.value);
    }
    return { label, breakoutValues: breakoutValues(label), values };
  });

  // Compute percentages before discarding any rows. Do not collapse windows or
  // use native distribution statistics as a substitute for resolution values.
  if (percentOfTotal) {
    for (const row of rows) {
      for (const point of row.values) {
        const total = totalsByWindow.get(windowKey(point)).value;
        point.percentOfTotal = total === 0 ? null : (Number(point.value) / total) * 100;
      }
    }
  }
  if (filter != null) rows = rows.filter((row) => row.values.length > 0 && passesFilter(Number(row.values[0].value)));
  const direction = sortOrder === 'asc' ? 1 : -1;
  rows.sort((a, b) => {
    const byLabel = compareLabels(a.label, b.label);
    if (sort === 'label') return direction * byLabel;
    // Empty series have no sortable value; display them last in either direction.
    if (!a.values.length || !b.values.length) return b.values.length - a.values.length || byLabel;
    return direction * (Number(a.values[0].value) - Number(b.values[0].value)) || byLabel;
  });
  if (top !== undefined) rows = rows.slice(0, top);
  data.values = Object.fromEntries(rows.map((row) => [row.label, row.values]));
  if (data.distributionStats) {
    data.distributionStats = Object.fromEntries(
      rows
        .filter((row) => Object.hasOwn(data.distributionStats, row.label))
        .map((row) => [row.label, data.distributionStats[row.label]])
    );
  }
  return { data, rows, totals: [...totalsByWindow.values()], percentOfTotal };
}

module.exports = { transformMetricData };
