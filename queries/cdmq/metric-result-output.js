'use strict';

const { sprintf } = require('sprintf-js');

function timestamp(end, dateFormat, timestampRows) {
  if (dateFormat === 'epoch_ms') return [String(end)];
  const date = new Date(end);
  const day = sprintf('%02d-%02d-%04d', date.getUTCDate(), date.getUTCMonth() + 1, date.getUTCFullYear());
  const time = sprintf('%02d:%02d:%02d', date.getUTCHours(), date.getUTCMinutes(), date.getUTCSeconds());
  return timestampRows === 2 ? [day, time] : [day + '/' + time];
}

function csvCell(value) {
  const text = String(value);
  return /[",\r\n]/.test(text) ? '"' + text.replace(/"/g, '""') + '"' : text;
}

/** Render transformMetricData's result without logging or changing it. */
function renderMetricData(result, options = {}) {
  const {
    outputFormat = 'table',
    source = '',
    type = '',
    dateFormat = 'default',
    decimalPlaces = 2,
    percentDecimalPlaces = decimalPlaces,
    outputContent = 'all',
    horizontalBreak = 'yes'
  } = options;
  if (!['json', 'table', 'csv'].includes(outputFormat)) throw new Error('outputFormat must be json, table, or csv');
  if (outputFormat === 'json') return JSON.stringify(result.data, null, 2);
  const digits = Number(decimalPlaces);
  if (!Number.isInteger(digits) || digits < 0 || digits > 100) {
    throw new Error('decimalPlaces must be an integer from 0 to 100');
  }
  const percentDigits = Number(percentDecimalPlaces);
  if (!Number.isInteger(percentDigits) || percentDigits < 0 || percentDigits > 100) {
    throw new Error('percentDecimalPlaces must be an integer from 0 to 100');
  }
  const timestampRows = dateFormat === 'epoch_ms' ? 1 : Number(options.timestampRows ?? 2);
  if (![1, 2].includes(timestampRows)) throw new Error('timestampRows must be 1 or 2');
  const windows = result.totals.slice().sort((a, b) => a.begin - b.begin || a.end - b.end);
  const percentages = result.percentOfTotal;
  const labels = Array.from({ length: timestampRows }, () => []);
  const values = Array.from({ length: timestampRows }, () => []);
  labels[timestampRows - 1] = ['source', 'type', ...(result.data.usedBreakouts || [])];
  if (timestampRows === 2) labels[0] = labels[1].map(() => '');
  for (const window of windows) {
    const parts = timestamp(window.end, dateFormat, timestampRows);
    parts.forEach((part, row) => {
      values[row].push(part);
      if (percentages) values[row].push(part + (row === timestampRows - 1 ? ' % of total' : ''));
    });
  }
  const windowKey = (point) => JSON.stringify([point.begin, point.end]);
  for (const row of result.rows) {
    labels.push([source, type, ...row.breakoutValues]);
    const points = new Map(row.values.map((point) => [windowKey(point), point]));
    const cells = [];
    for (const window of windows) {
      const point = points.get(windowKey(window));
      cells.push(point ? Number(point.value).toFixed(digits) : '');
      if (percentages) {
        cells.push(point?.percentOfTotal == null ? '' : point.percentOfTotal.toFixed(percentDigits));
      }
    }
    values.push(cells);
  }

  const start = outputContent === 'values' ? timestampRows : 0;
  const end = outputContent === 'headers' ? timestampRows : labels.length;
  const lines = [];
  if (outputFormat === 'csv') {
    for (let row = start; row < end; row++) lines.push([...labels[row], ...values[row]].map(csvCell).join(','));
    return lines.join('\n');
  }
  const widths = (rows) => {
    const lengths = [];
    for (const row of rows) row.forEach((cell, col) => (lengths[col] = Math.max(lengths[col] || 0, cell.length)));
    return lengths;
  };
  const labelWidths = widths(labels);
  const valueWidths = widths(values);
  const pad = (cells, lengths) => cells.map((cell, col) => ' ' + cell.padStart(lengths[col])).join('');
  for (let row = start; row < end; row++) {
    if (horizontalBreak === 'yes' && row === timestampRows) {
      lines.push('-'.repeat([...labelWidths, ...valueWidths].reduce((total, width) => total + width + 1, 0)));
    }
    lines.push(pad(labels[row], labelWidths) + pad(values[row], valueWidths));
  }
  if (
    result.data.distributionStats &&
    Object.keys(result.data.distributionStats).length &&
    outputContent !== 'headers'
  ) {
    lines.push('\nDistribution statistics (native timeline):');
    for (const row of result.rows) {
      const stats = result.data.distributionStats[row.label];
      if (!stats) continue;
      const formatted = Object.keys(stats)
        .map((stat) => stat + '=' + Number(stats[stat]).toFixed(digits))
        .join(' ');
      lines.push('  ' + (row.label || '<all>') + ': ' + formatted);
    }
  }
  return lines.join('\n');
}

module.exports = { renderMetricData };
