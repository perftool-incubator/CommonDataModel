import React, { useState, useEffect, useMemo, useCallback, useRef, useImperativeHandle, forwardRef } from 'react';
import { buildIterItems } from '../utils/iterLabel';
import { ComposedChart, Bar, Line, XAxis, YAxis, CartesianGrid, Tooltip, ErrorBar, ResponsiveContainer, Legend, Cell, ReferenceLine, LabelList } from 'recharts';
import * as api from '../api/cdm';
import { timeWork } from '../debugLog';

const COLORS = [
  '#5b8def', '#4ade80', '#fbbf24', '#f87171', '#a78bfa',
  '#34d399', '#fb923c', '#f472b6', '#38bdf8', '#facc15',
  '#818cf8', '#2dd4bf', '#e879f9', '#f97316', '#a3e635',
];

const SUPP_COLORS = ['#f97316', '#e879f9', '#14b8a6', '#ef4444', '#8b5cf6', '#06b6d4'];
const ITER_THEME_BASES = ['#5b8def', '#ef5b5b', '#5bef8d', '#b85bef', '#5bcdef', '#efb85b'];
const MAX_DEEP_DIVE_ITERS = 6;

// Smart Y-axis tick formatter: adjusts decimal precision based on value magnitude
function formatYTick(value) {
  if (value == null) return '';
  var abs = Math.abs(value);
  if (abs === 0) return '0';
  if (abs >= 1000000) return (value / 1000000).toFixed(1) + 'M';
  if (abs >= 10000) return (value / 1000).toFixed(1) + 'k';
  if (abs >= 100) return Math.round(value).toString();
  if (abs >= 10) return value.toFixed(1);
  if (abs >= 1) return value.toFixed(2);
  if (abs >= 0.1) return value.toFixed(3);
  return value.toPrecision(3);
}

// Compact value for bar labels — max 4 significant digits
function formatBarLabel(v) {
  if (v == null) return '';
  v = Number(v);
  if (isNaN(v)) return '';
  var abs = Math.abs(v);
  if (abs === 0) return '0';
  if (abs >= 1000000) return (v / 1000000).toPrecision(3) + 'M';
  if (abs >= 1000) return (v / 1000).toPrecision(3) + 'k';
  if (abs >= 100) return v.toPrecision(4);
  if (abs >= 1) return v.toPrecision(3);
  return v.toPrecision(2);
}

function formatValue(v) {
  if (v == null) return '';
  v = Number(v);
  if (isNaN(v)) return '';
  if (Math.abs(v) >= 1000) return v.toFixed(0);
  if (Math.abs(v) >= 1) return v.toFixed(2);
  return v.toPrecision(3);
}

function getDimValue(it, dim) {
  if (!dim || dim === 'none') return '__all__';
  if (dim === 'run') return it.runId;
  if (dim === 'benchmark') return it.benchmark || '';
  if (dim.startsWith('param:')) {
    var arg = dim.substring(6);
    var p = (it.params || []).find(function (pp) { return pp.arg === arg; });
    return p ? String(p.val) : '';
  }
  if (dim.startsWith('tag:')) {
    var name = dim.substring(4);
    var t = (it.tags || []).find(function (tt) { return tt.name === name; });
    return t ? t.val : '';
  }
  return '';
}

function formatDimLabel(dim) {
  if (!dim || dim === 'none') return '';
  if (dim === 'run') return 'Run';
  if (dim === 'benchmark') return 'Benchmark';
  if (dim.startsWith('param:')) return dim.substring(6);
  if (dim.startsWith('tag:')) return dim.substring(4);
  return dim;
}

function formatDimValue(dim, val) {
  if (dim === 'run') return val ? val.substring(0, 8) : val;
  return val || '(empty)';
}

// Compute which params and tags are common (same value across all iterations)
// vs varying (different values). Returns { common: [{key,val}], varyingKeys: Set }
function computeCommonVarying(iters, hiddenSet) {
  if (iters.length === 0) return { common: [], varyingKeys: new Set() };
  var hidden = hiddenSet || new Set();

  var runIds = new Set();
  var benchmarks = new Set();
  var paramValues = {};
  var tagValues = {};

  iters.forEach(function (it) {
    if (it.runId && !hidden.has('run')) runIds.add(it.runId);
    if (it.benchmark && !hidden.has('benchmark')) benchmarks.add(it.benchmark);
    (it.params || []).forEach(function (p) {
      if (hidden.has('param:' + p.arg)) return;
      if (!paramValues[p.arg]) paramValues[p.arg] = new Set();
      paramValues[p.arg].add(String(p.val));
    });
    (it.tags || []).forEach(function (t) {
      if (hidden.has('tag:' + t.name)) return;
      if (!tagValues[t.name]) tagValues[t.name] = new Set();
      tagValues[t.name].add(t.val);
    });
  });

  var common = [];
  var varyingKeys = new Set();

  // Run
  if (runIds.size > 1) {
    varyingKeys.add('run');
  }

  // Benchmark
  if (benchmarks.size === 1) {
    common.push({ key: 'benchmark', val: Array.from(benchmarks)[0], type: 'benchmark' });
  } else if (benchmarks.size > 1) {
    varyingKeys.add('benchmark');
  }

  Object.keys(paramValues).sort().forEach(function (arg) {
    if (paramValues[arg].size === 1) {
      common.push({ key: arg, val: Array.from(paramValues[arg])[0], type: 'param' });
    } else {
      varyingKeys.add('param:' + arg);
    }
  });
  Object.keys(tagValues).sort().forEach(function (name) {
    if (tagValues[name].size === 1) {
      common.push({ key: name, val: Array.from(tagValues[name])[0], type: 'tag' });
    } else {
      varyingKeys.add('tag:' + name);
    }
  });

  return { common: common, varyingKeys: varyingKeys };
}

// Build label from an iteration showing only varying params/tags/benchmark
// that are NOT already shown by the group-by or series-by dimensions
function buildIterLabel(it, varyingKeys, excludeKeys) {
  // Collect varying params and tags, then group by value to consolidate
  // e.g., bs=4k, rw=4k, size=4k becomes bs,rw,size=4k
  var items = [];
  if (varyingKeys.has('benchmark') && !excludeKeys.has('benchmark')) {
    items.push({ name: 'benchmark', val: it.benchmark || '' });
  }
  (it.params || []).forEach(function (p) {
    var key = 'param:' + p.arg;
    if (varyingKeys.has(key) && !excludeKeys.has(key)) {
      items.push({ name: p.arg, val: String(p.val) });
    }
  });
  (it.tags || []).forEach(function (t) {
    var key = 'tag:' + t.name;
    if (varyingKeys.has(key) && !excludeKeys.has(key)) {
      items.push({ name: t.name, val: t.val });
    }
  });
  // Group names that share the same value
  var byVal = {};
  var valOrder = [];
  items.forEach(function (item) {
    if (!byVal[item.val]) {
      byVal[item.val] = [];
      valOrder.push(item.val);
    }
    byVal[item.val].push(item.name);
  });
  var parts = [];
  valOrder.forEach(function (val) {
    parts.push(byVal[val].join(',') + '=' + val);
  });
  return parts.join(', ') || it.iterationId.substring(0, 8);
}

function computeStddev(mv) {
  if (!mv || !mv.sampleValues || mv.sampleValues.length <= 1 || mv.mean == null) return 0;
  var mean = mv.mean;
  var variance = 0;
  for (var v = 0; v < mv.sampleValues.length; v++) {
    variance += (mv.sampleValues[v] - mean) * (mv.sampleValues[v] - mean);
  }
  return Math.sqrt(variance / (mv.sampleValues.length - 1));
}

// Natural sort: compare as numbers when both values are numeric, otherwise as strings
function naturalCompare(a, b) {
  var na = Number(a);
  var nb = Number(b);
  if (!isNaN(na) && !isNaN(nb)) return na - nb;
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

// Custom X-axis tick that wraps long labels into multiple lines
function WrappedAxisTick(props) {
  var x = props.x, y = props.y, payload = props.payload;
  if (!payload || !payload.value) return null;

  var value = String(payload.value);
  var segments = value.split(', ');
  var lines = [];
  var current = '';
  for (var i = 0; i < segments.length; i++) {
    if (current && (current + ', ' + segments[i]).length > 30) {
      lines.push(current);
      current = segments[i];
    } else {
      current = current ? current + ', ' + segments[i] : segments[i];
    }
  }
  if (current) lines.push(current);

  return (
    <g transform={'translate(' + x + ',' + y + ')'}>
      <text
        textAnchor="end"
        fontSize={11}
        fill="var(--text-secondary)"
        transform="rotate(-30)"
      >
        {lines.map(function (line, li) {
          return <tspan key={li} x={0} dy={li === 0 ? 0 : 14}>{line}</tspan>;
        })}
      </text>
    </g>
  );
}

// Build group info including per-group common items
// (items that vary globally but are the same within this group)
function buildGroupInfo(groupValue, size, iters, globalVaryingKeys, excludeKeys) {
  // groupValue is already the formatted compound label (e.g., "nthreads=1, gro=on")
  var label = groupValue;

  if (iters.length <= 1) {
    return { label: label, size: size, groupCommon: [] };
  }

  // Find params/tags that are in globalVaryingKeys but common within this group
  var groupCommon = [];
  var paramValues = {};
  var tagValues = {};
  var benchmarks = new Set();

  iters.forEach(function (it) {
    if (it.benchmark) benchmarks.add(it.benchmark);
    (it.params || []).forEach(function (p) {
      var key = 'param:' + p.arg;
      if (!globalVaryingKeys.has(key) || excludeKeys.has(key)) return;
      if (!paramValues[p.arg]) paramValues[p.arg] = new Set();
      paramValues[p.arg].add(String(p.val));
    });
    (it.tags || []).forEach(function (t) {
      var key = 'tag:' + t.name;
      if (!globalVaryingKeys.has(key) || excludeKeys.has(key)) return;
      if (!tagValues[t.name]) tagValues[t.name] = new Set();
      tagValues[t.name].add(t.val);
    });
  });

  if (globalVaryingKeys.has('benchmark') && !excludeKeys.has('benchmark') && benchmarks.size === 1) {
    groupCommon.push(Array.from(benchmarks)[0]);
  }
  Object.keys(paramValues).sort().forEach(function (arg) {
    if (paramValues[arg].size === 1) {
      groupCommon.push(arg + '=' + Array.from(paramValues[arg])[0]);
    }
  });
  Object.keys(tagValues).sort().forEach(function (name) {
    if (tagValues[name].size === 1) {
      groupCommon.push(name + '=' + Array.from(tagValues[name])[0]);
    }
  });

  return { label: label, size: size, groupCommon: groupCommon };
}

// Compute compound group key from multiple group-by dimensions
function getCompoundGroupValue(it, groupByList) {
  if (!groupByList || groupByList.length === 0) return '__all__';
  return groupByList.map(function (dim) {
    return formatDimLabel(dim) + '=' + formatDimValue(dim, getDimValue(it, dim));
  }).join(', ');
}

function hasGroupBy(groupByList) {
  return groupByList && groupByList.length > 0;
}

// Parse a breakout label like "<host1>-<0>" into segments ["<host1>", "<0>"]
function parseBreakoutSegments(label) {
  if (!label) return [];
  // Match all <...> segments
  var matches = label.match(/<[^>]*>/g);
  return matches || [label];
}

// Render breakout items as a table with rowSpan for repeated segment values
// items: [{ segments: ["<host1>", "<0>"], value: "45.2", color: "#..." }, ...]
// breakoutNames: ["hostname", "package"]
function renderGroupedBreakouts(items, depth, breakoutNames) {
  if (items.length === 0) return null;

  // Build rows: each row has parsed segment values and the metric value
  var rows = items.map(function (it) {
    var segVals = it.segments.map(function (s) { return s.replace(/^</, '').replace(/>$/, ''); });
    return { segVals: segVals, value: it.value, color: it.color };
  });

  // Sort rows by segments (natural sort, left to right)
  rows.sort(function (a, b) {
    for (var i = 0; i < Math.max(a.segVals.length, b.segVals.length); i++) {
      var cmp = naturalCompare(a.segVals[i] || '', b.segVals[i] || '');
      if (cmp !== 0) return cmp;
    }
    return 0;
  });

  var numCols = rows.length > 0 ? rows[0].segVals.length : 0;

  // Compute rowSpans for each cell
  // rowSpans[row][col] = number of rows this cell spans, or 0 if hidden (spanned by cell above)
  var rowSpans = rows.map(function () { return new Array(numCols).fill(1); });
  for (var col = 0; col < numCols; col++) {
    for (var row = rows.length - 1; row > 0; row--) {
      // Check if this cell and all cells to its left match the row above
      var matches = true;
      for (var c = 0; c <= col; c++) {
        if (rows[row].segVals[c] !== rows[row - 1].segVals[c]) { matches = false; break; }
      }
      if (matches) {
        rowSpans[row][col] = 0; // hidden
        rowSpans[row - 1][col] += rowSpans[row][col] || 1; // not right, need to find the span start
      }
    }
  }
  // Recompute spans properly: scan top-down
  for (var col2 = 0; col2 < numCols; col2++) {
    rowSpans.forEach(function (r) { r[col2] = 1; }); // reset
    var spanStart = 0;
    for (var row2 = 1; row2 <= rows.length; row2++) {
      var same = row2 < rows.length;
      if (same) {
        for (var c2 = 0; c2 <= col2; c2++) {
          if (rows[row2].segVals[c2] !== rows[spanStart].segVals[c2]) { same = false; break; }
        }
      }
      if (!same) {
        rowSpans[spanStart][col2] = row2 - spanStart;
        for (var r2 = spanStart + 1; r2 < row2; r2++) rowSpans[r2][col2] = 0;
        spanStart = row2;
      }
    }
  }

  // Build column headers and deduplicate common suffixes from text values
  var headers = [];
  var commonSuffixes = [];
  for (var h = 0; h < numCols; h++) {
    var nameEntry = (breakoutNames && h < breakoutNames.length) ? breakoutNames[h] : '';
    var name = (typeof nameEntry === 'object' && nameEntry !== null && nameEntry.name) ? nameEntry.name : String(nameEntry);
    if (name.indexOf('=') >= 0) name = name.substring(0, name.indexOf('='));

    // Collect unique values for this column
    var uniqueVals = [];
    var seen = {};
    rows.forEach(function (r) {
      var v = r.segVals[h] || '';
      if (!seen[v]) { seen[v] = true; uniqueVals.push(v); }
    });

    var suffix = '';
    var prefix = '';
    var delimiters = '.,-_/';
    // Only dedupe if: >1 unique value, all look like text (not purely numeric)
    if (uniqueVals.length > 1 && !uniqueVals.every(function (v) { return /^\d+$/.test(v); })) {
      // Try common suffix first (at a delimiter boundary)
      var first = uniqueVals[0];
      for (var si = first.length - 1; si > 0; si--) {
        if (delimiters.indexOf(first[si]) >= 0 || delimiters.indexOf(first[si - 1]) >= 0) {
          var candSuffix = first.substring(si);
          if (candSuffix.length >= 2 && uniqueVals.every(function (v) { return v.endsWith(candSuffix); })) {
            suffix = candSuffix;
          }
        }
      }
      // If no suffix found, try common prefix (at a delimiter boundary)
      if (!suffix) {
        for (var pi = 1; pi < first.length; pi++) {
          if (delimiters.indexOf(first[pi]) >= 0 || delimiters.indexOf(first[pi - 1]) >= 0) {
            var candPrefix = first.substring(0, pi + 1);
            if (candPrefix.length >= 2 && uniqueVals.every(function (v) { return v.startsWith(candPrefix); })) {
              prefix = candPrefix;
            }
          }
        }
      }
    }

    // Strip suffix or prefix from row values
    if (suffix) {
      rows.forEach(function (r) {
        if (r.segVals[h]) r.segVals[h] = r.segVals[h].substring(0, r.segVals[h].length - suffix.length);
      });
    } else if (prefix) {
      rows.forEach(function (r) {
        if (r.segVals[h]) r.segVals[h] = r.segVals[h].substring(prefix.length);
      });
    }

    commonSuffixes.push({ suffix: suffix, prefix: prefix });
    headers.push(name);
  }

  // Recompute rowSpans after suffix stripping may have changed values
  for (var col3 = 0; col3 < numCols; col3++) {
    var spanStart2 = 0;
    for (var row3 = 0; row3 <= rows.length; row3++) {
      var same2 = row3 < rows.length;
      if (same2) {
        for (var c3 = 0; c3 <= col3; c3++) {
          if (rows[row3].segVals[c3] !== rows[spanStart2].segVals[c3]) { same2 = false; break; }
        }
      }
      if (!same2) {
        rowSpans[spanStart2][col3] = row3 - spanStart2;
        for (var r3 = spanStart2 + 1; r3 < row3; r3++) rowSpans[r3][col3] = 0;
        spanStart2 = row3;
      }
    }
  }

  return (
    <table className="compare-sidebar-table">
      <thead>
        <tr>
          {headers.map(function (hdr, hi) {
            var cs = commonSuffixes[hi];
            return (
              <th key={hi}>
                {cs.prefix && <span className="compare-sidebar-table-affix">{cs.prefix}</span>}
                {hdr}
                {cs.suffix && <span className="compare-sidebar-table-affix">{cs.suffix}</span>}
              </th>
            );
          })}
          <th>value</th>
        </tr>
      </thead>
      <tbody>
        {rows.map(function (row, ri) {
          return (
            <tr key={ri}>
              {row.segVals.map(function (sv, ci) {
                if (rowSpans[ri][ci] === 0) return null;
                var span = rowSpans[ri][ci];
                return <td key={ci} rowSpan={span} className="compare-sidebar-table-seg">
                  {span > 1 ? <div className="compare-sidebar-seg-sticky">{sv}</div> : sv}
                </td>;
              })}
              <td className="compare-sidebar-table-val" style={{ color: row.color }}>{row.value}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function buildDimOptions(iterations) {
  var opts = [{ value: 'none', label: 'None' }];
  // Only include dimensions that have more than one distinct value
  var runs = new Set();
  var benchmarks = new Set();
  var paramValues = {};
  var tagValues = {};
  for (var i = 0; i < iterations.length; i++) {
    var it = iterations[i];
    if (it.runId) runs.add(it.runId);
    if (it.benchmark) benchmarks.add(it.benchmark);
    (it.params || []).forEach(function (p) {
      if (!paramValues[p.arg]) paramValues[p.arg] = new Set();
      paramValues[p.arg].add(String(p.val));
    });
    (it.tags || []).forEach(function (t) {
      if (!tagValues[t.name]) tagValues[t.name] = new Set();
      tagValues[t.name].add(t.val);
    });
  }
  if (runs.size > 1) opts.push({ value: 'run', label: 'Run' });
  if (benchmarks.size > 1) opts.push({ value: 'benchmark', label: 'Benchmark' });
  Object.keys(paramValues).sort().forEach(function (arg) {
    if (paramValues[arg].size > 1) opts.push({ value: 'param:' + arg, label: 'Param: ' + arg });
  });
  Object.keys(tagValues).sort().forEach(function (name) {
    if (tagValues[name].size > 1) opts.push({ value: 'tag:' + name, label: 'Tag: ' + name });
  });
  return opts;
}

const CompareView = forwardRef(function CompareView({ selected, groupByList, setGroupByList, hiddenFields, setHiddenFields, restoredMetrics, setRestoredMetrics, supplementalMetrics, setSupplementalMetrics, deepDiveMetrics, setDeepDiveMetrics, deepDiveIterations, setDeepDiveIterations }, ref) {
  var [metricValues, setMetricValues] = useState({});
  var [loading, setLoading] = useState(false);
  var [availableSources, setAvailableSources] = useState(null);
  var [availableTypes, setAvailableTypes] = useState(null);
  var [addMetricSource, setAddMetricSource] = useState('');
  var [addMetricType, setAddMetricType] = useState('');
  var [addMetricLoading, setAddMetricLoading] = useState(false);
  var [addMetricDisplay, setAddMetricDisplay] = useState('panel'); // 'overlay' or 'panel'
  var [showAddMetric, setShowAddMetric] = useState(false);
  var [pinnedEntry, setPinnedEntry] = useState(null);
  var [breakoutValueCache, setBreakoutValueCache] = useState({}); // { "source::type": { "hostname": ["h1","h2"], ... } }
  var [openBreakoutDropdown, setOpenBreakoutDropdown] = useState(null); // index of metric with open dropdown
  var [breakoutSelections, setBreakoutSelections] = useState({}); // { "dimName": Set of selected values }
  var [breakoutRegex, setBreakoutRegex] = useState({}); // { "dimName": "regexString" }
  var [breakoutAggregate, setBreakoutAggregate] = useState({}); // { "dimName": bool }
  var breakoutDropdownRef = useRef(null);

  // Close breakout dropdown on outside click
  useEffect(function () {
    if (openBreakoutDropdown == null) return;
    function handleClick(e) {
      if (breakoutDropdownRef.current && !breakoutDropdownRef.current.contains(e.target)) {
        setOpenBreakoutDropdown(null);
      }
    }
    document.addEventListener('mousedown', handleClick);
    return function () { document.removeEventListener('mousedown', handleClick); };
  }, [openBreakoutDropdown]);

  var iterations = useMemo(function () {
    return Array.from(selected.values());
  }, [selected]);

  // Helper to get run IDs and date range from iterations
  function getRunContext() {
    var runIdSet = new Set();
    iterations.forEach(function (it) { runIdSet.add(it.runId); });
    var runIds = Array.from(runIdSet);
    var begins = iterations.filter(function (it) { return it.runBegin; }).map(function (it) { return Number(it.runBegin); });
    var startDate = begins.length > 0 ? new Date(Math.min.apply(null, begins)) : null;
    var endDate = begins.length > 0 ? new Date(Math.max.apply(null, begins)) : null;
    var start = startDate ? startDate.getFullYear() + '.' + String(startDate.getMonth() + 1).padStart(2, '0') : null;
    var end = endDate ? endDate.getFullYear() + '.' + String(endDate.getMonth() + 1).padStart(2, '0') : null;
    var iterPairs = iterations.map(function (it) { return { iterationId: it.iterationId, runId: it.runId }; });
    return { runIds: runIds, start: start, end: end, iterations: iterPairs };
  }

  useEffect(function () {
    if (iterations.length === 0) return;
    var ctx = getRunContext();
    setLoading(true);
    timeWork('Fetch metric values for compare (' + iterations.length + ' iterations)', function () {
      return api.getIterationMetricValues(ctx.runIds, ctx.start, ctx.end);
    }).then(function (res) {
      setMetricValues(res.values || {});
    }).catch(function (err) {
      console.error('Failed to fetch metric values:', err);
    }).finally(function () {
      setLoading(false);
    });
  }, [iterations]);

  var hiddenSet = useMemo(function () { return new Set(hiddenFields); }, [hiddenFields]);

  var dimOptions = useMemo(function () {
    return buildDimOptions(iterations).filter(function (o) { return !hiddenSet.has(o.value); });
  }, [iterations, hiddenSet]);

  // All dimension options (including hidden) for the hide field picker
  var allDimOptions = useMemo(function () {
    return buildDimOptions(iterations).filter(function (o) { return o.value !== 'none'; });
  }, [iterations]);

  var handleAutoGroup = useCallback(function () {
    // Compute distinct value counts for each varying dimension
    var dimCounts = [];
    dimOptions.forEach(function (o) {
      if (o.value === 'none') return;
      var vals = new Set();
      iterations.forEach(function (it) {
        var v = getDimValue(it, o.value);
        if (v !== '') vals.add(v); // skip empty (missing tag/param)
      });
      if (vals.size > 1) {
        dimCounts.push({ value: o.value, count: vals.size });
      }
    });
    // Sort by distinct count ascending (fewest values = best grouping level)
    dimCounts.sort(function (a, b) { return a.count - b.count; });
    // All varying dimensions participate in group-by
    if (dimCounts.length > 0) {
      setGroupByList(dimCounts.map(function (d) { return d.value; }));
    }
  }, [iterations, dimOptions]);

  // Auto-group on first render when no group-by is set
  useEffect(function () {
    if (groupByList.length === 0 && iterations.length > 0 && dimOptions.length > 1) {
      handleAutoGroup();
    }
  }, [iterations.length > 0 && dimOptions.length > 1]);

  // Fetch values for supplemental metrics that have empty values (e.g., hydrated from URL with configs only)
  useEffect(function () {
    if (iterations.length === 0) return;
    if (supplementalMetrics.length === 0) return;
    var needsFetch = supplementalMetrics.some(function (m) { return !m.values || Object.keys(m.values).length === 0; });
    if (!needsFetch) return;
    var ctx = getRunContext();
    supplementalMetrics.forEach(function (sm, si) {
      if (sm.values && Object.keys(sm.values).length > 0) return;
      var bestIndices = computeBestSampleIndices();
      var sIdx = sm.sampleIndex != null ? sm.sampleIndex : bestIndices;
      timeWork('Fetch ' + sm.source + '::' + sm.type, function () {
        return api.getSupplementalMetric({
          iterations: ctx.iterations, start: ctx.start, end: ctx.end,
          source: sm.source, type: sm.type,
          breakout: sm.breakouts || [],
          filter: sm.filter || null,
          sampleIndex: sIdx,
        });
      }).then(function (res) {
        setSupplementalMetrics(function (prev) {
          var next = prev.slice();
          var idx = next.findIndex(function (m) { return m.source === sm.source && m.type === sm.type; });
          if (idx >= 0) {
            next[idx] = Object.assign({}, next[idx], {
              values: res.values || {},
              remainingBreakouts: res.remainingBreakouts || [],
              loading: false,
            });
          }
          return next;
        });
      });
    });
  }, [iterations.length > 0, supplementalMetrics.length]);

  var handleShowAddMetric = useCallback(function () {
    setShowAddMetric(true);
    setAddMetricSource('');
    setAddMetricType('');
    setAvailableTypes(null);
    if (!availableSources) {
      var ctx = getRunContext();
      api.getIterationMetricSources(ctx.runIds, ctx.start, ctx.end).then(function (res) {
        setAvailableSources(res.sources || []);
      });
    }
  }, [iterations, availableSources]);

  var handleSourceChange = useCallback(function (source) {
    setAddMetricSource(source);
    setAddMetricType('');
    setAvailableTypes(null);
    if (source) {
      var ctx = getRunContext();
      api.getIterationMetricTypes(ctx.runIds, ctx.start, ctx.end, source).then(function (res) {
        setAvailableTypes(res.types || []);
      });
    }
  }, [iterations]);

  // Compute best sample index per iteration from primary metric values (closest to mean)
  function computeBestSampleIndices() {
    var indices = {};
    for (var itId in metricValues) {
      var mv = metricValues[itId];
      if (mv && mv.sampleValues && mv.sampleValues.length > 1) {
        var sum = 0;
        for (var v = 0; v < mv.sampleValues.length; v++) sum += mv.sampleValues[v];
        var mean = sum / mv.sampleValues.length;
        var bestDiff = Infinity;
        var bestIdx = 0;
        for (var s = 0; s < mv.sampleValues.length; s++) {
          var diff = Math.abs(mv.sampleValues[s] - mean);
          if (diff < bestDiff) { bestDiff = diff; bestIdx = s; }
        }
        indices[itId] = bestIdx;
      } else {
        indices[itId] = 0;
      }
    }
    return indices;
  }

  // Backward-compatible: single best index (from first iteration with multiple samples)
  function computeBestSampleIndex() {
    var indices = computeBestSampleIndices();
    for (var itId in indices) {
      var mv = metricValues[itId];
      if (mv && mv.sampleValues && mv.sampleValues.length > 1) return indices[itId];
    }
    return 0;
  }

  var handleAddMetric = useCallback(function () {
    if (!addMetricSource || !addMetricType) return;
    var exists = supplementalMetrics.some(function (m) { return m.source === addMetricSource && m.type === addMetricType; });
    if (exists) { setShowAddMetric(false); return; }
    var ctx = getRunContext();
    var bestIndices = computeBestSampleIndices();
    setAddMetricLoading(true);
    timeWork('Fetch ' + addMetricSource + '::' + addMetricType, function () {
      return api.getSupplementalMetric({ iterations: ctx.iterations, start: ctx.start, end: ctx.end, source: addMetricSource, type: addMetricType, sampleIndex: bestIndices });
    }).then(function (res) {
      setSupplementalMetrics(function (prev) {
        return prev.concat([{
          source: addMetricSource,
          type: addMetricType,
          values: res.values || {},
          display: addMetricDisplay,
          chartType: 'bar',         // 'bar', 'stacked', 'line'
          filter: '',               // e.g., 'gt:0.01', 'lt:100'
          sampleIndex: bestIndices, // per-iteration best sample indices
          breakouts: [],            // active breakout dimensions
          remainingBreakouts: res.remainingBreakouts || [],
          loading: false,
        }]);
      });
      setShowAddMetric(false);
    }).catch(function (err) {
      console.error('Failed to fetch supplemental metric:', err);
    }).finally(function () {
      setAddMetricLoading(false);
    });
  }, [iterations, addMetricSource, addMetricType, addMetricDisplay, supplementalMetrics]);

  // Fetch breakout distinct values for a metric's remaining breakouts (lazy, cached)
  function fetchBreakoutValues(source, type, breakoutNames) {
    var cacheKey = source + '::' + type;
    if (breakoutValueCache[cacheKey]) return; // already fetched
    if (!breakoutNames || breakoutNames.length === 0) return;
    var ctx = getRunContext();
    api.getBreakoutValues({
      runIds: ctx.runIds, start: ctx.start, end: ctx.end,
      source: source, type: type, breakouts: breakoutNames,
    }).then(function (res) {
      setBreakoutValueCache(function (prev) {
        var next = Object.assign({}, prev);
        next[cacheKey] = res.breakouts || {};
        return next;
      });
    }).catch(function (err) {
      console.error('Failed to fetch breakout values:', err);
    });
  }

  var handleAddBreakout = useCallback(function (si, breakoutName) {
    var sm = supplementalMetrics[si];
    var newBreakouts = sm.breakouts.concat([breakoutName]);
    // Mark as loading
    setSupplementalMetrics(function (prev) {
      var next = prev.slice();
      next[si] = Object.assign({}, next[si], { loading: true });
      return next;
    });
    var ctx = getRunContext();
    timeWork('Breakout ' + sm.source + '::' + sm.type + ' by ' + breakoutName, function () {
      return api.getSupplementalMetric({ iterations: ctx.iterations, start: ctx.start, end: ctx.end, source: sm.source, type: sm.type, breakout: newBreakouts, filter: sm.filter, sampleIndex: sm.sampleIndex });
    }).then(function (res) {
      setSupplementalMetrics(function (prev) {
        var next = prev.slice();
        next[si] = Object.assign({}, next[si], {
          values: res.values || {},
          sampleInfo: res.sampleInfo || next[si].sampleInfo,
          breakouts: newBreakouts,
          remainingBreakouts: res.remainingBreakouts || [],
          loading: false,
        });
        return next;
      });
    }).catch(function (err) {
      console.error('Failed to add breakout:', err);
      setSupplementalMetrics(function (prev) {
        var next = prev.slice();
        next[si] = Object.assign({}, next[si], { loading: false });
        return next;
      });
    });
  }, [iterations, supplementalMetrics]);

  var handleRemoveBreakout = useCallback(function (si, breakoutIdx) {
    var sm = supplementalMetrics[si];
    var newBreakouts = sm.breakouts.slice(0, breakoutIdx);
    setSupplementalMetrics(function (prev) {
      var next = prev.slice();
      next[si] = Object.assign({}, next[si], { loading: true });
      return next;
    });
    var ctx = getRunContext();
    timeWork('Remove breakout from ' + sm.source + '::' + sm.type, function () {
      return api.getSupplementalMetric({ iterations: ctx.iterations, start: ctx.start, end: ctx.end, source: sm.source, type: sm.type, breakout: newBreakouts, filter: sm.filter, sampleIndex: sm.sampleIndex });
    }).then(function (res) {
      setSupplementalMetrics(function (prev) {
        var next = prev.slice();
        next[si] = Object.assign({}, next[si], {
          values: res.values || {},
          sampleInfo: res.sampleInfo || next[si].sampleInfo,
          breakouts: newBreakouts,
          remainingBreakouts: res.remainingBreakouts || [],
          loading: false,
        });
        return next;
      });
    });
  }, [iterations, supplementalMetrics]);

  var handleSampleChange = useCallback(function (si, newSampleIndex, iterationId) {
    var sm = supplementalMetrics[si];
    var newIndices;
    if (newSampleIndex === 'auto') {
      newIndices = computeBestSampleIndices();
    } else if (iterationId) {
      // Per-iteration override: update just this iteration's sample index
      newIndices = typeof sm.sampleIndex === 'object' && sm.sampleIndex ? Object.assign({}, sm.sampleIndex) : computeBestSampleIndices();
      newIndices[iterationId] = parseInt(newSampleIndex, 10);
    } else {
      // Global override: set all iterations to the same index
      var idx = parseInt(newSampleIndex, 10);
      newIndices = {};
      iterations.forEach(function (it) { newIndices[it.iterationId] = idx; });
    }
    setSupplementalMetrics(function (prev) {
      var next = prev.slice();
      next[si] = Object.assign({}, next[si], { sampleIndex: newIndices, loading: true });
      return next;
    });
    var ctx = getRunContext();
    timeWork('Switch sample for ' + sm.source + '::' + sm.type, function () {
      return api.getSupplementalMetric({ iterations: ctx.iterations, start: ctx.start, end: ctx.end, source: sm.source, type: sm.type, breakout: sm.breakouts, filter: sm.filter, sampleIndex: newIndices });
    }).then(function (res) {
      setSupplementalMetrics(function (prev) {
        var next = prev.slice();
        next[si] = Object.assign({}, next[si], {
          values: res.values || {},
          sampleInfo: res.sampleInfo || next[si].sampleInfo,
          remainingBreakouts: res.remainingBreakouts || [],
          loading: false,
        });
        return next;
      });
    }).catch(function (err) {
      console.error('Failed to switch sample:', err);
      setSupplementalMetrics(function (prev) {
        var next = prev.slice();
        next[si] = Object.assign({}, next[si], { loading: false });
        return next;
      });
    });
  }, [iterations, supplementalMetrics]);

  // Update metric filter value locally (no re-query yet)
  var handleUpdateFilter = useCallback(function (si, newFilter) {
    setSupplementalMetrics(function (prev) {
      var next = prev.slice();
      next[si] = Object.assign({}, next[si], { filter: newFilter });
      return next;
    });
  }, []);

  // Apply metric filter (re-query)
  var handleApplyFilter = useCallback(function (si) {
    var sm = supplementalMetrics[si];
    setSupplementalMetrics(function (prev) {
      var next = prev.slice();
      next[si] = Object.assign({}, next[si], { loading: true });
      return next;
    });
    var ctx = getRunContext();
    timeWork('Apply filter for ' + sm.source + '::' + sm.type, function () {
      return api.getSupplementalMetric({ iterations: ctx.iterations, start: ctx.start, end: ctx.end, source: sm.source, type: sm.type, breakout: sm.breakouts, filter: sm.filter, sampleIndex: sm.sampleIndex });
    }).then(function (res) {
      setSupplementalMetrics(function (prev) {
        var next = prev.slice();
        next[si] = Object.assign({}, next[si], {
          values: res.values || {},
          remainingBreakouts: res.remainingBreakouts || [],
          loading: false,
        });
        return next;
      });
    }).catch(function (err) {
      console.error('Failed to apply filter:', err);
      setSupplementalMetrics(function (prev) {
        var next = prev.slice();
        next[si] = Object.assign({}, next[si], { loading: false });
        return next;
      });
    });
  }, [iterations, supplementalMetrics]);

  // Update breakout filter value locally (no re-query yet)
  var handleUpdateBreakoutFilter = useCallback(function (si, bi, newBreakout) {
    setSupplementalMetrics(function (prev) {
      var next = prev.slice();
      var breakouts = next[si].breakouts.slice();
      breakouts[bi] = newBreakout;
      next[si] = Object.assign({}, next[si], { breakouts: breakouts });
      return next;
    });
  }, []);

  // Re-query metric with current breakout filters
  var handleApplyBreakoutFilter = useCallback(function (si) {
    var sm = supplementalMetrics[si];
    setSupplementalMetrics(function (prev) {
      var next = prev.slice();
      next[si] = Object.assign({}, next[si], { loading: true });
      return next;
    });
    var ctx = getRunContext();
    timeWork('Apply breakout filter for ' + sm.source + '::' + sm.type, function () {
      return api.getSupplementalMetric({ iterations: ctx.iterations, start: ctx.start, end: ctx.end, source: sm.source, type: sm.type, breakout: sm.breakouts, filter: sm.filter, sampleIndex: sm.sampleIndex });
    }).then(function (res) {
      setSupplementalMetrics(function (prev) {
        var next = prev.slice();
        next[si] = Object.assign({}, next[si], {
          values: res.values || {},
          remainingBreakouts: res.remainingBreakouts || [],
          loading: false,
        });
        return next;
      });
    }).catch(function (err) {
      console.error('Failed to apply breakout filter:', err);
      setSupplementalMetrics(function (prev) {
        var next = prev.slice();
        next[si] = Object.assign({}, next[si], { loading: false });
        return next;
      });
    });
  }, [iterations, supplementalMetrics]);

  var handleChartTypeChange = useCallback(function (si, chartType) {
    setSupplementalMetrics(function (prev) {
      var next = prev.slice();
      next[si] = Object.assign({}, next[si], { chartType: chartType });
      return next;
    });
  }, []);

  var handleRemoveMetric = useCallback(function (idx) {
    setSupplementalMetrics(function (prev) {
      var removed = prev[idx];
      if (removed && deepDiveMetrics) {
        var metricKey = removed.source + '::' + removed.type;
        if (deepDiveMetrics.has(metricKey)) {
          setDeepDiveMetrics(function (prevDD) {
            var next = new Set(prevDD);
            next.delete(metricKey);
            return next;
          });
        }
      }
      return prev.filter(function (_, i) { return i !== idx; });
    });
  }, [deepDiveMetrics]);

  // Build chart data: one entry per iteration, sorted/grouped, with gaps between groups
  var charts = useMemo(function () {
    var byMetric = {};
    for (var i = 0; i < iterations.length; i++) {
      var it = iterations[i];
      var pm = it.primaryMetric || 'unknown';
      if (!byMetric[pm]) byMetric[pm] = [];
      byMetric[pm].push(it);
    }

    var result = [];
    Object.keys(byMetric).forEach(function (metricName) {
      var iters = byMetric[metricName];

      // Compute common vs varying params/tags across these iterations
      var cv = computeCommonVarying(iters, hiddenSet);
      var varyingKeys = cv.varyingKeys;
      var commonItems = cv.common;

      // Sort by each group-by dimension individually (natural/numeric sort per dimension)
      var sorted = iters.slice().sort(function (a, b) {
        for (var gi = 0; gi < groupByList.length; gi++) {
          var va = getDimValue(a, groupByList[gi]);
          var vb = getDimValue(b, groupByList[gi]);
          var cmp = naturalCompare(va, vb);
          if (cmp !== 0) return cmp;
        }
        return 0;
      });


      // Precompute per-group common keys (items that vary globally but are common within a group)
      var perGroupCommonKeys = {};
      if (hasGroupBy(groupByList)) {
        var groupedIters = {};
        sorted.forEach(function (it) {
          var gv = getCompoundGroupValue(it, groupByList);
          if (!groupedIters[gv]) groupedIters[gv] = [];
          groupedIters[gv].push(it);
        });
        Object.keys(groupedIters).forEach(function (gv) {
          var gIters = groupedIters[gv];
          if (gIters.length <= 1) { perGroupCommonKeys[gv] = new Set(); return; }
          var pv = {};
          var tv = {};
          gIters.forEach(function (it) {
            (it.params || []).forEach(function (p) {
              if (!pv[p.arg]) pv[p.arg] = new Set();
              pv[p.arg].add(String(p.val));
            });
            (it.tags || []).forEach(function (t) {
              if (!tv[t.name]) tv[t.name] = new Set();
              tv[t.name].add(t.val);
            });
          });
          var common = new Set();
          Object.keys(pv).forEach(function (arg) {
            if (pv[arg].size === 1 && varyingKeys.has('param:' + arg)) common.add('param:' + arg);
          });
          Object.keys(tv).forEach(function (name) {
            if (tv[name].size === 1 && varyingKeys.has('tag:' + name)) common.add('tag:' + name);
          });
          perGroupCommonKeys[gv] = common;
        });
      }

      // Build chart data with gap entries between groups
      var chartData = [];
      var prevGroup = null;
      for (var i = 0; i < sorted.length; i++) {
        var it = sorted[i];
        var gv = getCompoundGroupValue(it, groupByList);

        // No gap insertion — spanning chips show grouping visually
        prevGroup = gv;

        var mv = metricValues[it.iterationId];
        var mean = mv ? mv.mean : null;
        var stddev = computeStddev(mv);
        // Build label excluding: group-by and per-group common keys.
        var excludeKeys = new Set();
        groupByList.forEach(function (dim) { excludeKeys.add(dim); });
        hiddenSet.forEach(function (dim) { excludeKeys.add(dim); });
        var groupCommon = perGroupCommonKeys[gv];
        if (groupCommon) groupCommon.forEach(function (k) { excludeKeys.add(k); });
        var label = buildIterLabel(it, varyingKeys, excludeKeys);

        var entry = {
          name: label,
          value: mean,
          errorY: stddev,
          iterationId: it.iterationId,
          stddevPct: mv ? mv.stddevPct : null,
          samples: mv ? mv.sampleValues.length : 0,
          groupValue: gv,
          color: COLORS[i % COLORS.length],
          isGap: false,
        };
        // Add supplemental metric values with stddev for error bars
        // Format: sm.values[iterId] = { labels: { label: { mean, stddevPct, sampleValues } } }
        supplementalMetrics.forEach(function (sm, si) {
          var smv = sm.values[it.iterationId];
          if (smv && smv.labels) {
            var labelKeys = Object.keys(smv.labels);
            // Use the first label for the aggregate value (works for no-breakout case)
            if (labelKeys.length >= 1) {
              var lv = smv.labels[labelKeys[0]];
              entry['supp_' + si] = lv.mean;
              entry['supp_' + si + '_stddevPct'] = lv.stddevPct;
              entry['supp_' + si + '_error'] = computeStddev(lv);
              entry['supp_' + si + '_samples'] = lv.sampleValues ? lv.sampleValues.length : 0;
            }
            // Store per-label data for breakout sidebar display
            if (labelKeys.length >= 1) {
              labelKeys.forEach(function (lk) {
                var lv = smv.labels[lk];
                entry['supp_' + si + '_' + lk] = lv.mean;
              });
            }
          } else {
            entry['supp_' + si] = null;
            entry['supp_' + si + '_stddevPct'] = null;
            entry['supp_' + si + '_error'] = 0;
            entry['supp_' + si + '_samples'] = 0;
          }
        });
        chartData.push(entry);
      }


      // Compute group sizes and per-group common items for labels above the chart
      var groupInfo = [];
      if (hasGroupBy(groupByList)) {
        // Collect iterations per group
        var groupIters = {};
        sorted.forEach(function (it) {
          var gv = getCompoundGroupValue(it, groupByList);
          if (!groupIters[gv]) groupIters[gv] = [];
          groupIters[gv].push(it);
        });
        // Keys to exclude from per-group common: group-by dims, series-by
        var excludeFromGroupCommon = new Set();
        groupByList.forEach(function (dim) { excludeFromGroupCommon.add(dim); });

        var currentGroup = null;
        var currentCount = 0;
        chartData.forEach(function (d) {
          if (d.isGap) return;
          if (d.groupValue !== currentGroup) {
            if (currentGroup !== null) {
              var gi = buildGroupInfo(currentGroup, currentCount, groupIters[currentGroup] || [], varyingKeys, excludeFromGroupCommon);
              groupInfo.push(gi);
            }
            currentGroup = d.groupValue;
            currentCount = 0;
          }
          currentCount++;
        });
        if (currentGroup !== null) {
          var gi = buildGroupInfo(currentGroup, currentCount, groupIters[currentGroup] || [], varyingKeys, excludeFromGroupCommon);
          groupInfo.push(gi);
        }
      }

      result.push({ metricName: metricName, data: chartData, commonItems: commonItems, groupInfo: groupInfo, varyingKeys: varyingKeys, sortedIterations: sorted });
    });

    return result;
  }, [iterations, metricValues, groupByList, supplementalMetrics, hiddenSet]);

  // Resolve the pinned entry from current chart data so sidebars always
  // read fresh data (pinnedEntry.entry may be stale after chart recomputation)
  var resolvedPinnedEntry = useMemo(function () {
    if (!pinnedEntry || !pinnedEntry.entry) return null;
    var itId = pinnedEntry.entry.iterationId;
    for (var ci = 0; ci < charts.length; ci++) {
      for (var di = 0; di < charts[ci].data.length; di++) {
        if (charts[ci].data[di].iterationId === itId) {
          return { entry: charts[ci].data[di], metricName: pinnedEntry.metricName };
        }
      }
    }
    return pinnedEntry; // fallback to original if not found
  }, [pinnedEntry, charts]);

  if (loading) {
    return (
      <div className="compare-view">
        <div className="compare-loading"><span className="spinner" /> Loading metric values...</div>
      </div>
    );
  }

  if (iterations.length === 0) {
    return (
      <div className="compare-view">
        <div className="empty-msg">Select iterations from the Search view to compare.</div>
      </div>
    );
  }

  function renderMetricControls(sm, si) {
    var color = SUPP_COLORS[si % SUPP_COLORS.length];
    return (
      <div key={'ctrl-' + si} className="compare-metric-row" style={{ borderLeftColor: color }}>
        <div className="compare-metric-row-header">
          {sm.loading && <span className="spinner" style={{ marginLeft: 8 }} />}
          {!sm.loading && sm.remainingBreakouts && sm.remainingBreakouts.length > 0 && (function () {
            var cacheKey = sm.source + '::' + sm.type;
            var bvCache = breakoutValueCache[cacheKey] || {};
            var isOpen = openBreakoutDropdown === si;
            // Sort: multi-value breakouts first, single-value last; alphabetical within each group
            var sortedBreakouts = sm.remainingBreakouts.slice().sort(function (a, b) {
              var ca = bvCache[a] ? bvCache[a].length : -1;
              var cb = bvCache[b] ? bvCache[b].length : -1;
              var aMulti = ca === -1 || ca > 1 ? 1 : 0;
              var bMulti = cb === -1 || cb > 1 ? 1 : 0;
              if (aMulti !== bMulti) return bMulti - aMulti;
              if (a < b) return -1;
              if (a > b) return 1;
              return 0;
            });
            return (
              <div className="breakout-dropdown-wrap" ref={isOpen ? breakoutDropdownRef : undefined}>
                <button className="btn btn-sm btn-secondary breakout-dropdown-trigger" onClick={function () {
                  if (isOpen) {
                    setOpenBreakoutDropdown(null);
                    setBreakoutSelections({});
                    setBreakoutRegex({});
                    setBreakoutAggregate({});
                  } else {
                    setOpenBreakoutDropdown(si);
                    setBreakoutSelections({});
                    setBreakoutRegex({});
                    setBreakoutAggregate({});
                    fetchBreakoutValues(sm.source, sm.type, sm.remainingBreakouts);
                  }
                }}>+ Breakout</button>
                {isOpen && (
                  <div className="breakout-dropdown-menu">
                    {sortedBreakouts.map(function (b) {
                      var vals = bvCache[b];
                      var count = vals ? vals.length : null;
                      var isSingle = count === 1;
                      var selected_vals = breakoutSelections[b]; // Set or undefined
                      var hasSelection = selected_vals && selected_vals.size > 0;
                      var allSelected = hasSelection && vals && selected_vals.size === vals.length;
                      return (
                        <div key={b} className={'breakout-dropdown-item' + (isSingle ? ' breakout-single' : '')}>
                          <div className="breakout-dropdown-values">
                            <span className="breakout-dropdown-label">{b}</span>
                            {!isSingle && vals && vals.length > 1 && (
                              <input
                                className={'breakout-regex-input' + (breakoutRegex[b] && (function () { try { new RegExp(breakoutRegex[b]); return false; } catch (e) { return true; } })() ? ' breakout-regex-invalid' : '')}
                                type="text"
                                placeholder="regex filter"
                                value={breakoutRegex[b] || ''}
                                onClick={function (e) { e.stopPropagation(); }}
                                onKeyDown={function (e) { e.stopPropagation(); }}
                                onChange={function (e) {
                                  var pattern = e.target.value;
                                  var dimName = b;
                                  setBreakoutRegex(function (prev) {
                                    var next = Object.assign({}, prev);
                                    if (pattern) { next[dimName] = pattern; } else { delete next[dimName]; }
                                    return next;
                                  });
                                  if (!pattern) {
                                    setBreakoutSelections(function (prev) {
                                      var next = Object.assign({}, prev);
                                      delete next[dimName];
                                      return next;
                                    });
                                    return;
                                  }
                                  try {
                                    var re = new RegExp(pattern);
                                    var matching = new Set();
                                    if (vals) {
                                      vals.forEach(function (v) { if (re.test(v)) matching.add(v); });
                                    }
                                    setBreakoutSelections(function (prev) {
                                      var next = Object.assign({}, prev);
                                      if (matching.size > 0) { next[dimName] = matching; } else { delete next[dimName]; }
                                      return next;
                                    });
                                  } catch (e) { /* invalid regex — leave selections unchanged */ }
                                }}
                              />
                            )}
                            <span className={'breakout-dropdown-val breakout-val-all' + (!hasSelection || allSelected ? ' breakout-val-selected' : ' breakout-val-unselected')}
                              onClick={function (e) {
                                e.stopPropagation();
                                setOpenBreakoutDropdown(null);
                                setBreakoutSelections({});
                                setBreakoutRegex({});
                    setBreakoutAggregate({});
                                handleAddBreakout(si, { name: b });
                              }}
                            >all</span>
                            {vals && vals.map(function (v) {
                              var isSelected = hasSelection && selected_vals.has(v);
                              return (
                                <span key={v}
                                  className={'breakout-dropdown-val' + (isSelected ? ' breakout-val-selected' : '') + (hasSelection && !isSelected && !allSelected ? ' breakout-val-unselected' : '')}
                                  onClick={function (e) {
                                    e.stopPropagation();
                                    setBreakoutSelections(function (prev) {
                                      var next = Object.assign({}, prev);
                                      var set = next[b] ? new Set(next[b]) : new Set();
                                      if (set.has(v)) { set.delete(v); } else { set.add(v); }
                                      if (set.size === 0) { delete next[b]; } else { next[b] = set; }
                                      return next;
                                    });
                                  }}
                                >{v}</span>
                              );
                            })}
                            {hasSelection && !allSelected && (
                              <>
                              <label className="breakout-aggregate-check" onClick={function (e) { e.stopPropagation(); }}>
                                <input type="checkbox" checked={!!breakoutAggregate[b]} onChange={function () {
                                  setBreakoutAggregate(function (prev) {
                                    var next = Object.assign({}, prev);
                                    next[b] = !prev[b];
                                    return next;
                                  });
                                }} />
                                <span>Sum</span>
                              </label>
                              <button className="btn btn-sm btn-secondary breakout-dropdown-add" onClick={function (e) {
                                e.stopPropagation();
                                var breakoutObj = { name: b, values: Array.from(selected_vals) };
                                if (breakoutAggregate[b]) breakoutObj.aggregate = true;
                                setOpenBreakoutDropdown(null);
                                setBreakoutSelections({});
                                setBreakoutRegex({});
                                setBreakoutAggregate({});
                                handleAddBreakout(si, breakoutObj);
                              }}>Add</button>
                              </>
                            )}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            );
          })()}
          {sm.breakouts.length > 0 && (
            <select className="compare-breakout-select" value={sm.chartType || 'bar'} onChange={function (e) { handleChartTypeChange(si, e.target.value); }}>
              <option value="bar">Bars</option>
              <option value="stacked">Stacked</option>
              <option value="line">Lines</option>
            </select>
          )}
          {(function () {
            // Check if any iteration has multiple samples
            var hasMultiSample = iterations.some(function (it) {
              var mv2 = metricValues[it.iterationId];
              return mv2 && mv2.sampleValues && mv2.sampleValues.length > 1;
            });
            if (!hasMultiSample) return null;
            // Determine if all iterations use auto-best
            var currentIndices = typeof sm.sampleIndex === 'object' && sm.sampleIndex ? sm.sampleIndex : null;
            var bestIndices = computeBestSampleIndices();
            var isAuto = !currentIndices || iterations.every(function (it) {
              return (currentIndices[it.iterationId] == null || currentIndices[it.iterationId] === bestIndices[it.iterationId]);
            });
            return (
              <span className="compare-filter-group">
                <label className="compare-filter-label">Sample:</label>
                <select className="compare-breakout-select" value={isAuto ? 'auto' : 'custom'} onChange={function (e) { if (e.target.value === 'auto') handleSampleChange(si, 'auto'); }}>
                  <option value="auto">Best (auto)</option>
                  <option value="custom" disabled>Per-iteration</option>
                </select>
              </span>
            );
          })()}
          <span className="compare-filter-group">
            <label className="compare-filter-label">Filter:</label>
            <input className="compare-filter-input" type="text" placeholder="e.g. gt:0.01" value={sm.filter || ''} title="gt:N, ge:N, lt:N, le:N"
              onClick={function (e) { e.stopPropagation(); }}
              onChange={function (e) { handleUpdateFilter(si, e.target.value); }}
              onKeyDown={function (e) { if (e.key === 'Enter') { e.preventDefault(); handleApplyFilter(si); } }}
            />
            {sm.filter && (
              <button className="btn btn-sm btn-secondary" onClick={function () { handleApplyFilter(si); }} disabled={sm.loading} style={{ fontSize: 10, padding: '2px 6px' }}>Apply</button>
            )}
          </span>
          <button className="compare-metric-remove" onClick={function () { handleRemoveMetric(si); }}>&times;</button>
        </div>
        {sm.breakouts.length > 0 && (
          <div className="compare-metric-breakouts">
            {sm.breakouts.map(function (b, bi) {
              var isObj = typeof b === 'object' && b !== null && b.name;
              var fieldName = isObj ? b.name : (b.indexOf('=') >= 0 ? b.substring(0, b.indexOf('=')) : b);
              var filterVal = isObj ? (b.values ? b.values.join('+') : (b.regex || '')) : (b.indexOf('=') >= 0 ? b.substring(b.indexOf('=') + 1) : '');
              var isAggregate = isObj && b.aggregate;
              return (
                <span key={bi} className={'compare-breakout-chip' + (isAggregate ? ' compare-breakout-aggregate' : '')}>
                  <span className="compare-breakout-field">{fieldName}{isAggregate ? ' (sum)' : ''}</span>
                  <input className="compare-breakout-filter" type="text" placeholder="all" value={filterVal}
                    title="Filter values"
                    onClick={function (e) { e.stopPropagation(); }}
                    onChange={function (e) {
                      var newVal = e.target.value;
                      if (isObj) {
                        var updated = Object.assign({}, b);
                        if (newVal) { updated.values = newVal.split('+'); delete updated.regex; } else { delete updated.values; delete updated.regex; }
                        handleUpdateBreakoutFilter(si, bi, updated);
                      } else {
                        handleUpdateBreakoutFilter(si, bi, newVal ? fieldName + '=' + newVal : fieldName);
                      }
                    }}
                    onKeyDown={function (e) { if (e.key === 'Enter') { e.preventDefault(); handleApplyBreakoutFilter(si); } }}
                  />
                  <button onClick={function () { handleRemoveBreakout(si, bi); }}>&times;</button>
                </span>
              );
            })}
            <button className="btn btn-sm btn-secondary" onClick={function () { handleApplyBreakoutFilter(si); }} disabled={sm.loading} style={{ fontSize: 10, padding: '2px 6px' }}>Apply</button>
          </div>
        )}
        {/* Per-iteration sample overrides (expandable) */}
        {(function () {
          var hasMultiSample = iterations.some(function (it) {
            var mv2 = metricValues[it.iterationId];
            return mv2 && mv2.sampleValues && mv2.sampleValues.length > 1;
          });
          if (!hasMultiSample) return null;
          var currentIndices = typeof sm.sampleIndex === 'object' && sm.sampleIndex ? sm.sampleIndex : {};
          var bestIndices = computeBestSampleIndices();
          var hasOverride = iterations.some(function (it) {
            return currentIndices[it.iterationId] != null && currentIndices[it.iterationId] !== bestIndices[it.iterationId];
          });
          return (
            <details className="compare-sample-details">
              <summary className="compare-sample-summary">
                Per-iteration samples{hasOverride ? ' (customized)' : ''}
              </summary>
              <div className="compare-sample-list">
                {iterations.map(function (it) {
                  var mv2 = metricValues[it.iterationId];
                  if (!mv2 || !mv2.sampleValues || mv2.sampleValues.length <= 1) return null;
                  var currentIdx = currentIndices[it.iterationId] != null ? currentIndices[it.iterationId] : (bestIndices[it.iterationId] || 0);
                  var label = buildIterLabel(it, charts.length > 0 ? charts[0].varyingKeys : new Set(), new Set());
                  return (
                    <div key={it.iterationId} className="compare-sample-row">
                      <span className="compare-sample-iter-label" title={it.iterationId}>{label || it.iterationId.substring(0, 8)}</span>
                      <select className="compare-breakout-select" value={currentIdx} onChange={function (e) { handleSampleChange(si, e.target.value, it.iterationId); }}>
                        {mv2.sampleValues.map(function (pmv, idx2) {
                          var slabel = 'Sample ' + (idx2 + 1);
                          if (pmv != null) slabel += ' (' + formatValue(pmv) + ')';
                          if (idx2 === (bestIndices[it.iterationId] || 0)) slabel += ' *';
                          return <option key={idx2} value={idx2}>{slabel}</option>;
                        })}
                      </select>
                    </div>
                  );
                })}
              </div>
            </details>
          );
        })()}
      </div>
    );
  }

  return (
    <div className="compare-view">
      {charts.map(function (chart, ci) {
        var nonGapData = chart.data.filter(function (d) { return !d.isGap; });
        if (nonGapData.length === 0) {
          return (
            <div key={ci} className="compare-chart-panel">
              <h3>{chart.metricName}</h3>
              <div className="empty-msg">No metric values available for these iterations.</div>
            </div>
          );
        }

        // Cap chart height at 50% of viewport width to prevent overly tall charts
        var maxHeight = Math.floor(window.innerWidth * 0.3);
        var chartHeight = Math.min(Math.max(300, nonGapData.length * 30 + 150), maxHeight);
        var hasOverlays = supplementalMetrics.some(function (m) { return m.display !== 'panel'; });

        return (
          <div key={ci} className="compare-chart-panel">
            <h3>{chart.metricName}</h3>

            {chart.commonItems.length > 0 && (
              <div className="compare-subtitle">
                <span className="compare-subtitle-label">Common:</span>
                {chart.commonItems.map(function (c, ci) {
                  return (
                    <span key={ci} className={c.type === 'benchmark' ? 'benchmark-badge' : c.type === 'tag' ? 'tag' : 'param param-common'}>
                      {c.type === 'tag' && <span className="tag-key">{c.key}</span>}
                      {c.type === 'tag' ? '=' + c.val : c.type === 'benchmark' ? c.val : c.key + '=' + c.val}
                    </span>
                  );
                })}
              </div>
            )}


            {/* Panel-mode supplemental metrics: rendered above the primary chart */}
            {supplementalMetrics.map(function (sm, si) {
              if (sm.display !== 'panel') return null;
              var color = SUPP_COLORS[si % SUPP_COLORS.length];
              var dataKey = 'supp_' + si;
              var vals = [];
              chart.data.forEach(function (d) {
                if (d.isGap) return;
                if (d[dataKey] != null) vals.push(d[dataKey]);
                Object.keys(d).forEach(function (k) {
                  if (k.startsWith(dataKey + '_') && !k.endsWith('_stddevPct') && !k.endsWith('_error') && !k.endsWith('_samples') && d[k] != null) {
                    vals.push(d[k]);
                  }
                });
              });
              var min = vals.length > 0 ? Math.min.apply(null, vals) : 0;
              var max = vals.length > 0 ? Math.max.apply(null, vals) : 1;
              var pad = (max - min) * 0.1 || 0.1;
              return (
                <React.Fragment key={'panel-top-' + si}>
                {renderMetricControls(sm, si)}
                <div className="compare-panel-metric">
                  <div className="compare-chart-with-labels">
                    <div className="compare-yaxis-label compare-yaxis-left" style={{ color: color }}>
                      {sm.source}::{sm.type}
                      {deepDiveMetrics && (
                        <label className="compare-yaxis-dive" onClick={function (e) { e.stopPropagation(); }}>
                          <input type="checkbox" checked={deepDiveMetrics.has(sm.source + '::' + sm.type)} onChange={function () {
                            var metricKey = sm.source + '::' + sm.type;
                            setDeepDiveMetrics(function (prev) {
                              var next = new Set(prev);
                              if (next.has(metricKey)) next.delete(metricKey); else next.add(metricKey);
                              return next;
                            });
                          }} />
                          <span>Dive</span>
                        </label>
                      )}
                    </div>
                    <div className="compare-chart-area" style={{ width: Math.max(600, nonGapData.length * 120 + 120), flex: 'none' }}>
                  <ResponsiveContainer width="100%" height={180}>
                    <ComposedChart data={chart.data} margin={{ top: 10, right: 30, left: 60, bottom: 5 }} barCategoryGap="10%">
                      <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                      <XAxis dataKey="name" hide={true} />
                      <YAxis
                        yAxisId="left"
                        domain={[Math.max(0, min - pad), max + pad]}
                        tick={{ fontSize: 11, fill: 'var(--text-secondary)' }}
                        tickFormatter={formatYTick}
                        stroke="var(--border)"
                      />
                      <Tooltip
                        content={function (props) {
                          if (!props.active || !props.payload || props.payload.length === 0) return null;
                          var entry = props.payload[0].payload;
                          if (!entry || entry.isGap) return null;
                          return (
                            <div className="compare-tooltip-mini">
                              {entry.name}
                            </div>
                          );
                        }}
                      />
                      {hasOverlays ? (
                        <YAxis yAxisId="right" orientation="right" width={80} tick={false} axisLine={false} />
                      ) : (
                        <YAxis yAxisId="right" orientation="right" width={1} tick={false} axisLine={false} />
                      )}
                      {pinnedEntry && pinnedEntry.entry && pinnedEntry.entry.name && (
                        <ReferenceLine x={pinnedEntry.entry.name} yAxisId="left" stroke="#ff6b6b" strokeDasharray="6 4" strokeWidth={2} />
                      )}
                      {(function () {
                        if (sm.breakouts.length > 0) {
                          var labelSet = new Set();
                          chart.data.forEach(function (d) {
                            if (d.isGap) return;
                            Object.keys(d).forEach(function (k) {
                              var prefix = dataKey + '_';
                              if (k.startsWith(prefix) && !k.endsWith('_stddevPct') && !k.endsWith('_error') && !k.endsWith('_samples')) {
                                labelSet.add(k);
                              }
                            });
                          });
                          var labels = Array.from(labelSet).sort(naturalCompare);
                          var ct = sm.chartType || 'bar';
                          if (labels.length > 0) {
                            return labels.map(function (lk, li) {
                              var labelName = lk.substring((dataKey + '_').length);
                              var itemColor = SUPP_COLORS[(si + li) % SUPP_COLORS.length];
                              if (ct === 'line') {
                                return (
                                  <Line key={lk} dataKey={lk} yAxisId="left" type="monotone"
                                    stroke={itemColor} strokeWidth={2}
                                    dot={{ r: 4, fill: itemColor }}
                                    connectNulls={false} name={labelName} />
                                );
                              }
                              return (
                                <Bar key={lk} dataKey={lk} yAxisId="left"
                                  radius={ct === 'stacked' ? [0, 0, 0, 0] : [3, 3, 0, 0]}
                                  stackId={ct === 'stacked' ? 'stack' : undefined}
                                  name={labelName} style={{ cursor: 'pointer' }}
                                  onClick={function (data) {
                                    if (data && !data.isGap) {
                                      setPinnedEntry(function (prev) {
                                        if (prev && prev.entry && prev.entry.iterationId === data.iterationId) return null;
                                        return { entry: data, metricName: chart.metricName };
                                      });
                                    }
                                  }}>
                                  <LabelList dataKey={lk} content={function (props) {
                                    if (ct === 'stacked') {
                                      var val3 = props.value;
                                      var w3 = props.width;
                                      var h3 = props.height;
                                      if (val3 == null || w3 == null || h3 == null) return null;
                                      var text3 = formatBarLabel(val3);
                                      if (text3.length * 8 > w3 - 4 || Math.abs(h3) < 14) return null;
                                      return (
                                        <text x={props.x + w3 / 2} y={props.y + h3 / 2} textAnchor="middle" dominantBaseline="middle"
                                          fontSize={12} fontWeight={700} fontFamily="ui-monospace, Consolas, monospace"
                                          fill="rgba(255,255,255,0.9)">{text3}</text>
                                      );
                                    }
                                    var val2 = props.value;
                                    var w2 = props.width;
                                    var h2 = props.height;
                                    if (val2 == null || w2 == null || h2 == null) return null;
                                    var text2 = formatBarLabel(val2);
                                    if (text2.length * 8 > w2 - 4 || h2 < 16) return null;
                                    return (
                                      <text x={props.x + w2 / 2} y={props.y + h2 / 2} textAnchor="middle" dominantBaseline="middle"
                                        fontSize={12} fontWeight={700} fontFamily="ui-monospace, Consolas, monospace"
                                        fill="rgba(255,255,255,0.9)">{text2}</text>
                                    );
                                  }} />
                                  {chart.data.map(function (entry, idx) {
                                    var isPinnedBk = pinnedEntry && pinnedEntry.entry && pinnedEntry.entry.iterationId === entry.iterationId;
                                    var bkOpacity = pinnedEntry ? (isPinnedBk ? 0.9 : 0.2) : 0.7;
                                    return <Cell key={idx} fill={entry.isGap ? 'transparent' : itemColor} fillOpacity={bkOpacity} />;
                                  })}
                                </Bar>
                              );
                            });
                          }
                        }
                        return (
                          <Bar dataKey={dataKey} yAxisId="left" radius={[3, 3, 0, 0]} style={{ cursor: 'pointer' }}
                            onClick={function (data) {
                              if (data && !data.isGap) {
                                setPinnedEntry(function (prev) {
                                  if (prev && prev.entry && prev.entry.iterationId === data.iterationId) return null;
                                  return { entry: data, metricName: chart.metricName };
                                });
                              }
                            }}
                          >
                            <ErrorBar dataKey={dataKey + '_error'} width={4} strokeWidth={2} stroke="var(--text-secondary)" />
                            <LabelList dataKey={dataKey} content={function (props) {
                              var val4 = props.value;
                              var w4 = props.width;
                              var h4 = props.height;
                              if (val4 == null || w4 == null || h4 == null) return null;
                              var text4 = formatBarLabel(val4);
                              if (text4.length * 8 > w4 - 4 || h4 < 16) return null;
                              return (
                                <text x={props.x + w4 / 2} y={props.y + h4 / 2} textAnchor="middle" dominantBaseline="middle"
                                  fontSize={12} fontWeight={700} fontFamily="ui-monospace, Consolas, monospace"
                                  fill="rgba(255,255,255,0.9)">{text4}</text>
                              );
                            }} />
                            {chart.data.map(function (entry, idx) {
                              var isPinnedCell = pinnedEntry && pinnedEntry.entry && pinnedEntry.entry.iterationId === entry.iterationId;
                              var cellOpacity = pinnedEntry ? (isPinnedCell ? 0.9 : 0.2) : 0.7;
                              return <Cell key={idx} fill={entry.isGap ? 'transparent' : color} fillOpacity={cellOpacity} />;
                            })}
                          </Bar>
                        );
                      })()}
                    </ComposedChart>
                  </ResponsiveContainer>
                    </div>
                    {supplementalMetrics.length > 0 && <div className="compare-yaxis-label compare-yaxis-right">&nbsp;</div>}
                    <div className="compare-sidebar" style={{ maxHeight: 180 }}>
                    {resolvedPinnedEntry && resolvedPinnedEntry.entry && !resolvedPinnedEntry.entry.isGap ? (function () {
                      var e = resolvedPinnedEntry.entry;
                      if (sm.breakouts.length > 0) {
                        var prefix = dataKey + '_';
                        var flatItems = [];
                        Object.keys(e).filter(function (k) {
                          return k.startsWith(prefix) && !k.endsWith('_stddevPct') && !k.endsWith('_error') && !k.endsWith('_samples');
                        }).sort(naturalCompare).forEach(function (k, ki) {
                          var labelName = k.substring(prefix.length);
                          flatItems.push({ label: labelName, value: e[k] != null ? formatValue(e[k]) : '-', color: SUPP_COLORS[(si + ki) % SUPP_COLORS.length] });
                        });
                        var groupItems = flatItems.map(function (item) {
                          return { segments: parseBreakoutSegments(item.label), value: item.value, color: item.color };
                        });
                        return renderGroupedBreakouts(groupItems, 0, sm.breakouts);
                      } else {
                        var v = e[dataKey];
                        return (
                          <div className="compare-sidebar-item" style={{ color: color }}>
                            <div className="compare-sidebar-label">{sm.source}::{sm.type}</div>
                            <div className="compare-sidebar-value">{v != null ? formatValue(v) : '-'}</div>
                          </div>
                        );
                      }
                    })() : <div className="compare-sidebar-empty">Click a bar</div>}
                    </div>
                  </div>
                </div>
              </React.Fragment>
              );
            })}

            {/* Overlay-mode metric controls */}
            {supplementalMetrics.map(function (sm, si) {
              if (sm.display === 'panel') return null;
              return renderMetricControls(sm, si);
            })}

            {false && supplementalMetrics.map(function (sm, si) {
              if (sm.display !== 'panel') return null;
              var color = SUPP_COLORS[si % SUPP_COLORS.length];
              var dataKey = 'supp_' + si;
              var vals = [];
              chart.data.forEach(function (d) {
                if (d.isGap) return;
                if (d[dataKey] != null) vals.push(d[dataKey]);
                // Also include breakout label values for domain
                Object.keys(d).forEach(function (k) {
                  if (k.startsWith(dataKey + '_') && !k.endsWith('_stddevPct') && !k.endsWith('_error') && !k.endsWith('_samples') && d[k] != null) {
                    vals.push(d[k]);
                  }
                });
              });
              var min = vals.length > 0 ? Math.min.apply(null, vals) : 0;
              var max = vals.length > 0 ? Math.max.apply(null, vals) : 1;
              var pad = (max - min) * 0.1 || 0.1;
              return (
                <React.Fragment key={'panel-' + si}>
                <div className="compare-panel-metric">
                  <div className="compare-chart-with-labels">
                    <div className="compare-yaxis-label compare-yaxis-left" style={{ color: color }}>
                      {sm.source}::{sm.type}
                      {deepDiveMetrics && (
                        <label className="compare-yaxis-dive" onClick={function (e) { e.stopPropagation(); }}>
                          <input type="checkbox" checked={deepDiveMetrics.has(sm.source + '::' + sm.type)} onChange={function () {
                            var metricKey = sm.source + '::' + sm.type;
                            setDeepDiveMetrics(function (prev) {
                              var next = new Set(prev);
                              if (next.has(metricKey)) next.delete(metricKey); else next.add(metricKey);
                              return next;
                            });
                          }} />
                          <span>Dive</span>
                        </label>
                      )}
                    </div>
                    <div className="compare-chart-area" style={{ width: Math.max(600, nonGapData.length * 120 + 120), flex: 'none' }}>
                  <ResponsiveContainer width="100%" height={180}>
                    <ComposedChart data={chart.data} margin={{ top: 10, right: 30, left: 60, bottom: 5 }} barCategoryGap="10%">

                      <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                      <XAxis dataKey="name" hide={true} />
                      <YAxis
                        yAxisId="left"
                        domain={[Math.max(0, min - pad), max + pad]}
                        tick={{ fontSize: 11, fill: 'var(--text-secondary)' }}
                        tickFormatter={formatYTick}
                        stroke="var(--border)"
                      />
                      <Tooltip
                        content={function (props) {
                          if (!props.active || !props.payload || props.payload.length === 0) return null;
                          var entry = props.payload[0].payload;
                          if (!entry || entry.isGap) return null;
                          return (
                            <div className="compare-tooltip-mini">
                              {entry.name}
                            </div>
                          );
                        }}
                      />
                      {hasOverlays ? (
                        <YAxis yAxisId="right" orientation="right" width={80} tick={false} axisLine={false} />
                      ) : (
                        <YAxis yAxisId="right" orientation="right" width={1} tick={false} axisLine={false} />
                      )}
                      {(function () {
                        // Detect breakout labels from chart data
                        if (sm.breakouts.length > 0) {
                          var labelSet = new Set();
                          chart.data.forEach(function (d) {
                            if (d.isGap) return;
                            Object.keys(d).forEach(function (k) {
                              var prefix = dataKey + '_';
                              if (k.startsWith(prefix) && !k.endsWith('_stddevPct') && !k.endsWith('_error') && !k.endsWith('_samples')) {
                                labelSet.add(k);
                              }
                            });
                          });
                          var labels = Array.from(labelSet).sort(naturalCompare);
                          var ct = sm.chartType || 'bar';
                          if (labels.length > 0) {
                            return labels.map(function (lk, li) {
                              var labelName = lk.substring((dataKey + '_').length);
                              var itemColor = SUPP_COLORS[(si + li) % SUPP_COLORS.length];
                              if (ct === 'line') {
                                return (
                                  <Line key={lk} dataKey={lk} yAxisId="left" type="monotone"
                                    stroke={itemColor} strokeWidth={2}
                                    dot={{ r: 4, fill: itemColor }}
                                    connectNulls={false} name={labelName} />
                                );
                              }
                              return (
                                <Bar key={lk} dataKey={lk} yAxisId="left"
                                  radius={ct === 'stacked' ? [0, 0, 0, 0] : [3, 3, 0, 0]}
                                  stackId={ct === 'stacked' ? 'stack' : undefined}
                                  name={labelName} style={{ cursor: 'pointer' }}
                                  onClick={function (data) {
                                    if (data && !data.isGap) {
                                      setPinnedEntry(function (prev) {
                                        if (prev && prev.entry && prev.entry.iterationId === data.iterationId) return null;
                                        return { entry: data, metricName: chart.metricName };
                                      });
                                    }
                                  }}>
                                  <LabelList dataKey={lk} content={function (props) {
                                    if (ct === 'stacked') {
                                      // For stacked: check both width and individual segment height
                                      var val3 = props.value;
                                      var w3 = props.width;
                                      var h3 = props.height;
                                      if (val3 == null || w3 == null || h3 == null) return null;
                                      var text3 = formatBarLabel(val3);
                                      if (text3.length * 8 > w3 - 4 || Math.abs(h3) < 14) return null;
                                      return (
                                        <text x={props.x + w3 / 2} y={props.y + h3 / 2} textAnchor="middle" dominantBaseline="middle"
                                          fontSize={12} fontWeight={700} fontFamily="ui-monospace, Consolas, monospace"
                                          fill="rgba(255,255,255,0.9)">{text3}</text>
                                      );
                                    }
                                    var val2 = props.value;
                                    var w2 = props.width;
                                    var h2 = props.height;
                                    if (val2 == null || w2 == null || h2 == null) return null;
                                    var text2 = formatBarLabel(val2);
                                    if (text2.length * 8 > w2 - 4 || h2 < 16) return null;
                                    return (
                                      <text x={props.x + w2 / 2} y={props.y + h2 / 2} textAnchor="middle" dominantBaseline="middle"
                                        fontSize={12} fontWeight={700} fontFamily="ui-monospace, Consolas, monospace"
                                        fill="rgba(255,255,255,0.9)">{text2}</text>
                                    );
                                  }} />
                                  {chart.data.map(function (entry, idx) {
                                    var isPinnedBk = pinnedEntry && pinnedEntry.entry && pinnedEntry.entry.iterationId === entry.iterationId;
                                    var bkOpacity = pinnedEntry ? (isPinnedBk ? 0.9 : 0.2) : 0.7;
                                    return <Cell key={idx} fill={entry.isGap ? 'transparent' : itemColor} fillOpacity={bkOpacity} />;
                                  })}
                                </Bar>
                              );
                            });
                          }
                        }
                        // No breakouts — single bar
                        return (
                          <Bar dataKey={dataKey} yAxisId="left" radius={[3, 3, 0, 0]} style={{ cursor: 'pointer' }}
                            onClick={function (data) {
                              if (data && !data.isGap) {
                                setPinnedEntry(function (prev) {
                                  if (prev && prev.entry && prev.entry.iterationId === data.iterationId) return null;
                                  return { entry: data, metricName: chart.metricName };
                                });
                              }
                            }}
                          >
                            <ErrorBar dataKey={dataKey + '_error'} width={4} strokeWidth={2} stroke="var(--text-secondary)" />
                            <LabelList dataKey={dataKey} content={function (props) {
                              var val4 = props.value;
                              var w4 = props.width;
                              var h4 = props.height;
                              if (val4 == null || w4 == null || h4 == null) return null;
                              var text4 = formatBarLabel(val4);
                              if (text4.length * 8 > w4 - 4 || h4 < 16) return null;
                              return (
                                <text x={props.x + w4 / 2} y={props.y + h4 / 2} textAnchor="middle" dominantBaseline="middle"
                                  fontSize={12} fontWeight={700} fontFamily="ui-monospace, Consolas, monospace"
                                  fill="rgba(255,255,255,0.9)">{text4}</text>
                              );
                            }} />
                            {chart.data.map(function (entry, idx) {
                              var isPinnedCell = pinnedEntry && pinnedEntry.entry && pinnedEntry.entry.iterationId === entry.iterationId;
                              var cellOpacity = pinnedEntry ? (isPinnedCell ? 0.9 : 0.2) : 0.7;
                              return <Cell key={idx} fill={entry.isGap ? 'transparent' : color} fillOpacity={cellOpacity} />;
                            })}
                          </Bar>
                        );
                      })()}
                    </ComposedChart>
                  </ResponsiveContainer>
                    </div>
                    {supplementalMetrics.length > 0 && <div className="compare-yaxis-label compare-yaxis-right">&nbsp;</div>}
                    <div className="compare-sidebar" style={{ maxHeight: 180 }}>
                    {resolvedPinnedEntry && resolvedPinnedEntry.entry && !resolvedPinnedEntry.entry.isGap ? (function () {
                      var e = resolvedPinnedEntry.entry;
                      if (sm.breakouts.length > 0) {
                        var prefix = dataKey + '_';
                        var flatItems = [];
                        Object.keys(e).filter(function (k) {
                          return k.startsWith(prefix) && !k.endsWith('_stddevPct') && !k.endsWith('_error') && !k.endsWith('_samples');
                        }).sort(naturalCompare).forEach(function (k, ki) {
                          var labelName = k.substring(prefix.length);
                          flatItems.push({ label: labelName, value: e[k] != null ? formatValue(e[k]) : '-', color: SUPP_COLORS[(si + ki) % SUPP_COLORS.length] });
                        });
                          // Parse labels into segments for hierarchical grouping
                        var groupItems = flatItems.map(function (item) {
                          return { segments: parseBreakoutSegments(item.label), value: item.value, color: item.color };
                        });
                        return renderGroupedBreakouts(groupItems, 0, sm.breakouts);
                      } else {
                        var v = e[dataKey];
                        return (
                          <div className="compare-sidebar-item" style={{ color: color }}>
                            <div className="compare-sidebar-label">{sm.source}::{sm.type}</div>
                            <div className="compare-sidebar-value">{v != null ? formatValue(v) : '-'}</div>
                          </div>
                        );
                      }
                    })() : <div className="compare-sidebar-empty">Click a bar</div>}
                    </div>
                  </div>
                </div>
              </React.Fragment>
              );
            })}

            <div className="compare-chart-with-labels">
              <div className="compare-yaxis-label compare-yaxis-left">
                {chart.metricName}
                {deepDiveMetrics && (
                  <label className="compare-yaxis-dive" onClick={function (e) { e.stopPropagation(); }}>
                    <input type="checkbox" checked={deepDiveMetrics.has(chart.metricName)} onChange={function () {
                      setDeepDiveMetrics(function (prev) {
                        var next = new Set(prev);
                        if (next.has(chart.metricName)) next.delete(chart.metricName); else next.add(chart.metricName);
                        return next;
                      });
                    }} />
                    <span>Dive</span>
                  </label>
                )}
              </div>
              <div className="compare-chart-scroll">
              <div className="compare-chart-area" style={{ width: Math.max(600, nonGapData.length * 120 + 120) }}>
            {/* Toolbar: hidden dims, add, auto, clear — above headers */}
            <div className="compare-hier-toolbar">
              <button className="btn btn-sm btn-secondary" onClick={handleAutoGroup} style={{ fontSize: 10, padding: '2px 6px' }}>Auto</button>
            </div>
            <ResponsiveContainer width="100%" height={chartHeight}>
              <ComposedChart data={chart.data} margin={{ top: 20, right: 30, left: 60, bottom: 5 }} barCategoryGap="10%">
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                <XAxis
                  dataKey="name"
                  hide={true}
                />
                <YAxis
                  yAxisId="left"
                  width={60}
                  tick={{ fontSize: 11, fill: 'var(--text-secondary)' }}
                  tickFormatter={formatYTick}
                  stroke="var(--border)"
                />
                {supplementalMetrics.some(function (m) { return m.display !== 'panel'; }) && (function () {
                  // Compute domain from overlay-mode supplemental values only
                  var allVals = [];
                  chart.data.forEach(function (d) {
                    if (d.isGap) return;
                    supplementalMetrics.forEach(function (sm, si) {
                      if (sm.display === 'panel') return;
                      var v = d['supp_' + si];
                      if (v != null) allVals.push(v);
                    });
                  });
                  var min = allVals.length > 0 ? Math.min.apply(null, allVals) : 0;
                  var max = allVals.length > 0 ? Math.max.apply(null, allVals) : 1;
                  var pad = (max - min) * 0.1 || 0.1;
                  return (
                    <YAxis
                      yAxisId="right"
                      orientation="right"
                      width={80}
                      domain={[Math.max(0, min - pad), max + pad]}
                      tick={{ fontSize: 12, fill: 'var(--text-secondary)' }}
                      tickFormatter={formatYTick}
                      stroke="var(--border)"
                    />
                  );
                })()}
                {!hasOverlays && (
                  <YAxis yAxisId="right" orientation="right" width={1} tick={false} axisLine={false} />
                )}
                <Tooltip
                  content={function (props) {
                    if (!props.active || !props.payload || props.payload.length === 0) return null;
                    var entry = props.payload[0].payload;
                    if (!entry || entry.isGap || entry.value == null) return null;
                    return (
                      <div className="compare-tooltip-mini">
                        {entry.name}
                      </div>
                    );
                  }}
                />
                {pinnedEntry && pinnedEntry.entry && pinnedEntry.entry.name && (
                  <ReferenceLine x={pinnedEntry.entry.name} yAxisId="left" stroke="#ff6b6b" strokeDasharray="6 4" strokeWidth={2} />
                )}
                <Bar dataKey="value" yAxisId="left" radius={[4, 4, 0, 0]} style={{ cursor: 'pointer' }}
                  onClick={function (data) {
                    if (data && !data.isGap && data.value != null) {
                      setPinnedEntry(function (prev) {
                        if (prev && prev.entry && prev.entry.iterationId === data.iterationId) return null;
                        return { entry: data, metricName: chart.metricName };
                      });
                    }
                  }}
                >
                  <ErrorBar dataKey="errorY" width={4} strokeWidth={2} stroke="var(--text-secondary)" />
                  <LabelList dataKey="value" content={function (props) {
                    var val = props.value;
                    var w = props.width;
                    var h = props.height;
                    if (val == null || w == null || h == null) return null;
                    var text = formatBarLabel(val);
                    var charWidth = 8; // approximate pixels per character at font-size 12
                    var textWidth = text.length * charWidth;
                    // Show inside bar if it fits width-wise and bar is tall enough
                    if (textWidth > w - 4) return null;
                    if (h < 16) return null;
                    return (
                      <text x={props.x + w / 2} y={props.y + h / 2} textAnchor="middle" dominantBaseline="middle"
                        fontSize={12} fontWeight={700} fontFamily="ui-monospace, Consolas, monospace"
                        fill="rgba(255,255,255,0.9)">
                        {text}
                      </text>
                    );
                  }} />
                  {chart.data.map(function (entry, idx) {
                    var isPinned = pinnedEntry && pinnedEntry.entry && pinnedEntry.entry.iterationId === entry.iterationId;
                    var opacity = pinnedEntry ? (isPinned ? 1 : 0.3) : 1;
                    return <Cell key={idx} fill={entry.isGap ? 'transparent' : entry.color} fillOpacity={opacity} />;
                  })}
                </Bar>
                {supplementalMetrics.map(function (sm, si) {
                  if (sm.display === 'panel') return null;
                  var color = SUPP_COLORS[si % SUPP_COLORS.length];
                  // If breakouts produce multiple labels, render one line per label
                  if (sm.breakouts.length > 0) {
                    var labelSet = new Set();
                    chart.data.forEach(function (d) {
                      if (d.isGap) return;
                      Object.keys(d).forEach(function (k) {
                        if (k.startsWith('supp_' + si + '_') && !k.endsWith('_stddevPct') && !k.endsWith('_error') && !k.endsWith('_samples') && k !== 'supp_' + si) {
                          labelSet.add(k);
                        }
                      });
                    });
                    return Array.from(labelSet).sort(naturalCompare).map(function (lk, li) {
                      var labelName = lk.substring(('supp_' + si + '_').length);
                      return (
                        <Line
                          key={si + '-' + li}
                          dataKey={lk}
                          yAxisId="right"
                          type="monotone"
                          stroke={SUPP_COLORS[(si + li) % SUPP_COLORS.length]}
                          strokeWidth={2}
                          dot={{ r: 4, fill: SUPP_COLORS[(si + li) % SUPP_COLORS.length] }}
                          connectNulls={false}
                          name={labelName}
                        />
                      );
                    });
                  }
                  return (
                    <Line
                      key={si}
                      dataKey={'supp_' + si}
                      yAxisId="right"
                      type="monotone"
                      stroke={color}
                      strokeWidth={2}
                      dot={{ r: 5, fill: color }}
                      connectNulls={false}
                      name={sm.source + '::' + sm.type}
                    />
                  );
                })}
              </ComposedChart>
            </ResponsiveContainer>
            {/* Chips grid below bars — one row per varying dimension */}
            <div className="compare-chips-grid" style={{
              gridTemplateColumns: '120px repeat(' + chart.data.length + ', minmax(0, 1fr))',
              marginRight: (hasOverlays ? 110 : 31) + 'px'
            }}>
              {/* Row 1: deep-dive selection */}
              <div className="compare-chips-grid-label compare-dd-label" style={{ gridRow: 1, gridColumn: 1 }}>
                <span className="compare-dim-name">{'Deep Dive →'}</span>
              </div>
              {chart.data.map(function (d, di) {
                if (d.isGap) return null;
                var col = di + 2;
                var ddSelected = deepDiveIterations && deepDiveIterations.has(d.iterationId);
                var atLimit = deepDiveIterations && deepDiveIterations.size >= MAX_DEEP_DIVE_ITERS;
                var ddArr = deepDiveIterations ? Array.from(deepDiveIterations) : [];
                var themeIdx = ddSelected ? ddArr.indexOf(d.iterationId) : -1;
                var themeColor = themeIdx >= 0 ? ITER_THEME_BASES[themeIdx % ITER_THEME_BASES.length] : null;
                var letter = themeIdx >= 0 ? String.fromCharCode(65 + themeIdx) : '';
                return (
                  <div key={'dd-' + di}
                    className={'compare-dd-cell' + (ddSelected ? ' compare-dd-selected' : '') + (!ddSelected && atLimit ? ' compare-dd-disabled' : '')}
                    style={ddSelected && themeColor ? { gridRow: 1, gridColumn: col, backgroundColor: themeColor, borderColor: themeColor } : { gridRow: 1, gridColumn: col }}
                    onClick={function () {
                      if (!ddSelected && atLimit) return;
                      setDeepDiveIterations(function (prev) {
                        var next = new Set(prev);
                        if (next.has(d.iterationId)) next.delete(d.iterationId); else next.add(d.iterationId);
                        return next;
                      });
                    }}
                    title={ddSelected ? 'Remove from deep dive (' + letter + ')' : (atLimit ? 'Max ' + MAX_DEEP_DIVE_ITERS + ' reached' : 'Select for deep dive')}
                  >
                    {letter}
                  </div>
                );
              })}
              {(function () {
                var orderedDims = [];
                groupByList.forEach(function (dim) {
                  if (chart.varyingKeys.has(dim)) orderedDims.push(dim);
                });
                chart.varyingKeys.forEach(function (dim) {
                  if (orderedDims.indexOf(dim) < 0) orderedDims.push(dim);
                });

                return orderedDims.map(function (dim, dimIdx) {
                  var row = dimIdx + 2;
                  var groupByIdx = groupByList.indexOf(dim);
                  var isGroupBy = groupByIdx >= 0;
                  var runs = [];
                  for (var di = 0; di < chart.data.length; di++) {
                    var d = chart.data[di];
                    if (d.isGap) continue;
                    var origIter = iterations.find(function (it) { return it.iterationId === d.iterationId; });
                    var val = origIter ? getDimValue(origIter, dim) : '';
                    if (runs.length === 0 || val !== runs[runs.length - 1].val) {
                      runs.push({ val: val, firstDi: di, lastDi: di, iterIds: [d.iterationId] });
                    } else {
                      runs[runs.length - 1].lastDi = di;
                      runs[runs.length - 1].iterIds.push(d.iterationId);
                    }
                  }
                  return (
                    <React.Fragment key={'dim-' + dimIdx}>
                      <div className={'compare-chips-grid-label' + (isGroupBy ? ' compare-dim-groupby' : '')} style={{ gridRow: row, gridColumn: 1 }}>
                        <span className="compare-dim-name">{formatDimLabel(dim)}</span>
                        {dimIdx > 0 && (
                          <button className="compare-dim-btn" onClick={function () {
                            var prevDim = orderedDims[dimIdx - 1];
                            setGroupByList(function (prev) {
                              var next = prev.slice();
                              var myIdx = next.indexOf(dim);
                              var pIdx = next.indexOf(prevDim);
                              if (myIdx < 0) { next.push(dim); }
                              if (pIdx < 0) { next.push(prevDim); }
                              myIdx = next.indexOf(dim);
                              pIdx = next.indexOf(prevDim);
                              next[myIdx] = prevDim;
                              next[pIdx] = dim;
                              return next;
                            });
                          }} title="Move up">{'▲'}</button>
                        )}
                        {dimIdx < orderedDims.length - 1 && (
                          <button className="compare-dim-btn" onClick={function () {
                            var nextDim = orderedDims[dimIdx + 1];
                            setGroupByList(function (prev) {
                              var next = prev.slice();
                              var myIdx = next.indexOf(dim);
                              var nIdx = next.indexOf(nextDim);
                              if (myIdx < 0) { next.push(dim); }
                              if (nIdx < 0) { next.push(nextDim); }
                              myIdx = next.indexOf(dim);
                              nIdx = next.indexOf(nextDim);
                              next[myIdx] = nextDim;
                              next[nIdx] = dim;
                              return next;
                            });
                          }} title="Move down">{'▼'}</button>
                        )}
                        <button className="compare-dim-btn compare-dim-btn-x" onClick={function () {
                          setHiddenFields(function (prev) { return prev.concat([dim]); });
                          setGroupByList(function (prev) { return prev.filter(function (d) { return d !== dim; }); });
                        }} title="Hide dimension">{'×'}</button>
                      </div>
                      {runs.map(function (run, ri) {
                        var colStart = run.firstDi + 2;
                        var span = run.lastDi - run.firstDi + 1;
                        var formatted = formatDimValue(dim, run.val);
                        return (
                          <div key={'span-' + ri}
                            className={'compare-span-chip ' + (dim === 'benchmark' ? 'benchmark-badge' : dim.startsWith('tag:') ? 'tag' : 'param')}
                            style={{ gridRow: row, gridColumn: colStart + ' / span ' + span }}
                            title={formatted}
                          >
                            {formatted}
                          </div>
                        );
                      })}
                    </React.Fragment>
                  );
                });
              })()}
            </div>
              </div>
              </div>
              {hasOverlays ? (
                <div className="compare-yaxis-label compare-yaxis-right">
                  {supplementalMetrics.filter(function (m) { return m.display !== 'panel'; }).map(function (m) { return m.source + '::' + m.type; }).join(', ')}
                </div>
              ) : supplementalMetrics.length > 0 ? (
                <div className="compare-yaxis-label compare-yaxis-right">&nbsp;</div>
              ) : null}
              <div className="compare-sidebar" style={{ maxHeight: chartHeight }}>
                {hiddenFields.length > 0 && (
                  <div className="compare-sidebar-hidden">
                    <div className="compare-sidebar-hidden-label">Hidden</div>
                    {hiddenFields.map(function (dim) {
                      var opt = allDimOptions.find(function (o) { return o.value === dim; });
                      return (
                        <span key={dim} className="compare-sidebar-hidden-chip" onClick={function () {
                          setHiddenFields(hiddenFields.filter(function (d) { return d !== dim; }));
                          if (!groupByList.includes(dim)) setGroupByList(groupByList.concat([dim]));
                        }} title="Click to restore">{opt ? opt.label : formatDimLabel(dim)}</span>
                      );
                    })}
                  </div>
                )}
                {resolvedPinnedEntry && resolvedPinnedEntry.entry && !resolvedPinnedEntry.entry.isGap && resolvedPinnedEntry.entry.value != null ? (function () {
                  var e = resolvedPinnedEntry.entry;
                  var items = [];
                  var pmText = formatValue(e.value);
                  if (e.samples > 1 && e.stddevPct != null) pmText += ' (\u00b1' + e.stddevPct.toFixed(1) + '%)';
                  items.push({ label: chart.metricName, value: pmText, color: e.color });
                  supplementalMetrics.forEach(function (sm2, si2) {
                    if (sm2.display === 'panel') return;
                    var sv = e['supp_' + si2];
                    items.push({ label: sm2.source + '::' + sm2.type, value: sv != null ? formatValue(sv) : '-', color: SUPP_COLORS[si2 % SUPP_COLORS.length] });
                  });
                  return (
                    <>
                      <div className="compare-sidebar-iter">{e.name}</div>
                      {items.map(function (item, ii) {
                        return (
                          <div key={ii} className="compare-sidebar-item" style={{ color: item.color }}>
                            <div className="compare-sidebar-label">{item.label}</div>
                            <div className="compare-sidebar-value">{item.value}</div>
                          </div>
                        );
                      })}
                    </>
                  );
                })() : <div className="compare-sidebar-empty">Click a bar</div>}
              </div>
            </div>

            {/* Primary metric controls */}
            {(function () {
              var pmStr = iterations.length > 0 ? iterations[0].primaryMetric : null;
              if (!pmStr || typeof pmStr !== 'string') return null;
              var pmParts = pmStr.split('::');
              if (pmParts.length < 2) return null;
              var alreadyAdded = supplementalMetrics.some(function (m) { return m.source === pmParts[0] && m.type === pmParts[1]; });
              return (
                <div className="compare-primary-controls">
                  {!alreadyAdded && (
                    <button className="btn btn-sm btn-secondary" onClick={function () {
                      var ctx = getRunContext();
                      var bestIndices = computeBestSampleIndices();
                      setAddMetricLoading(true);
                      timeWork('Add primary metric refinement ' + pmStr, function () {
                        return api.getSupplementalMetric({
                          iterations: ctx.iterations, start: ctx.start, end: ctx.end,
                          source: pmParts[0], type: pmParts[1], sampleIndex: bestIndices,
                        });
                      }).then(function (res) {
                        setSupplementalMetrics(function (prev) {
                          return prev.concat([{
                            source: pmParts[0], type: pmParts[1],
                            values: res.values || {}, display: 'panel',
                            chartType: 'bar', filter: '', sampleIndex: bestIndices,
                            breakouts: [], remainingBreakouts: res.remainingBreakouts || [],
                            loading: false,
                          }]);
                        });
                      }).finally(function () { setAddMetricLoading(false); });
                    }}>
                      Refine {pmStr}
                    </button>
                  )}
                </div>
              );
            })()}

            {/* Old panel-mode metrics removed — now rendered above the primary chart */}
            {false && supplementalMetrics.map(function (sm, si) {
              if (sm.display !== 'panel') return null;
              var color = SUPP_COLORS[si % SUPP_COLORS.length];
              var dataKey = 'supp_' + si;
              var vals = [];
              chart.data.forEach(function (d) {
                if (d.isGap) return;
                if (d[dataKey] != null) vals.push(d[dataKey]);
                Object.keys(d).forEach(function (k) {
                  if (k.startsWith(dataKey + '_') && !k.endsWith('_stddevPct') && !k.endsWith('_error') && !k.endsWith('_samples') && d[k] != null) {
                    vals.push(d[k]);
                  }
                });
              });
              var min = vals.length > 0 ? Math.min.apply(null, vals) : 0;
              var max = vals.length > 0 ? Math.max.apply(null, vals) : 1;
              var pad = (max - min) * 0.1 || 0.1;
              return (
                <React.Fragment key={'panel-' + si}>
                <div className="compare-panel-metric">
                  <div className="compare-chart-with-labels">
                    <div className="compare-yaxis-label compare-yaxis-left" style={{ color: color }}>
                      {sm.source}::{sm.type}
                      {deepDiveMetrics && (
                        <label className="compare-yaxis-dive" onClick={function (e) { e.stopPropagation(); }}>
                          <input type="checkbox" checked={deepDiveMetrics.has(sm.source + '::' + sm.type)} onChange={function () {
                            var metricKey = sm.source + '::' + sm.type;
                            setDeepDiveMetrics(function (prev) {
                              var next = new Set(prev);
                              if (next.has(metricKey)) next.delete(metricKey); else next.add(metricKey);
                              return next;
                            });
                          }} />
                          <span>Dive</span>
                        </label>
                      )}
                    </div>
                    <div className="compare-chart-area" style={{ width: Math.max(600, nonGapData.length * 120 + 120), flex: 'none' }}>
                  <ResponsiveContainer width="100%" height={180}>
                    <ComposedChart data={chart.data} margin={{ top: 10, right: 30, left: 60, bottom: 5 }} barCategoryGap="10%">
                      <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                      <XAxis dataKey="name" hide={true} />
                      <YAxis
                        yAxisId="left"
                        domain={[Math.max(0, min - pad), max + pad]}
                        tick={{ fontSize: 11, fill: 'var(--text-secondary)' }}
                        tickFormatter={formatYTick}
                        stroke="var(--border)"
                      />
                      <Tooltip
                        content={function (props) {
                          if (!props.active || !props.payload || props.payload.length === 0) return null;
                          var entry = props.payload[0].payload;
                          if (!entry || entry.isGap) return null;
                          return (
                            <div className="compare-tooltip-mini">
                              {entry.name}
                            </div>
                          );
                        }}
                      />
                      {hasOverlays ? (
                        <YAxis yAxisId="right" orientation="right" width={80} tick={false} axisLine={false} />
                      ) : (
                        <YAxis yAxisId="right" orientation="right" width={1} tick={false} axisLine={false} />
                      )}
                      {pinnedEntry && pinnedEntry.entry && pinnedEntry.entry.name && (
                        <ReferenceLine x={pinnedEntry.entry.name} yAxisId="left" stroke="#ff6b6b" strokeDasharray="6 4" strokeWidth={2} />
                      )}
                      {(function () {
                        if (sm.breakouts.length > 0) {
                          var labelSet = new Set();
                          chart.data.forEach(function (d) {
                            if (d.isGap) return;
                            Object.keys(d).forEach(function (k) {
                              var prefix = dataKey + '_';
                              if (k.startsWith(prefix) && !k.endsWith('_stddevPct') && !k.endsWith('_error') && !k.endsWith('_samples')) {
                                labelSet.add(k);
                              }
                            });
                          });
                          var labels = Array.from(labelSet).sort(naturalCompare);
                          var ct = sm.chartType || 'bar';
                          if (labels.length > 0) {
                            return labels.map(function (lk, li) {
                              var labelName = lk.substring((dataKey + '_').length);
                              var itemColor = SUPP_COLORS[(si + li) % SUPP_COLORS.length];
                              if (ct === 'line') {
                                return (
                                  <Line key={lk} dataKey={lk} yAxisId="left" type="monotone"
                                    stroke={itemColor} strokeWidth={2}
                                    dot={{ r: 4, fill: itemColor }}
                                    connectNulls={false} name={labelName} />
                                );
                              }
                              return (
                                <Bar key={lk} dataKey={lk} yAxisId="left"
                                  radius={ct === 'stacked' ? [0, 0, 0, 0] : [3, 3, 0, 0]}
                                  stackId={ct === 'stacked' ? 'stack' : undefined}
                                  name={labelName} style={{ cursor: 'pointer' }}
                                  onClick={function (data) {
                                    if (data && !data.isGap) {
                                      setPinnedEntry(function (prev) {
                                        if (prev && prev.entry && prev.entry.iterationId === data.iterationId) return null;
                                        return { entry: data, metricName: chart.metricName };
                                      });
                                    }
                                  }}>
                                  <LabelList dataKey={lk} content={function (props) {
                                    if (ct === 'stacked') {
                                      // For stacked: check both width and individual segment height
                                      var val3 = props.value;
                                      var w3 = props.width;
                                      var h3 = props.height;
                                      if (val3 == null || w3 == null || h3 == null) return null;
                                      var text3 = formatBarLabel(val3);
                                      if (text3.length * 8 > w3 - 4 || Math.abs(h3) < 14) return null;
                                      return (
                                        <text x={props.x + w3 / 2} y={props.y + h3 / 2} textAnchor="middle" dominantBaseline="middle"
                                          fontSize={12} fontWeight={700} fontFamily="ui-monospace, Consolas, monospace"
                                          fill="rgba(255,255,255,0.9)">{text3}</text>
                                      );
                                    }
                                    var val2 = props.value;
                                    var w2 = props.width;
                                    var h2 = props.height;
                                    if (val2 == null || w2 == null || h2 == null) return null;
                                    var text2 = formatBarLabel(val2);
                                    if (text2.length * 8 > w2 - 4 || h2 < 16) return null;
                                    return (
                                      <text x={props.x + w2 / 2} y={props.y + h2 / 2} textAnchor="middle" dominantBaseline="middle"
                                        fontSize={12} fontWeight={700} fontFamily="ui-monospace, Consolas, monospace"
                                        fill="rgba(255,255,255,0.9)">{text2}</text>
                                    );
                                  }} />
                                  {chart.data.map(function (entry, idx) {
                                    var isPinnedBk = pinnedEntry && pinnedEntry.entry && pinnedEntry.entry.iterationId === entry.iterationId;
                                    var bkOpacity = pinnedEntry ? (isPinnedBk ? 0.9 : 0.2) : 0.7;
                                    return <Cell key={idx} fill={entry.isGap ? 'transparent' : itemColor} fillOpacity={bkOpacity} />;
                                  })}
                                </Bar>
                              );
                            });
                          }
                        }
                        return (
                          <Bar dataKey={dataKey} yAxisId="left" radius={[3, 3, 0, 0]} style={{ cursor: 'pointer' }}
                            onClick={function (data) {
                              if (data && !data.isGap) {
                                setPinnedEntry(function (prev) {
                                  if (prev && prev.entry && prev.entry.iterationId === data.iterationId) return null;
                                  return { entry: data, metricName: chart.metricName };
                                });
                              }
                            }}
                          >
                            <ErrorBar dataKey={dataKey + '_error'} width={4} strokeWidth={2} stroke="var(--text-secondary)" />
                            <LabelList dataKey={dataKey} content={function (props) {
                              var val4 = props.value;
                              var w4 = props.width;
                              var h4 = props.height;
                              if (val4 == null || w4 == null || h4 == null) return null;
                              var text4 = formatBarLabel(val4);
                              if (text4.length * 8 > w4 - 4 || h4 < 16) return null;
                              return (
                                <text x={props.x + w4 / 2} y={props.y + h4 / 2} textAnchor="middle" dominantBaseline="middle"
                                  fontSize={12} fontWeight={700} fontFamily="ui-monospace, Consolas, monospace"
                                  fill="rgba(255,255,255,0.9)">{text4}</text>
                              );
                            }} />
                            {chart.data.map(function (entry, idx) {
                              var isPinnedCell = pinnedEntry && pinnedEntry.entry && pinnedEntry.entry.iterationId === entry.iterationId;
                              var cellOpacity = pinnedEntry ? (isPinnedCell ? 0.9 : 0.2) : 0.7;
                              return <Cell key={idx} fill={entry.isGap ? 'transparent' : color} fillOpacity={cellOpacity} />;
                            })}
                          </Bar>
                        );
                      })()}
                    </ComposedChart>
                  </ResponsiveContainer>
                    </div>
                    {supplementalMetrics.length > 0 && <div className="compare-yaxis-label compare-yaxis-right">&nbsp;</div>}
                    <div className="compare-sidebar" style={{ maxHeight: 180 }}>
                    {resolvedPinnedEntry && resolvedPinnedEntry.entry && !resolvedPinnedEntry.entry.isGap ? (function () {
                      var e = resolvedPinnedEntry.entry;
                      if (sm.breakouts.length > 0) {
                        var prefix = dataKey + '_';
                        var flatItems = [];
                        Object.keys(e).filter(function (k) {
                          return k.startsWith(prefix) && !k.endsWith('_stddevPct') && !k.endsWith('_error') && !k.endsWith('_samples');
                        }).sort(naturalCompare).forEach(function (k, ki) {
                          var labelName = k.substring(prefix.length);
                          flatItems.push({ label: labelName, value: e[k] != null ? formatValue(e[k]) : '-', color: SUPP_COLORS[(si + ki) % SUPP_COLORS.length] });
                        });
                          // Parse labels into segments for hierarchical grouping
                        var groupItems = flatItems.map(function (item) {
                          return { segments: parseBreakoutSegments(item.label), value: item.value, color: item.color };
                        });
                        return renderGroupedBreakouts(groupItems, 0, sm.breakouts);
                      } else {
                        var v = e[dataKey];
                        return (
                          <div className="compare-sidebar-item" style={{ color: color }}>
                            <div className="compare-sidebar-label">{sm.source}::{sm.type}</div>
                            <div className="compare-sidebar-value">{v != null ? formatValue(v) : '-'}</div>
                          </div>
                        );
                      }
                    })() : <div className="compare-sidebar-empty">Click a bar</div>}
                    </div>
                  </div>
                </div>
                {renderMetricControls(sm, si)}
              </React.Fragment>
              );
            })}

          </div>
        );
      })}

      <div className="compare-add-metric-bar">
        {!showAddMetric && (
          <button className="btn btn-sm btn-secondary" onClick={handleShowAddMetric}>
            + Add Metric
          </button>
        )}
        {showAddMetric && (
          <div className="compare-control">
            <label>Source</label>
            <select value={addMetricSource} onChange={function (e) { handleSourceChange(e.target.value); }}>
              <option value="">Select...</option>
              {(availableSources || []).map(function (s) { return <option key={s} value={s}>{s}</option>; })}
            </select>
          </div>
        )}
        {showAddMetric && addMetricSource && (
          <div className="compare-control">
            <label>Type</label>
            <select value={addMetricType} onChange={function (e) { setAddMetricType(e.target.value); }}>
              <option value="">Select...</option>
              {(availableTypes || []).map(function (t) { return <option key={t} value={t}>{t}</option>; })}
            </select>
          </div>
        )}
        {showAddMetric && addMetricSource && addMetricType && (
          <div className="compare-control">
            <label>Display</label>
            <div className="compare-display-toggle">
              <button className={'btn btn-sm ' + (addMetricDisplay === 'overlay' ? 'btn-primary' : 'btn-secondary')} onClick={function () { setAddMetricDisplay('overlay'); }}>Overlay</button>
              <button className={'btn btn-sm ' + (addMetricDisplay === 'panel' ? 'btn-primary' : 'btn-secondary')} onClick={function () { setAddMetricDisplay('panel'); }}>Own Panel</button>
            </div>
          </div>
        )}
        {showAddMetric && addMetricSource && addMetricType && (
          <button className="btn btn-sm btn-primary" onClick={handleAddMetric} disabled={addMetricLoading}>
            {addMetricLoading ? <><span className="spinner" style={{ marginRight: 4 }} /> Loading...</> : 'Add'}
          </button>
        )}
        {showAddMetric && (
          <button className="btn btn-sm btn-secondary" onClick={function () { setShowAddMetric(false); }}>Cancel</button>
        )}
      </div>

    </div>
  );
});

export default CompareView;
