import { useState, useEffect, useMemo, useRef, useCallback } from 'react';
import { ComposedChart, Line, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine, ReferenceArea } from 'recharts';
import * as api from '../api/cdm';
import { timeWork } from '../debugLog';
import { buildIterItems, iterItemsToString } from '../utils/iterLabel';

// Per-iteration color themes: each iteration gets a hue family
// Within each family, brightness/saturation varies for different labels
var ITER_THEMES = [
  // Blues
  { base: '#5b8def', shades: ['#3a6fd4', '#5b8def', '#7ca4f5', '#9dbcfa', '#bdd4ff', '#4a7de0', '#6991e8', '#89a8f0', '#a9bff8', '#c9d7ff'] },
  // Reds/oranges
  { base: '#ef5b5b', shades: ['#d43a3a', '#ef5b5b', '#f57c7c', '#fa9d9d', '#ffbdbd', '#e04a4a', '#e86969', '#f08989', '#f8a9a9', '#ffc9c9'] },
  // Greens
  { base: '#5bef8d', shades: ['#3ad46a', '#5bef8d', '#7cf5a4', '#9dfabc', '#bdffd4', '#4ae07a', '#69e891', '#89f0a8', '#a9f8bf', '#c9ffd7'] },
  // Purples
  { base: '#b85bef', shades: ['#9a3ad4', '#b85bef', '#cc7cf5', '#dd9dfa', '#eebdff', '#a84ae0', '#bc69e8', '#cf89f0', '#e0a9f8', '#f0c9ff'] },
  // Teals
  { base: '#5bcdef', shades: ['#3ab4d4', '#5bcdef', '#7cd8f5', '#9de4fa', '#bdf0ff', '#4ac2e0', '#69cce8', '#89d6f0', '#a9e0f8', '#c9ebff'] },
  // Ambers
  { base: '#efb85b', shades: ['#d49a3a', '#efb85b', '#f5cc7c', '#fadd9d', '#ffeebf', '#e0a84a', '#e8bc69', '#f0cf89', '#f8e0a9', '#fff0c9'] },
];

function getIterColor(iterIdx, labelIdx) {
  var theme = ITER_THEMES[iterIdx % ITER_THEMES.length];
  return theme.shades[labelIdx % theme.shades.length];
}

function formatValue(v) {
  if (v == null) return '';
  v = Number(v);
  if (isNaN(v)) return '';
  if (Math.abs(v) >= 1000) return v.toFixed(0);
  if (Math.abs(v) >= 1) return v.toFixed(2);
  return v.toPrecision(3);
}

function formatElapsed(ms) {
  if (ms == null) return '';
  var sec = ms / 1000;
  if (sec < 60) return sec.toFixed(1) + 's';
  if (sec < 3600) return (sec / 60).toFixed(1) + 'm';
  return (sec / 3600).toFixed(1) + 'h';
}

// Render iteration items as box-in-box chips
function renderIterChips(it, allIterations, hiddenFields) {
  var items = buildIterItems(it, allIterations, hiddenFields);
  if (items.length === 0) return it.iterationId.substring(0, 8);
  return items.map(function (item, i) {
    var label = item.type === 'benchmark' ? item.val : item.names.join(',') + '=' + item.val;
    return (
      <span key={i} className={'deepdive-iter-param ' + (item.type === 'benchmark' ? 'benchmark-badge' : item.type === 'tag' ? 'tag' : 'param')}>
        {item.type === 'tag' && <span className="tag-key">{item.names.join(',')}</span>}
        {item.type === 'tag' ? '=' + item.val : label}
      </span>
    );
  });
}

// Parse breakout label like "<host1>-<0>" into segments ["host1", "0"]
function parseSegments(label) {
  if (!label) return [];
  var matches = label.match(/<[^>]*>/g);
  if (!matches) return [label];
  return matches.map(function (s) { return s.replace(/^</, '').replace(/>$/, ''); });
}

// Natural sort
function naturalCompare(a, b) {
  var na = Number(a);
  var nb = Number(b);
  if (!isNaN(na) && !isNaN(nb)) return na - nb;
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

export default function DeepDiveView({ selected, deepDiveMetrics, metricConfigs: metricConfigsProp, hiddenFields }) {
  var [resolution, setResolution] = useState(100);
  var [periodInfo, setPeriodInfo] = useState(null);
  var [metricData, setMetricData] = useState({});
  var [loadingPeriods, setLoadingPeriods] = useState(false);
  var [loadingMetrics, setLoadingMetrics] = useState(new Set());
  var [pinnedElapsed, setPinnedElapsed] = useState(null);
  var [hoverElapsed, setHoverElapsed] = useState(null);
  var initialPinSet = useRef(false);

  // Auto-set hover position to 50% when first data arrives
  useEffect(function () {
    if (initialPinSet.current) return;
    if (Object.keys(metricData).length === 0) return;
    // Find the first metric with data to compute the midpoint
    for (var key in metricData) {
      var metricResults = metricData[key];
      for (var itId in metricResults) {
        var result = metricResults[itId];
        if (result && result.periodBegin && result.periodEnd) {
          var duration = Number(result.periodEnd) - Number(result.periodBegin);
          setHoverElapsed(Math.round(duration / 2));
          initialPinSet.current = true;
          return;
        }
      }
    }
  }, [metricData]);
  var [perMetricOpts, setPerMetricOpts] = useState({});
  // Zoom: percentage of total elapsed time range (0.0 to 1.0)
  var [zoomRange, setZoomRange] = useState(null); // null = full range, { startPct, endPct }
  var [brushStart, setBrushStart] = useState(null); // elapsed time of brush start (during drag)
  var [brushEnd, setBrushEnd] = useState(null); // elapsed time of brush end (during drag)
  var abortRef = useRef(false);

  var iterations = useMemo(function () {
    return Array.from(selected.values());
  }, [selected]);

  var metricList = useMemo(function () {
    return Array.from(deepDiveMetrics);
  }, [deepDiveMetrics]);

  // Build a lookup of metric configs from the snapshot passed by App
  var configLookup = useMemo(function () {
    var lookup = {};
    (metricConfigsProp || []).forEach(function (sm) {
      var key = sm.source + '::' + sm.type;
      lookup[key] = {
        breakouts: sm.breakouts || [],
        filter: sm.filter || null,
        sampleIndex: sm.sampleIndex,
      };
    });
    return lookup;
  }, [metricConfigsProp]);

  // Fetch period info on mount
  useEffect(function () {
    if (iterations.length === 0 || metricList.length === 0) return;
    abortRef.current = false;
    initialPinSet.current = false;
    setLoadingPeriods(true);
    setMetricData({});

    var ctx = {
      iterations: iterations.map(function (it) { return { iterationId: it.iterationId, runId: it.runId }; }),
    };
    // Infer date range
    var begins = iterations.filter(function (it) { return it.runBegin; }).map(function (it) { return Number(it.runBegin); });
    var startDate = begins.length > 0 ? new Date(Math.min.apply(null, begins)) : null;
    var endDate = begins.length > 0 ? new Date(Math.max.apply(null, begins)) : null;
    ctx.start = startDate ? startDate.getFullYear() + '.' + String(startDate.getMonth() + 1).padStart(2, '0') : null;
    ctx.end = endDate ? endDate.getFullYear() + '.' + String(endDate.getMonth() + 1).padStart(2, '0') : null;

    timeWork('Fetch period info for deep dive', function () {
      return api.getPeriodInfo(ctx);
    }).then(function (res) {
      if (abortRef.current) return;
      setPeriodInfo(res.periods || {});
      setLoadingPeriods(false);

      // Fetch metric data sequentially per metric, iterations within each metric run concurrently.
      // Serializing metrics avoids overwhelming OpenSearch with concurrent aggregation queries
      // that cause thread pool contention and multi-minute stalls.
      var periods = res.periods || {};
      (async function () {
        for (var mi = 0; mi < metricList.length; mi++) {
          if (abortRef.current) return;
          var metricKey = metricList[mi];
          var parts = metricKey.split('::');
          if (parts.length < 2) continue;
          var source = parts[0];
          var type = parts[1];
          var config = configLookup[metricKey] || {};
          var breakouts = config.breakouts || [];

          // Mark all iterations for this metric as loading
          var loadKeys = [];
          iterations.forEach(function (it) {
            if (periods[it.iterationId]) {
              var loadKey = metricKey + '::' + it.iterationId;
              loadKeys.push(loadKey);
              setLoadingMetrics(function (prev) { var next = new Set(prev); next.add(loadKey); return next; });
            }
          });

          // Fetch all iterations for this metric concurrently, then wait for all to complete
          var promises = iterations.map(function (it) {
            var pi = periods[it.iterationId];
            var loadKey = metricKey + '::' + it.iterationId;
            if (!pi) {
              setLoadingMetrics(function (prev) { var next = new Set(prev); next.delete(loadKey); return next; });
              return Promise.resolve();
            }

            var queryBegin = Number(pi.begin);
            var queryEnd = Number(pi.end);
            var periodDuration = queryEnd - queryBegin;
            if (zoomRange) {
              queryBegin = Number(pi.begin) + Math.round(periodDuration * zoomRange.startPct);
              queryEnd = Number(pi.begin) + Math.round(periodDuration * zoomRange.endPct);
            }

            return timeWork('Deep dive ' + source + '::' + type + ' ' + it.iterationId.substring(0, 8), function () {
              return api.getMetricData({
                run: pi.runId,
                period: pi.periodId,
                source: source,
                type: type,
                begin: String(queryBegin),
                end: String(queryEnd),
                resolution: resolution,
                breakout: breakouts,
                filter: config.filter || null,
              });
            }).then(function (data) {
              if (abortRef.current) return;
              var mk = metricKey;
              setMetricData(function (prev) {
                var next = Object.assign({}, prev);
                if (!next[mk]) next[mk] = {};
                next[mk][it.iterationId] = {
                  values: data.values || {},
                  periodBegin: String(queryBegin),
                  periodEnd: String(queryEnd),
                };
                return next;
              });
            }).catch(function (err) {
              console.error('Deep dive fetch failed:', source, type, it.iterationId, err);
            }).finally(function () {
              setLoadingMetrics(function (prev) { var next = new Set(prev); next.delete(loadKey); return next; });
            });
          });

          // Wait for all iterations of this metric to complete before starting next metric
          await Promise.all(promises);
          // Yield to let React render this metric's data before fetching the next
          await new Promise(function (resolve) { setTimeout(resolve, 0); });
        }
      })();
    }).catch(function (err) {
      console.error('Failed to fetch period info:', err);
      setLoadingPeriods(false);
    });

    return function () { abortRef.current = true; };
  }, [iterations.length, metricList.join(','), JSON.stringify(metricConfigsProp), resolution, zoomRange]);

  if (loadingPeriods) {
    return (
      <div className="deepdive-view">
        <div className="compare-loading"><span className="spinner" /> Loading period info...</div>
      </div>
    );
  }

  if (!periodInfo || metricList.length === 0) {
    return (
      <div className="deepdive-view">
        <div className="empty-msg">Select metrics in Compare view using the "Dive" checkboxes, then switch to Deep Dive.</div>
      </div>
    );
  }

  return (
    <div className="deepdive-view">
      <div className="deepdive-controls">
        <span className="compare-filter-group">
          <label className="compare-filter-label">Resolution:</label>
          <input type="number" className="compare-filter-input" value={resolution} min={10} max={1000} step={10}
            style={{ width: 70 }}
            onChange={function (e) { setResolution(parseInt(e.target.value, 10) || 100); }}
          />
          <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>data points</span>
        </span>
        {zoomRange && (
          <button className="btn btn-sm btn-secondary" onClick={function () { setZoomRange(null); setPinnedElapsed(null); setHoverElapsed(null); }}>
            Reset Zoom ({Math.round(zoomRange.startPct * 100)}%-{Math.round(zoomRange.endPct * 100)}%)
          </button>
        )}
        <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>{zoomRange ? 'Click + drag to zoom further' : 'Click + drag on chart to zoom'}</span>
        {loadingMetrics.size > 0 && (
          <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>
            <span className="spinner" style={{ marginRight: 4 }} />
            Loading {loadingMetrics.size} metric(s)...
          </span>
        )}
      </div>

      {/* Iteration context: common properties and per-iteration identity */}
      {(function () {
        var hiddenSet = hiddenFields ? new Set(hiddenFields) : new Set();
        // Compute common vs varying
        var paramValues = {};
        var tagValues = {};
        var benchmarks = new Set();
        iterations.forEach(function (it) {
          if (it.benchmark) benchmarks.add(it.benchmark);
          (it.params || []).forEach(function (p) {
            if (!paramValues[p.arg]) paramValues[p.arg] = new Set();
            paramValues[p.arg].add(String(p.val));
          });
          (it.tags || []).forEach(function (t) {
            if (!tagValues[t.name]) tagValues[t.name] = new Set();
            tagValues[t.name].add(t.val);
          });
        });
        var commonItems = [];
        if (benchmarks.size === 1) commonItems.push({ key: 'benchmark', val: Array.from(benchmarks)[0], type: 'benchmark' });
        Object.keys(paramValues).sort().forEach(function (arg) {
          if (paramValues[arg].size === 1 && !hiddenSet.has('param:' + arg)) {
            commonItems.push({ key: arg, val: Array.from(paramValues[arg])[0], type: 'param' });
          }
        });
        Object.keys(tagValues).sort().forEach(function (name) {
          if (tagValues[name].size === 1 && !hiddenSet.has('tag:' + name)) {
            commonItems.push({ key: name, val: Array.from(tagValues[name])[0], type: 'tag' });
          }
        });

        return (
          <div className="deepdive-context">
            {commonItems.length > 0 && (
              <details className="deepdive-common-details" open>
                <summary className="deepdive-common-summary">Common ({commonItems.length})</summary>
                <div className="deepdive-common">
                  {commonItems.map(function (p, i) {
                    return (
                      <span key={i} className={p.type === 'benchmark' ? 'benchmark-badge' : p.type === 'tag' ? 'tag' : 'param param-common'}>
                        {p.type === 'tag' && <span className="tag-key">{p.key}</span>}
                        {p.type === 'tag' ? '=' + p.val : p.type === 'benchmark' ? p.val : p.key + '=' + p.val}
                      </span>
                    );
                  })}
                  <span className="deepdive-chip-legend">
                    <span className="benchmark-badge" style={{fontSize:9}}>bench</span>
                    <span className="tag" style={{fontSize:9}}>tag</span>
                    <span className="param" style={{fontSize:9}}>param</span>
                  </span>
                </div>
              </details>
            )}
            <div className="deepdive-iterations-divider"></div>
            <div className="deepdive-iterations">
              <span className="deepdive-iterations-label">Iterations:</span>
              {iterations.map(function (it, ii) {
                var theme = ITER_THEMES[ii % ITER_THEMES.length];
                return (
                  <div key={it.iterationId} className="deepdive-iter-card" style={{ borderColor: theme.base, backgroundColor: theme.base + '15' }}>
                    {renderIterChips(it, iterations, hiddenFields)}
                  </div>
                );
              })}
            </div>
          </div>
        );
      })()}

      {metricList.map(function (metricKey, mi) {
        var parts = metricKey.split('::');
        var source = parts[0];
        var type = parts[1];
        var metricResults = metricData[metricKey] || {};

        // Build per-iteration info
        var iterInfo = [];
        iterations.forEach(function (it, ii) {
          var result = metricResults[it.iterationId];
          if (!result || !result.values) return;
          iterInfo.push({
            iterIdx: ii,
            iterationId: it.iterationId,
            iterLabel: iterItemsToString(buildIterItems(it, iterations, hiddenFields)) || it.iterationId.substring(0, 8),
            periodBegin: Number(result.periodBegin),
            periodEnd: Number(result.periodEnd),
            labelKeys: Object.keys(result.values),
            values: result.values,
          });
        });

        // Build unified breakout label set (union across all iterations)
        var allBreakoutLabels = new Set();
        iterInfo.forEach(function (info) {
          info.labelKeys.forEach(function (lk) { allBreakoutLabels.add(lk); });
        });
        var sortedLabels = Array.from(allBreakoutLabels).sort(function (a, b) {
          var sa = parseSegments(a);
          var sb = parseSegments(b);
          for (var i = 0; i < Math.max(sa.length, sb.length); i++) {
            var cmp = naturalCompare(sa[i] || '', sb[i] || '');
            if (cmp !== 0) return cmp;
          }
          return 0;
        });

        // Build chart data and line keys with per-iteration color themes.
        // Use sample index as X coordinate so all iterations align perfectly.
        // The metric data is continuous — each series has exactly N samples
        // covering the full period with no gaps. Using the sample index (0..N-1)
        // ensures every series has a value at every X position.
        var lineKeys = [];
        var lineColors = {};

        // Find the longest period duration (for X-axis labels in elapsed time)
        var maxDuration = 0;
        var maxSamples = 0;
        iterInfo.forEach(function (info) {
          var dur = info.periodEnd - info.periodBegin;
          if (dur > maxDuration) maxDuration = dur;
        });

        // First pass: determine max sample count and build line keys
        iterInfo.forEach(function (info) {
          sortedLabels.forEach(function (lk, labelIdx) {
            var entries = info.values[lk];
            if (!entries || !Array.isArray(entries)) return;
            if (entries.length > maxSamples) maxSamples = entries.length;
            var lineKey = 'iter' + info.iterIdx + ':' + lk;
            var color = getIterColor(info.iterIdx, labelIdx);
            lineKeys.push({ key: lineKey, iterIdx: info.iterIdx, iterationId: info.iterationId, breakoutLabel: lk });
            lineColors[lineKey] = color;
          });
        });

        // Build chart data array: one entry per sample index
        var chartData = [];
        for (var si = 0; si < maxSamples; si++) {
          // Convert sample index to elapsed time using the longest period
          var elapsed = maxDuration > 0 && maxSamples > 0 ? Math.round(maxDuration * (si + 0.5) / maxSamples) : si;
          chartData.push({ elapsed: elapsed, _sampleIdx: si });
        }

        // Fill in values: each iteration's entries are indexed by sample position
        iterInfo.forEach(function (info) {
          var periodDuration = info.periodEnd - info.periodBegin;
          sortedLabels.forEach(function (lk) {
            var entries = info.values[lk];
            if (!entries || !Array.isArray(entries)) return;
            var lineKey = 'iter' + info.iterIdx + ':' + lk;
            // Sort entries by begin time to ensure correct order
            var sorted = entries.slice().sort(function (a, b) { return Number(a.begin) - Number(b.begin); });
            sorted.forEach(function (entry, ei) {
              if (ei < chartData.length) {
                chartData[ei][lineKey] = entry.value;
              }
            });
          });
        });

        var hasData = lineKeys.length > 0 && chartData.length > 0;

        // Build unified legend rows: one row per breakout label, columns per iteration
        var legendRows = sortedLabels.map(function (lk, labelIdx) {
          var segments = lk ? parseSegments(lk) : [];
          var iterCells = iterInfo.map(function (info) {
            var lineKey = 'iter' + info.iterIdx + ':' + lk;
            return { lineKey: lineKey, color: getIterColor(info.iterIdx, labelIdx), hasData: !!info.values[lk] };
          });
          return { breakoutLabel: lk, segments: segments, iterCells: iterCells };
        });

        // Get breakout dimension names from config
        var config = configLookup[metricKey] || {};
        var breakoutNames = (config.breakouts || []).map(function (b) {
          if (typeof b === 'object' && b !== null && b.name) return b.name;
          var eqIdx = b.indexOf('=');
          return eqIdx >= 0 ? b.substring(0, eqIdx) : b;
        });

        // Compute segment column stripping and rowSpans
        var numCols = legendRows.length > 0 && legendRows[0].segments.length > 0 ? legendRows[0].segments.length : 0;
        var colStripped = [];
        for (var col = 0; col < numCols; col++) {
          var vals = legendRows.map(function (r) { return r.segments[col] || ''; });
          var unique = Array.from(new Set(vals));
          var stripped = vals;
          if (unique.length > 1) {
            var suffix = '';
            var first = unique[0];
            for (var si2 = first.length - 1; si2 > 0; si2--) {
              var ch = first[si2];
              if (ch === '.' || ch === '-' || ch === '_') {
                var candidate = first.substring(si2);
                if (unique.every(function (v) { return v.endsWith(candidate); })) suffix = candidate;
              }
            }
            if (suffix) {
              stripped = vals.map(function (v) { return v.substring(0, v.length - suffix.length); });
            } else {
              var prefix = '';
              for (var pi = 0; pi < first.length - 1; pi++) {
                if (first[pi] === '.' || first[pi] === '-' || first[pi] === '_') {
                  var pcandidate = first.substring(0, pi + 1);
                  if (unique.every(function (v) { return v.startsWith(pcandidate); })) prefix = pcandidate;
                }
              }
              if (prefix) stripped = vals.map(function (v) { return v.substring(prefix.length); });
            }
          }
          colStripped.push(stripped);
        }

        var rowSpans = legendRows.map(function () { return new Array(numCols).fill(1); });
        for (var col2 = 0; col2 < numCols; col2++) {
          var spanStart = 0;
          for (var row = 1; row <= legendRows.length; row++) {
            var same = row < legendRows.length;
            if (same) {
              for (var c = 0; c <= col2; c++) {
                if ((colStripped[c] ? colStripped[c][row] : '') !== (colStripped[c] ? colStripped[c][spanStart] : '')) { same = false; break; }
              }
            }
            if (!same) {
              rowSpans[spanStart][col2] = row - spanStart;
              for (var r = spanStart + 1; r < row; r++) rowSpans[r][col2] = 0;
              spanStart = row;
            }
          }
        }

        // Get active entry: find nearest data point to the shared elapsed time
        var activeElapsed = pinnedElapsed != null ? pinnedElapsed : hoverElapsed;
        var isPinned = pinnedElapsed != null;
        var activeEntry = null;
        if (activeElapsed != null && chartData.length > 0) {
          // Binary-ish search for nearest elapsed time in this chart's data
          var bestIdx = 0;
          var bestDiff = Math.abs(chartData[0].elapsed - activeElapsed);
          for (var ai = 1; ai < chartData.length; ai++) {
            var diff = Math.abs(chartData[ai].elapsed - activeElapsed);
            if (diff < bestDiff) { bestDiff = diff; bestIdx = ai; }
            if (chartData[ai].elapsed > activeElapsed) break; // sorted, can stop early
          }
          activeEntry = chartData[bestIdx];
        }

        var opts = perMetricOpts[metricKey] || {};
        var splitByIter = !!opts.split;
        var chartType = opts.chartType || 'line';

        return (
          <div key={metricKey} className="deepdive-chart-panel">
            <div className="deepdive-chart-header">
              <h3 className="deepdive-chart-title">{source}::{type}</h3>
              {hasData && iterInfo.length > 1 && (
                <div className="deepdive-chart-controls">
                  <button className={'btn btn-sm ' + (!splitByIter ? 'btn-primary' : 'btn-secondary')} onClick={function () {
                    setPerMetricOpts(function (prev) { var n = Object.assign({}, prev); n[metricKey] = Object.assign({}, n[metricKey], { split: false }); return n; });
                  }}>Combined</button>
                  <button className={'btn btn-sm ' + (splitByIter ? 'btn-primary' : 'btn-secondary')} onClick={function () {
                    setPerMetricOpts(function (prev) { var n = Object.assign({}, prev); n[metricKey] = Object.assign({}, n[metricKey], { split: true }); return n; });
                  }}>Split</button>
                  {splitByIter && (
                    <>
                      <span className="deepdive-chart-controls-sep">|</span>
                      <button className={'btn btn-sm ' + (chartType === 'line' ? 'btn-primary' : 'btn-secondary')} onClick={function () {
                        setPerMetricOpts(function (prev) { var n = Object.assign({}, prev); n[metricKey] = Object.assign({}, n[metricKey], { chartType: 'line' }); return n; });
                      }}>Lines</button>
                      <button className={'btn btn-sm ' + (chartType === 'stacked' ? 'btn-primary' : 'btn-secondary')} onClick={function () {
                        setPerMetricOpts(function (prev) { var n = Object.assign({}, prev); n[metricKey] = Object.assign({}, n[metricKey], { chartType: 'stacked' }); return n; });
                      }}>Stacked</button>
                    </>
                  )}
                </div>
              )}
            </div>
            {!hasData && (
              <div className="deepdive-chart-loading">
                {loadingMetrics.size > 0 ? (
                  <><span className="spinner" style={{ marginRight: 4 }} /> Loading...</>
                ) : 'No data available'}
              </div>
            )}
            {hasData && (
              <>
                {(function () {
                  // Render chart(s): combined or split by iteration
                  function renderOneChart(data, lines, label, height, useStacked, yDomain) {
                    var thisActiveEntry = null;
                    if (activeElapsed != null && data.length > 0) {
                      var bi = 0;
                      var bd = Math.abs(data[0].elapsed - activeElapsed);
                      for (var j = 1; j < data.length; j++) {
                        var d = Math.abs(data[j].elapsed - activeElapsed);
                        if (d < bd) { bd = d; bi = j; }
                        if (data[j].elapsed > activeElapsed) break;
                      }
                      thisActiveEntry = data[bi];
                    }
                    return (
                      <div className="deepdive-chart-wrap">
                        {label && <div className="deepdive-chart-sublabel">{label}</div>}
                        <ResponsiveContainer width="100%" height={height}>
                          <ComposedChart data={data} margin={{ top: 10, right: 30, left: 60, bottom: label ? 10 : 30 }}
                            onMouseDown={function (e) {
                              if (e && e.activeTooltipIndex != null) {
                                var entry = data[e.activeTooltipIndex];
                                if (entry) {
                                  setBrushStart(entry.elapsed);
                                  setBrushEnd(null);
                                }
                              }
                            }}
                            onMouseMove={function (e) {
                              if (e && e.activeTooltipIndex != null) {
                                var entry = data[e.activeTooltipIndex];
                                if (entry) {
                                  if (brushStart != null) {
                                    setBrushEnd(entry.elapsed);
                                  } else if (pinnedElapsed == null) {
                                    setHoverElapsed(entry.elapsed);
                                  }
                                }
                              }
                            }}
                            onMouseUp={function () {
                              if (brushStart != null && brushEnd != null && brushStart !== brushEnd) {
                                // Compute zoom as percentage of total elapsed range
                                var minElapsed = data[0].elapsed;
                                var maxElapsed = data[data.length - 1].elapsed;
                                var totalRange = maxElapsed - minElapsed;
                                if (totalRange > 0) {
                                  var left = Math.min(brushStart, brushEnd);
                                  var right = Math.max(brushStart, brushEnd);
                                  // If already zoomed, compose with existing zoom
                                  var basePct = zoomRange || { startPct: 0, endPct: 1 };
                                  var baseRange = basePct.endPct - basePct.startPct;
                                  var newStartPct = basePct.startPct + baseRange * ((left - minElapsed) / totalRange);
                                  var newEndPct = basePct.startPct + baseRange * ((right - minElapsed) / totalRange);
                                  setZoomRange({ startPct: newStartPct, endPct: newEndPct });
                                  setPinnedElapsed(null);
                                  setHoverElapsed(null);
                                }
                              } else if (brushStart != null && (brushEnd == null || brushStart === brushEnd)) {
                                // Click without drag — pin/unpin
                                var ce = brushStart;
                                setPinnedElapsed(function (prev) {
                                  if (prev != null) { setHoverElapsed(ce); return null; }
                                  return ce;
                                });
                              }
                              setBrushStart(null);
                              setBrushEnd(null);
                            }}
                            onMouseLeave={function () {
                              setBrushStart(null);
                              setBrushEnd(null);
                            }}
                          >
                            <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                            <XAxis dataKey="elapsed" tickFormatter={formatElapsed} stroke="var(--border)"
                              tick={{ fontSize: 11, fill: 'var(--text-secondary)' }}
                              label={!label ? { value: 'Elapsed Time', position: 'insideBottom', offset: -15, fontSize: 11, fill: 'var(--text-muted)' } : undefined}
                            />
                            <YAxis tick={{ fontSize: 11, fill: 'var(--text-secondary)' }} stroke="var(--border)"
                              domain={yDomain || ['auto', 'auto']} />
                            <Tooltip content={function () { return <div style={{ display: 'none' }} />; }}
                              cursor={{ stroke: 'var(--text-muted)', strokeWidth: 1, strokeDasharray: '3 3' }} />
                            {thisActiveEntry && !brushStart && (
                              <ReferenceLine x={thisActiveEntry.elapsed} stroke={isPinned ? '#ff6b6b' : 'var(--text-muted)'} strokeDasharray={isPinned ? '6 4' : '3 3'} strokeWidth={isPinned ? 2 : 1} />
                            )}
                            {brushStart != null && brushEnd != null && (
                              <ReferenceArea x1={Math.min(brushStart, brushEnd)} x2={Math.max(brushStart, brushEnd)} fill="var(--accent)" fillOpacity={0.2} stroke="var(--accent)" strokeOpacity={0.5} />
                            )}
                            {lines.map(function (lk) {
                              if (useStacked) {
                                return (
                                  <Area key={lk.key} dataKey={lk.key} type="monotone" stackId="stack"
                                    stroke={lineColors[lk.key]} fill={lineColors[lk.key]} fillOpacity={0.6}
                                    strokeWidth={0.5} dot={false} connectNulls={false} name={lk.key} />
                                );
                              }
                              return (
                                <Line key={lk.key} dataKey={lk.key} type="monotone" stroke={lineColors[lk.key]}
                                  strokeWidth={1.5} dot={false} connectNulls={true} name={lk.key} />
                              );
                            })}
                          </ComposedChart>
                        </ResponsiveContainer>
                      </div>
                    );
                  }

                  if (!splitByIter) {
                    return <div key="combined">{renderOneChart(chartData, lineKeys, null, 300, false, null)}</div>;
                  } else {
                    // Compute global Y-axis domain across all iterations for consistent scale
                    var globalMax = 0;
                    chartData.forEach(function (entry) {
                      lineKeys.forEach(function (lk) {
                        if (entry[lk.key] != null && entry[lk.key] > globalMax) globalMax = entry[lk.key];
                      });
                      // For stacked mode, compute sum per time point per iteration
                      if (chartType === 'stacked') {
                        iterInfo.forEach(function (info) {
                          var sum = 0;
                          lineKeys.forEach(function (lk) {
                            if (lk.iterIdx === info.iterIdx && entry[lk.key] != null) sum += entry[lk.key];
                          });
                          if (sum > globalMax) globalMax = sum;
                        });
                      }
                    });
                    var yDomain = [0, globalMax * 1.05];

                    return iterInfo.map(function (info) {
                      var iterLines = lineKeys.filter(function (lk) { return lk.iterIdx === info.iterIdx; });
                      if (iterLines.length === 0) return null;
                      var iterData = chartData.map(function (entry) {
                        var d = { elapsed: entry.elapsed };
                        iterLines.forEach(function (lk) {
                          if (entry[lk.key] != null) d[lk.key] = entry[lk.key];
                        });
                        return d;
                      }).filter(function (d) {
                        return iterLines.some(function (lk) { return d[lk.key] != null; });
                      });
                      return renderOneChart(iterData, iterLines, info.iterLabel, 200, chartType === 'stacked', yDomain);
                    });
                  }
                })()}

                {/* Unified series legend table — one row per breakout label, columns per iteration */}
                <div className="deepdive-legend">
                  <div className="deepdive-legend-header">
                    {activeEntry ? (
                      <span className="deepdive-legend-time">
                        {isPinned ? '\u{1F512} ' : ''}{formatElapsed(activeEntry.elapsed)}
                        {isPinned && <button className="deepdive-legend-unpin" onClick={function () { setPinnedElapsed(null); }}>&times;</button>}
                      </span>
                    ) : (
                      <span className="deepdive-legend-hint">Move pointer over chart to see values</span>
                    )}
                  </div>
                  <div className="deepdive-legend-body">
                    <table className="deepdive-legend-table">
                      {breakoutNames.length > 0 && (
                        <thead>
                          <tr>
                            {breakoutNames.map(function (name, ni) {
                              return <th key={ni}>{name}</th>;
                            })}
                            {iterInfo.map(function (info) {
                              return <th key={info.iterIdx} colSpan={2}></th>;
                            })}
                          </tr>
                        </thead>
                      )}
                      <tbody>
                        {legendRows.map(function (row, ri) {
                          return (
                            <tr key={row.breakoutLabel || ri}>
                              {numCols > 0 ? (function () {
                                var cells = [];
                                for (var ci = 0; ci < numCols; ci++) {
                                  if (rowSpans[ri][ci] > 0) {
                                    var span = rowSpans[ri][ci];
                                    cells.push(
                                      <td key={ci} className="deepdive-legend-seg" rowSpan={span > 1 ? span : undefined}>
                                        {span > 1 ? (
                                          <div className="deepdive-legend-seg-sticky">{colStripped[ci] ? colStripped[ci][ri] : row.segments[ci]}</div>
                                        ) : (colStripped[ci] ? colStripped[ci][ri] : row.segments[ci])}
                                      </td>
                                    );
                                  }
                                }
                                return cells;
                              })() : <td className="deepdive-legend-seg">-</td>}
                              {row.iterCells.map(function (cell) {
                                var value = activeEntry ? activeEntry[cell.lineKey] : null;
                                return [
                                  <td key={cell.lineKey + '-v'} className="deepdive-legend-val deepdive-legend-iter-first">{cell.hasData ? (value != null ? formatValue(value) : '-') : ''}</td>,
                                  <td key={cell.lineKey + '-c'} className="deepdive-legend-swatch-cell">{cell.hasData && <span className="deepdive-legend-swatch" style={{ backgroundColor: cell.color }}></span>}</td>
                                ];
                              })}
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </div>
              </>
            )}
          </div>
        );
      })}
    </div>
  );
}
