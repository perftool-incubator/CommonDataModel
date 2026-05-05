import { useState, useMemo, useCallback, useEffect, useRef } from 'react';
import * as api from '../api/cdm';
import { timeWork } from '../debugLog';

function buildRunUrl(source) {
  if (!source) return null;
  // source format: "hostname//var/lib/crucible/run/<run-dir>"
  // splitting on "//" gives ["hostname", "var/lib/crucible/run/<run-dir>"]
  var parts = source.split('//');
  if (parts.length < 2) return null;
  var host = parts[0];
  var path = '/' + parts.slice(1).join('//');
  var runPath = path.replace(/^\/var\/lib\/crucible\/run\//, '/run/');
  return 'http://' + host + ':8080' + runPath;
}

function formatDate(ts) {
  if (!ts) return '-';
  var d = new Date(Number(ts));
  return d.toLocaleDateString() + ' ' + d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

function formatMetric(pm) {
  if (!pm) return '-';
  if (typeof pm === 'string') return pm;
  const source = pm.source || '';
  const type = pm.type || '';
  return [source, type].filter(Boolean).join('::') || '-';
}

function formatValue(v) {
  if (v == null) return '';
  if (Math.abs(v) >= 1000) return v.toFixed(0);
  if (Math.abs(v) >= 1) return v.toFixed(2);
  return v.toPrecision(3);
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

// Get the value of a dimension for an iteration
function getDimValue(it, dim) {
  if (!dim || dim === 'none') return '__all__';
  if (dim === 'run') return it.runId;
  if (dim === 'date') return it.runBegin || '0';
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

// Format the display value for a dimension (used in group cells)
function formatDimDisplayValue(dim, value) {
  if (dim === 'date') {
    if (!value || value === '0') return '-';
    var d = new Date(Number(value));
    return d.toLocaleDateString() + ' ' + d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  }
  return value;
}

// Get a display label for a dimension
function formatDimLabel(dim) {
  if (!dim || dim === 'none') return '';
  if (dim === 'run') return 'run';
  if (dim === 'date') return 'date';
  if (dim === 'benchmark') return 'benchmark';
  if (dim.startsWith('param:')) return dim.substring(6);
  if (dim.startsWith('tag:')) return dim.substring(4);
  return dim;
}

// Insert zero-width spaces after natural separator characters to allow clean wrapping
function wrapFriendly(str) {
  if (!str) return str;
  // Insert \u200B (zero-width space) after -, _, ., ,, /, :
  return String(str).replace(/([_\-.,/:])(?!$)/g, '$1\u200B');
}

// Get the CSS class for a dimension chip
function dimChipClass(dim) {
  if (dim === 'benchmark') return 'benchmark-badge';
  if (dim.startsWith('tag:')) return 'tag';
  return 'param';
}

// Compute varying dimensions sorted by distinct value count (fewest first)
function computeGroupDims(iterations) {
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
  // Count distinct dates (by run — each run has one date)
  var dates = new Set();
  for (var j = 0; j < iterations.length; j++) {
    if (iterations[j].runBegin) dates.add(iterations[j].runBegin);
  }
  var dims = [];
  if (runs.size > 1) dims.push({ dim: 'run', count: runs.size });
  if (dates.size > 1) dims.push({ dim: 'date', count: dates.size });
  if (benchmarks.size > 1) dims.push({ dim: 'benchmark', count: benchmarks.size });
  Object.keys(paramValues).sort().forEach(function (arg) {
    if (paramValues[arg].size > 1) dims.push({ dim: 'param:' + arg, count: paramValues[arg].size });
  });
  Object.keys(tagValues).sort().forEach(function (name) {
    if (tagValues[name].size > 1) dims.push({ dim: 'tag:' + name, count: tagValues[name].size });
  });
  // Sort by distinct count ascending (fewest values = best top-level grouping)
  dims.sort(function (a, b) { return a.count - b.count; });
  return dims.map(function (d) { return d.dim; });
}

// Build a recursive tree grouping iterations by each dimension level
// sortDirs is an object: { dim: 'asc' | 'desc' }, defaults to 'asc'
function buildGroupTree(iterations, dims, depth, sortDirs) {
  if (depth >= dims.length) {
    // Leaf level: return individual iterations
    return { iterations: iterations };
  }
  var dim = dims[depth];
  var groups = {};
  var groupOrder = [];
  iterations.forEach(function (it) {
    var val = getDimValue(it, dim);
    if (!groups[val]) {
      groups[val] = [];
      groupOrder.push(val);
    }
    groups[val].push(it);
  });
  // Sort group keys naturally, respecting per-dimension sort direction
  var dir = (sortDirs && sortDirs[dim]) || 'asc';
  groupOrder.sort(function (a, b) {
    var cmp = naturalCompare(a, b);
    return dir === 'desc' ? -cmp : cmp;
  });
  var children = groupOrder.map(function (val) {
    var subtree = buildGroupTree(groups[val], dims, depth + 1, sortDirs);
    subtree.value = val;
    subtree.dim = dim;
    return subtree;
  });
  return { children: children };
}

// Count total leaf iterations in a tree node
function countLeaves(node) {
  if (node.iterations) return node.iterations.length;
  var total = 0;
  (node.children || []).forEach(function (c) { total += countLeaves(c); });
  return total;
}

// Collect all leaf iterations from a tree node
function collectLeaves(node) {
  if (node.iterations) return node.iterations;
  var result = [];
  (node.children || []).forEach(function (c) {
    result = result.concat(collectLeaves(c));
  });
  return result;
}

// Flatten the tree into table rows (group headers + leaf rows)
// Each row is either { type: 'group', dim, value, depth, rowSpan, iterations } or
// { type: 'leaf', iteration, depth, coveredDims }
function flattenTree(node, depth, coveredDims, groupDims) {
  var rows = [];
  if (node.children) {
    node.children.forEach(function (child, childIdx) {
      var leafCount = countLeaves(child);
      var leaves = collectLeaves(child);
      // Add group header row
      rows.push({
        type: 'group',
        dim: child.dim,
        value: child.value,
        depth: depth,
        rowSpan: leafCount,
        iterations: leaves,
        groupIdx: childIdx,
      });
      var newCovered = coveredDims.concat([child.dim]);
      var subRows = flattenTree(child, depth + 1, newCovered, groupDims);
      rows = rows.concat(subRows);
    });
  } else if (node.iterations) {
    node.iterations.forEach(function (it) {
      rows.push({
        type: 'leaf',
        iteration: it,
        depth: depth,
        coveredDims: coveredDims,
      });
    });
  }
  return rows;
}

export default function IterationTable({ iterations, selected, onToggleSelect, onToggleSelectAll, loading, onAddTagFilter, onAddParamFilter, columnOrder, onColumnOrderChange, columnHidden, onColumnHiddenChange }) {
  const [sortKey, setSortKey] = useState(null);
  const [sortDir, setSortDir] = useState('asc');
  const [paramFilter, setParamFilter] = useState('');
  const [metricValues, setMetricValues] = useState({}); // { iterationId: { mean, stddevPct, sampleValues } }
  const [metricLoading, setMetricLoading] = useState(false);

  const fetchMetricValues = useCallback(async () => {
    if (iterations.length === 0) return;
    setMetricLoading(true);
    try {
      // Collect unique run IDs and date range from iterations
      var runIdSet = new Set();
      iterations.forEach(function (it) { runIdSet.add(it.runId); });
      var runIds = Array.from(runIdSet);
      // Infer start/end from run dates
      var starts = iterations.filter(function (it) { return it.runBegin; }).map(function (it) { return it.runBegin; });
      var minBegin = starts.length > 0 ? Math.min.apply(null, starts) : null;
      var maxBegin = starts.length > 0 ? Math.max.apply(null, starts) : null;
      var startMonth = minBegin ? new Date(Number(minBegin)) : null;
      var endMonth = maxBegin ? new Date(Number(maxBegin)) : null;
      var start = startMonth ? startMonth.getFullYear() + '.' + String(startMonth.getMonth() + 1).padStart(2, '0') : null;
      var end = endMonth ? endMonth.getFullYear() + '.' + String(endMonth.getMonth() + 1).padStart(2, '0') : null;

      var res = await timeWork('Fetch metric values for ' + iterations.length + ' iteration(s)', function () {
        return api.getIterationMetricValues(runIds, start, end);
      });
      setMetricValues(res.values || {});
    } catch (err) {
      console.error('Failed to fetch metric values:', err);
    } finally {
      setMetricLoading(false);
    }
  }, [iterations]);

  const handleSort = (key) => {
    if (sortKey === key) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortKey(key);
      setSortDir('asc');
    }
  };

  const filtered = useMemo(() => {
    if (!paramFilter) return iterations;
    const q = paramFilter.toLowerCase();
    // Support "arg=val" syntax: split on first "=" and match both sides
    const eqIdx = q.indexOf('=');
    if (eqIdx >= 0) {
      const argQ = q.substring(0, eqIdx);
      const valQ = q.substring(eqIdx + 1);
      return iterations.filter((it) =>
        it.params.some(
          (p) =>
            (!argQ || (p.arg && p.arg.toLowerCase().includes(argQ))) &&
            (!valQ || (p.val && String(p.val).toLowerCase().includes(valQ))),
        ),
      );
    }
    return iterations.filter((it) =>
      it.params.some(
        (p) =>
          (p.arg && p.arg.toLowerCase().includes(q)) ||
          (p.val && String(p.val).toLowerCase().includes(q)),
      ),
    );
  }, [iterations, paramFilter]);

  const sorted = useMemo(() => {
    if (!sortKey) return filtered;
    const arr = [...filtered];
    arr.sort((a, b) => {
      let va, vb;
      switch (sortKey) {
        case 'benchmark':
          va = a.benchmark || '';
          vb = b.benchmark || '';
          break;
        case 'samples':
          va = a.sampleCount;
          vb = b.sampleCount;
          break;
        case 'status':
          va = a.passCount;
          vb = b.passCount;
          break;
        case 'metric':
          va = (metricValues[a.iterationId] && metricValues[a.iterationId].mean) || 0;
          vb = (metricValues[b.iterationId] && metricValues[b.iterationId].mean) || 0;
          break;
        case 'run':
          va = a.runId;
          vb = b.runId;
          break;
        case 'date':
          va = a.runBegin || 0;
          vb = b.runBegin || 0;
          break;
        default:
          return 0;
      }
      if (va < vb) return sortDir === 'asc' ? -1 : 1;
      if (va > vb) return sortDir === 'asc' ? 1 : -1;
      return 0;
    });
    return arr;
  }, [filtered, sortKey, sortDir, metricValues]);

  const thClass = (key) => {
    if (sortKey !== key) return '';
    return sortDir === 'asc' ? 'sorted-asc' : 'sorted-desc';
  };

  const allOnPageSelected = sorted.length > 0 && sorted.every((it) => selected.has(it.iterationId));

  // Compute globally common items (same value across ALL iterations)
  const commonList = useMemo(() => {
    if (iterations.length === 0) return [];
    var paramValues = {};
    var tagValues = {};
    var benchmarks = new Set();
    for (var i = 0; i < iterations.length; i++) {
      var it = iterations[i];
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
    var common = [];
    if (benchmarks.size === 1) {
      common.push({ key: 'benchmark', val: Array.from(benchmarks)[0], type: 'benchmark' });
    }
    Object.keys(paramValues).sort().forEach(function (arg) {
      if (paramValues[arg].size === 1) {
        common.push({ key: arg, val: Array.from(paramValues[arg])[0], type: 'param' });
      }
    });
    Object.keys(tagValues).sort().forEach(function (name) {
      if (tagValues[name].size === 1) {
        common.push({ key: name, val: Array.from(tagValues[name])[0], type: 'tag' });
      }
    });
    return common;
  }, [iterations]);

  // Group-by dimensions as state so user can reorder/hide/sort
  const [groupDims, setGroupDimsLocal] = useState([]);
  const [hiddenDims, setHiddenDimsLocal] = useState([]);
  const [dimSortDir, setDimSortDir] = useState({}); // { dim: 'asc' | 'desc' }, default asc
  const prevIterCount = useRef(0);

  function setGroupDims(newDims) {
    setGroupDimsLocal(newDims);
    if (onColumnOrderChange) onColumnOrderChange(newDims);
  }
  function setHiddenDims(val) {
    if (typeof val === 'function') {
      setHiddenDimsLocal(function (prev) {
        var next = val(prev);
        if (onColumnHiddenChange) onColumnHiddenChange(next);
        return next;
      });
    } else {
      setHiddenDimsLocal(val);
      if (onColumnHiddenChange) onColumnHiddenChange(val);
    }
  }

  // Auto-compute group dims when iterations change, merging with saved order
  useEffect(function () {
    if (iterations.length !== prevIterCount.current) {
      prevIterCount.current = iterations.length;
      var computed = computeGroupDims(iterations);
      if (columnOrder && columnOrder.length > 0) {
        // Merge: keep saved order for dims that still exist, append new ones
        var computedSet = new Set(computed);
        var merged = columnOrder.filter(function (d) { return computedSet.has(d); });
        computed.forEach(function (d) { if (merged.indexOf(d) < 0) merged.push(d); });
        setGroupDimsLocal(merged);
        if (onColumnOrderChange) onColumnOrderChange(merged);
      } else {
        setGroupDimsLocal(computed);
        if (onColumnOrderChange) onColumnOrderChange(computed);
      }
      if (columnHidden && columnHidden.length > 0) {
        var validHidden = columnHidden.filter(function (d) { return new Set(computed).has(d); });
        setHiddenDimsLocal(validHidden);
        if (onColumnHiddenChange) onColumnHiddenChange(validHidden);
      } else {
        setHiddenDimsLocal([]);
      }
      setDimSortDir({});
    }
  }, [iterations]);

  // Active group dims = groupDims minus hidden
  const activeGroupDims = useMemo(function () {
    var hiddenSet = new Set(hiddenDims);
    return groupDims.filter(function (d) { return !hiddenSet.has(d); });
  }, [groupDims, hiddenDims]);

  // Build the hierarchical tree from active group dims
  const tableRows = useMemo(function () {
    if (activeGroupDims.length === 0) {
      return sorted.map(function (it) {
        return { type: 'leaf', iteration: it, depth: 0, coveredDims: [] };
      });
    }
    var tree = buildGroupTree(sorted, activeGroupDims, 0, dimSortDir);
    return flattenTree(tree, 0, [], activeGroupDims);
  }, [sorted, activeGroupDims, dimSortDir]);

  // Reorder group dimensions (operates on the full groupDims list)
  function moveGroupDim(dim, direction) {
    // Find index in activeGroupDims
    var activeIdx = activeGroupDims.indexOf(dim);
    var targetActiveIdx = activeIdx + direction;
    if (targetActiveIdx < 0 || targetActiveIdx >= activeGroupDims.length) return;
    // Swap in the full groupDims array
    var fullIdx = groupDims.indexOf(activeGroupDims[activeIdx]);
    var fullTargetIdx = groupDims.indexOf(activeGroupDims[targetActiveIdx]);
    var newDims = groupDims.slice();
    var tmp = newDims[fullIdx];
    newDims[fullIdx] = newDims[fullTargetIdx];
    newDims[fullTargetIdx] = tmp;
    setGroupDims(newDims);
  }

  function hideGroupDim(dim) {
    setHiddenDims(function (prev) { return prev.concat([dim]); });
  }

  function unhideGroupDim(dim) {
    setHiddenDims(function (prev) { return prev.filter(function (d) { return d !== dim; }); });
  }

  function toggleDimSort(dim) {
    setDimSortDir(function (prev) {
      var next = Object.assign({}, prev);
      next[dim] = prev[dim] === 'desc' ? 'asc' : 'desc';
      return next;
    });
  }

  // For leaf rows, compute which varying items are NOT covered by group headers
  function getLeafVarying(it, coveredDims) {
    var coveredSet = new Set(coveredDims);
    var items = [];
    // Only include items from varying dimensions that aren't covered by a group header
    if (!coveredSet.has('benchmark') && iterations.length > 0) {
      var benchmarks = new Set(iterations.map(function (i) { return i.benchmark; }));
      if (benchmarks.size > 1) {
        items.push({ key: 'benchmark', val: it.benchmark || '', type: 'benchmark' });
      }
    }
    (it.params || []).forEach(function (p) {
      var dim = 'param:' + p.arg;
      if (coveredSet.has(dim)) return;
      // Only show if this param varies globally
      var vals = new Set(iterations.map(function (i) {
        var pp = (i.params || []).find(function (x) { return x.arg === p.arg; });
        return pp ? String(pp.val) : '';
      }));
      if (vals.size > 1) {
        items.push({ key: p.arg, val: p.val, type: 'param' });
      }
    });
    (it.tags || []).forEach(function (t) {
      var dim = 'tag:' + t.name;
      if (coveredSet.has(dim)) return;
      var vals = new Set(iterations.map(function (i) {
        var tt = (i.tags || []).find(function (x) { return x.name === t.name; });
        return tt ? tt.val : '';
      }));
      if (vals.size > 1) {
        items.push({ key: t.name, val: t.val, type: 'tag' });
      }
    });
    return items;
  }

  // Track which group header cells to render (rowSpan logic)
  // Each group row may emit cells at multiple depth levels on the first row of a group
  const renderPlan = useMemo(function () {
    // For each table row index, determine which group cells to render
    // A group cell is rendered on the first leaf row of that group
    var plan = [];
    var groupStack = []; // stack of { dim, value, startRow, rowSpan, depth }
    var leafIdx = 0;

    for (var i = 0; i < tableRows.length; i++) {
      var row = tableRows[i];
      if (row.type === 'group') {
        groupStack.push({
          dim: row.dim,
          value: row.value,
          rowSpan: row.rowSpan,
          depth: row.depth,
          iterations: row.iterations,
          rendered: false,
        });
      } else {
        // Leaf row — collect any unrendered group cells
        var cells = [];
        for (var g = 0; g < groupStack.length; g++) {
          if (!groupStack[g].rendered) {
            cells.push({
              dim: groupStack[g].dim,
              value: groupStack[g].value,
              rowSpan: groupStack[g].rowSpan,
              depth: groupStack[g].depth,
              iterations: groupStack[g].iterations,
            });
            groupStack[g].rendered = true;
            groupStack[g].remaining = groupStack[g].rowSpan;
          }
        }
        plan.push({
          iteration: row.iteration,
          coveredDims: row.coveredDims,
          groupCells: cells,
          leafIdx: leafIdx,
        });
        leafIdx++;
        // Decrement remaining and pop finished groups
        for (var g = groupStack.length - 1; g >= 0; g--) {
          if (groupStack[g].remaining != null) {
            groupStack[g].remaining--;
            if (groupStack[g].remaining <= 0) {
              groupStack.splice(g, 1);
            }
          }
        }
      }
    }
    return plan;
  }, [tableRows]);

  // Compute alternating group colors based on top-level group index
  const rowGroupParity = useMemo(function () {
    if (renderPlan.length === 0) return [];
    var parity = [];
    var currentParity = 0;
    var currentTopValue = null;
    for (var i = 0; i < renderPlan.length; i++) {
      // Check if any depth-0 group cell starts on this row
      var topCell = renderPlan[i].groupCells.find(function (c) { return c.depth === 0; });
      if (topCell) {
        if (currentTopValue !== null && topCell.value !== currentTopValue) {
          currentParity = 1 - currentParity;
        }
        currentTopValue = topCell.value;
      }
      parity.push(currentParity);
    }
    return parity;
  }, [renderPlan]);

  return (
    <div className="results-panel">
      <div className="results-header">
        <h2>Iterations {iterations.length > 0 && `(${iterations.length})`}</h2>
        {iterations.length > 0 && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <span className="benchmark-badge" style={{fontSize:9}}>bench</span>
            <span className="tag" style={{fontSize:9}}>tag</span>
            <span className="param" style={{fontSize:9}}>param</span>
            <button
              className="btn btn-sm btn-secondary"
              onClick={fetchMetricValues}
              disabled={metricLoading || iterations.length === 0}
            >
              {metricLoading ? (
                <><span className="spinner" style={{ marginRight: 4 }} /> Loading...</>
              ) : Object.keys(metricValues).length > 0 ? 'Refresh Values' : 'Show Values'}
            </button>
            <div className="field" style={{ flexDirection: 'row', alignItems: 'center', gap: 6 }}>
              <label style={{ textTransform: 'none', letterSpacing: 0 }}>Filter params:</label>
              <input
                type="text"
                placeholder="e.g. bs=4k"
                value={paramFilter}
                onChange={(e) => setParamFilter(e.target.value)}
                style={{ width: 160 }}
              />
            </div>
          </div>
        )}
      </div>
      {(commonList.length > 0 || hiddenDims.length > 0) && (
        <div className="results-common">
          {commonList.length > 0 && (
            <details className="results-common-details" open>
              <summary className="results-common-summary">Common ({commonList.length})</summary>
              <div className="results-common-chips">
                {commonList.map(function (p, i) {
                  return (
                    <span key={i} className={p.type === 'benchmark' ? 'benchmark-badge' : p.type === 'tag' ? 'tag' : 'param param-common'}>
                      {p.type === 'tag' && <span className="tag-key">{p.key}</span>}
                      {p.type === 'tag' ? '=' + p.val : p.type === 'benchmark' ? p.val : p.key + '=' + p.val}
                    </span>
                  );
                })}
              </div>
            </details>
          )}
          {hiddenDims.length > 0 && (
            <div className="results-common-hidden">
              <span className="results-common-label">Hidden:</span>
              {hiddenDims.map(function (dim) {
                return (
                  <span key={dim} className={dimChipClass(dim) + ' hidden-dim-chip'} onClick={function () { unhideGroupDim(dim); }} title="Click to restore">
                    {formatDimLabel(dim)}
                  </span>
                );
              })}
            </div>
          )}
        </div>
      )}
      <div className="results-table-wrap">
        <table className="results-table">
          <thead>
            <tr>
              {activeGroupDims.map(function (dim, di) {
                var label = formatDimLabel(dim);
                var sortDir = dimSortDir[dim] || 'asc';
                return (
                  <th key={dim} className="group-header-th">
                    <span className={'group-header-name ' + dimChipClass(dim)}>{label}</span>
                    <div className="group-header-controls">
                      {di > 0 && (
                        <button className="group-reorder-btn" onClick={function () { moveGroupDim(dim, -1); }} title="Move left">&lt;</button>
                      )}
                      <button className={'group-sort-btn' + (sortDir === 'desc' ? ' sort-desc' : '')} onClick={function () { toggleDimSort(dim); }} title={sortDir === 'asc' ? 'Sort descending' : 'Sort ascending'}>
                        {sortDir === 'asc' ? '\u25B2' : '\u25BC'}
                      </button>
                      <button className="group-hide-btn" onClick={function () { hideGroupDim(dim); }} title="Hide this dimension">&times;</button>
                      {di < activeGroupDims.length - 1 && (
                        <button className="group-reorder-btn" onClick={function () { moveGroupDim(dim, 1); }} title="Move right">&gt;</button>
                      )}
                    </div>
                  </th>
                );
              })}
              <th>
                <input
                  type="checkbox"
                  checked={allOnPageSelected}
                  onChange={() => onToggleSelectAll(sorted)}
                  disabled={sorted.length === 0}
                />
              </th>
              <th>
                Details
                {sorted.length > 0 && activeGroupDims.length === 0 && <div className="column-hint">click any value to add filter</div>}
              </th>
              <th className={thClass('metric')} onClick={() => handleSort('metric')}>
                Primary Metric
              </th>
              <th className={thClass('samples')} onClick={() => handleSort('samples')}>
                Samples
              </th>
              <th className={thClass('status')} onClick={() => handleSort('status')}>
                Status
              </th>
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr className="loading-row">
                <td colSpan={6 + activeGroupDims.length}>
                  <span className="spinner" /> Loading iterations...
                </td>
              </tr>
            )}
            {!loading && sorted.length === 0 && iterations.length === 0 && (
              <tr className="loading-row">
                <td colSpan={6 + activeGroupDims.length}>
                  <div className="workflow-guide">
                    <div className="workflow-title">Getting Started</div>
                    <div className="workflow-steps">
                      <div className="workflow-step">
                        <span className="workflow-num">1</span>
                        <span>Set a <b>date range</b> and optionally filter by benchmark, tags, params, or user above</span>
                      </div>
                      <div className="workflow-step">
                        <span className="workflow-num">2</span>
                        <span>Click <b>Search</b> to find matching iterations</span>
                      </div>
                      <div className="workflow-step">
                        <span className="workflow-num">3</span>
                        <span>Click values in the results to refine your filters, then search again</span>
                      </div>
                      <div className="workflow-step">
                        <span className="workflow-num">4</span>
                        <span>Select iterations with <b>checkboxes</b>, then click <b>Compare</b> to see bar charts</span>
                      </div>
                    </div>
                  </div>
                </td>
              </tr>
            )}
            {!loading && sorted.length === 0 && iterations.length > 0 && (
              <tr className="loading-row">
                <td colSpan={6 + activeGroupDims.length}>
                  <span className="empty-msg">No iterations match the current filter.</span>
                </td>
              </tr>
            )}
            {!loading &&
              renderPlan.map(function (row, rowIdx) {
                var it = row.iteration;
                var rowClasses = [];
                if (selected.has(it.iterationId)) rowClasses.push('selected');
                rowClasses.push(rowGroupParity[rowIdx] === 0 ? 'run-group-even' : 'run-group-odd');
                // Add border when the top-level group changes
                if (rowIdx > 0 && rowGroupParity[rowIdx] !== rowGroupParity[rowIdx - 1]) {
                  rowClasses.push('run-group-border');
                }
                var leafVarying = getLeafVarying(it, row.coveredDims);

                return (
                  <tr
                    key={it.iterationId}
                    className={rowClasses.join(' ')}
                    onClick={() => onToggleSelect(it)}
                    style={{ cursor: 'pointer' }}
                  >
                    {row.groupCells.map(function (cell) {
                      var allInGroup = cell.iterations.every(function (gi) { return selected.has(gi.iterationId); });
                      var displayValue = cell.value;
                      var chipClass = dimChipClass(cell.dim);
                      var label = formatDimLabel(cell.dim);

                      // For run dimension, show run ID as a link
                      if (cell.dim === 'run') {
                        var runIt = cell.iterations[0];
                        return (
                          <td key={cell.dim + ':' + cell.value} rowSpan={cell.rowSpan} className="group-cell" onClick={function (e) { e.stopPropagation(); }}>
                            <div className="group-cell-content">
                              <input
                                type="checkbox"
                                checked={allInGroup}
                                onChange={function () { onToggleSelectAll(cell.iterations); }}
                                title={'Select all ' + cell.rowSpan + ' iteration(s) in this group'}
                              />
                              {buildRunUrl(runIt.runSource) ? (
                                <a className="run-id" href={buildRunUrl(runIt.runSource)} target="_blank" rel="noopener noreferrer">{wrapFriendly(displayValue)}</a>
                              ) : (
                                <span className="run-id">{wrapFriendly(displayValue)}</span>
                              )}
                            </div>
                          </td>
                        );
                      }

                      var formattedValue = formatDimDisplayValue(cell.dim, displayValue);

                      return (
                        <td key={cell.dim + ':' + cell.value} rowSpan={cell.rowSpan} className="group-cell" onClick={function (e) { e.stopPropagation(); }}>
                          <div className="group-cell-content">
                            <input
                              type="checkbox"
                              checked={allInGroup}
                              onChange={function () { onToggleSelectAll(cell.iterations); }}
                              title={'Select all ' + cell.rowSpan + ' iteration(s) in this group'}
                            />
                            <span
                              className={chipClass + (cell.dim.startsWith('tag:') || cell.dim.startsWith('param:') ? ' clickable-filter' : '')}
                              title={cell.dim.startsWith('tag:') || cell.dim.startsWith('param:') ? 'Click to filter by ' + label + '=' + displayValue : formattedValue}
                              onClick={function (e) {
                                e.stopPropagation();
                                if (cell.dim.startsWith('tag:')) onAddTagFilter(label, displayValue);
                                else if (cell.dim.startsWith('param:')) onAddParamFilter(label, displayValue);
                              }}
                            >
                              {wrapFriendly(formattedValue)}
                            </span>
                          </div>
                        </td>
                      );
                    })}
                    <td onClick={(e) => e.stopPropagation()}>
                      <input
                        type="checkbox"
                        checked={selected.has(it.iterationId)}
                        onChange={() => onToggleSelect(it)}
                      />
                    </td>
                    <td>
                      {leafVarying.length > 0
                        ? leafVarying.map(function (p, i) {
                            return (
                              <span key={i}
                                className={(p.type === 'benchmark' ? 'benchmark-badge' : p.type === 'tag' ? 'tag' : 'param') + (p.type !== 'benchmark' ? ' clickable-filter' : '')}
                                title={p.type !== 'benchmark' ? 'Click to filter by ' + p.key + '=' + p.val : p.val}
                                onClick={function (e) {
                                  e.stopPropagation();
                                  if (p.type === 'tag' && onAddTagFilter) onAddTagFilter(p.key, p.val);
                                  else if (p.type === 'param' && onAddParamFilter) onAddParamFilter(p.key, p.val);
                                }}
                              >
                                {p.type === 'tag' && <span className="tag-key">{p.key}</span>}
                                {p.type === 'tag' ? '=' + p.val : p.type === 'benchmark' ? p.val : p.key + '=' + p.val}
                              </span>
                            );
                          })
                        : <span className="text-muted">-</span>}
                    </td>
                    <td className="metric-value">
                      {formatMetric(it.primaryMetric)}
                      {metricValues[it.iterationId] && metricValues[it.iterationId].mean != null && (
                        <span className="metric-number">
                          {' '}{formatValue(metricValues[it.iterationId].mean)}
                          {metricValues[it.iterationId].stddevPct != null && metricValues[it.iterationId].sampleValues.length > 1 && (
                            <span className="metric-stddev"> ({metricValues[it.iterationId].stddevPct.toFixed(1)}%)</span>
                          )}
                        </span>
                      )}
                    </td>
                    <td>{it.sampleCount}</td>
                    <td>
                      {it.passCount > 0 && <span className="status-pass">{it.passCount}P</span>}
                      {it.passCount > 0 && it.failCount > 0 && ' '}
                      {it.failCount > 0 && <span className="status-fail">{it.failCount}F</span>}
                      {it.passCount === 0 && it.failCount === 0 && '-'}
                    </td>
                  </tr>
                );
              })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
