# Crucible Web UI — Design and Implementation Guide

This document explains the design, architecture, data flow, and key implementation decisions of the Crucible Web UI. It is intended for developers (human or AI) who need to debug, maintain, or extend the codebase.

## Table of Contents

1. [Overview](#overview)
2. [Technology Stack](#technology-stack)
3. [Application Structure](#application-structure)
4. [Data Model](#data-model)
5. [Search Workflow](#search-workflow)
6. [Iteration Table](#iteration-table)
7. [Compare Workflow](#compare-workflow)
8. [Supplemental Metrics and Breakouts](#supplemental-metrics-and-breakouts)
9. [Autocomplete Dropdowns](#autocomplete-dropdowns)
10. [Theme System](#theme-system)
11. [URL State and Sharing](#url-state-and-sharing)
12. [Debug Console](#debug-console)
13. [Server API Endpoints](#server-api-endpoints)
14. [CDM Library Integration](#cdm-library-integration)
15. [Performance Considerations](#performance-considerations)
16. [Known Limitations and Future Work](#known-limitations-and-future-work)

---

## Overview

The Crucible Web UI is a React single-page application that enables performance engineers to:

1. **Search** for benchmark iterations across multiple runs using filters (benchmark, tags, params, user, date range)
2. **Compare** selected iterations visually with bar charts, grouping/series dimensions, and supplemental metric overlays
3. **Share** workflows via URL-encoded state

The UI communicates exclusively with the CDM API server (`server.js` on port 3000), which queries OpenSearch for all data. No direct OpenSearch access from the browser.

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Framework | React 19 + Vite | UI rendering, hot reload, build |
| Charting | Recharts 3.x | Bar/line/composed charts with tooltips and error bars |
| HTTP Client | Browser Fetch API | All API communication |
| Styling | Plain CSS with CSS variables | Theming (dark/light), responsive layout |
| API Server | Express (Node.js) | REST endpoints over OpenSearch/CDM |
| Data Store | OpenSearch | Persistent storage for CDM documents |

---

## Application Structure

```
web-ui/src/
  main.jsx              React entry point
  App.jsx               Root component: view routing, selection state, URL persistence
  index.css             All styles (theme variables, component styles)
  debugLog.js           Pub/sub logging store for API timing
  api/
    cdm.js              API client (fetch wrappers for all endpoints)
  components/
    SearchPanel.jsx     Search form with autocomplete filters
    IterationTable.jsx  Results table with sorting, grouping, clickable filters
    SelectionBar.jsx    Shows selected iteration count and chips
    CompareView.jsx     Bar charts with grouping, series, supplemental metrics
    AutocompleteInput.jsx  Reusable dropdown with single/multi-select
    DebugConsole.jsx    Collapsible timing/network log
```

### Component Hierarchy

```
App
  SearchPanel (ref: exposes addTagFilter, addParamFilter, setFiltersAndSearch, getFilters)
  SelectionBar
  IterationTable
  CompareView
  DebugConsole
```

### State Ownership

| State | Owner | Passed To |
|-------|-------|-----------|
| `iterations` (search results) | App | SearchPanel (as prop), IterationTable |
| `selected` (Map of iterationId -> iteration) | App | IterationTable, SelectionBar, CompareView |
| `view` (search/compare/deepdive) | App | Controls which view renders |
| `groupByList` | App | CompareView |
| `hiddenFields` | App | CompareView |
| `filters` (search form state) | SearchPanel | Internal; exposed via ref |
| `metricValues` (primary metric) | IterationTable | Internal (fetched on demand) |
| `supplementalMetrics` | CompareView | Internal |
| `theme` | App | CSS via `data-theme` attribute |

---

## Data Model

The CDM hierarchy is: **run > iteration > sample > period > metric**

### Iteration Object (Client-Side)

This is the core data structure passed around the UI. Each iteration object is assembled server-side by the `/iterations/details` endpoint:

```javascript
{
  runId: "uuid",                    // Parent run ID
  iterationId: "uuid",             // Unique iteration identifier
  benchmark: "uperf",              // Benchmark name
  tags: [{ name: "gro", val: "on" }],  // Run-level tags
  params: [{ arg: "wsize", val: "64" }, ...],  // All params for this iteration
  commonParams: [...],             // Params shared across all iterations in this run
  uniqueParams: [...],             // Params that vary within this run
  sampleCount: 3,                  // Number of samples (repetitions)
  passCount: 3,                    // Passing samples
  failCount: 0,                    // Failing samples
  primaryMetric: "uperf::Gbps",    // Primary metric name (source::type)
  runBegin: "1771369758322",       // Run start time (epoch ms, stored as string)
  runSource: "hostname//var/lib/crucible/run/...",  // Run source path
  runName: "\"Karl Rister\"",      // User name (may contain embedded quotes)
  runEmail: "krister@redhat.com"   // User email
}
```

**Important notes:**
- `commonParams` and `uniqueParams` are computed per-run by the server. The UI recomputes "globally unique" params/tags across all displayed iterations (see [Iteration Table](#iteration-table)).
- `runBegin` is an epoch-millisecond timestamp stored as a string in OpenSearch. The client parses it with `Number(ts)` before passing to `new Date()`.
- `runSource` format is `hostname//path`. The `//` separates hostname from the filesystem path. When building a URL, prepend `/` to the path portion after splitting on `//`.
- `runName` may contain embedded double quotes (a data quality issue). The CDM library's `mSearch` function builds term queries as JS objects (not string concatenation) to handle this.

### Metric Value Object

```javascript
{
  mean: 1234.56,           // Average across passing samples
  stddevPct: 2.3,          // Standard deviation as percentage of mean
  sampleValues: [1200, 1250, 1254]  // Individual sample values
}
```

### Supplemental Metric Object

```javascript
{
  source: "mpstat",
  type: "Busy-CPU",
  display: "panel" | "overlay",     // Panel = own chart; overlay = line on primary chart
  breakouts: ["direction"],          // Active breakout dimensions
  remainingBreakouts: ["hostname", "num", "type"],  // Available for further drilling
  loading: false,
  values: {
    "iterationId": {
      labels: {
        "<rx>": { mean: 5.2, stddevPct: 1.1, sampleValues: [5.1, 5.3] },
        "<tx>": { mean: 3.8, stddevPct: 0.8, sampleValues: [3.7, 3.9] }
      }
    }
  }
}
```

**Label encoding:** With no breakouts, the label is `""` (empty string). With breakouts, labels encode the breakout values: `"<rx>"`, `"<host1>-<cpu0>"`, etc. This is the format returned by `cdm.getMetricDataSets()`. The angle brackets are part of the label string.

---

## Search Workflow

### User Flow

1. User sets filters (benchmark, tags, params, date range, etc.)
2. Clicks Search (or presses Enter)
3. Server searches for matching run IDs, then batch-fetches iteration details
4. Results displayed in IterationTable
5. User can click unique param/tag badges to add as filters, then search again

### Implementation Details

**SearchPanel.jsx** manages the filter form state internally. It exposes methods via `useImperativeHandle` + `forwardRef`:

- `getFilters()` — returns current filter state
- `setFiltersAndSearch(filters)` — sets filters and triggers search (used for URL state restoration)
- `addTagFilter(name, val)` — adds a tag filter (appends to existing if same name)
- `addParamFilter(arg, val)` — adds a param filter (appends to existing if same arg)

**Search execution flow** (`handleSearch`):

1. Build `apiFilters` object from non-empty filter values
2. Call `api.searchRuns(apiFilters)` — server returns matching run IDs
3. Call `api.getIterationDetails(runIds, start, end)` — server returns hydrated iteration objects
4. **Client-side filtering**: params and primaryMetric are per-iteration, but the server only filters at the run level. The client filters iterations within the returned runs to match param/metric criteria.
5. Pass results to `onResults(iterations)` callback (App.jsx)

**Multi-value filter handling:**
- Tags and params support comma-separated values via the multi-select AutocompleteInput
- Server-side: OR within a field (union run IDs), AND across fields (intersect)
- Client-side: `fp.val.split(',')` then `vals.includes(String(p.val))`

**Date range scoping:**
- `buildYearDotMonthRange(instance, docType, start, end)` returns comma-separated index suffixes like `@2026.02,@2026.03`
- `getIndexName()` expands each suffix with the correct `baseName+docType` for any document type
- If no indices match the date range, returns `@<start>` (targets a non-existent index, returns empty results) instead of `@*` (which would query everything)

---

## Iteration Table

### Visual Design

The iteration table uses hierarchical grouping to organize iterations by their varying dimensions:

**Hierarchical grouping:**
- All varying dimensions (params, tags, run ID, date, benchmark) are auto-computed
- Dimensions sorted by distinct value count (fewest first) — same algorithm as compare view
- Each dimension gets its own column with a header showing the dimension name
- Group cells use `rowSpan` to span all child iterations, with checkboxes for bulk selection
- Alternating background colors distinguish top-level groups

**Column controls (in header):**
- **`<` / `>`**: Reorder columns (changes grouping hierarchy)
- **`▲` / `▼`**: Toggle sort direction (ascending/descending) per dimension
- **`×`**: Hide dimension column (shown as strikethrough chip in "Hidden:" area, click to restore)

**Date as independent dimension:**
- Run date is its own group-by column, separate from run ID
- Can be moved independently (e.g., to the rightmost position)
- Sorted by timestamp with natural sort (supports descending for newest-first)
- Displayed as formatted date/time string

**Common section** (above the table): params/tags/benchmark that have the same value across ALL displayed iterations — shown once, not repeated in the table.

**Details column**: Shows remaining varying items not covered by group headers. Color-coded legend chips (`bench`, `tag`, `param`) displayed in the header bar.

**Clickable filters:**
- Group cell param/tag badges are clickable — clicking adds them as search filters
- This calls `SearchPanel.addTagFilter()` or `addParamFilter()` via the ref exposed by `useImperativeHandle`

**Primary metric values:**
- Not loaded with the initial search (too slow for many iterations)
- "Show Values" button triggers a separate API call (`/iterations/metric-values`)
- Values displayed inline: `uperf::Gbps 1234.56 (2.3%)`

**Text wrapping:**
- Long values wrap at natural separators (`-`, `_`, `.`, `,`, `/`, `:`) using zero-width spaces inserted by `wrapFriendly()`
- `overflow-wrap: break-word` without `word-break: break-all` ensures breaks only at separator boundaries

### Param Filter

The text input at the top filters iterations by param name/value:
- Without `=`: searches both arg and val fields
- With `=`: splits on first `=`, matches arg on the left and val on the right
- Example: `wsize=64` matches iterations where any param's arg contains "wsize" AND val contains "64"

---

## Compare Workflow

### User Flow

1. Select iterations via checkboxes in the search view
2. Click "Compare (N)" button in the nav bar
3. Primary metric values auto-fetched
4. Bar chart displayed with one bar per iteration
5. Group-by auto-populated (sorted by distinct value count, fewest first)
6. Add supplemental metrics via "+ Add Metric"
7. Click a bar to pin it — shows values in sidebars, dims other bars

### Chart Architecture

The chart uses Recharts' `ComposedChart` which supports mixing Bar and Line components. The chart data is a flat array of entries, one per iteration, with gap entries inserted between groups. Chart height is capped at 30% of viewport width to prevent overly tall charts.

**Chart data entry structure:**
```javascript
{
  name: "wsize=64",             // X-axis label (only remaining varying params/tags)
  value: 1234.56,               // Primary metric mean
  errorY: 23.4,                 // Stddev for error bars
  iterationId: "uuid",
  groupValue: "nthreads=1, protocol=tcp",  // Compound group key
  color: "#5b8def",             // Bar fill color
  isGap: false,                 // Gap entries for visual separation
  'supp_0': 0.42,              // Supplemental metric 0 value
  'supp_0_stddevPct': 1.5,
  'supp_0_error': 0.006,
  'supp_0_samples': 3,
  'supp_0_<rx>': 5.2,          // Breakout label values (when breakouts active)
  'supp_0_<tx>': 3.8,
}
```

### Label Hierarchy

The chart applies a hierarchical approach to reduce label redundancy:

1. **Chart subtitle**: Globally common params/tags/benchmark (same across all selected iterations)
2. **Hierarchical group headers**: Each group-by dimension gets its own row above the chart. Each row shows the dimension name on the left and value labels spanning the bars they cover, creating a pivot-table-like header.
3. **Bar labels** (X-axis): Only the remaining varying dimension(s) not used in group-by
4. **Bar value labels**: Metric values shown inside bars when they fit (checked against bar width and height). Hidden automatically when bars are too narrow.
5. **Value consolidation**: Params/tags with the same value are grouped in labels (e.g., `bs,rw,size=4k` instead of repeating `=4k` three times)

### Auto-Grouping

When entering the compare view, group-by dimensions are auto-populated:
1. `buildDimOptions()` scans iterations for varying dimensions only
2. Dimensions sorted by distinct value count (fewest first — best grouping levels)
3. All but the last dimension become group-by levels
4. The last dimension stays as the bar label
5. User can reorder chips with left/right arrow buttons
6. "Auto" button recomputes; "Clear" removes all

### Hide Fields

Users can hide specific params/tags from the compare view:
- Hidden fields excluded from group-by/series-by dropdowns, common/varying computation, and bar labels
- Shown as red strikethrough chips
- Hiding auto-removes from group-by if currently used

### Click-to-Pin and Selection Indicators

- **Click a bar** in the primary chart to pin its data in all sidebars
- **Red dashed ReferenceLine** appears at the selected bar's position in all chart panels
- **Non-selected bars dim to 20% opacity** across all panels
- Click the same bar again to unpin

### Grouping Implementation

1. `buildDimOptions()` scans iterations for varying dimensions only (>1 distinct value) — common dimensions are excluded from dropdowns
2. Multi-group-by: `groupByList` is an array of dimension strings, compound key computed by `getCompoundGroupValue()`
3. Iterations sorted by compound group key using `naturalCompare` (numeric-aware: 64 before 256 before 1024)
4. Per-group common items computed: params/tags that vary globally but are constant within a group
5. `buildIterLabel()` excludes all group-by dimensions and per-group common items from bar labels; consolidates params with same value
6. Gap entries (`{ isGap: true }`) inserted between groups for visual separation
7. Hierarchical headers rendered inside `compare-chart-area` with matching chart margins

### Y-Axis Management

- **Left Y-axis**: Primary metric (always present)
- **Right Y-axis**: Overlay-mode supplemental metrics (shown only when overlays exist)
- **Panel Y-axes**: Each panel-mode metric gets its own left Y-axis
- **Alignment**: Hidden right Y-axis (width=80 or width=1) added to all charts so bars align across panels
- **HTML labels**: Y-axis metric names rendered as HTML `<div>` elements outside the SVG to avoid Recharts clip-path issues
- **Tick formatting**: `formatYTick()` adapts precision based on magnitude (e.g., "1.2k", "0.423", "80")
- **Sidebar**: Each chart panel has a sidebar (300px) showing pinned iteration values

---

## Supplemental Metrics and Breakouts

### Adding a Metric

1. Click "+ Add Metric" — loads available metric sources from server
2. Select source (e.g., `mpstat`) — loads available types
3. Select type (e.g., `Busy-CPU`)
4. Choose display mode: **Overlay** (line on primary chart with right Y-axis) or **Own Panel** (separate bar chart)
5. Click "Add" — fetches metric values for all selected iterations

### Primary Metric Refinement

The primary metric chart includes a "Refine" button that adds the primary metric as a supplemental metric panel. This gives the primary metric the same controls as supplemental metrics: breakouts, filters, sample selection, and chart type. Once refined, the button disappears to prevent duplicates.

### Metric Control Row

Each metric's controls render directly below its chart panel (not grouped at the top). Controls include:
- Colored left border (consistent color across chart and panel)
- **+ Breakout** button — opens a custom dropdown (see Breakout Value Preview below)
- **Chart type** selector (Bars, Stacked, Lines) — visible when breakouts are active
- **Sample** selector — choose which sample to display (auto-selects the sample closest to the primary metric mean)
- **Filter** input — accepts `gt:N`, `ge:N`, `lt:N`, `le:N` syntax for server-side label filtering
- Remove button (x)
- Active breakout chips with editable filter values
- Overlay-mode metric controls render below the primary chart

### Per-Sample Selection

Supplemental metrics query a single sample instead of averaging across all:
- Client computes the best sample index from `metricValues.sampleValues` (closest to mean)
- Server uses the provided `sampleIndex` directly
- Sample dropdown shows each sample's primary metric value for reference
- Filters work correctly since they operate on single-sample data

### Breakout Value Preview

The "+ Breakout" button opens a custom div-based dropdown (not a native `<select>`) that shows:
- Each remaining breakout dimension as a row
- **Dimension name** (bold) followed by an **"all"** chip and individual **value chips**
- Values fetched lazily on first open via `POST /api/v1/iterations/breakout-values`, cached per source::type
- **Single-value dimensions** dimmed to 35% opacity and sorted to the bottom
- **Multi-value dimensions** sorted to the top, full opacity
- Value chips are **clickable toggles** — select specific values to pre-filter the breakout
- Clicking **"all"** adds the breakout with no filter
- Clicking **"Add"** (appears when specific values selected) adds the breakout as `name=val1+val2`

### Breakout Workflow

1. Click "+ Breakout" — dropdown shows dimensions with their values
2. Click "all" or select specific values and click "Add"
3. Client re-queries the metric with `breakout: ["direction"]` (or `["direction=rx+tx"]` if filtered)
4. Server returns multi-label values: `{ "<rx>": { mean, ... }, "<tx>": { mean, ... } }`
5. Server also returns updated `remainingBreakouts` for further drilling
6. Chart renders one bar/line per label; chart type selectable (Bars/Stacked/Lines)
7. User can add another breakout level (e.g., "hostname") — labels become `"<rx>-<host1>"`, etc.
8. Breakout chips have editable filter inputs accepting exact values, `val1+val2`, `r/regex/`, or `R/regex/`
9. "Apply" button re-queries with the filter applied
10. Removing a breakout re-queries with the reduced breakout array

### Data Format

Without breakouts:
```json
{ "labels": { "": { "mean": 5.2, "stddevPct": 1.1, "sampleValues": [5.1, 5.3] } } }
```

With breakout by "direction":
```json
{ "labels": { "<rx>": { "mean": 5.2, ... }, "<tx>": { "mean": 3.8, ... } } }
```

The empty string label `""` with no breakouts is correct — it means there's no additional dimension to encode. See the CDM metric label convention.

### Chart Rendering with Breakouts

**Panel mode:** Each label becomes its own `<Bar>` component with a distinct color. Chart type selectable:
- **Bars**: Side-by-side bars per breakout label
- **Stacked**: Bars stacked using Recharts `stackId`
- **Lines**: Line chart with dots per breakout label

**Overlay mode:** Each label becomes its own `<Line>` component on the right Y-axis.

**Supplemental panels render below the primary chart** so the primary chart with its X-axis labels appears first.

Label detection is done dynamically by scanning chart data entry keys for the pattern `supp_{index}_{label}` (excluding keys ending in `_stddevPct`, `_error`, `_samples`).

### Breakout Sidebar (Value Legend)

When a bar is clicked (pinned), each chart panel shows a sidebar with metric values:

- **No breakouts**: Simple label + value display
- **With breakouts**: Table layout with rowSpan for repeated segment values (like the iteration table's run grouping)
  - Column headers from breakout dimension names
  - Labels parsed into `<segment>` patterns and grouped hierarchically
  - Common text suffixes stripped and shown in header (e.g., `.local.net` removed from hostnames)
  - Common text prefixes stripped if no suffix found (e.g., `host-` removed)
  - Numeric-only columns skip deduplication
  - Delimiter-boundary detection (`.`, `-`, `_`, `/`) prevents splitting words

### Bar Value Labels

Metric values are displayed inside bars when they fit:
- `formatBarLabel()` produces compact values (3-4 significant digits, k/M suffixes)
- Custom `<LabelList>` content function checks `props.width` and `props.height` against text width estimate
- **Side-by-side bars**: shown if bar width > text width + 4px AND height > 16px
- **Stacked bars**: shown if segment height > 14px
- **Lines**: labels always skipped
- Font: 12px bold monospace, white with 90% opacity

---

## Autocomplete Dropdowns

`AutocompleteInput.jsx` is a custom replacement for native `<datalist>` that supports:

### Single Mode
- Text input with dropdown
- Typing filters options by substring
- Click or Enter selects a value

### Multi Mode (`multi` prop)
- Selected values shown as removable chips
- Input field for filtering
- Already-selected values hidden from dropdown
- Backspace in empty input removes last chip
- Dropdown stays open after selection for adding more

### Context-Aware Partitioning

Options are split into two groups based on `presentValues` (a Set):
1. **Present** (normal text): values that exist in current search results
2. **Absent** (grayed out): values from the API that don't appear in current results
3. A thin divider line separates the two groups

This is computed in `SearchPanel.jsx` via a `useMemo` that scans all iteration objects to build Sets of benchmark names, tag names/values, param args/values, etc.

### Lazy Loading

Options are fetched from the server on first focus via `loadOptions()`. A `ref`-based cache (`optionsCache`) prevents re-fetching. Tag/param value dropdowns refresh when their parent name/arg changes.

---

## Theme System

Two themes defined via CSS variables:
- `:root` / `[data-theme='light']`: Light background, dark text
- `[data-theme='dark']`: Dark background, light text (default)

Key variables: `--bg`, `--surface`, `--surface-alt`, `--border`, `--border-strong`, `--text`, `--text-secondary`, `--text-muted`, `--accent`, `--accent-light`, `--success`, `--danger`, `--warning`

Theme preference saved in `localStorage('theme')` and applied via `document.documentElement.setAttribute('data-theme', theme)`.

**Important:** SVG elements (Recharts) may not resolve CSS variables for `fill` and `stroke`. The Recharts Y-axis labels were moved to HTML `<div>` elements to avoid this issue. Tick colors using `fill: 'var(--text-secondary)'` work in most browsers but may need hardcoded fallbacks in edge cases.

---

## URL State and Sharing

The "Share" button encodes the current workflow state into the URL hash:

```
http://host:3000/#%7B%22benchmark%22%3A%22uperf%22%2C%22start%22%3A%222026.01%22%2C...%7D
```

### Encoded State

```javascript
{
  benchmark: "uperf",          // Search filters
  start: "2026.01",
  end: "2026.04",
  tags: [{ name: "gro", val: "on" }],
  params: [{ arg: "wsize", val: "64,256" }],
  selectedRuns: ["uuid1", "uuid2"],  // Run IDs (not iteration IDs — much shorter)
  view: "compare",
  groupBy: ["param:nthreads", "param:protocol"],  // Array of group-by dimensions
  hidden: ["param:primary-metric"],  // Hidden field dimensions
  metrics: [                   // Supplemental metrics with full configuration
    {
      source: "mpstat",
      type: "Busy-CPU",
      display: "panel",
      chartType: "bar",        // Omitted if "bar" (default)
      breakouts: ["direction"],
      filter: "gt:0",
      sampleIndex: 2
    }
  ]
}
```

### Restoration Flow

1. On mount: `decodeState(window.location.hash)` parsed, stored in `restoredState.current`
2. `groupByList` and `hiddenFields` set immediately (handles both array and legacy single-string format)
3. `restoredMetrics` saved to React state (not ref) so it survives timing between mount and CompareView render
4. `view` NOT set yet (deferred until search completes)
5. After SearchPanel mounts: `setFiltersAndSearch(filters)` called via ref
6. SearchPanel updates filters, triggers search via `pendingSearch` ref + useEffect
7. `handleSearchResults` receives results, auto-selects iterations from matching run IDs
8. View switched to saved view (e.g., "compare")
9. `restoredState.current` cleared to prevent re-application on next search
10. CompareView mounts and detects `restoredMetrics` prop — re-fetches each metric with saved configuration (source, type, display, chartType, breakouts, filter, sampleIndex)
11. If no groupByList was saved, auto-group runs on CompareView mount

**Key design decisions:**
- View switch is deferred until after search + selection to avoid showing an empty Compare view while data is loading.
- `restoredMetrics` uses React state (not a ref) because the ref would be cleared before CompareView mounts and reads it.
- Filters are saved in `lastFilters.current` on every search result, because SearchPanel is unmounted when the user navigates to Compare view. The Share button uses `searchRef.current.getFilters() || lastFilters.current`.
- Supplemental metrics are retrieved from CompareView via `compareRef.current.getSupplementalMetrics()` (exposed via `useImperativeHandle`).

---

## Debug Console

`DebugConsole.jsx` provides a collapsible panel at the bottom of the page showing:

- API calls with method, path, duration, status code
- Work items (client-side operations) with duration
- Info entries (search lifecycle markers)

**Implementation:** Uses `debugLog.js`, a simple pub/sub store:
- `addEntry(entry)` — creates a timestamped entry with a unique ID
- `updateEntry(id, updates)` — updates status, elapsed time, etc.
- `onChange(fn)` — registers a listener; returns unsubscribe function
- `timeWork(label, asyncFn)` — wraps an async operation with automatic timing

The API client (`api/cdm.js`) automatically logs every request via `addEntry` on start and `updateEntry` on completion.

---

## Server API Endpoints

The Express server (`server.js`) exposes the following endpoint categories:

### Run Discovery
| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/v1/runs` | Search runs by filters (name, email, benchmark, tags, params, date range) |

Supports comma-separated run IDs (OR) and comma-separated tag/param values (OR within field, AND across fields).

### Batch Iteration Fetching
| Method | Path | Purpose |
|--------|------|---------|
| POST | `/api/v1/iterations/details` | Hydrate iterations for multiple runs in one call |
| POST | `/api/v1/iterations/metric-values` | Fetch primary metric mean/stddev for iterations |

The `/iterations/details` endpoint accepts `{ runIds, start, end }` and returns fully assembled iteration objects. It uses `buildYearDotMonthRange` to scope queries, then sequentially calls:
1. `mgetBenchmarkName`, `mgetRunData`, `mgetIterations`, `mgetTags` (run-level)
2. `mgetParams`, `mgetSamples`, `mgetPrimaryMetric`, `mgetPrimaryPeriodName` (iteration-level)
3. `mgetSampleStatuses` (sample-level)
4. Computes common vs. unique params per run

### Supplemental Metrics
| Method | Path | Purpose |
|--------|------|---------|
| POST | `/api/v1/iterations/metric-sources` | Available metric sources across runs |
| POST | `/api/v1/iterations/metric-types` | Types for a given source |
| POST | `/api/v1/iterations/supplemental-metric` | Fetch metric values with optional breakouts |
| POST | `/api/v1/iterations/breakout-values` | Distinct values per breakout dimension |

The breakout-values endpoint accepts `{ runIds, start, end, source, type, breakouts: [...] }` and returns `{ breakouts: { "hostname": ["h1", "h2"], "num": ["0", "1"] } }`. Uses a single `_msearch` request with one terms aggregation per breakout dimension on `metric_desc.names.<dim>`. Results cached client-side per source::type.

The supplemental-metric endpoint accepts `{ iterations, runIds, start, end, source, type, breakout, filter, sampleIndex }`:
- `iterations`: array of `{iterationId, runId}` for targeted queries (avoids discovering all iterations from runIds)
- `sampleIndex`: which sample to query (client computes best sample from primary metric values)
- `breakout`: array of breakout dimensions with optional filters
- `filter`: value filter (gt:N, ge:N, lt:N, le:N)

Processing:
1. Uses provided iteration IDs directly (or discovers from runIds as fallback)
2. Gets primary period IDs and ranges
3. Builds metric data sets with the specified source, type, and breakouts
4. Calls `cdm.getMetricDataSets()` to fetch from OpenSearch
5. Aggregates per-label values across samples (mean, stddev)
6. Returns `{ values: { iterId: { labels: {...} } }, remainingBreakouts: [...] }`

### Field Values (for Dropdowns)
| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/v1/fields/months` | Available YYYY.MM from index names |
| GET | `/api/v1/fields/run-ids` | Distinct run UUIDs |
| GET | `/api/v1/fields/names` | User names |
| GET | `/api/v1/fields/emails` | Emails |
| GET | `/api/v1/fields/benchmarks` | Benchmark names |
| GET | `/api/v1/fields/tag-names` | Tag names |
| GET | `/api/v1/fields/tag-values?name=X` | Tag values (optionally filtered by name) |
| GET | `/api/v1/fields/param-args` | Param arg names |
| GET | `/api/v1/fields/param-values?arg=X` | Param values (optionally filtered by arg) |
| GET | `/api/v1/fields/primary-metrics` | Primary metric names |

All field endpoints accept `?start=YYYY.MM&end=YYYY.MM` for date scoping.

### Per-Run Endpoints (Legacy, used by get-result-summary.js)
These endpoints use a `resolveRun` middleware that finds the OpenSearch instance and yearDotMonth for a run ID:
- GET `/api/v1/run/:id/tags`, `/benchmark`, `/iterations`, `/metric-sources`
- POST `/api/v1/run/:id/iterations/params`, `/primary-metric`, `/samples`, `/primary-period-name`
- POST `/api/v1/run/:id/samples/statuses`, `/primary-period-id`
- POST `/api/v1/run/:id/periods/range`, `/metric-types`
- POST `/api/v1/metric-data` (optionally include `distribution-stats` to obtain duration-weighted native-timeline statistics alongside the resolution series)

---

## CDM Library Integration

### Key cdm.js Functions Used by server.js

**Index Management:**
- `buildYearDotMonthRange(instance, docType, start, end)` — Returns comma-separated `@YYYY.MM` suffixes. The docType parameter is ignored (suffixes are docType-independent); `getIndexName` expands each suffix with the correct baseName+docType.
- `getIndexName(docType, instance, yearDotMonth)` — Splits comma-separated suffixes and expands each into a full index name.
- `checkCreateIndex(instance, index)` — Handles comma-separated index names by splitting and checking/creating each.

**Multi-Index Queries:**
- When yearDotMonth produces a multi-index name (containing commas), `esJsonArrRequest` puts the index in each NDJSON header line (`{"index": ["idx1", "idx2"]}`) instead of the URL path (OpenSearch rejects commas in URL path index names).
- Fetch concurrency scales down with multi-index queries: `batchSize = Math.floor(16 / numIndices)` to avoid overwhelming OpenSearch's search thread pool.

**Batch Query Functions:**
- `mgetBenchmarkName`, `mgetIterations`, `mgetTags`, `mgetRunData` — Run-level batched queries
- `mgetParams`, `mgetSamples`, `mgetPrimaryMetric`, `mgetPrimaryPeriodName` — Iteration-level
- `mgetSampleStatuses`, `mgetPrimaryPeriodId`, `mgetPeriodRange` — Sample/period-level
- `mgetMetricSources`, `mgetMetricTypes` — Metric discovery
- `mgetBreakoutValues` — Distinct values per breakout dimension via terms aggregation on `metric_desc.names.*`
- `getMetricDataSets` — Full metric data retrieval with breakout support

**Important:** `mgetPrimaryMetric` and `mgetPrimaryPeriodName` return 1D arrays (collapsed from 2D). Do NOT access `result[i][0]` — use `result[i]` directly. Accessing `[0]` on a string returns just the first character (e.g., "measurement"[0] = "m").

### mSearch Term Query Construction

The `mSearch` function builds term queries as JS objects (not string concatenation):
```javascript
var termObj = { term: {} };
termObj.term[termKeys[x]] = values[x][i];
req.query.bool.filter.push(termObj);
```
This correctly handles values with embedded special characters (e.g., user names with quotes).

---

## Performance Considerations

### OpenSearch Thread Pool

OpenSearch has a search thread pool with limited queue capacity (~1000). Multi-month queries across many runs can generate thousands of shard queries. Mitigations:

1. **Sequential mget calls:** The `/iterations/details` endpoint runs mget operations sequentially (not Promise.all) to avoid multiplying concurrent load
2. **Scaled fetch concurrency:** `batchSize = Math.floor(16 / numIndices)` reduces parallel HTTP requests for multi-index queries
3. **Graceful degradation:** `mSearch` returns empty arrays (not undefined) when individual sub-queries fail with 429 (thread pool exhaustion)
4. **Scoped queries:** Date range filtering limits which monthly indices are queried

### Metric Value Loading

Primary metric values are NOT loaded with iteration details (would add ~7 seconds for 177 iterations). Instead:
- "Show Values" button triggers a separate on-demand fetch
- Compare view auto-fetches only for selected iterations
- Supplemental metrics fetched individually per metric added

### Start-Server Restart Loop

`start-server.sh` runs server.js in a `while true` loop with `npm ci` and web UI build gated by stamp files (only re-run when `package-lock.json` changes). Killing the Node process (`pkill -f 'node ./server.js'`) triggers an automatic restart. Full restart via `sudo crucible stop opensearch && sudo crucible start opensearch`.

### Metric Query Performance (20x improvement)

The `getMetricDataFromIdsSets` function was rewritten to address a 100-second bottleneck when querying 130+ breakout labels at resolution=100:

1. **Time-range templates**: The 4 queries per time window (weighted avg, total weight, 2 boundary doc fetches) share identical structure except timestamps. Templates are built once per set with `__IDS__` placeholder, then reused per label via `String.replace()`.

2. **Periodic flushing**: Instead of accumulating all queries (104K+ array entries) before sending to OpenSearch, flush every 10 labels. Keeps array sizes small and lets OpenSearch process while building the next batch.

3. **Native fetch**: Replaced `then-request` (which spawns child processes via `sync-rpc`) with Node.js native `fetch` for all async OpenSearch HTTP requests.

4. **Debug function short-circuit**: `numMBytes()` and `memUsage()` now return immediately when `debugOut == 0`, avoiding `JSON.stringify` on large arrays.

5. **Two-pass filter**: When `filter` is set with `resolution > 1`, first queries at resolution=1 to determine surviving labels, then re-queries at the requested resolution with only those labels' UUIDs.

---

## Deep Dive Workflow

### Overview

The Deep Dive view provides time-series line charts for selected metrics at high resolution (default 100 data points), with multiple iterations overlaid.

### Entry Flow

1. In Compare view, check "Dive" on metric panels to select metrics for deep dive
2. "Deep Dive (N)" button becomes enabled in the nav bar
3. Clicking it snapshots the supplemental metric configs (breakouts, filters) and switches view
4. DeepDiveView fetches period info, then metric data sequentially per metric

### Data Alignment

CDM metric data is continuous — each sample covers a `[begin, end]` range in epoch-ms with no gaps. All series at the same resolution have exactly N samples. The chart uses **sample index** as the X coordinate (not raw elapsed midpoints) to ensure all series from different iterations align perfectly on the same grid. The X-axis displays elapsed time based on the longest period's duration.

### Chart Modes

Each metric chart has independent controls:
- **Combined**: All iterations overlaid on one chart (300px)
- **Split**: One chart per iteration stacked vertically (200px each), with consistent Y-axis scale across iterations
- **Lines / Stacked**: In split mode, toggle between individual lines and stacked area charts (useful for CPU utilization breakdown)

### Zoom

- **Click + drag** on any chart to select a time range (blue highlight)
- All charts re-query with the zoomed time range at the same resolution (more detail)
- Zoom is composable — zoom again within a zoomed view
- "Reset Zoom" button shows current zoom percentage
- Zoom is percentage-based: each iteration's begin/end adjusted proportionally

### Series Legend

Below each chart, a unified legend table shows all breakout labels once (not duplicated per iteration):

- **Segment columns**: Breakout dimension values with rowSpan grouping and sticky text for tall cells
- **Per-iteration columns**: Color swatch + value pair for each iteration, with iteration chip header matching the context bar style
- **Live tracking**: Values update as pointer moves across any chart, synchronized across all charts via shared elapsed time
- **Click-to-pin**: Click locks all charts; click again to resume live tracking
- **Common prefix/suffix stripping**: Hostnames like `f35-h17-000-r640.rdu2.scalelab.redhat.com` shown as `f35-h17-000-r640`
- **Empty series**: No color swatch shown when an iteration lacks data for a label

### Per-Iteration Color Themes

Each iteration gets a color family (blues, reds, greens, purples, teals, ambers). Within each family, shade varies per breakout label. This makes it easy to identify which iteration a line belongs to.

### Context Bar

Above the charts, a context section shows:
- **Common**: Params/tags/benchmark shared across all iterations (chip-styled, respects hidden fields)
- **Chip legend**: bench/tag/param color reference
- **Iterations**: Labeled chips with iteration-specific varying params, colored with the iteration's theme

### Server Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/api/v1/iterations/period-info` | Period IDs and time ranges per iteration |
| POST | `/api/v1/metric-data` | Time-series metric values with resolution and breakouts |

### Progressive Loading

Metrics are fetched sequentially (one metric at a time). Within each metric, iterations run concurrently. Charts render progressively as data arrives.

---

## Known Limitations and Future Work

### Current Limitations

- **Large result sets:** Searching across many months with hundreds of runs can be slow due to sequential OpenSearch queries
- **Bundle size:** Recharts adds ~400KB to the bundle. Code splitting could help.
- **Breakout label parsing:** CDM may omit breakout dimensions with single values from labels, making label-to-dimension mapping imperfect. The sidebar uses segment-based grouping to work around this.
- **Deep dive color differentiation:** With many breakout labels, shades within an iteration's color theme can be hard to distinguish

### Planned Features

- **Deep dive series filtering:** Click-to-hide individual series or groups in the legend
- **Deep dive breakout controls:** Add/remove breakouts directly in deep dive view
- **"Other" aggregate series:** For filtered-out labels, show a single aggregated line
- **Save/load workflows:** Server-side or localStorage persistence of named workflows
- **Drag-to-reorder:** Group-by chips currently use arrow buttons; drag-and-drop would be more intuitive
