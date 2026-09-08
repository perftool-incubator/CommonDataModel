# Crucible Web UI — Architecture & Design

> **Note:** This document covers the initial architecture overview. For comprehensive
> documentation covering all phases (search, compare, deep dive, supplemental metrics,
> breakouts, URL sharing, performance optimizations, etc.), see **[DESIGN.md](DESIGN.md)**.

## Overview

The Crucible Web UI is a React-based single-page application that provides
performance engineers with an interactive interface for searching, comparing,
and investigating benchmark results stored in OpenSearch via the CDM
(Common Data Model) data layer.

The UI is served by the same Express server (`server.js`) that hosts the CDM
REST API on port 3000. During development, Vite's dev server on port 5173
proxies API requests to the Express backend.

## Technology Stack

| Component       | Technology                    | Purpose                                  |
|-----------------|-------------------------------|------------------------------------------|
| Framework       | React 19 + Vite               | UI rendering and build tooling           |
| Charting        | Recharts (installed)          | Bar charts (Phase 2) and line charts (Phase 3) |
| HTTP Client     | Browser Fetch API             | All communication with the CDM API       |
| Styling         | Plain CSS with CSS variables  | Theming-ready, no CSS framework dependency |
| API Server      | Express 5 (Node.js)           | REST endpoints over OpenSearch/CDM       |
| Data Store      | OpenSearch                    | Persistent storage for all CDM documents |

## Project Structure

```
queries/cdmq/
├── cdm.js                  # Core CDM query library (OpenSearch interface)
├── server.js               # Express HTTP server (REST API + static file serving)
├── start-server.sh         # Startup script (npm install, web-ui build, launch)
├── web-ui/                 # React application (this project)
│   ├── ARCHITECTURE.md     # This file (initial architecture overview)
│   ├── DESIGN.md           # Comprehensive design & implementation guide
│   ├── vite.config.js      # Vite configuration (proxy, build output)
│   ├── index.html          # HTML entry point
│   ├── package.json        # Dependencies (react, recharts, vite)
│   ├── src/
│   │   ├── main.jsx        # React DOM entry point
│   │   ├── index.css       # Global styles
│   │   ├── debugLog.js     # Shared debug/timing log store
│   │   ├── App.jsx         # Root component, view routing, selection state
│   │   ├── api/
│   │   │   └── cdm.js      # API client (fetch wrappers for all CDM endpoints)
│   │   └── components/
│   │       ├── SearchPanel.jsx     # Search form with autocomplete filters
│   │       ├── IterationTable.jsx  # Results table with sorting/filtering
│   │       ├── SelectionBar.jsx    # Persistent selection display
│   │       ├── CompareView.jsx     # Bar charts with grouping, metrics, breakouts
│   │       ├── DeepDiveView.jsx   # Time-series line charts with zoom and legends
│   │       ├── AutocompleteInput.jsx # Reusable dropdown (single/multi-select)
│   │       └── DebugConsole.jsx    # Timing/debug console panel
│   └── dist/               # Build output (served by Express in production)
```

## Data Flow Architecture

### CDM Data Model Hierarchy

```
run                          One benchmark execution (has tags, benchmark name)
 └── iteration               One parameter configuration within a run
      ├── params              Benchmark parameters (e.g., bs=4k, rw=randread)
      ├── primary-metric      Main result metric name (e.g., fio::iops)
      └── sample              One repetition (for statistical confidence)
           ├── status         pass/fail
           └── period         Time window (measurement, warmup, etc.)
                └── metric    Time-series data with source, type, breakouts
```

### OpenSearch Index Organization

CDM v9dev organizes documents into time-partitioned indices:

```
cdm-v9dev-run@2025.03         Run documents for March 2025
cdm-v9dev-iteration@2025.03   Iteration documents
cdm-v9dev-param@2025.03       Parameter documents
cdm-v9dev-tag@2025.03         Tag documents
cdm-v9dev-sample@2025.03      Sample documents
cdm-v9dev-period@2025.03      Period documents
cdm-v9dev-metric_desc@2025.03 Metric descriptor documents
cdm-v9dev-metric_data@2025.03 Metric data documents
```

Older CDM versions (v7dev, v8dev) use a single index per document type
without time partitioning.

### Request Flow

```
Browser (React)
    │
    │  HTTP (fetch)
    ▼
Express server.js (port 3000)
    │
    │  cdm.js library calls
    ▼
OpenSearch (port 9200)
    │
    │  mSearch / esJsonArrRequest
    ▼
OpenSearch indices (cdm-v9dev-*)
```

All data flows through the CDM API server. The web UI never contacts
OpenSearch directly.

## API Layer Design

### Client-Side API Client (`web-ui/src/api/cdm.js`)

A thin wrapper around `fetch` that:

1. Prepends `/api/v1/` to all paths
2. Handles JSON serialization/deserialization
3. Throws on non-2xx responses with the server's error message
4. Logs every request to the debug log with timing data

Every exported function maps to exactly one REST endpoint. The function
signatures mirror the endpoint's parameter structure.

### Server-Side Endpoints (`server.js`)

The server provides three categories of endpoints:

#### 1. Run Discovery

| Method | Endpoint | Parameters | Description |
|--------|----------|------------|-------------|
| GET | `/api/v1/runs` | `?run=&name=&email=&benchmark=&primaryMetric=&start=&end=&tags=[...]&params=[...]` | Search for runs with server-side filtering |

The runs endpoint performs multi-stage filtering:

1. **Run index query**: Filters by run-level fields (name, email, benchmark)
   using OpenSearch term queries via `cdm.mSearch`.
2. **Cross-index intersection**: For filters that target other indices
   (primary-metric → iteration index, tags → tag index, params → param
   index), the server queries each index to find matching `run.run-uuid`
   values via aggregation, then intersects with the run query results.
3. **Date range scoping**: The `start` and `end` parameters (YYYY.MM format)
   limit which time-partitioned indices are queried, avoiding full scans of
   historical data. Default in the UI: last 3 months.

#### 2. Run-Scoped Queries

These require a run ID in the URL path and use the `resolveRun` middleware
to look up the OpenSearch instance and yearDotMonth for that run.

| Method | Endpoint | Body | Returns |
|--------|----------|------|---------|
| GET | `/api/v1/run/:id/tags` | — | `{ tags }` |
| GET | `/api/v1/run/:id/benchmark` | — | `{ benchmark }` |
| GET | `/api/v1/run/:id/iterations` | — | `{ iterations }` |
| POST | `/api/v1/run/:id/iterations/params` | `{ iterations }` | `{ params }` |
| POST | `/api/v1/run/:id/iterations/primary-period-name` | `{ iterations }` | `{ periodNames }` |
| POST | `/api/v1/run/:id/iterations/samples` | `{ iterations }` | `{ samples }` (2D) |
| POST | `/api/v1/run/:id/samples/statuses` | `{ sampleIds }` (2D) | `{ statuses }` (2D) |
| POST | `/api/v1/run/:id/samples/primary-period-id` | `{ sampleIds, periodNames }` | `{ periodIds }` (2D) |
| POST | `/api/v1/run/:id/periods/range` | `{ periodIds }` (2D) | `{ ranges }` (2D) |
| POST | `/api/v1/run/:id/iterations/primary-metric` | `{ iterations }` | `{ primaryMetrics }` |
| GET | `/api/v1/run/:id/metric-sources` | — | `{ sources }` |
| POST | `/api/v1/run/:id/metric-types` | `{ sources }` | `{ types }` (2D) |

#### 3. Field Value Endpoints (for search dropdowns)

These return distinct values from OpenSearch using terms aggregations,
enabling autocomplete dropdowns in the search UI. All accept optional
`?start=YYYY.MM&end=YYYY.MM` to scope the query to specific time ranges.

| Method | Endpoint | Parameters | Returns |
|--------|----------|------------|---------|
| GET | `/api/v1/fields/months` | — | Available YYYY.MM values from index names |
| GET | `/api/v1/fields/benchmarks` | `?start=&end=` | Distinct `run.benchmark` values |
| GET | `/api/v1/fields/tag-names` | `?start=&end=` | Distinct `tag.name` values |
| GET | `/api/v1/fields/tag-values` | `?name=&start=&end=` | Distinct `tag.val` values (optionally filtered by tag name) |
| GET | `/api/v1/fields/param-args` | `?start=&end=` | Distinct `param.arg` values |
| GET | `/api/v1/fields/param-values` | `?arg=&start=&end=` | Distinct `param.val` values (optionally filtered by param arg) |
| GET | `/api/v1/fields/primary-metrics` | `?start=&end=` | Distinct `iteration.primary-metric` values |

#### 4. Metric Data

| Method | Endpoint | Body | Returns |
|--------|----------|------|---------|
| POST | `/api/v1/metric-data` | `{ run, period, source, type, resolution, breakout, filter, distribution-stats }` | `{ values, usedBreakouts, remainingBreakouts, distributionStats? }` |

### CDM Library Functions Added (`cdm.js`)

The following functions were added to support the web UI:

**Distinct value aggregations** (for dropdown population):
- `getDistinctBenchmarks(instance, yearDotMonth)`
- `getDistinctTagNames(instance, yearDotMonth)`
- `getDistinctTagValues(instance, yearDotMonth, tagName?)`
- `getDistinctParamArgs(instance, yearDotMonth)`
- `getDistinctParamValues(instance, yearDotMonth, paramArg?)`
- `getDistinctPrimaryMetrics(instance, yearDotMonth)`

**Run ID lookups via cross-index aggregation** (for search filtering):
- `getRunIdsByParam(instance, yearDotMonth, paramArg, paramVal)`
- `getRunIdsByTag(instance, yearDotMonth, tagName, tagVal)`
- `getRunIdsByPrimaryMetric(instance, yearDotMonth, primaryMetric)`

**Date range utilities**:
- `getAvailableMonths(instance)` — extracts YYYY.MM from index names
- `buildYearDotMonthRange(instance, docType, start, end)` — constructs
  the OpenSearch multi-index pattern for a date range

All aggregation functions follow the established pattern: pass an `aggs`
object to `mSearch` with the key name `source` (required by `mSearch`'s
response parser), using a `terms` aggregation on the target field.

## Phase 1: Search & Selection — Component Design

### SearchPanel

The search form provides multiple filter types:

**Basic filters** (text inputs with autocomplete datalists):
- Run ID, Benchmark, Primary Metric, Run Name, Email
- From/To date range (YYYY.MM, defaults to last 3 months)

**Multi-row filters** (add/remove pattern):
- Tags: multiple name+value pairs, each with autocomplete
- Params: multiple arg+value pairs, each with autocomplete

**Dropdown population strategy**:
- Dropdowns are lazy-loaded on first focus via the `/api/v1/fields/*` endpoints
- Values are cached in a ref to avoid re-fetching
- Cascading: tag value dropdowns refresh when the tag name changes;
  param value dropdowns refresh when the param arg changes

**Search execution flow**:
1. Build API filter object from form state
2. Call `searchRuns(filters)` → server returns matching run IDs
3. Call `loadIterationsForRuns(runIds)` which, for each run:
   a. Fetches benchmark name, iteration IDs, and tags (in parallel)
   b. Fetches params, samples, primary metrics, period names (in parallel)
   c. Fetches sample statuses
   d. Computes common vs. unique params across iterations
   e. Builds iteration objects with all metadata
4. Apply client-side iteration filtering for params and primary-metric
   (these are per-iteration, not per-run, so server-side only narrows runs)
5. Pass results to IterationTable

### IterationTable

Displays iterations as sortable, filterable rows:

- **Columns**: checkbox, Run ID (truncated UUID with tooltip), Benchmark,
  Tags, Unique Params, Primary Metric name, Sample count, Pass/Fail status
- **Sorting**: click column headers to sort (asc/desc toggle)
- **Param filter**: text input filters iterations by param name or value
- **Selection**: checkboxes toggle selection; header checkbox selects/deselects all visible
- **Row click**: toggles selection for the entire row

### SelectionBar

Shows when iterations are selected:
- Displays count and up to 10 chips with benchmark + unique params
- Each chip has a remove button
- "Clear All" button to deselect everything
- Selections persist across searches (user can search multiple times and
  accumulate selections)

### DebugConsole

A fixed-position panel at the bottom of the screen for performance diagnosis:

- **Collapsed by default**, click to expand
- **Header summary**: entry count, pending count, total API time
- **Entry types**:
  - `API` (blue): HTTP requests with method, path, duration, status code
  - `WORK` (orange): client-side work items with duration
  - `INFO` (green): search lifecycle markers (started, found N runs, complete)
- **Timing instrumentation**: the API client (`api/cdm.js`) automatically logs
  every request; SearchPanel uses `timeWork()` for major phases

## Static File Serving

The Express server serves the React build output:

```javascript
// In server.js
var webUiDist = path.join(__dirname, 'web-ui', 'dist');
if (fs.existsSync(webUiDist)) {
  app.use(express.static(webUiDist));
  app.get('*path', (req, res) => {
    res.sendFile(path.join(webUiDist, 'index.html'));  // SPA fallback
  });
}
```

The SPA fallback ensures that direct navigation to any route (e.g.,
`/compare`) serves `index.html`, allowing React to handle routing.

The `start-server.sh` script builds the web UI before starting the server:
1. `npm install` for cdmq dependencies
2. `cd web-ui && npm install && npm run build`
3. `node ./server.js`

Note: The web-ui `npm install` will eventually move to crucible's dependency
management system, which already handles npm packages for other CDM code.

## Development Workflow

### Local Development (with hot reload)

```bash
# Terminal 1: Start the CDM API server (requires OpenSearch running)
cd queries/cdmq
node server.js --host localhost:9200

# Terminal 2: Start Vite dev server
cd queries/cdmq/web-ui
npm run dev
# Open http://localhost:5173
```

Vite proxies `/api/*` and `/health` to `localhost:3000` (configured in
`vite.config.js`).

### Production Build

```bash
cd queries/cdmq/web-ui
npm run build
# Output: dist/ directory served by Express on port 3000
```

### Via Crucible

```bash
crucible start opensearch   # Starts OpenSearch + CDM server (builds web UI)
# Open http://<host>:3000
```

## Implementation Status

### Phase 1: Search & Selection — Implemented
See sections above and [DESIGN.md](DESIGN.md) for full details.

### Phase 2: Compare — Implemented
Bar chart comparison with hierarchical group-by headers, supplemental metrics (overlay + panel modes), breakouts with filter/sample selection, click-to-pin with reference lines, URL state sharing. See [DESIGN.md](DESIGN.md) for full details.

### Phase 3: Deep Dive (Time-Series Line Charts) — Implemented

Time-series line charts with per-iteration color themes, combined/split/stacked modes, brush-to-zoom with re-query, unified series legend with rowSpan grouping, live value tracking across all charts, and two-pass filter support. See [DESIGN.md](DESIGN.md) for full details.

## Design Decisions & Rationale

### Why Vite + React (not a heavier framework)?

Crucible's UI needs are focused: search, chart, interact. React provides the
component model and ecosystem (Recharts) without the overhead of Next.js or
similar. Vite gives fast builds and HMR for development. The entire build
output is a few hundred KB.

### Why serve from the same Express server?

Eliminates CORS configuration, simplifies deployment (one container, one
port), and means the UI is automatically available wherever the CDM API runs.
No separate web server or reverse proxy needed.

### Why a custom AutocompleteInput instead of a select library?

`AutocompleteInput.jsx` replaces native `<datalist>` elements with a custom
dropdown that supports multi-select (chips), context-aware partitioning
(present vs. absent values based on current search results), and keyboard
navigation. This avoids third-party React dropdown dependencies while
providing the UX needed for multi-value tag/param filtering.

### Why client-side iteration filtering after server-side run filtering?

Params and primary-metric are per-iteration properties, but runs contain
multiple iterations. The server narrows from potentially thousands of runs
to a handful (the expensive part), then the client filters iterations within
those runs (cheap, since each run has few iterations). This avoids adding
complex iteration-level filtering to the REST API.

### Why lazy-load dropdown values instead of preloading?

Some fields (params, tags) can have thousands of distinct values. Loading
all of them on page load would waste bandwidth and delay the initial render.
Lazy loading on first focus keeps the UI responsive and only fetches what
the user actually interacts with.
