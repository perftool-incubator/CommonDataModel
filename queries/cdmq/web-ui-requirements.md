# Crucible Web UI — Requirements Document

## Purpose

Build a web-based UI for crucible performance engineers to search for benchmark iterations, visually compare results across iterations, and deep-dive into time-series metric data. The UI consumes the CDM API server (`server.js`) running on the same host.

## Target Users

Performance engineers who need to:
- Find and filter benchmark iterations across multiple runs
- Compare primary metrics and supplemental metrics across iterations
- Investigate performance differences by drilling into per-CPU, per-device, or per-component metrics over time
- Overlay metrics from different iterations/samples to identify regressions or improvements

## Technology

- **Framework**: React (standard, widely supported)
- **Deployment**: Runs in the same Podman container that hosts OpenSearch and the CDM API server (port 3000)
- **API**: All data comes from the CDM API server at `http://localhost:3000/api/v1/`
- **Charting**: Use a capable charting library (e.g., Recharts, Chart.js, or Plotly) that supports bar charts, line charts, overlays, and dynamic series

## CDM API Endpoints Available

The UI communicates exclusively with the CDM API server. No direct OpenSearch access.

### Run Discovery
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/runs?run=&name=&email=&harness=` | Search for runs by filters |

### Run-Scoped Queries (all require run ID)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/run/:id/tags` | Get tags for a run |
| GET | `/api/v1/run/:id/benchmark` | Get benchmark name |
| GET | `/api/v1/run/:id/iterations` | Get iteration UUIDs |
| POST | `/api/v1/run/:id/iterations/params` | Get params for iterations |
| POST | `/api/v1/run/:id/iterations/primary-period-name` | Get primary period names |
| POST | `/api/v1/run/:id/iterations/samples` | Get sample IDs per iteration |
| POST | `/api/v1/run/:id/samples/statuses` | Get pass/fail per sample |
| POST | `/api/v1/run/:id/samples/primary-period-id` | Get primary period UUIDs |
| POST | `/api/v1/run/:id/periods/range` | Get begin/end times |
| POST | `/api/v1/run/:id/iterations/primary-metric` | Get primary metric per iteration |
| GET | `/api/v1/run/:id/metric-sources` | Get all metric sources |
| POST | `/api/v1/run/:id/metric-types` | Get types per source |

### Metric Data
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/metric-data` | Get metric values with breakouts, resolution, filters |

#### Metric Data Request Body
```json
{
  "run": "uuid",
  "period": "uuid",
  "source": "mpstat",
  "type": "Busy-CPU",
  "begin": 1770958543837,
  "end": 1770958601744,
  "resolution": 1,
  "breakout": ["hostname", "num", "type"],
  "filter": "gt:0.01",
  "distribution-stats": ["min", "max", "mean", "median", "stddev", "p95"]
}
```

#### Metric Data Response
```json
{
  "values": {
    "<label1>": [{ "begin": 123, "end": 456, "value": 0.85 }, ...],
    "<label2>": [{ "begin": 123, "end": 456, "value": 0.42 }, ...]
  },
  "usedBreakouts": ["hostname", "num"],
  "remainingBreakouts": ["type", "core", "package"],
  "valueSeriesLabelDecoder": "-<hostname>-<num>",
  "distributionStats": {
    "<label1>": {
      "min": 0.42,
      "max": 0.91,
      "mean": 0.68,
      "median": 0.67,
      "stddev": 0.12,
      "p95": 0.89
    }
  }
}
```

- `resolution=1` returns a single averaged value per label
- `resolution=N` returns N time-series datapoints per label
- `distribution-stats` is optional and returns duration-weighted statistics over the native reconstructed timeline, independent of `resolution`
- Supported statistics are `min`, `max`, `mean`, `median`, `stddev`, and exact duration-weighted `pNN` percentiles such as `p95`
- `stddev` is population standard deviation, percentiles use duration-weighted nearest rank, `median` is `p50`, and a single native interval has `stddev=0`
- `breakout` controls grouping dimensions; `remainingBreakouts` shows what's still available
- Labels encode the breakout values: `<host-1>-<cpu-0>` for breakouts `hostname,num`
- Values are numeric (e.g., mpstat values are 0.0–1.0 where 1.0 = 100% busy)

## Data Model Context

The CDM hierarchy is: **run → iteration → sample → period → metric**

- A **run** is a single benchmark execution, identified by a UUID. Runs have tags (key-value metadata) and a benchmark name.
- An **iteration** represents one configuration of benchmark parameters within a run. A run may have many iterations (e.g., varying message size, thread count, protocol).
- Each iteration has **params** — the benchmark parameters for that iteration (e.g., `bs=4k`, `rw=randread`). Params that are the same across all iterations are "common params"; params that differ are "unique params" and identify the iteration.
- A **sample** is a repetition of the same iteration (for statistical confidence). Samples have a pass/fail status.
- A **period** is a time window within a sample (e.g., "measurement", "warmup"). The "primary period" is the one used for results.
- **Metrics** have a source (e.g., `fio`, `mpstat`, `sar-net`), a type (e.g., `iops`, `Busy-CPU`, `L2-Gbps`), and breakout dimensions (e.g., `hostname`, `cpu`, `device`).
- The **primary metric** is the main benchmark result (e.g., `fio::iops`, `uperf::Gbps`). It is defined per iteration.

## Implementation Phases

### Phase 1: Iteration Search and Selection

**Goal**: Allow users to find and select iterations from across multiple runs for comparison.

#### Search/Filter Capabilities
- Search by **tags**: runs have key-value tags (e.g., `run-type=net-test`, `topology=bridge`). Users should be able to filter by tag name and/or value.
- Search by **benchmark name**: e.g., `trafficgen`, `fio`, `uperf`
- Search by **run age**: e.g., "last 7 days", "last 30 days", or a custom date range
- Search by **user info**: run name or email
- Search by **benchmark params**: e.g., find all iterations where `bs=4k` or `rw=randread`
- Search by **endpoint type**: the crucible endpoint used (remotehosts, kube, etc.) — this is available as a tag or param

#### Search Results Display
- Show iterations as rows in a table/list
- Each row shows: run ID (or a short identifier), benchmark name, tags, unique params, primary metric value, sample count, pass/fail status, date/time
- Allow selecting multiple iterations (checkboxes) for comparison
- Support sorting and additional filtering on the result set

#### Iteration Selection State
- Selected iterations persist across searches (user can search, select some, search again with different criteria, select more)
- Show a summary of selected iterations (count, labels)
- Allow removing individual iterations from the selection
- Selected iterations are the input to Phase 2

### Phase 2: Result Summary — Comparison Bar Graphs

**Goal**: Visualize and compare the primary metric (and additional user-selected metrics) across the selected iterations as bar charts.

#### Primary Metric Bar Chart
- Display one bar per selected iteration
- Y-axis: primary metric value (e.g., IOPS, Gbps)
- X-axis: iterations, labeled by their unique params
- If iterations have multiple passing samples, show: mean value as the bar height, with error bars or whiskers showing min/max or stddev
- If iterations come from different benchmarks with different primary metrics, group them accordingly

#### Clustering / Grouping
- Allow the user to choose how bars are clustered/grouped
- **Cluster by param value**: e.g., group all iterations with `bs=4k` together, then all with `bs=8k` — useful for seeing the effect of one parameter while others vary
- **Cluster by run**: group iterations from the same run together
- **Cluster by tag value**: group by a specific tag
- The user selects which dimension to cluster by; the UI rearranges the bars accordingly

#### Adding Supplemental Metrics
- Allow the user to dynamically add additional metrics to the comparison view
- Workflow: select a metric source (e.g., `mpstat`), then a metric type (e.g., `Busy-CPU`), then optionally add breakouts
- Supplemental metrics can be displayed as:
  - **Side-by-side bars**: additional bar(s) next to the primary metric bar for each iteration (use a secondary Y-axis if the scale differs significantly)
  - **Floating dots/lines**: overlay data points on top of the primary metric bars (useful for showing a metric like CPU utilization alongside throughput)
- Each supplemental metric should be visually distinct (different color, pattern, or marker)
- Allow removing supplemental metrics

#### Interactivity
- Hovering over a bar shows a tooltip with: iteration details (params, tags), metric value, sample values
- Clicking a bar/iteration could navigate to Phase 3 (deep dive) for that iteration

### Phase 3: Deep Dive — Time-Series Line Graphs

**Goal**: Graph metrics over time at high resolution for one or more iterations, enabling detailed performance investigation.

#### Basic Time-Series View
- User selects a metric source, type, and breakout dimensions
- Query metric data with `resolution` set to a reasonable number of datapoints (e.g., 100) for the selected period
- Display as a line chart: X-axis is time, Y-axis is metric value
- Each breakout combination is a separate line (e.g., per-CPU utilization shows one line per CPU)

#### Overlaying Iterations
- Allow overlaying the same metric from two or more iterations on the same chart
- Since iterations have different time ranges, align them by relative time (offset from period begin) rather than absolute timestamps
- Visual distinction: different colors or line styles per iteration
- Legend shows which iteration each line belongs to

#### Sample Handling
- An iteration may have multiple samples (repetitions). The user should be able to:
  - **Select a specific sample** to graph
  - **Average across samples**: compute the mean value at each time point across all passing samples from the same iteration
  - **Graph all samples**: each sample as its own line (labeled by sample number), useful for spotting outlier samples

#### Interactive Breakout Exploration

The deep dive should be highly interactive. Users build up their view incrementally by transforming existing lines, not by configuring everything upfront.

**Right-click context menu on any line:**
- **Add breakout**: expands the selected line into multiple lines by splitting on a new dimension
  - Shows `remainingBreakouts` as submenu options
  - Selecting a breakout (e.g., `num`) re-queries and replaces the single line with one line per value (e.g., one line per CPU)
  - The original aggregated line can optionally be kept as a reference
- **Filter breakout values**: when adding a breakout, the user can optionally:
  - Specify a **regex** to match values (e.g., `r/[0-9]+/` to match numeric CPU IDs, filtering out aggregate labels)
  - Specify an **exact string** to match a single value (e.g., `hostname=host-1`)
  - Specify **multiple values** (e.g., `num=0,1,2,3` to show only CPUs 0–3)
- **Remove breakout**: collapses lines back by removing a breakout dimension, re-aggregating the data
- **Isolate this line**: hide all other lines, keeping only the selected one — useful when there are dozens of lines and the user wants to focus on one

**Example workflow:**
1. User starts with a single line: `mpstat::Busy-CPU` (aggregated across all hosts, CPUs, types)
2. Right-clicks the line → Add breakout → `hostname` → now sees one line per host
3. Right-clicks the `host-1` line → Add breakout → `num` → now sees one line per CPU on host-1 (host-2 lines remain aggregated)
4. Right-clicks one high-CPU line → Add breakout → `type` → sees usr/sys/softirq/irq breakdown for that specific CPU
5. At any point, right-click → Remove breakout to collapse back up

**Breakout state is per-line**: different lines in the same chart can have different breakout depths. One line might show per-CPU detail while another remains aggregated at the host level.

#### Common Deep Dive Scenarios
These are the patterns performance engineers most commonly use:

1. **CPU utilization by type**: source=`mpstat`, type=`Busy-CPU`, breakout=`hostname,num,type` — shows usr, sys, softirq, irq per CPU
2. **Interrupt rates**: source=`procstat`, type=`interrupts-sec`, breakout=`hostname,cpu,desc` — shows per-IRQ per-CPU interrupt handling
3. **Network throughput**: source=`sar-net`, type=`L2-Gbps`, breakout=`hostname,direction` — rx vs tx per host
4. **Benchmark throughput over time**: source=`trafficgen`, type=`rx-pps`, breakout=`port_pair` — per-port-pair receive rate

#### Interactivity
- Zoom into time ranges (click-drag to zoom, scroll to pan)
- Toggle individual lines on/off via legend clicks
- Tooltip on hover shows: time, value, breakout label, iteration/sample info
- Option to export data as CSV

## General UI/UX Guidelines

- **Responsive layout**: should work on typical desktop/laptop screens (no mobile requirement)
- **Dark/light mode**: not required for MVP, but don't hardcode colors that prevent it later
- **Performance**: some queries may return hundreds of breakout labels (e.g., 128 CPUs × interrupt types). The UI should handle large datasets gracefully — consider lazy loading, virtualization, or limiting displayed series with a "show top N" option
- **State preservation**: navigating between phases should preserve selections (e.g., going to deep dive and back shouldn't lose the iteration selection or bar chart configuration)
- **Error handling**: display API errors clearly (the API returns `{ "code": "ERROR_CODE", "error": "message" }` on failure)
- **Loading states**: show spinners or progress indicators during API calls — some metric queries can take several seconds

## File Organization

The web UI source should live under:
```
queries/cdmq/web-ui/
```

The build output (static files) should be servable by the existing `server.js` via Express static file middleware.

## Out of Scope (for now)

- User authentication / multi-tenancy
- Saving/sharing dashboard configurations
- Alerting or automated regression detection
- Mobile/tablet layout
- Real-time streaming of in-progress run data
