# CommonDataModel (CDM) - Performance Data Model

## Purpose
Defines a unified data model for storing and querying performance test data in OpenSearch. Provides index templates, query tools, and an HTTP query server.

## Languages
- **JavaScript/Node.js**: Query library and server (`queries/cdmq/`)
- **Bash**: Template management scripts (`templates/`)

## Key Directories
| Path | Purpose |
|------|---------|
| `queries/cdmq/` | Node.js query library and HTTP server |
| `templates/` | OpenSearch index templates and management scripts |
| `workflows/` | Documentation (result-calculation methodology) |

## Key Files in `queries/cdmq/`
| File | Purpose |
|------|---------|
| `cdm.js` | Core query library — document CRUD, search, aggregation |
| `server.js` | HTTP server exposing CDM queries as REST endpoints |
| `add-run.js` | Indexes a complete benchmark run |
| `delete-run.js` | Removes a run from the index |
| `get-metric-data.js` | Retrieves metric data for a run |
| `get-result-summary.js` | Generates result summaries |
| `get-primary-periods.js` | Retrieves primary measurement periods |
| `package.json` | Node.js dependencies |

## Data Model Hierarchy
`run` > `iteration` > `sample` > `period` > `metric_desc` + `metric_data`

Supporting document types: `param`, `tag`, `config_*`

## Versioning
- Versions tracked as git branches and in `VERSION` file (currently `v10dev`)
- `cdm.js` exports `supportedCdmVersions` array: `['v7dev', 'v8dev', 'v9dev', 'v10dev']`
- Index naming pattern: `cdm{VERSION}-{DOCTYPE}*` (e.g., `cdmv10dev-metric_data*`)
- v10dev's key addition is `default-aggregation` — a per-metric field on `metric_desc` (`sum`/`avg`/`max`/`min`) telling query-time aggregation how to combine values across breakout dimensions, instead of always duration-weighted summing

## Templates (`templates/`)
- `.base` files define index mappings for each document type
- `build.sh` / `Makefile` generate actual template commands
- `init.sh` initializes the OpenSearch indices

## Code Style
- JavaScript: Prettier formatting enforced (2-space indent, checked in CI via `cdm-ci.yaml`)
- Bash: Standard 4-space indentation with vim/emacs modelines
