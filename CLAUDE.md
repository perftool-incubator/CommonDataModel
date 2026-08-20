# CommonDataModel (CDM) - Performance Data Model

## Purpose
Defines a unified data model for storing and querying performance test data in OpenSearch. Provides index templates, query tools, and an HTTP query server.

## Languages
- **JavaScript/Node.js**: Query library and server (`queries/cdmq/`)
- **Bash**: `templates/delete.sh`, a standalone index-cleanup script

## Key Directories
| Path | Purpose |
|------|---------|
| `queries/cdmq/` | Node.js query library and HTTP server |
| `templates/` | Just `delete.sh` — a destructive cleanup script invoked by crucible's `reinit_opensearch()`. Index mappings themselves are defined in `queries/cdmq/cdm.js` (see Templates section below) |
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
- Index mappings for every document type are defined in `queries/cdmq/cdm.js`'s `indexDefs` object (`v8dev` hand-written, `v9dev`/`v10dev` built forward via `deepClone`). This is the only live schema — `add-run.js`'s `checkCreateIndex()`/`updateIndexMappings()` use it to create/update OpenSearch indices, and also use it as a client-side validation gate, rejecting any document field not present in it before the document is ever sent to OpenSearch.
- `templates/delete.sh` is the sole surviving file in this directory — a standalone, destructive script (deletes every index on `localhost:9200`) invoked by crucible's `reinit_opensearch()`. It has no dependency on `cdm.js` or any schema definition.
- All indices are `"dynamic": "strict"` — every `metric_desc.names` breakout dimension a tool/benchmark might emit must be pre-registered as an explicit `keyword`/`double` field in `cdm.js`'s `indexDefs`, or indexing rejects it. `dynamic_templates` do NOT provide an exception to this (verified empirically) — a field matching a `dynamic_templates` glob is rejected exactly like any other unregistered field under `"dynamic": "strict"`. A dimension family with an unbounded/platform-dependent set of names (e.g. one field per CPU cache level) still needs each concrete name registered explicitly (e.g. `shared-l1-domain` .. `shared-l4-domain`) rather than a wildcard shortcut.

## Code Style
- JavaScript: Prettier formatting enforced (2-space indent, checked in CI via `cdm-ci.yaml`)
- Bash: Standard 4-space indentation with vim/emacs modelines
