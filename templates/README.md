## Introduction
This directory contains a single file: `delete.sh`, a destructive cleanup
script that deletes every index on a local OpenSearch instance
(`localhost:9200` only, hardcoded).

## Usage
Crucible's `reinit_opensearch()` (in `bin/base`) invokes this script to wipe
a local OpenSearch instance's indices before reinitializing it. It requires
`curl` and takes no arguments.

    $ ./delete.sh
    # Using localhost:9200 for URL
    Deleting indices:
    Deleting index cdmv10dev-run...success
    Deleting index cdmv10dev-metric_data...success
    ...

## Index mappings
The actual OpenSearch index mappings for CommonDataModel's document types are
defined in `queries/cdmq/cdm.js`'s `indexDefs` object, not in this directory.
`add-run.js` creates and updates indices from `indexDefs` automatically as
part of normal indexing — no manual template or index setup is required.
