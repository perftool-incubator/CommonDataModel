# CommonDataModel
A reference data model that uses OpenSearch to unify data for monitoring and benchmarking
## Introduction
### What's The Problem?
Our Data is the problem.  When it comes to performance benchmarking, or just monitoring performance, we have a data compatibility problem.  We don't have a good standard to describe our environment and include performance characterization together.  When we don't have this, many of the solutions created to help us visualize, investigate, and identify performance issues are incompatible with each other.  Solutions are often designed with very specific environments and cannot be reused for other environments.
### How Can We Fix This?
We can define a common way to store information about our environment, our performance tests (if any), and metrics and events we collect.  Having a common way to process this information allows us to query, summarize, and visualize performance data across many situations, from comparing compiler performance to identifying bottlenecks in large cloud deployments.
### What This Project Includes
We provide enough information so that anyone can start storing and querying this data in a common way:
* OpenSearch index mapping definitions (`queries/cdmq/cdm.js`)
* A Node.js query library and HTTP query server (`queries/cdmq/`)
### What This Project Does Not Include
* data conversion scripts: conversion of data is expected to be in the other projects, for example: [uperf-post-process](https://github.com/perftool-incubator/bench-uperf/blob/master/uperf-post-process)
* data indexing scripts: indexing of data is also expected to be in other projects, for example: [rickshaw-index](https://github.com/perftool-incubator/rickshaw/blob/master/rickshaw-index)
## Directory/Layout
[./templates](./templates)
`delete.sh`, a destructive OpenSearch index cleanup script. Index mapping
definitions themselves live in `queries/cdmq/cdm.js`, not here.

[./queries](./queries)
The `cdmq` query implementation, built on Node.js.  Includes a core query library (`cdm.js`), command-line query scripts, and an HTTP server (`server.js`) that exposes CDM queries as REST endpoints.

[./workflows](./workflows)
Documentation for result calculation methodology.

[./VERSION](./VERSION)
The current CDM schema version (e.g., `v8dev`).
## Versioning
The common data model will be versioned, and for each version, the number of document-types and their field-names may change.  In general, newer versions will attempt to include all document-types and field-names of previous versions.  The current version is tracked in the `VERSION` file.  Once a new version is established, only minor fixes should be applied to that version, with no changes to the schema.  If there is a major problem with a version, it should be marked as non-functional.
## Using With Other Projects
This project is not really intended to be used standalone (however it is possible).  In most cases, this project should be incorporated into a larger suite of automation for benchmarking and/or reporting, like [crucible](https://github.com/perftool-incubator/crucible).
## Project Status
The templates and query tools are actively leveraged in the [crucible](https://github.com/perftool-incubator/crucible) project.
