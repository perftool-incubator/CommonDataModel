# CommonDataModel
A reference data model that uses elasticsearch to unify data for monitoring and benchmarking
## Introduction
### What's The Problem?
Our Data is the problem.  When it comes to performance benchmarking, or just monitoring performance, we have a data compatibility problem.  We don't have a good standard to describe our environment and include performance characterization together.  When we don't have this, many of the solutions created to help us visualize, investigate, and identify performance issues are incompatible with each other.  Solutions are often designed with very specific environments and cannot be reused for other environments.
### How Can We Fix This?
We can define a common way to store information about our environment, our performance tests (if any), and metrics and events we collect.  Having a common way to process this information allows us to query, summarize, and visualize performance data across many situations, from comparing compiler performance to identifying bottlenecks in large cloud deployments.
### What This Project Will Include
We want to provide enough information so that anyone can start storing and querying this data in a common way.  We aim to provide the following:
* elasticsearch index templates
* elasticsearch query scripts
### What This Project Will Not Include
* data conversion scripts: conversion of data is expected to be in the other projects, for example: [uperf-post-process](https://github.com/perftool-incubator/bench-uperf/blob/master/uperf-post-process)
* data indexing scripts: indexing of data is also expected to be in other projects, for example: [rickshaw-index](https://github.com/perftool-incubator/rickshaw/blob/master/rickshaw-index)
## Directory/Layout
[./templates](./templates)
This is where all ES templates will reside, ready for emitting to ES
[./queries](./queries)
One or more query implementations may be found here.  All query implementations use the same fundamental queries to get data, but may differ in output, features, and programming language.
## Versioning
The common data model will be versioned, and for each version, the number of document-types and their field-names may change.  In general, newer versions will attempt to include all document-types and field-names of previous versions.  The version number is represented in whole numbers, with a git branch for each.  Once a new version is established, only minor fixes should be applied to that version, with no changes to the schema.  If there is a major problem with a version, it should be marked as non-functional. 
## Using With Other Projects
This project is not really intended to be used standalone (however it is possible).  In most cases, this project should be incorporated into a larger suite of automation for benchmarking and/or reporting, like [crucible](https://github.com/perftool-incubator/crucible)
## Project Status
The project is still under heavy development, but minimally viable.  The templates and scripts are actively leveraged in the [crucible](https://github.com/perftool-incubator/crucible) project, for example, [here](https://github.com/perftool-incubator/crucible/blob/master/config/init-es.sh) and [here](https://github.com/perftool-incubator/crucible/blob/c8ca954aaf1cd0dd7f28c5b4167cb5708e32c3d9/bin/_main#L235)
