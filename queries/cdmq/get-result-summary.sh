#!/bin/bash
# -*- mode: sh; indent-tabs-mode: nil; sh-basic-offset: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=bash

# This script will query *all* runs and try to output the primary-metric
# for each of the benchmark iteration/samples.  It will also attempt
# to find any tools and list the metric_source (like sar), and for each
# metric_source, list the metric_types (like L2-Gbps).  Uncomment the
# "node get-metric-data ..." lines to output a specific tool metric

# This script assumes Elasticsearch is installed on the same host
# and has data loaded from soemthing like crucible/rickshaw

project_dir=$(dirname `readlink -e $0`)

run_id=""
run_dir=""
capture_run_dir=0
for arg in "$@"; do
    if [ "${capture_run_dir}" == 1 ]; then
        capture_run_dir=0
        run_dir="${arg}"
    fi
    if echo "${arg}" | grep -q "base-run-dir="; then
        run_dir=$( echo "${arg}" | sed -e "s/.*=//" )
    elif echo "${arg}" | grep -q "base-run-dir"; then
        capture_run_dir=1
    fi
done
if [ -n "${run_dir}" ]; then
    result="${run_dir}/run/rickshaw-run.json"
    if [ -e "${result}.xz" ]; then
        result_dumper="xzcat"
        result+=".xz"
    elif [ -e "${result}" ]; then
        result_dump="cat"
    else
        echo "Could not determine run-ID from --base-run-dir"
        exit 1
    fi
    run_id=$( ${result_dumper} ${result} | jq -r '. "run-id"' )
    echo "Found run-ID ${run_id} in result directory ${run_dir}"
    run_id=" --run ${run_id} "
fi

pushd "$project_dir" >/dev/null
node ./get-result-summary.js "$@" ${run_id}
rc=$?
popd >/dev/null
exit $rc
