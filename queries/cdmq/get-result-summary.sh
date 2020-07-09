#!/bin/bash
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=bash
# -*- mode: sh; indent-tabs-mode: nil; sh-basic-offset: 4 -*-

url="--url localhost:9200"
run_ids=`node ./get-runs.js $url | jq -r '.ids[]'`
for run_id in $run_ids; do
    echo
    echo run-id: $run_id
    benchmark=`node ./get-benchmark-name.js $url --run $run_id`
    echo benchmark $benchmark
    tags=`node ./get-tags.js $url --run $run_id`
    echo tags $tags
    iterations=`node ./get-iterations.js $url --run $run_id | jq -r '.ids[]'`
    echo "iteraitons: $iterations"
    for iteration in $iterations; do
        primary_metric=`node ./get-primary-metric.js $url --iteration $iteration`
        echo primary metric: $primary_metric
        samples=`node ./get-samples.js $url --iteration $iteration | jq -r '.ids[]'`
        echo samples: $samples
        echo node ./get-primary-period-name.js $url --iteration $iteration 
        primary_period_name=`node ./get-primary-period-name.js $url --iteration $iteration`
        echo primary period name: $primary_period_name
        for sample in $samples; do
            echo sample: $sample
            primary_period_id=`node ./get-primary-period-id.js $url --sample $sample --period-name "$primary_period_name"`
            echo primary period id: $primary_period_id
            name_format=`node get-name-format $url --period $primary_period_id --source $benchmark --type $primary_metric`
            echo name_format: $name_format
            range=`node get-period-range $url --period $primary_period_id`
            begin=`echo -e $range | jq -r '.begin'`
            end=`echo -e $range | jq -r '.end'`
            echo begin: $begin  end: $end
            # to report a single number:
            value=`node get-metric-data-from-period $url --begin $begin --end $end --period $primary_period_id --source $benchmark --type $primary_metric --resolution 1 | jq -r '.values.""[0].value'`
            echo value: $value
            # to get data for a line graph:
            #node get-metric-data-from-period $url --begin $begin --end $end --period $primary_period_id --source $benchmark --type $primary_metric --resolution 200
        done
    done
done
