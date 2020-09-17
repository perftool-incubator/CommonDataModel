#!/bin/bash
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=bash
# -*- mode: sh; indent-tabs-mode: nil; sh-basic-offset: 4 -*-

# This script will query *all* runs and try to output the primary-metric
# for each of the benchmark iteration/samples.  It will also attempt
# to find any tools and list the metric_source (like sar), and for each
# metric_source, list the metric_types (like L2-Gbps).  Uncomment the
# "node get-metric-data ..." lines to output a specific tool metric

# This script assumes Elasticsearch is installed on the same host
# and has data loaded from soemthing like crucible/rickshaw

url="--url localhost:9200"
if [ ! -z $1 ]; then
    run_ids="$1"
else
    run_ids=`node ./get-runs.js $url | jq -r '.ids[]'`
fi
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
            status=`node ./get-sample-status.js $url --sample $sample`
            echo status: $status
            if [ "$status" == "pass" ]; then
                primary_period_id=`node ./get-primary-period-id.js $url --sample $sample --period-name "$primary_period_name"`
                echo primary period id: $primary_period_id
                echo node get-name-format $url --period $primary_period_id --source $benchmark --type $primary_metric
                name_format=`node get-name-format $url --period $primary_period_id --source $benchmark --type $primary_metric`
                echo name-format: $name_format
                range=`node get-period-range $url --period $primary_period_id`
                begin=`echo -e $range | jq -r '.begin'`
                end=`echo -e $range | jq -r '.end'`
                echo begin: $begin  end: $end
                # to report a single number:
                echo node get-metric-data-from-period $url --begin $begin --end $end --period $primary_period_id --source $benchmark --type $primary_metric --resolution 1
                value=`node get-metric-data-from-period $url --begin $begin --end $end --period $primary_period_id --source $benchmark --type $primary_metric --resolution 1 | jq -r '.values.""[0].value'`
                echo value: $value
                #node get-metric-data-from-period $url --begin $begin --end $end --period $primary_period_id --source $benchmark --type $primary_metric --resolution 1
                tools=`node ./get-metric-sources.js --url localhost:9200 --run 4DBC07A2-BFEB-11EA-899B-0D186BE91F5C | jq -r '.[]' | grep -v $benchmark`
                echo tools: $tools
                for tool in $tools; do
                    echo "  tool: $tool"
                    tool_types=`node ./get-metric-types.js --url localhost:9200 --run 4DBC07A2-BFEB-11EA-899B-0D186BE91F5C --source $tool| jq -r '.[]' | grep -v $benchmark`
                    for tool_type in $tool_types; do
                        name_format=`node get-name-format $url --source $tool --type $tool_type`
                        echo "    type: $tool_type"
                        echo "      name_format: $name_format"
                        #node get-metric-data $url --begin $begin --end $end --source $tool --type $tool_type --resolution 1 --breakout dev=genev_sys_6081
                        #node get-metric-data $url --begin $begin --end $end --source $tool --type $tool_type --resolution 1 --breakout cstype=worker,csid=1,dev,direction
                        #node get-metric-data $url --begin $begin --end $end --source $tool --type $tool_type --resolution 1 --breakout cstype=worker,csid=1,dev=eth0,direction
                    done
                done
            fi
        done
    done
done
