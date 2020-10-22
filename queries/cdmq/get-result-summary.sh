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

function debug_log() {
    if [ $debug == "1" ]; then
        echo "$1"
    fi
}

url="--url localhost:9200"
jq_cmd="jq --sort-keys -r"
debug="0"


longopts="run-id:,metric-source:,metric-type:,breakout:,debug"
opts=$(getopt -q -o "" --longoptions "$longopts" -n "getopt.sh" -- "$@");
if [ $? -ne 0 ]; then
    echo "\nUnrecognized option specified: $@\n\n"
    exit 1
fi
eval set -- "$opts";
while true; do
    case "$1" in
        --run-id)
            shift;
            run_ids="$1"
            shift;
            ;;
        --metric-source)
            shift;
            metric_source=$1
            shift;
            ;;
        --metric-type)
            shift;
            metric_type="$1"
            shift;
            ;;
        --breakout)
            shift;
            breakout="$1"
            breakout_opt=" --breakout $breakout"
            shift;
            ;;
        --debug)
            shift;
            debug="1"
            ;;
        --)
            shift;
            break;
           ;;
        *)
            exit_error "Unexpected argument [$1]"
            shift;
            break;
            ;;
    esac
done

if [ -z "$run_ids" ]; then
    run_ids=`node ./get-runs.js $url | $jq_cmd '.ids[]'`
fi
for run_id in $run_ids; do
    echo
    echo run-id: $run_id
    benchmark=`node ./get-benchmark-name.js $url --run $run_id`
    echo "  benchmark: $benchmark"
    tags=`node ./get-tags.js $url --run $run_id`
    echo "  tags: $tags"
    echo "  metrics:"
    metric_sources=`node ./get-metric-sources.js --url localhost:9200 --run $run_id | $jq_cmd .[]`
    for this_metric_source in $metric_sources; do
        printf "    %s:" $this_metric_source
        metric_types=`node ./get-metric-types.js --url localhost:9200 --run $run_id --source $this_metric_source | $jq_cmd .[] | tr '\n' ' '`
        for this_metric_type in $metric_types; do
            printf "  %s" $this_metric_type
        done
        printf "\n"
    done
    iterations=`node ./get-iterations.js $url --run $run_id | $jq_cmd '.ids[]'`
    for iteration in $iterations; do
        echo "  iteration: $iteration"
        printf "    parameters:"
        node ./get-params.js $url --iteration $iteration | jq -r '. | sort_by(.arg) |.[] | "\(.arg)=\(.val)"' | while read line; do
            printf "  %s" $line
        done
        printf "\n"
        primary_metric=`node ./get-primary-metric.js $url --iteration $iteration`
        samples=`node ./get-samples.js $url --iteration $iteration | $jq_cmd '.ids[]'`
        debug_log "node ./get-primary-period-name.js $url --iteration $iteration"
        primary_period_name=`node ./get-primary-period-name.js $url --iteration $iteration`
        debug_log "  primary period name: $primary_period_name"
        if [ "$primary_period_name" == "undefined" ]; then
            echo "  skipping this iteration because primary period name is undefined"
            continue
        fi
        num_pass_samples=0
        bench_sample_vals=""
        metric_sample_vals=""
        bench_sum_value=0
        metric_sum_value=0
        for sample in $samples; do
            status=`node ./get-sample-status.js $url --sample $sample`
            if [ "$status" == "pass" ]; then
                let num_pass_samples=$num_pass_samples+1
                debug_log "  node ./get-primary-period-id.js $url --sample $sample --period-name $primary_period_name"
                primary_period_id=`node ./get-primary-period-id.js $url --sample $sample --period-name "$primary_period_name"`
                debug_log "  primary period id: $primary_period_id"
                debug_log "  node get-name-format $url --period $primary_period_id --source $benchmark --type $primary_metric"
                name_format=`node get-name-format $url --period $primary_period_id --source $benchmark --type $primary_metric`
                range=`node get-period-range $url --period $primary_period_id`
                begin=`echo -e $range | $jq_cmd '.begin'`
                end=`echo -e $range | $jq_cmd '.end'`
                # to report a single number:
                debug_log "  node get-metric-data-from-period $url --begin $begin --end $end --run $run_id --period $primary_period_id --source $benchmark --type $primary_metric --resolution 1"

                bench_value=`node get-metric-data-from-period $url --begin $begin --end $end --run $run_id --period $primary_period_id --source $benchmark --type $primary_metric --resolution 1 | $jq_cmd '.values.""[0].value'`
                bench_sample_vals="$bench_sample_vals $bench_value"
                bench_sum_value=`echo "$bench_sum_value + $bench_value" | bc`

                if [ ! -z "$metric_source" -a ! -z "$metric_type" ]; then
                    metric_value=`node get-metric-data-from-period $url --begin $begin --end $end --run $run_id --source $metric_source --type $metric_type --resolution 1 $breakout_opt | $jq_cmd '.values | to_entries | .[0].value[0].value'`
                    debug_log "  node get-metric-data-from-period $url --begin $begin --end $end --run $run_id --source $metric_source --type $metric_type --resolution 1 $breakout_opt | $jq_cmd '.values | to_entries | .[0].value[0].value'"
                    metric_sample_vals="$metric_sample_vals $metric_value"
                    metric_sum_value=`echo "$metric_sum_value + $metric_value" | bc`
                fi

                tools=`node ./get-metric-sources.js --url localhost:9200 --run $run_id | $jq_cmd '.[]' | grep -v $benchmark`
                for tool in $tools; do
                    tool_types=`node ./get-metric-types.js --url localhost:9200 --run $run_id --source $tool| $jq_cmd '.[]' | grep -v $benchmark`
                    for tool_type in $tool_types; do
                        name_format=`node get-name-format $url --source $tool --type $tool_type`
                    done
                done
            fi
        done
        bench_mean_value=`echo "scale=4; $bench_sum_value / $num_pass_samples" | bc`
        echo "    benchmark samples: $bench_sample_vals  mean: $bench_mean_value $primary_metric"
        if [ ! -z "$metric_source" -a ! -z "$metric_type" ]; then
            metric_mean_value=`echo "scale=4; $metric_sum_value / $num_pass_samples" | bc`
            echo "    metric samples: $metric_sample_vals  mean: $metric_mean_value $metric_type $breakout"
        fi
    done
done
