# cdmq

## Introduction
The contents of this directory contain a collection of scripts in Javascript intended to be executed with [node.js](https://nodejs.org).  These scripts get data from an elasticsearch instance.  The data must be in Common Data Format. which is documented in this project under [templates](../templates).  The scripts here are meant to help inspect, compare, and export data from benchmarks and performance & resource-utilization tools, in order to report and investigate performance.

In order to generate this data, you must run a benchmark via automation framework which uses the Common Data Format and index that data into elasticsearch.  One of those automation frameworks is the [crucible](https://github.com/perftool-incubator/crucible) project.  A subproject of crucible, [crucible-examples](https://github.com/perftool-incubator/crucible-examples), includes scenarios to run some of these benchmarks.

## Terms
Many of the scripts refer to different terms we associate with either running a benchmark or examining the resulting data, and these terms are not always universally known or agreed upon for specific benchmarks (like uperf and fio), or even benchmark automation frameworks,  Nevertheless, the CommonDataModel project has adopted the following terms, which originate from the crucible project.  You will need to become familiar with these terms in order to use these scripts:

- benchmark: A specific benchmark, like fio, uperf, trafficgen, or oslat.
- run: An invocation of a command, like `crucible run`, which facilitates the execution of a benchmark, often running the benchmark many times.  In the context `cdmq`, this usually refers to the data that was generated for that run.
- iteration: A set of parameters to execute a benchmark, for example, for uperf:  `test-type: stream, wsize: 256, nthreads: 16, duration: 90`.  One or more unique iterations typically make up a run
- parameter: An option used for the underlying benchmark.  Most parameters are unique to a specific benchmark.
- sample: An actual execution of an iteration.  Often there will be multiple samples for an iteration, in order to provide an average and standard-deviation.
- period: A time-period for a sample.  When a sample is executed, there may be one or more periods which represent a certain phase for the benchmark, like warmup, measurement, etc.
- primary-period: A period where a benchmark's primary metric is measured
- primary-metric: A benchmark's most common metric, like `Gbps`, or `IOPS`.  Each iteration has a primary-metric, but different iterations (different combinations of parameters) might have a different primary-metric.  For example uperf samples with `test-type: stream` have a primary metric of `Gbps`, while `test-type: rr` uses `transactions-sec`
- metric: Some unit of measure, either a measure of throughput (work/time) or a "count" (elapsed-time, latency, level, occupancy, etc), or a simple "pass/fail"
## Scripts
Below are documented most common scripts used for this project.  All of these scripts can be run via `node ./script-name.js`, and some have wrapper scripts `script-name.sh`  which provide the casual user a more convenient invocation.   If you are using [crucible](https://github.com/perftool-incubator/crucible), it may provide an alternative way to use this script (documented in each script subsection below).
### get-result-summary.js
This script produces a summary of a single run., including tags, metrics present, as well as all the iterations and their samples.  To run this script, you must specify a run-id: `node ./get-result-summary.js --run 0bda53c3-f0b2-416a-be54-cee738b75010`.  If you are using the crucible project, you will likely be using the crucible command-line `crucible get result --run 0bda53c3-f0b2-416a-be54-cee738b75010`.  In this example, the following output is produced:

    run-id: 0bda53c3-f0b2-416a-be54-cee738b75010
      tags: datapath=ovn-k-tc irq=bal kernel=4.18.0-305.34.2.el8_4.x86_64 mtu=1400 offload=False osruntime=chroot pods-per-worker=16 proto=tcp rcos=410.84.202202110840-0 scale_out_factor=1 sdn=OVNKubernetes test=stream topo=internode userenv=stream8
      metrics:
        source: procstat
          types: interrupts-sec
        source: mpstat
          types: Busy-CPU NonBusy-CPU
        source: uperf
          types: Gbps
        source: sar-net
          types: L2-Gbps packets-sec errors-sec
        source: ovs
          types: Gbps packets-sec errors-sec conntrack
        source: iostat
          types: avg-req-size-kB avg-service-time-ms kB-sec operations-sec avg-queue-length operations-merged-sec percent-merged percent-utilization
        source: sar-mem
          types: Page-faults-sec KB-Paged-in-sec KB-Paged-out-sec Pages-freed-sec VM-Efficiency scanned-pages-sec
        source: sar-scheduler
          types: IO-Blocked-Tasks Load-Average-01m Load-Average-05m Load-Average-15m Process-List-Size Run-Queue-Length
        source: sar-tasks
          types: Context-switches-sec Processes-created-sec
      iterations:
        common params: duration=90 ifname=eth0 protocol=tcp test-type=stream
        iteration-id: 487471A8-AD33-11EC-981F-ADE96E3275F7
          unique params: nthreads=1 rsize=64
          primary-period name: measurement
          samples:
            sample-id: 488B39E2-AD33-11EC-8629-ADE96E3275F7
              primary period-id: 48919B3E-AD33-11EC-B811-ADE96E3275F7
              period range: begin: 1648109018282 end: 1648109106687
            sample-id: 48D0F478-AD33-11EC-B11C-ADE96E3275F7
              primary period-id: 48D547D0-AD33-11EC-B70F-ADE96E3275F7
              period range: begin: 1648109247010 end: 1648109335220
            sample-id: 4912E7F2-AD33-11EC-9054-ADE96E3275F7
              primary period-id: 491799F0-AD33-11EC-AE06-ADE96E3275F7
              period range: begin: 1648109476004 end: 1648109564247
            result: (Gbps) samples: 6.27 6.34 6.30 mean: 6.30 min: 6.27 max: 6.34 stddev: 0.04 stddevpct: 0.56
        iteration-id: 49778432-AD33-11EC-9DDE-ADE96E3275F7
          unique params: nthreads=1 rsize=512
          primary-period name: measurement
          samples:
            sample-id: 499F183A-AD33-11EC-9FAE-ADE96E3275F7
              primary period-id: 49A6F622-AD33-11EC-8F6E-ADE96E3275F7
              period range: begin: 1648109706606 end: 1648109794085
            sample-id: 4A409156-AD33-11EC-BC0F-ADE96E3275F7
              primary period-id: 4A4920BE-AD33-11EC-A158-ADE96E3275F7
              period range: begin: 1648109933986 end: 1648110022201
            sample-id: 4B1597F2-AD33-11EC-B28B-ADE96E3275F7
              primary period-id: 4B1CF0F6-AD33-11EC-874B-ADE96E3275F7
              period range: begin: 1648110167246 end: 1648110254281
            result: (Gbps) samples: 6.34 6.25 6.30 mean: 6.30 min: 6.25 max: 6.34 stddev: 0.05 stddevpct: 0.75
        iteration-id: 4BAF5BD0-AD33-11EC-A354-ADE96E3275F7
          unique params: nthreads=1 rsize=1024
          primary-period name: measurement
          samples:
            sample-id: 4BD1C440-AD33-11EC-BE5D-ADE96E3275F7
              primary period-id: 4BDEBEE8-AD33-11EC-B320-ADE96E3275F7
              period range: begin: 1648110396524 end: 1648110484285
            sample-id: 4CACB41A-AD33-11EC-82F5-ADE96E3275F7
              primary period-id: 4CAEC1D8-AD33-11EC-8DF9-ADE96E3275F7
              period range: begin: 1648110624415 end: 1648110712204
            sample-id: 4D63E892-AD33-11EC-9887-ADE96E3275F7
              primary period-id: 4D676468-AD33-11EC-8F08-ADE96E3275F7
              period range: begin: 1648110852950 end: 1648110940228
            result: (Gbps) samples: 6.29 6.30 6.28 mean: 6.29 min: 6.28 max: 6.30 stddev: 0.01 stddevpct: 0.14
        iteration-id: 4DCB6DAA-AD33-11EC-9C4D-ADE96E3275F7
          unique params: nthreads=1 rsize=16384
          primary-period name: measurement
          samples:
            sample-id: 4DF9F328-AD33-11EC-9E41-ADE96E3275F7
              primary period-id: 4E009ED0-AD33-11EC-971A-ADE96E3275F7
              period range: begin: 1648111088800 end: 1648111176505
            sample-id: 4E71EAA4-AD33-11EC-97BD-ADE96E3275F7
              primary period-id: 4E752796-AD33-11EC-9A9A-ADE96E3275F7
              period range: begin: 1648111319747 end: 1648111408207
            sample-id: 4F04D60C-AD33-11EC-ADA7-ADE96E3275F7
              primary period-id: 4F1014D6-AD33-11EC-94E3-ADE96E3275F7
              period range: begin: 1648111546729 end: 1648111635267
            result: (Gbps) samples: 6.37 6.26 6.30 mean: 6.31 min: 6.26 max: 6.37 stddev: 0.05 stddevpct: 0.87
When investigating performance, users often start with a get-result-summary, and then drill-down to a specific instance to view various metrics.  Note that all timestamps are millisecond epoch-time.
### get-metric-result.js
This script is used to dig deeper into the metrics (tool or benchmark data) found in a run or period.  To find out which metrics are available for a run, look at the `metrics:`    section from a get-result-summary.js output:

      metrics:
        source: procstat
          types: interrupts-sec
        source: mpstat
          types: Busy-CPU NonBusy-CPU
        source: uperf
          types: Gbps
        source: sar-net
          types: L2-Gbps packets-sec errors-sec
        source: ovs
          types: Gbps packets-sec errors-sec conntrack
        source: iostat
          types: avg-req-size-kB avg-service-time-ms kB-sec operations-sec avg-queue-length operations-merged-sec percent-merged percent-utilization
        source: sar-mem
          types: Page-faults-sec KB-Paged-in-sec KB-Paged-out-sec Pages-freed-sec VM-Efficiency scanned-pages-sec
        source: sar-scheduler
          types: IO-Blocked-Tasks Load-Average-01m Load-Average-05m Load-Average-15m Process-List-Size Run-Queue-Length
        source: sar-tasks
          types: Context-switches-sec Processes-created-sec

This script requires either a `--period` option or a combination of `--run`, `--begin`, and `--end`, plus a `--source` and `--type`.  In the following example, a query for uperf for Gbps is used:

    # node ./get-metric-data.js --period 52FB1F1E-AD33-11EC-B16C-ADE96E3275F7 --source uperf --type Gbps
    This produces a JSON output for this metric:
    
        {
          "name": "uperf",
          "type": "Gbps",
          "label": "",
          "values": {
            "": [
              {
                "begin": 1648111546729,
                "end": 1648111635267,
                "value": "6.329"
              }
            ]
          },
          "breakouts": [
            "cmd",
            "csid",
            "cstype"
          ]
        }

The same query can be used for tool data, such as sar:

    # node ./get-metric-data.js --period 4F1014D6-AD33-11EC-94E3-ADE96E3275F7 --source sar-net --type L2-Gbps
        {
          "name": "sar-net",
          "type": "L2-Gbps",
          "label": "",
          "values": {
            "": [
              {
                "begin": 1648111546729,
                "end": 1648111635267,
                "value": "32.23"
              }
            ]
          },
          "breakouts": [
            "csid",
            "cstype",
            "dev",
            "direction",
            "type"
          ]
        }

Note that the value for sar-net, L2-Gbps is quite different than what is reported for Uperf, Gbps.  This can be the case for many reasons, but in this case let's show how the two are actually can be similar.  First, one must understand that while these both report Gbps, the information comes from different sources.  One is measuring Gbps as reported by the client program in uperf, and another is total network throughput for all systems which were running sar.

To help explain the difference, let's use the `breakout` function of the get-metric-data.js script.  By default, the query is reporting this metric from all sources of `sar-net` and type `Gbps`.  Depending on where the sar tool was used, it may be collecting this information from multiple hosts, and on those hosts from multiple network type (and specific interfaces, and a direction for each, and so on).  These queries, by default, do not assume how the user wants to breakout and filter this metric.  The user can, however, choose to include any available breakout, which are found in the output:

    "breakouts": [
                "csid",
                "cstype",
                "dev",
                "direction",
                "type"
              ]

A breakout will divide the metric into multiple metrics, one for each value of that breakout.  For example, if metric data that was collected has a `csid` of 1 and 2, a breakout of csid will include two metrics.  In the example below, a breakout for csid and cstype are used:

 
    # node ./get-metric-data.js --period 4F1014D6-AD33-11EC-94E3-ADE96E3275F7 --source sar-net --type L2-Gbps --breakout csid,cstype
    {
      "name": "sar-net",
      "type": "L2-Gbps",
      "label": "<csid>-<cstype>",
      "values": {
        "<2>-<worker>": [
          {
            "begin": 1648111546729,
            "end": 1648111635267,
            "value": "12.99"
          }
        ],
        "<1>-<worker>": [
          {
            "begin": 1648111546729,
            "end": 1648111635267,
            "value": "19.24"
          }
        ]
      },
      "breakouts": [
        "dev",
        "direction",
        "type"
      ]
    }
Now we can see that the metric is broken-out by `cs-type` and `cs-id`.  These are terms used to describe some type of physical component in your test environment.  These tests happen to be from Openshift, so the nodes where these benchmarks are run are `worker` (for cs-type) nodes with `1` and `2` (for cs-id).  However, this breakout is not enough to get close to the uperf metric, but we also have more breakouts available:

    "breakouts": [
            "dev",
            "direction",
            "type"
          ]
So, let's use `type`, which breaks out the Gbps by virtual and physical interfaces:

    # node ./get-metric-data.js --period 4F1014D6-AD33-11EC-94E3-ADE96E3275F7 --source sar-net --type L2-Gbps --breakout csid,cstype,type
    Checking for httpd...appears to be running
    Checking for elasticsearch...appears to be running
    {
      "name": "sar-net",
      "type": "L2-Gbps",
      "label": "<csid>-<cstype>-<type>",
      "values": {
        "<2>-<worker>-<virtual>": [
          {
            "begin": 1648111546729,
            "end": 1648111635267,
            "value": "6.186"
          }
        ],
        "<2>-<worker>-<unknown>": [
          {
            "begin": 1648111546729,
            "end": 1648111635267,
            "value": "0.000"
          }
        ],
        "<2>-<worker>-<physical>": [
          {
            "begin": 1648111546729,
            "end": 1648111635267,
            "value": "6.806"
          }
        ],
        "<1>-<worker>-<virtual>": [
          {
            "begin": 1648111546729,
            "end": 1648111635267,
            "value": "12.43"
          }
        ],
        "<1>-<worker>-<physical>": [
          {
            "begin": 1648111546729,
            "end": 1648111635267,
            "value": "6.809"
          }
        ]
      },
      "breakouts": [
        "dev",
        "direction"
      ]
    }
We are one step closer, in that multiple metrics, such as `<1>-<worker>-<physical>` and `<2>-<worker>-<physical>` show a Gbps value which is close to what uperf reports.  Uperf, however, reports the data transfer for the client, and when using the `stream` test-type, this is the writes that the client is doing, which would be Tx out the client and Rx into the server.  To make this more clear where this is happening, let's use another breakout available to sar-net. Gbps, `direction`.

    {
      "name": "sar-net",
      "type": "L2-Gbps",
      "label": "<csid>-<cstype>-<type>-<direction>",
      "values": {
        "<2>-<worker>-<virtual>-<tx>": [
          {
            "begin": 1648111546729,
            "end": 1648111635267,
            "value": "6.168"
          }
        ],
        "<2>-<worker>-<virtual>-<rx>": [
          {
            "begin": 1648111546729,
            "end": 1648111635267,
            "value": "0.01733"
          }
        ],
        "<2>-<worker>-<unknown>-<tx>": [
          {
            "begin": 1648111546729,
            "end": 1648111635267,
            "value": "0.000"
          }
        ],
        "<2>-<worker>-<unknown>-<rx>": [
          {
            "begin": 1648111546729,
            "end": 1648111635267,
            "value": "0.000"
          }
        ],
        "<2>-<worker>-<physical>-<tx>": [
          {
            "begin": 1648111546729,
            "end": 1648111635267,
            "value": "0.02352"
          }
        ],
        "<2>-<worker>-<physical>-<rx>": [
          {
            "begin": 1648111546729,
            "end": 1648111635267,
            "value": "6.783"
          }
        ],
        "<1>-<worker>-<virtual>-<tx>": [
          {
            "begin": 1648111546729,
            "end": 1648111635267,
            "value": "6.221"
          }
        ],
        "<1>-<worker>-<virtual>-<rx>": [
          {
            "begin": 1648111546729,
            "end": 1648111635267,
            "value": "6.213"
          }
        ],
        "<1>-<worker>-<physical>-<tx>": [
          {
            "begin": 1648111546729,
            "end": 1648111635267,
            "value": "6.786"
          }
        ],
        "<1>-<worker>-<physical>-<rx>": [
          {
            "begin": 1648111546729,
            "end": 1648111635267,
            "value": "0.02329"
          }
        ]
      },
      "breakouts": [
        "dev"
      ]
    }
When evaluating these breakouts, we can see that `<1>-<worker>-<physical>-<tx>` has 6.786 Gbps, not quite the same sas uperf, but uperf reports Gbps for the messages in the program, and not the Gbps for the additional headers for TCP, IP, and Ethernet.  This still does not show the Gbps for a specific interface, but that can be done with another breakout.  However, as shown above, more breakouts generally produces more output, some of which you may want to filter.  This can be accomplished in two ways, by limiting the value for a breakout and limiting the metrics based on the metric-value.  The following uses both of these methods:

    # node ./get-metric-data.js --period 4F1014D6-AD33-11EC-94E3-ADE96E3275F7 --source sar-net --type L2-Gbps --breakout csid=1,cstype=worker,type=physical,direction,dev --filter gt:0.01
    
    {
      "name": "sar-net",
      "type": "L2-Gbps",
      "label": "<csid>-<cstype>-<type>-<direction>-<dev>",
      "values": {
        "<1>-<worker>-<physical>-<tx>-<ens3f1>": [
          {
            "begin": 1648111546729,
            "end": 1648111635267,
            "value": "6.786"
          }
        ],
        "<1>-<worker>-<physical>-<rx>-<ens3f1>": [
          {
            "begin": 1648111546729,
            "end": 1648111635267,
            "value": "0.02328"
          }
        ]
      },
      "breakouts": []
    }
So far all of the metrics have been represented as a single value for a specific time period.  When `--period` is used, the script finds the begin and end times for this period, which in most cases, has a duration equal to the measurement time in the benchmark itself (around 90 seconds in these examples).  One can also specify `--run`, `--begin`,  and `--end` instead of `--period`, should they need to focus on a different period of time.  However, for benchmark metrics (such as uperf), it is important to limit the begin and end to within the actual measurement period for that sample.  Conversely, tool metrics can use a begin and end spanning any time period within the run, as the tool collection tends to run continuously for any particular run.   Whatever time period is used, one can also use `--resolution` to divide this time period into multiple data-samples, in order to generate things like line graphs:

    # node ./get-metric-data.js --period 4F1014D6-AD33-11EC-94E3-ADE96E3275F7 --source sar-net --type L2-Gbps --breakout csid=1,cstype=worker,type=physical,direction=tx,dev --filter gt:0.01 --resolution 10
    Checking for httpd...appears to be running
    Checking for elasticsearch...appears to be running
    {
      "name": "sar-net",
      "type": "L2-Gbps",
      "label": "<csid>-<cstype>-<type>-<direction>-<dev>",
      "values": {
        "<1>-<worker>-<physical>-<tx>-<ens3f1>": [
          {
            "begin": 1648111546729,
            "end": 1648111555582,
            "value": "6.723"
          },
          {
            "begin": 1648111555583,
            "end": 1648111564436,
            "value": "7.076"
          },
          {
            "begin": 1648111564437,
            "end": 1648111573290,
            "value": "6.508"
          },
          {
            "begin": 1648111573291,
            "end": 1648111582144,
            "value": "7.125"
          },
          {
            "begin": 1648111582145,
            "end": 1648111590998,
            "value": "6.815"
          },
          {
            "begin": 1648111590999,
            "end": 1648111599852,
            "value": "7.055"
          },
          {
            "begin": 1648111599853,
            "end": 1648111608706,
            "value": "7.024"
          },
          {
            "begin": 1648111608707,
            "end": 1648111617560,
            "value": "6.588"
          },
          {
            "begin": 1648111617561,
            "end": 1648111626414,
            "value": "6.374"
          },
          {
            "begin": 1648111626415,
            "end": 1648111635267,
            "value": "6.570"
          }
        ]
      },
      "breakouts": []
    }
### compare-results.js
This script is used to generate comparisons across one or more runs and provides to the ability to tailor how iterations are grouped when comparing them.  This script is particularly useful when you run a benchmark with different settings in your test-bed.  For example, if you were to test a MTU of 1500 and then 9000, you could use this script to generate output that compares the two runs.  You are, however, not limited to two runs, and you are not actually required to specify the run IDs at all.

`compare-results.js` has two primary purposes.  The first is to assemble the iterations to want to compare.  This is done with options to the script:
 * `--filter-by-params`
 * `--filter-by-tags`
 * `--filter-by-age`
 * `--add-runs`
 * `--add-iterations`
 
 When using the `--filter-by-*` options, iterations are queried from all three filters and then intersected.  Users can focus on specific benchmark params and test-bed configurations, for example:
 
 `node ./compare-results.js --filter-by-params test-type:stream --filter-by-tags study:protocols --dont-breakout-params protocol`

    All common tags: tuned:throughput-performance dir:forward study:protocols
    All common params: test-type:stream nthreads:1 duration:120
            
            
                                                                     label       mean  stddevpct                              iter-id
      nthreads:1
        wsize:256
                                                              protocol:tcp     4.5800     6.7400 C65ABB68-B9EA-11EC-B891-E3EA7B3275F7
                                                              protocol:udp     0.7500     3.0500 D86CF3AC-B9EA-11EC-9E01-2FED7B3275F7
        wsize:512
                                                              protocol:tcp     7.6500    13.7600 C68062AA-B9EA-11EC-83E9-E3EA7B3275F7
                                                              protocol:udp     1.5000     0.5000 D89710EC-B9EA-11EC-9C32-2FED7B3275F7
        wsize:1024
                                                              protocol:tcp    11.3200    25.9800 C6B45BFA-B9EA-11EC-B65D-E3EA7B3275F7
                                                              protocol:udp     3.0100     2.0600 D8CFF3B2-B9EA-11EC-AA4A-2FED7B3275F7
        wsize:16384
                                                              protocol:tcp     9.7800     1.3000 C6F1EBD2-B9EA-11EC-94EC-E3EA7B3275F7
                                                              protocol:udp     5.3800     1.4300 D8F4B4E0-B9EA-11EC-9940-2FED7B3275F7
        wsize:32768
                                                              protocol:tcp    19.9100    55.8000 C70AFF28-B9EA-11EC-860C-E3EA7B3275F7
                                                              protocol:udp     5.8000     0.1500 D9076644-B9EA-11EC-BD41-2FED7B3275F7
      nthreads:16
        wsize:256
                                                              protocol:tcp    50.8200     0.4000 CF52816A-B9EA-11EC-8024-01EC7B3275F7
                                                              protocol:udp    12.9900     3.3500 E16B9B02-B9EA-11EC-914C-4AEE7B3275F7
        wsize:512
                                                              protocol:tcp    65.0300     0.5600 CF76F2F2-B9EA-11EC-B5A0-01EC7B3275F7
                                                              protocol:udp    24.8300     5.2100 E19F65D6-B9EA-11EC-A292-4AEE7B3275F7
        wsize:1024
                                                              protocol:tcp    66.4600     0.4600 CF95A92C-B9EA-11EC-9322-01EC7B3275F7
        wsize:16384
                                                              protocol:tcp   155.5700     3.1600 CFBC6D5A-B9EA-11EC-A4E5-01EC7B3275F7
                                                              protocol:udp    76.6700     3.3700 E1F6D6A4-B9EA-11EC-9099-4AEE7B3275F7
        wsize:32768
                                                              protocol:tcp   122.5500    41.2100 CFEFB0A2-B9EA-11EC-A682-01EC7B3275F7

In the output above `--dont-breakout-params protocol` forces the `protocol` param to be pushed to the label instead of broken-out on the left.  In most cases, the user will choose at least one param and/or one tag to not breakout, in order to create a "cluster" of results with labels (which can later be used to form a clustered bar chart).  

Users can control both what gets pushed to the label, as well as the order of the breakout `--breakout-order-params`:

    # node ./compare-results.js --filter-by-params test-type:stream --filter-by-tags study:protocols --dont-breakout-params wsize --breakout-order-params protocol,threads
    
    All common tags: study:protocols
    All common params: test-type:stream duration:120
    
    
                                                                     label       mean  stddevpct                              iter-id
      protocol:tcp
        nthreads:1
                                                                 wsize:256     4.5800     6.7400 C65ABB68-B9EA-11EC-B891-E3EA7B3275F7
                                                                 wsize:512     7.6500    13.7600 C68062AA-B9EA-11EC-83E9-E3EA7B3275F7
                                                                wsize:1024    11.3200    25.9800 C6B45BFA-B9EA-11EC-B65D-E3EA7B3275F7
                                                               wsize:16384     9.7800     1.3000 C6F1EBD2-B9EA-11EC-94EC-E3EA7B3275F7
                                                               wsize:32768    19.9100    55.8000 C70AFF28-B9EA-11EC-860C-E3EA7B3275F7
        nthreads:16
                                                                 wsize:256    50.8200     0.4000 CF52816A-B9EA-11EC-8024-01EC7B3275F7
                                                                 wsize:512    65.0300     0.5600 CF76F2F2-B9EA-11EC-B5A0-01EC7B3275F7
                                                                wsize:1024    66.4600     0.4600 CF95A92C-B9EA-11EC-9322-01EC7B3275F7
                                                               wsize:16384   155.5700     3.1600 CFBC6D5A-B9EA-11EC-A4E5-01EC7B3275F7
                                                               wsize:32768   122.5500    41.2100 CFEFB0A2-B9EA-11EC-A682-01EC7B3275F7
      protocol:udp
        nthreads:1
                                                                 wsize:256     0.7500     3.0500 D86CF3AC-B9EA-11EC-9E01-2FED7B3275F7
                                                                 wsize:512     1.5000     0.5000 D89710EC-B9EA-11EC-9C32-2FED7B3275F7
                                                                wsize:1024     3.0100     2.0600 D8CFF3B2-B9EA-11EC-AA4A-2FED7B3275F7
                                                               wsize:16384     5.3800     1.4300 D8F4B4E0-B9EA-11EC-9940-2FED7B3275F7
                                                               wsize:32768     5.8000     0.1500 D9076644-B9EA-11EC-BD41-2FED7B3275F7
        nthreads:16
                                                                 wsize:256    12.9900     3.3500 E16B9B02-B9EA-11EC-914C-4AEE7B3275F7
                                                                 wsize:512    24.8300     5.2100 E19F65D6-B9EA-11EC-A292-4AEE7B3275F7
                                                               wsize:16384    76.6700     3.3700 E1F6D6A4-B9EA-11EC-9099-4AEE7B3275F7

Also, `--breakout-order-tags` and `--dont-breakout-tags` are also available with similar functions.

Note that that while not required, `--filter-by-age` has a default of `0-30`, which filters iterations between 0 to 30 *days* old.  This default is used so that queries do not unnecessarily query very old run data (unless you select a different age range). 
