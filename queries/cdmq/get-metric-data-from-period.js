// With a list of 1 or more labels in --breakout, output 1 or more
// metric groups, each group consisting of 1 or more metric IDs.
//
// To find valid labels, first run get-name-format with the same 
// --period, --source, and --type options:
//
// #node ./get-name-format.js --url $eshost:9200 --period $period --source=fio --type=iops
// %host%-%job%-%action%
//
//# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript

var cdm = require('./cdm');
var program = require('commander');

function list(val) {
  return val.split(',');
}

program
  .version('0.1.0')
  .option('--url <host:port>', 'The host and port of the Elasticsearch instance')
  .option('--period <uuid>', 'The UUID from the benchmark-iteration-sample-period')
  .option('--source <name>', 'The metric source, like a tool or benchmark name (sar, fio)')
  .option('--type <name>', 'The metric type, like Gbps or IOPS')
  .option('--begin [uint]', '[optional] Timestamp in epochtime_ms, within the period\'s begin-end time range, where the calculation of the metric will begin')
  .option('--end [uint]', '[optional] Timestamp in epochtime_ms, within the period\'s begin-end time range, where the calculation of the metric will end.  If no --begin and no -end are provided, a begin and end timestamp will be derived based on when all metrics of this source and type have data present.  If --begin is before or --end is after these derived begin/end vaules, they will be adjusted (--begin is increased and/or --end is decreased) to fit within this range.')
  .option('--resolution [uint]', 'The number of datapoints to produce in a data-series')
  .option('--breakout <label1,label2,label3...>', 'List of labels to break-out the metric, like --breakout=host,id with --source=sar -type=ProcessorBusyUtil', list, [])
  .parse(process.argv);

console.log(JSON.stringify(cdm.getMetricDataFromPeriod(program.url, program.period, program.source, program.type, program.begin, program.end, program.resolution, program.breakout), null, 2));

