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
  .option('--begin [uint]', 'Timestamp in epochtime_ms')
  .option('--end [uint]', 'Timestamp in epochtime_ms')
  .option('--resolution [uint]', 'The number of datapoints to produce in a data-series')
  .option('--ids <id1,id2,id3...>', 'list of metric IDs', list, [])
  .parse(process.argv);

console.log(JSON.stringify(cdm.getMetricDataFromIds(program.url, program.begin, program.end, program.resolution, program.ids)));

