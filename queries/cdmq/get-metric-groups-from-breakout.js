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
  .option('--period <period ID>', 'The UUID of the period document')
  .option('--source <metric source>', 'A metric source, like iostat or fio')
  .option('--type  <metric type>', 'A metric type, like iops or Gbps')
  .option('--breakout <label1,label2,labelN>', 'break-out metric by these labels', list, [])
  .parse(process.argv);

console.log(JSON.stringify(cdm.getMetricGroupsFromBreakout(program.url, program.period, program.source, program.type, program.breakout), null, 2));
