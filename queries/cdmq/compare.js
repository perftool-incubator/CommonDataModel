//# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript

var cdm = require('./cdm');
var program = require('commander');

function list(val) {
  return val.split(',');
}


program
  .version('0.1.0')
  .option('--filter-by-age <N:M>, newest-day:oldest-day',
          'Limit search for results that are between N and M days old (default is 0 to 30)',"0-30")
  .option('--filter-by-tags <tags>, tag1:value1[,tagN:valueN]',
          'Only include benchmark-iterations which match a tag <key>:<value> pair(s)', list, [])
  .option('--filter-by-params <params>, param1:value1[,paramN:valueN>',
          'Only include benchmark-iterations which match a param <key>:<value> pair(s)', list, [])
  .option('--separate-by-tags tag1[,tagN]',
          'Ensure that benchmark-iterations that have a different value for tag <key> are in organnized into diffrent groups', list, [])
  .option('--separate-by-params param1[,paramN]',
          'Ensure that benchmark-iterations that have a different value for param <key> are in organnized into diffrent groups', list, [])
  .parse(process.argv);

program.url = "localhost:9200";

console.log('Options: ', program.opts());
console.log('Remaining arguments: ', program.args);


getIters(program.url, program.filterByAge, program.filterByTags, program.filterByParams);

