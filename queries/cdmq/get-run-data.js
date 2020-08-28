var cdm = require('./cdm');
var program = require('commander');

program
  .version('0.1.0')
  .option('--run <run ID>')
  .option('--url <host:port>')
  .parse(process.argv);

var searchTerms = [];
if (program.run) {
  searchTerms.push({ "term": "run.id", "match": "eq", "value": program.run });
}
console.log(JSON.stringify(cdm.getRunData(program.url, searchTerms)));
