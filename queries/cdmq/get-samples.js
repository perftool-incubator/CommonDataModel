var cdm = require('./cdm');
var program = require('commander');

program
  .version('0.1.0')
  .option('--iteration <iteration ID>')
  .option('--email <email address>')
  .option('--url <host:port>')
  .parse(process.argv);

var searchTerms = [];
if (program.iteration) {
  searchTerms.push({ "term": "iteration.id", "match": "eq", "value": program.iteration });
}
if (program.email) {
  searchTerms.push({ "term": "run.user.email", "match": "eq", "value": program.email });
}
console.log(JSON.stringify(cdm.getIterations(program.url, searchTerms)));
console.log(JSON.stringify(cdm.getSamples(program.url, searchTerms)));
