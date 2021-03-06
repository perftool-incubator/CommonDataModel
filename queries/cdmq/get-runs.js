var cdm = require('./cdm');
var program = require('commander');

program
  .version('0.1.0')
  .option('--user <"full user name">')
  .option('--email <email address>')
  .option('--host <hostname>')
  .option('--harness <harness name>')
  .option('--url <host:port>')
  .parse(process.argv);

var searchTerms = [];
if (!program.url) {
  program.url = "localhost:9200";
}
if (program.user) {
  searchTerms.push({ "term": "run.name", "match": "eq", "value": program.user });
}
if (program.email) {
  searchTerms.push({ "term": "run.email", "match": "eq", "value": program.email });
}
if (program.host) {
  searchTerms.push({ "term": "run.host", "match": "eq", "value": program.host });
}
if (program.harness) {
  searchTerms.push({ "term": "run.harness", "match": "eq", "value": program.harness });
}
console.log(JSON.stringify(cdm.getRuns(program.url, searchTerms)));
