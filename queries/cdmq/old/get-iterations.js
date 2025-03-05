var cdm = require('./cdm');
var program = require('commander');

program
  .version('0.1.0')
  .option('--run <run ID>')
  .option('--email <email address>')
  .option('--url <host:port>')
  .parse(process.argv);

var searchTerms = [];
if (program.run) {
  searchTerms.push({ term: { 'run.run-uuid': program.run } });
}
if (program.email) {
  searchTerms.push({ term: { 'run.user.email': program.email } });
}
console.log(JSON.stringify(cdm.getIterations(program.url, searchTerms)));
