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

var termKeys = [];
var values = [];

if (!program.url) {
  program.url = 'localhost:9200';
}
if (program.user) {
  termKeys.push('run.name');
  values.push([program.user]);
}
if (program.email) {
  termKeys.push('run.email');
  values.push([program.email]);
}
if (program.run) {
  termKeys.push('run.run-uuid');
  values.push([program.run]);
}
if (program.harness) {
  termKeys.push('run.harness');
  values.push([program.harness]);
}

console.log(JSON.stringify(cdm.mSearch(program.url, 'run', termKeys, values, 'run.run-uuid', 1000)[0]));
