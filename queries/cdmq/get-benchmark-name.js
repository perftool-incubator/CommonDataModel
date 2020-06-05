var cdm = require('./cdm');
var program = require('commander');

program
  .version('0.1.0')
  .option('-r --run <run-ID>')
  .option('-u, --url <host:port>')
  .parse(process.argv);

console.log(cdm.getBenchmarkName(program.url, program.run));
