var cdm = require('./cdm');
var program = require('commander');

program
  .version('0.1.0')
  .option('--run <uuid>')
  .option('--source <metric-source>')
  .option('-u, --url <host:port>')
  .parse(process.argv);

console.log(JSON.stringify(cdm.getMetricTypes(program.url, program.run, program.source)));
