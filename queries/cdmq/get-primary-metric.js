var cdm = require('./cdm');
var program = require('commander');

program
  .version('0.1.0')
  .option('-i --iteration <iteration-ID>')
  .option('-u, --url <host:port>')
  .parse(process.argv);

console.log(cdm.getPrimaryMetric(program.url, program.iteration));
