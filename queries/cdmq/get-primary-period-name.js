var cdm = require('./cdm');
var program = require('commander');

program
  .version('0.1.0')
  .option('-i --sample <sample-ID>')
  .option('-u, --url <host:port>')
  .parse(process.argv);

console.log(cdm.getPrimaryPeriodName(program.url, program.sample));
