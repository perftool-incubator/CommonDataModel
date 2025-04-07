var cdm = require('./cdm');
var program = require('commander');

program
  .version('0.1.0')
  .option('-i --sample <sample-ID>')
  .option('-i --period-name <name>')
  .option('-u, --url <host:port>')
  .parse(process.argv);

console.log(cdm.getPrimaryPeriodId(program.url, program.sample, program.periodName));
