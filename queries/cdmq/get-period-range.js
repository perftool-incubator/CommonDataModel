var cdm = require('./cdm');
var program = require('commander');

program
  .version('0.1.0')
  .option('-u, --url <host:port>')
  .option('-i --period-id <name>')
  .parse(process.argv);

console.log(cdm.getPeriodRange(program.url, program.periodId));
