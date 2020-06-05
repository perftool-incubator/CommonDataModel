var cdm = require('./cdm');
var program = require('commander');

program
  .version('0.1.0')
  .option('--period <uuid>')
  .option('--source <metric-source>')
  .option('--type <metric-type>')
  .option('-u, --url <host:port>')
  .parse(process.argv);

console.log(cdm.getNameFormat(program.url, program.period, program.source, program.type));
