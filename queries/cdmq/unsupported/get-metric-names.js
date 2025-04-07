var cdm = require('./cdm');
var program = require('commander');

program
  .version('0.1.0')
  .option('--run <uuid>')
  .option('--period <uuid>')
  .option('--source <metric-source>')
  .option('--type <metric-type>')
  .option('--begin <timestamp-ms>')
  .option('--end <timestamp-ms>')
  .option('-u, --url <host:port>')
  .parse(process.argv);

console.log(
  JSON.stringify(
    cdm.getMetricNames(
      program.url,
      program.run,
      program.period,
      program.source,
      program.type,
      program.begin,
      program.end
    )
  )
);
