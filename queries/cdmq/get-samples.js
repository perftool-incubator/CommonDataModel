var cdm = require('./cdm');
var program = require('commander');

program
  .version('0.1.0')
  .option('--iteration <iteration ID>')
  .option('--url <host:port>')
  .parse(process.argv);

if (typeof(program.iteration) == "undefined") return;

console.log(JSON.stringify(cdm.getSamples(program.url, [ program.iteration ])));
