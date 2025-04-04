var cdm = require('./cdm');
var program = require('commander');

program.version('0.1.0').option('--run <run ID>').option('--url <host:port>').parse(process.argv);

console.log(JSON.stringify(cdm.getMetricDescs(program.url, program.run)));
