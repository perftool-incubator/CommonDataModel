var cdm = require('./cdm');
var program = require('commander');

program.version('0.1.0').option('--run <uuid>').option('-u, --url <host:port>').parse(process.argv);

console.log(JSON.stringify(cdm.getMetricSources(program.url, program.run)));
