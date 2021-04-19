var cdm = require('./cdm');
var program = require('commander');

program
  .version('0.1.0')
  .option('--run <run ID>')
  .option('--url <host:port>', 'The host and port of the Elasticsearch instance', 'localhost:9200')
  .parse(process.argv);

var docTypes = [ 'run', 'iteration', 'sample', 'period', 'param'];
var q = {};
if (program.run) {
    q = { 'query': { 'bool': { 'filter': [ {"term": {"run.id": program.run}} ] }}};
}
cdm.deleteMetrics(program.url, program.run);
cdm.deleteDocs(program.url, docTypes, q);
