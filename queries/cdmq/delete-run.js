var cdm = require('./cdm');
var program = require('commander');

program
  .version('0.1.0')
  .option('--run <run ID>')
  .option('--url <host:port>', 'The host and port of the OpenSearch instance', 'localhost:9200')
  .parse(process.argv);

async function waitFor(docTypes) {
  var numAttempts = 1;
  var maxAttempts = 10;
  while (numAttempts <= maxAttempts && docTypes.length > 0) {
    let promise = new Promise((resolve, reject) => {
      setTimeout(() => resolve('done!'), 2000);
    });
    let result = await promise;

    console.log('\nConfirming all documents are in deleted OpenSearch (attempt #' + numAttempts + ')');
    for (let i = 0; i < docTypes.length; i++) {
      var thisNumDocs = cdm.getDocCount(program.url, program.run, docTypes[i]);
      console.log('  ' + docTypes[i] + ': doc count: ' + thisNumDocs);
      if (thisNumDocs == 0) {
        remainingDocTypes = remainingDocTypes.filter((val) => val !== docTypes[i]);
      }
    }
    docTypes = remainingDocTypes;
    numAttempts++;
  }
  if (docTypes.lenth > 0) {
    console.log('ERROR: could not delete all documents for ' + docTypes + ' with ' + numAttempts);
  }
}

var allDocTypes = ['run', 'iteration', 'sample', 'period', 'param', 'tag', 'metric_desc', 'metric_data'];
var q = {};
if (program.run) {
  q = { query: { bool: { filter: [{ term: { 'run.run-uuid': program.run } }] } } };
}
cdm.deleteDocs(program.url, allDocTypes, q);
waitFor(allDocTypes);
