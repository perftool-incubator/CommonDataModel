var cdm = require('./cdm');
var program = require('commander');

program
  .version('0.1.0')
  .option('--run <run ID>')
  .option('--url <host:port>', 'The host and port of the OpenSearch instance', 'localhost:9200')
  .parse(process.argv);

async function waitFor(docTypes) {
  var numAttempts = 1;
  var maxAttempts = 30;
  var remainingDocTypes = docTypes;
  var totalDocCount = 0;
  var previousTotalDocCount = 0;
  var interval = 5;
  while (numAttempts <= maxAttempts && docTypes.length > 0) {
    let promise = new Promise((resolve, reject) => {
      setTimeout(() => resolve('done!'), interval * 1000);
    });
    let result = await promise;

    console.log('\nConfirming all documents are in deleted OpenSearch (attempt #' + numAttempts + ')');
    totalDocCount = 0;
    for (let i = 0; i < docTypes.length; i++) {
      var thisNumDocs = cdm.getDocCount(program.url, program.run, docTypes[i]);
      console.log('  ' + docTypes[i] + ': doc count: ' + thisNumDocs);
      totalDocCount += thisNumDocs;

      if (thisNumDocs == 0) {
        remainingDocTypes = remainingDocTypes.filter((val) => val !== docTypes[i]);
      }
    }
    docTypes = remainingDocTypes;
    numAttempts++;

    if (previousTotalDocCount != 0) {
      console.log('Document deletion rate: %.2f documents/sec\n', (previousTotalDocCount - totalDocCount) / interval);
    }
    previousTotalDocCount = totalDocCount;
  }
  if (docTypes.lenth > 0) {
    console.log('Warning: could not delete all documents for ' + docTypes + ' with ' + numAttempts);
    console.log(
      'These documents may continue to be deleted in the background.  To check on the status, run this utility again'
    );
  }
}

var allDocTypes = ['run', 'iteration', 'sample', 'period', 'param', 'tag', 'metric_desc', 'metric_data'];
var q = {};
if (program.run) {
  q = { query: { bool: { filter: [{ term: { 'run.run-uuid': program.run } }] } } };
}
cdm.deleteDocs(program.url, allDocTypes, q);
waitFor(allDocTypes);
