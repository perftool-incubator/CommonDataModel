var cdm = require('./cdm');
var program = require('commander');
var instances = []; // opensearch instances

function save_host(host) {
    var host_info = { 'host': host, 'header': { 'Content-Type': 'application/json' } };
    instances.push(host_info);
}

function save_userpass(userpass) {
    if (instances.length == 0) {
        console.log("You must specify a --url before a --userpass");
        process.exit(1);
    }
    instances[instances.length - 1]['header'] = { 'Content-Type': 'application/json', 'Authorization' : 'Basic ' + btoa(userpass) };
}

program
  .version('0.1.0')
  .option('--run <run ID>')
  .option('--host <host[:port]>', 'The host and optional port of the OpenSearch instance', save_host)
  .option('--userpass <user:pass>', 'The user and password for the most recent --host', save_userpass)
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
      var thisNumDocs = cdm.getDocCount(instance, program.run, docTypes[i]);
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

if (instances.length == 0) {
  console.log("You must provide at least one --host <host>");
  process.exit(1);
}
console.log("instances: " + JSON.stringify(instances, null, 2));
if (program.run) {
  q = { query: { bool: { filter: [{ term: { 'run.run-uuid': program.run } }] } } };
  var instance = findInstanceFromRun(instances, program.run);
  cdm.deleteDocs(instance, allDocTypes, q);
  waitFor(allDocTypes);
} else {
  console.log("You must provide a --run <run-id>");
  process.exit(1);
}
