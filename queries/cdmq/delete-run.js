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

function save_ver(ver) {
    if (instances.length == 0) {
        console.log("You must specify a --host before a --ver");
        process.exit(1);
    }
    if (/^v[7|8|9]dev$/.exec(ver)) {
      instances[instances.length - 1]['ver'] = ver;
    } else {
      console.log("The version must be v7dev, v8dev, or v9dev, not: " + ver);
      process.exit(1);
    }
}

program
  .version('0.1.0')
  .option('--run <run ID>')
  .option('--host <host[:port]>', 'The host and optional port of the OpenSearch instance', save_host)
  .option('--userpass <user:pass>', 'The user and password for the most recent --host', save_userpass)
  .option('--ver <v7dev|v8dev|v9dev>', 'The Common Data Model version to use for the most recent --host', save_ver)
  .parse(process.argv);

// If the user does not specify any hosts, assume localhost:9200 is used
if (instances.length == 0) {
  save_host("localhost:9200")
}

getInstancesInfo(instances);

async function waitFor(instance, docTypes) {
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
      console.log('Document deletion rate: ' + (previousTotalDocCount - totalDocCount) / interval + ' documents/sec');
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
if (program.run) {
  q = { query: { bool: { filter: [{ term: { 'run.run-uuid': program.run } }] } } };
  // When deleting, you must use exactly one instance, so we use the last provided.
  // We don't want to search for an instance with this run, because we don't want
  //  to delete just any copy of this run.
  cdm.deleteDocs(instances[instances.length - 1], allDocTypes, q);
  waitFor(instances[instances.length - 1], allDocTypes);
} else {
  console.log("You must provide a --run <run-id>");
  process.exit(1);
}
