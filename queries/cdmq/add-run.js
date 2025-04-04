var cdm = require('./cdm');
var fs = require('fs');
var xz = require('xz');
var readline = require('readline');
var path = require('path');
var program = require('commander');
var instances = []; // opensearch instances

function save_host(host) {
  var host_info = { host: host, header: { 'Content-Type': 'application/json' } };
  instances.push(host_info);
}

function save_userpass(userpass) {
  if (instances.length == 0) {
    console.log('You must specify a --url before a --userpass');
    process.exit(1);
  }
  instances[instances.length - 1]['header'] = {
    'Content-Type': 'application/json',
    Authorization: 'Basic ' + btoa(userpass)
  };
}

function save_ver(ver) {
  if (instances.length == 0) {
    console.log('You must specify a --host before a --ver');
    process.exit(1);
  }
  if (/^v[7|8|9]dev$/.exec(ver)) {
    instances[instances.length - 1]['ver'] = ver;
  } else {
    console.log('The version must be v7dev, v8dev, or v9dev, not: ' + ver);
    process.exit(1);
  }
}

async function readNdjsonXzToString(filePath) {
  return new Promise((resolve, reject) => {
    try {
      const fileStream = fs.createReadStream(filePath);
      const decompressStream = new xz.Decompressor();

      let decompressedString = '';

      fileStream.pipe(decompressStream);

      decompressStream.on('data', (chunk) => {
        decompressedString += chunk.toString('utf8');
      });

      decompressStream.on('end', () => {
        resolve(decompressedString);
      });

      decompressStream.on('error', (error) => {
        reject(error);
      });

      fileStream.on('error', (error) => {
        reject(error);
      });
    } catch (error) {
      reject(error);
    }
  });
}

async function processDir(instance, dir, mode) {
  var jsonArr = [];
  var files = fs.readdirSync(dir);

  for (var i = 0; i < files.length; i++) {
    var regExp = /\.ndjson\.xz$/;
    var matches = regExp.exec(files[i]);
    // skip non ndjson.xz files
    if (!matches) {
      continue;
    }
    try {
      const filePath = path.join(program.dir, files[i]);
      const decompressedData = await readNdjsonXzToString(filePath);
      var lines = decompressedData.split('\n');
      for (var j = 0; j < lines.length; j++) {
        // TODO: validate JSON syntax and possible validate document schema?
        if (lines[j] != '') {
          jsonArr.push(lines[j]);
        }
      }
    } catch (error) {
      console.error('Error processing NDJSON.XZ file:', error);
    }
  }
  if (mode == 'index') {
    var responses = await esJsonArrRequest(instance, '', '/_bulk', jsonArr);
  } else if (mode == 'getruns') {
    var runIds = [];
    for (var k = 1; k < jsonArr.length; k += 2) {
      try {
        var obj = JSON.parse(jsonArr[k]);
      } catch (jsonError) {
        console.log('Could not porse: [' + jsonArr[k] + ']');
        continue;
      }
      runId = obj['run']['run-uuid'];
      if (!runIds.includes(runId)) {
        runIds.push(runId);
      }
    }
    return runIds;
  } else {
    console.log('mode [' + mode + '] is not supported');
  }
}

async function main() {
  program
    .version('0.1.0')
    .option('--dir <a directory with ndjson files to index>')
    .option('--host <host[:port]>', 'The host and optional port of the OpenSearch instance', save_host)
    .option('--userpass <user:pass>', 'The user and password for the most recent --host', save_userpass)
    .option('--ver <v7dev|v8dev|v9dev>', 'The Common Data Model version to use for the most recent --host', save_ver)
    .parse(process.argv);

  // If the user does not specify any hosts, assume localhost:9200 is used
  if (instances.length == 0) {
    save_host('localhost:9200');
  }

  getInstancesInfo(instances);
  if (program.dir) {
    var allDocTypes = ['run', 'iteration', 'sample', 'period', 'param', 'tag', 'metric_desc', 'metric_data'];
    var runIds = await processDir(instances[instances.length - 1], program.dir, 'getruns');
    for (i = 0; i < runIds.length; i++) {
      console.log('Deleting any existing documents for runId ' + runIds[i]);
      var runId = runIds[i];
      var q = { query: { bool: { filter: [{ term: { 'run.run-uuid': runId } }] } } };
      cdm.deleteDocs(instances[instances.length - 1], allDocTypes, q);
      var numDocTypes = await cdm.waitForDeletedDocs(instances[instances.length - 1], runId, allDocTypes);
      if (numDocTypes > 0) {
        console.log('Warning: could not delete all documents for ' + docTypes + ' with ' + numAttempts);
        console.log(
          'These documents may continue to be deleted in the background.  To check on the status, run this utility again'
        );
      }
    }
    console.log('Indexing documents');
    await processDir(instances[instances.length - 1], program.dir, 'index');
  } else {
    console.log('You must provide a --dir <directory with ndjsons>');
    process.exit(1);
  }
  console.log('add-run is complete');
}

main();
