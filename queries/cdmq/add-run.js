//# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript

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

  } else if (mode == 'getinfo') {
    var info = { 'runIds': [], 'indices': {} };
    console.log("going to process " + jsonArr.length + " lines");
    for (var k = 1; k < jsonArr.length; k += 2) {
      try {
        var action = JSON.parse(jsonArr[k-1]);
        var doc = JSON.parse(jsonArr[k]);
      } catch (jsonError) {
        console.log('Could not porse: [' + jsonArr[k] + ']');
        continue;
      }
      runId = doc['run']['run-uuid'];
      if (!info['runIds'].includes(runId)) {
        info['runIds'].push(runId);
      }
      // { "index": { "_index": "cdmv8dev-metric_data" } }
      if (Object.keys(action).includes('index') && Object.keys(action['index']).includes('_index')) {
        const indexName = action['index']['_index'];
        const regExp = /^cdm-*(v7dev|v8dev|v9dev)-([^@]+)(@\d\d\d\d\.\d\d)*$/;
        var matches = regExp.exec(indexName);
        if (matches) {
          cdmVer = matches[1];
          if (!Object.keys(info['indices']).includes(cdmVer)) {
            info['indices'][cdmVer] = [];
          }
          if (!info['indices'][cdmVer].includes(indexName)) {
            debuglog("going to add indexname to info[indices][" + cdmVer + "]: " + indexName);
            info['indices'][cdmVer].push(indexName);
          }
        } else {
          console.log('ERROR: the index name [' + indexName + '] was not recognized');
          process.exit(1);
        }

      } else {
        console.log('the ndjson action [' + action + '] was not valid');
        process.exit(1);
      }

    }
    return info;
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
  // Always use the last instance (last --host) provided by the user
  var instance = instances[instances.length - 1];
  if (program.dir) {
    var allDocTypes = ['run', 'iteration', 'sample', 'period', 'param', 'tag', 'metric_desc', 'metric_data'];
    var info = await processDir(instance, program.dir, 'getinfo');
    //  The cdmVer going forward must be set based on the data found in the ndjson files.
    //  We cannot rely on automatic detection of cdmVer based on what is present in the instance
    //  because the instance may contain only cdmVer that is different from the *new* data we are adding.
    //
    //  Also, the cdm version is embedded in this new data that will be indexed.  It is not possible
    //  (without significant effort) to index data already in one cdm version to another [directly].
    if (Object.keys(info['indices']).length == 1) {
      const cdmVer = Object.keys(info['indices'])[0];
      if (!cdm.supportedCdmVersions.includes(cdmVer)) {
        console.log("ERROR: the CDM version found in the documents to be indexed [" + cdmver + "] is not included in the list of supported CDM versions [" + cdm.supportedCdmVersions + "]");
        process.exit(1);
      }
      instance['cdmVer'] = cdmVer;
      if (!Object.keys(instance['indices']).includes(cdmVer)) {
        instance['indices'][cdmVer] = [];
      }
    } else {
      console.log('ERROR: there was not exactly one CDM version found in the data to be indexed:\n');
      console.log(Object.keys(info['indices']));
      console.log('info\n' + JSON.stringify(info['indices'], null, 2));
      process.exit(1);
    }
    for (i = 0; i < info['runIds'].length; i++) {
      console.log('Deleting any existing documents for runId ' + info['runIds'][i]);
      var runId = info['runIds'][i];
      var q = { query: { bool: { filter: [{ term: { 'run.run-uuid': runId } }] } } };
      cdm.deleteDocs(instance, allDocTypes, q);
      var numDocTypes = await cdm.waitForDeletedDocs(instances[instances.length - 1], runId, allDocTypes);
      if (numDocTypes > 0) {
        console.log('Warning: could not delete all documents for ' + docTypes + ' with ' + numAttempts);
        console.log(
          'These documents may continue to be deleted in the background.  To check on the status, run this utility again'
        );
        process.exit(1);
      }
    }
    for (i = 0; i < info['indices'].length; i++) {
        cdm.checkCreateIndex(instance, info['indices'][i]);
    }
    console.log('Indexing documents');
    await processDir(instance, program.dir, 'index');
  } else {
    console.log('You must provide a --dir <directory with ndjsons>');
    process.exit(1);
  }
  console.log('add-run is complete');
}

main();
