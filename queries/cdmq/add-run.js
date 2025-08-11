//# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript

const { XzReadableStream } = require('xz-decompress');
const { Readable } = require('stream');
const cdm = require('./cdm');
const fs = require('fs');
const xz = require('xz');
const readline = require('readline');
const path = require('path');
const program = require('commander');
const instances = []; // opensearch instances

function save_host(host) {
  const host_info = { host: host, header: { 'Content-Type': 'application/json' } };
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

// Read an xz file and decompress to string
async function decompressXzFile(filename) {
  debuglog('decompressXzFile: reading ' + filename);
  // Create a readable stream from file
  const fileStream = Readable.toWeb(fs.createReadStream(filename));

  // Decompress using XzReadableStream
  const decompressedResponse = new Response(new XzReadableStream(fileStream));

  // Get the decompressed content as string
  const text = await decompressedResponse.text();
  return text;
}

async function processDir(instance, dir, docTypes, mode) {
  const jsonArr = [];
  const info = { runIds: {} };
  const allFiles = fs.readdirSync(dir);
  const regExp = /\.ndjson\.xz$/;
  const xzFiles = allFiles.filter((item) => regExp.test(item));
  var docTypeCounts = {};
  if (mode == 'index') {
    for (var x = 0; x < docTypes.length; x++) {
      docTypeCounts[docTypes[x]] = 0;
    }
  }
  for (var i = 0; i < xzFiles.length; i++) {
    const filePath = path.join(program.dir, xzFiles[i]);
    try {
      const decompressedData = await decompressXzFile(filePath);
      const lines = decompressedData.split('\n');
      for (var j = 0; j < lines.length; j++) {
        // TODO: validate JSON syntax and possible validate document schema?
        if (lines[j] != '') {
          jsonArr.push(lines[j]);
        }
        if (mode == 'index' && j % 2 == 0) {
          const indexRegExp = /.+_index\":\s+\"cdm(-){0,1}v\d+dev-([^@]+)/;
          const matches = indexRegExp.exec(lines[j]);
          if (matches) {
            const docType = matches[2];
            docTypeCounts[docType]++;
          }
        }
      }
    } catch (error) {
      console.error('Error processing NDJSON.XZ file:', error);
    }
    cdm.memUsage();

    // After so much data is accumulated or at the last file, index what we have then clear the array.
    if (jsonArr.length > 4000000 || i + 1 == xzFiles.length) {
      console.log(
        mode +
          ': After reading ' +
          (i + 1) +
          ' of ' +
          xzFiles.length +
          ' files, going to process ' +
          jsonArr.length +
          ' lines'
      );

      if (mode == 'index') {
        // The final argument for esJsonArrRequest, 'yearDotMonth', is not
        // required here because the jsonArr includes the exact index name
        // in the 'action' part of the array
        const responses = await esJsonArrRequest(instance, '', '/_bulk', jsonArr);
      } else if (mode == 'getinfo') {
        //const info = { runIds: [], indices: {} };
        for (var k = 1; k < jsonArr.length; k += 2) {
          try {
            var action = JSON.parse(jsonArr[k - 1]);
            var doc = JSON.parse(jsonArr[k]);
          } catch (jsonError) {
            console.log('Could not porse: [' + jsonArr[k] + ']');
            continue;
          }
          let runId = doc['run']['run-uuid'];
          if (!Object.keys(info['runIds']).includes(runId)) {
            info['runIds'][runId] = { indices: {}, yearDotMonth: '' };
          }
          // example action: { "index": { "_index": "cdmv8dev-metric_data" } }
          // exmaple doc:
          // {"cdm":{"ver":"v9dev"},
          //  "run":{"run-uuid":"c0e04edb-ddbc-4081-8bbc-9b9e84e6538d"}}
          if (Object.keys(action).includes('index') && Object.keys(action['index']).includes('_index')) {
            const indexName = action['index']['_index'];
            const regExp = /^cdm-*(v7dev|v8dev|v9dev)-([^@]+)(@\d\d\d\d\.\d\d)*$/;
            const matches = regExp.exec(indexName);
            //console.log("doc:\n" + JSON.stringify(doc, null, 2));
            if (matches) {
              const cdmVer = matches[1];
              let yearDotMonth = '';
              let runId = '';
              if (cdmVer == 'v7dev') {
                runId = doc['run']['id'];
              } else {
                // v8dev and newer use run-uuid
                runId = doc['run']['run-uuid'];
                yearDotMonth = matches[3];
              }
              if (!Object.keys(info['runIds'][runId]).includes('indices')) {
                info['runIds'][runId]['indices'] = {};
              }
              if (!Object.keys(info['runIds'][runId]['indices']).includes(cdmVer)) {
                info['runIds'][runId]['indices'][cdmVer] = [];
              }
              if (!info['runIds'][runId]['indices'][cdmVer].includes(indexName)) {
                debuglog(
                  'going to add indexname to info[runIds][' + runId + '][indices][' + cdmVer + ']: ' + indexName
                );
                info['runIds'][runId]['indices'][cdmVer].push(indexName);
              }
              if (yearDotMonth != '') {
                if (info['runIds'][runId]['yearDotMonth'] == '') {
                  info['runIds'][runId]['yearDotMonth'] = yearDotMonth;
                } else {
                  if (info['runIds'][runId]['yearDotMonth'] != yearDotMonth) {
                    console.log('ERROR: found more than one year and month for same runId [' + runId + ']');
                    process.exit(1);
                  }
                }
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
      } else {
        console.log('mode [' + mode + '] is not supported');
      }
      // Must make array empty here for next pass
      jsonArr.length = 0;
    } // if (jsonArr.length > 100000 || i == xzFiles.length)
  }
  if (mode == 'getinfo') {
    return info;
  }
  if (mode == 'index') {
    return docTypeCounts;
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
  const instance = instances[instances.length - 1];
  if (program.dir) {
    const allDocTypes = ['run', 'iteration', 'sample', 'period', 'param', 'tag', 'metric_desc', 'metric_data'];
    const info = await processDir(instance, program.dir, null, 'getinfo');
    //  The cdmVer going forward must be set based on the data found in the ndjson files.
    //  We cannot rely on automatic detection of cdmVer based on what is present in the instance
    //  because the instance may contain only cdmVer that is different from the *new* data we are adding.
    //
    //  Also, the cdm version is embedded in this new data that will be indexed.  It is not possible
    //  (without significant effort) to index data already in one cdm version to another [directly].
    runIds = Object.keys(info['runIds']);
    for (var runIdx = 0; runIdx < runIds.length; runIdx++) {
      const runId = runIds[runIdx];
      var cdmVer;
      if (Object.keys(info['runIds'][runId]['indices']).length == 1) {
        cdmVer = Object.keys(info['runIds'][runId]['indices'])[0];
        if (!cdm.supportedCdmVersions.includes(cdmVer)) {
          console.log(
            'ERROR: the CDM version found in the documents to be indexed [' +
              cdmVer +
              '] is not included in the list of supported CDM versions [' +
              cdm.supportedCdmVersions +
              ']'
          );
          process.exit(1);
        }
        instance['ver'] = cdmVer;
        //if (!Object.keys(instance['indices']).includes(cdmVer)) {
        //instance['indices'][cdmVer] = [];
        //}
      } else {
        console.log('ERROR: there was not exactly one CDM version found in the data to be indexed:\n');
        console.log(Object.keys(info['indices']));
        console.log('info\n' + JSON.stringify(info['indices'], null, 2));
        process.exit(1);
      }
      // For cdmv9 and newer, any time a document is to be indexed, it is imperitive
      // that a check for the existence of the index is done, and if not found, create
      // the index with the *correct* mappings and settings.  If this is not done, an index
      // may be auto-created with the *incorrect* mappings and settings, and documents can
      // be indexed, but not properly, and subsequent queries will *NOT* work.
      debuglog(JSON.stringify(info['runIds'][runId]['indices'][cdmVer], null, 2));
      for (var i = 0; i < info['runIds'][runId]['indices'][cdmVer].length; i++) {
        debuglog('checking for index ' + info['runIds'][runId]['indices'][cdmVer][i]);
        cdm.checkCreateIndex(instance, info['runIds'][runId]['indices'][cdmVer][i]);
      }
      // Before indexing any documents, we must check for any existing ones.  Having duplicate
      // documents is really bad
      console.log('Deleting any existing documents for runId ' + runId);
      const q = { query: { bool: { filter: [{ term: { 'run.run-uuid': runId } }] } } };
      cdm.deleteDocs(instance, allDocTypes, q, info['runIds'][runId]['yearDotMonth']);
      const numDocTypes = await cdm.waitForDeletedDocs(
        instances[instances.length - 1],
        runId,
        allDocTypes,
        info['runIds'][runId]['yearDotMonth']
      );
      if (numDocTypes > 0) {
        console.log('Warning: could not delete all documents for ' + docTypes + ' with ' + numAttempts);
        console.log(
          'These documents may continue to be deleted in the background.  To check on the status, run this utility again'
        );
        process.exit(1);
      }
      const begin = Date.now() / 1000;
      console.log('Indexing documents');
      const docTypeCounts = await processDir(instance, program.dir, cdm.docTypes[cdmVer], 'index');
      const end = Date.now() / 1000;
      console.log('Time (seconds) to submit all documents for indexing: ' + (end - begin));
      console.log('Waiting for submitted documents to be present in Opensearch');
      cdm.waitForIndexedDocs(instance, runId, docTypeCounts, info['runIds'][runId]['yearDotMonth'])
      console.log('Submitted documents are present in Opensearch');

    }
  } else {
    console.log('You must provide a --dir <directory with ndjsons>');
    process.exit(1);
  }
  console.log('add-run is complete');
}

main();
