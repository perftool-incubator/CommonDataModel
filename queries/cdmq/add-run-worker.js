//# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript

const { XzReadableStream } = require('xz-decompress');
const { Readable } = require('stream');
const cdm = require('./cdm');
const fs = require('fs');

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

module.exports = async ({instance, filePath, docTypes, mode}) => {
  const maxLength = 4000000;
  const jsonArr = [];
  const info = { runIds: {} };
  const regExp = /\.ndjson\.xz$/;
  var docTypeCounts = {};
  if (mode == 'index') {
    for (var x = 0; x < docTypes.length; x++) {
      docTypeCounts[docTypes[x]] = 0;
    }
  }
  try {
    debuglog("Attempting to open [" + filePath + "]");
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


  if (mode == 'index') {
    // The final argument for esJsonArrRequest, 'yearDotMonth', is not
    // required here because the jsonArr includes the exact index name
    // in the 'action' part of the array
    const responses = await esJsonArrRequest(instance, '', '/_bulk', jsonArr);
  } else if (mode == 'getinfo') {
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

  if (mode == 'getinfo') {
    return info;
  }
  if (mode == 'index') {
    return docTypeCounts;
  }
}

