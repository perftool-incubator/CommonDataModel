var cdm = require('./cdm');
var fs = require('fs');
var xz = require('xz');
var readline = require('readline');
var path = require('path');
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

async function readNdjsonXzToString(filePath) {
  return new Promise((resolve, reject) => {
    try {
      const fileStream = fs.createReadStream(filePath);
      const decompressStream = new xz.Decompressor();

      let decompressedString = '';

      fileStream.pipe(decompressStream);

      decompressStream.on('data', (chunk) => {
        decompressedString += chunk.toString('utf8');
        console.log("file: [" + filePath + "] reading chunk");
      });

      decompressStream.on('end', () => {
        console.log("done with [" + filePath + "]");
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

async function processDir(instance, dir) {
  var jsonArr = [];
  var files = fs.readdirSync(dir);

  for (var i = 0; i < files.length; i++) {
    // TODO: only allow .ndjson.xz files
    try {
        const filePath = path.join(program.dir, files[i]);
        const decompressedData = await readNdjsonXzToString(filePath);
        console.log("finished reading file " + filePath);
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
  console.log("finished reading ALL files");
  console.log("processDir(): Going to index " + jsonArr.length/2 + " documents");
  var responses = await esJsonArrRequest(instance, '', '/_bulk', jsonArr);
  console.log("processDir(): responses.length: " + responses.length);
  console.log("processDir(): responses: " + JSON.stringify(responses, null, 2));
  console.log("processDir(): done");
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
    save_host("localhost:9200")
  }

  getInstancesInfo(instances);
  if (program.dir) {
    await processDir(instances[instances.length - 1], program.dir);
  } else {
    console.log("You must provide a --dir <directory with ndjsons>");
    process.exit(1);
  }
  console.log("add-run is complete");
}


main();

