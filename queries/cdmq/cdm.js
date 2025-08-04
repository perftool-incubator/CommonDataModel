//# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript
var request = require('sync-request');
var thenRequest = require('then-request');
var bigQuerySize = 262144;
const docTypes = {
  v7dev: ['run', 'tag', 'iteration', 'param', 'sample', 'period', 'metric_desc', 'metric_data'],
  v8dev: ['run', 'tag', 'iteration', 'param', 'sample', 'period', 'metric_desc', 'metric_data'],
  v9dev: ['run', 'tag', 'iteration', 'param', 'sample', 'period', 'metric_desc', 'metric_data', 'metric_def']
};
const supportedCdmVersions = Object.keys(docTypes);
exports.supportedCdmVersions = supportedCdmVersions;
const debugOut = 0;
const indexSettings = {
  number_of_shards: 1,
  number_of_replicas: 1, // even on clusters?
  max_result_window: 262144,
  max_terms_count: 262144,
  codec: 'best_compression',
  refresh_interval: '5s'
};

// Index definitions

var indexDefs = { v7dev: {}, v8dev: {}, v9dev: {} };

// Most index mappings inherit mappings from other indices.  Copies of these indices
// are done with JSON.parse(JSON.stringify(src_index)) to facilitate deep copies.

// run_micro is a smaller version of the run mapping, used only for metric_data doc
// There is no actual run_micro index
indexDefs['v8dev']['run_micro'] = {
  settings: indexSettings,
  mappings: {
    dynamic: 'strict',
    properties: {
      cdm: {
        properties: {
          ver: { type: 'keyword' },
          doctype: { type: 'keyword' }
        }
      },
      run: {
        properties: {
          'run-uuid': { type: 'keyword' }
        }
      }
    }
  }
};
indexDefs['v9dev']['run_micro'] = JSON.parse(JSON.stringify(indexDefs['v8dev']['run_micro']));

// run is used for all mappings except metric_data
indexDefs['v8dev']['run'] = {
  settings: {
    number_of_shards: 1,
    number_of_replicas: 1,
    max_result_window: 262144,
    max_terms_count: 262144,
    codec: 'best_compression',
    refresh_interval: '5s'
  },
  mappings: {
    dynamic: 'strict',
    properties: {
      cdm: {
        properties: {
          ver: { type: 'keyword' },
          doctype: { type: 'keyword' }
        }
      },
      run: {
        properties: {
          'run-uuid': { type: 'keyword' },
          begin: { type: 'date' },
          end: { type: 'date' },
          harness: { type: 'keyword' },
          benchmark: { type: 'keyword' },
          host: { type: 'keyword' },
          email: { type: 'keyword' },
          name: { type: 'keyword' },
          desc: { type: 'text', analyzer: 'standard' },
          tags: { type: 'text', analyzer: 'whitespace', fields: { raw: { type: 'keyword' } } },
          source: { type: 'keyword' }
        }
      }
    }
  }
};
indexDefs['v9dev']['run'] = JSON.parse(JSON.stringify(indexDefs['v8dev']['run']));

// both tag and iteration start with the run mapping
indexDefs['v8dev']['tag'] = JSON.parse(JSON.stringify(indexDefs['v8dev']['run']));
indexDefs['v8dev']['tag']['mappings']['properties']['tag'] = {
  properties: {
    'tag-uuid': { type: 'keyword' },
    name: { type: 'keyword' },
    val: { type: 'keyword' }
  }
};
indexDefs['v9dev']['tag'] = JSON.parse(JSON.stringify(indexDefs['v8dev']['tag']));

indexDefs['v8dev']['iteration'] = JSON.parse(JSON.stringify(indexDefs['v8dev']['run']));
indexDefs['v8dev']['iteration']['mappings']['properties']['iteration'] = {
  properties: {
    'iteration-uuid': { type: 'keyword' },
    num: { type: 'unsigned_long' },
    status: { type: 'keyword' },
    path: { type: 'keyword' },
    'primary-metric': { type: 'keyword' },
    'primary-period': { type: 'keyword' }
  }
};
indexDefs['v9dev']['iteration'] = JSON.parse(JSON.stringify(indexDefs['v8dev']['iteration']));

// param and sample mappings start with the iteration mapping (which includes the run mapping)
indexDefs['v8dev']['param'] = JSON.parse(JSON.stringify(indexDefs['v8dev']['iteration']));
indexDefs['v8dev']['param']['mappings']['properties']['param'] = {
  properties: {
    'param-uuid': { type: 'keyword' },
    id: { type: 'keyword' },
    arg: { type: 'keyword' },
    role: { type: 'keyword' },
    val: { type: 'keyword' }
  }
};
indexDefs['v9dev']['param'] = JSON.parse(JSON.stringify(indexDefs['v8dev']['param']));

indexDefs['v8dev']['sample'] = JSON.parse(JSON.stringify(indexDefs['v8dev']['iteration']));
indexDefs['v8dev']['sample']['mappings']['properties']['sample'] = {
  properties: {
    'sample-uuid': { type: 'keyword' },
    num: { type: 'unsigned_long' },
    status: { type: 'keyword' },
    path: { type: 'keyword' }
  }
};
indexDefs['v9dev']['sample'] = JSON.parse(JSON.stringify(indexDefs['v8dev']['sample']));

// period mapping starts with the sample mapping (which has iteration, which has run)
indexDefs['v8dev']['period'] = JSON.parse(JSON.stringify(indexDefs['v8dev']['sample']));
indexDefs['v8dev']['period']['mappings']['properties']['period'] = {
  properties: {
    'period-uuid': { type: 'keyword' },
    begin: { type: 'date' },
    end: { type: 'date' },
    name: { type: 'keyword' },
    prev_id: { type: 'keyword' }
  }
};
indexDefs['v9dev']['period'] = JSON.parse(JSON.stringify(indexDefs['v8dev']['period']));

// metric_desc mapping starts with period mapping (which has sample, which has iteration, which has run)
indexDefs['v8dev']['metric_desc'] = JSON.parse(JSON.stringify(indexDefs['v8dev']['period']));
indexDefs['v8dev']['metric_desc']['mappings']['properties']['metric_desc'] = {
  properties: {
    'metric_desc-uuid': { type: 'keyword' },
    //"aggregation-method": { "type": "keyword" }, to be used for cdmv9
    class: { type: 'keyword' },
    type: { type: 'keyword' },
    source: { type: 'keyword' },
    'names-list': { type: 'keyword' },
    names: {
      properties: {
        'tool-name': { type: 'keyword' },
        'benchmark-name': { type: 'keyword' },
        'benchmark-group': { type: 'keyword' },
        'benchmark-id': { type: 'keyword' },
        'benchmark-role': { type: 'keyword' },
        hostname: { type: 'keyword' },
        'engine-type': { type: 'keyword' },
        'engine-role': { type: 'keyword' },
        'engine-id': { type: 'keyword' },
        userenv: { type: 'keyword' },
        osruntime: { type: 'keyword' },
        'endpoint-label': { type: 'keyword' },
        'hypervisor-host': { type: 'keyword' },
        'hosted-by': { type: 'keyword' },
        'interface-type': { type: 'keyword' },
        bridge: { type: 'keyword' },
        interface: { type: 'keyword' },
        id: { type: 'keyword' },
        num: { type: 'double' },
        class: { type: 'keyword' },
        type: { type: 'keyword' },
        host: { type: 'keyword' },
        role: { type: 'keyword' },
        dev: { type: 'keyword' },
        cmd: { type: 'keyword' },
        tid: { type: 'keyword' },
        pid: { type: 'keyword' },
        job: { type: 'keyword' },
        group: { type: 'keyword' },
        tier: { type: 'keyword' },
        level: { type: 'keyword' },
        package: { type: 'keyword' },
        die: { type: 'keyword' },
        core: { type: 'keyword' },
        thread: { type: 'keyword' },
        kthread: { type: 'keyword' },
        node: { type: 'keyword' },
        mode: { type: 'keyword' },
        socket: { type: 'keyword' },
        domain: { type: 'keyword' },
        cluster: { type: 'keyword' },
        container: { type: 'keyword' },
        cgroup: { type: 'keyword' },
        parent: { type: 'keyword' },
        source: { type: 'keyword' },
        controller: { type: 'keyword' },
        pod: { type: 'keyword' },
        port: { type: 'keyword' },
        tx_port: { type: 'keyword' },
        rx_port: { type: 'keyword' },
        port_pair: { type: 'keyword' },
        status: { type: 'keyword' },
        error: { type: 'keyword' },
        stream: { type: 'keyword' },
        direction: { type: 'keyword' },
        clientserver: { type: 'keyword' },
        protocol: { type: 'keyword' },
        action: { type: 'keyword' },
        cpu: { type: 'keyword' },
        irq: { type: 'keyword' },
        desc: { type: 'keyword' },
        counter: { type: 'keyword' },
        cstype: { type: 'keyword' },
        csid: { type: 'keyword' },
        cookie: { type: 'keyword' },
        table: { type: 'keyword' },
        priority: { type: 'keyword' },
        reg14: { type: 'keyword' },
        metadata: { type: 'keyword' },
        dp: { type: 'keyword' },
        flags: { type: 'keyword' },
        recirc_id: { type: 'keyword' },
        dp_hash: { type: 'keyword' },
        skb_priority: { type: 'keyword' },
        skb_mark: { type: 'keyword' },
        ct_state: { type: 'keyword' },
        ct_zone: { type: 'keyword' },
        ct_mark: { type: 'keyword' },
        ct_label: { type: 'keyword' },
        icmp_type: { type: 'keyword' },
        icmp_code: { type: 'keyword' },
        in_port: { type: 'keyword' },
        ipv4_src: { type: 'keyword' },
        ipv4_dst: { type: 'keyword' },
        ipv4_proto: { type: 'keyword' },
        ipv4_frag: { type: 'keyword' },
        udp_src: { type: 'keyword' },
        udp_dst: { type: 'keyword' },
        tcp_src: { type: 'keyword' },
        tcp_dst: { type: 'keyword' },
        eth_src: { type: 'keyword' },
        eth_dst: { type: 'keyword' },
        eth_type: { type: 'keyword' },
        vlan: { type: 'keyword' },
        dl_dst: { type: 'keyword' },
        dl_src: { type: 'keyword' },
        dl_vlan: { type: 'keyword' },
        ipv6_src: { type: 'keyword' },
        ipv6_dst: { type: 'keyword' },
        actions: { type: 'keyword' },
        ufid: { type: 'keyword' },
        src: { type: 'keyword' },
        dst: { type: 'keyword' },
        sport: { type: 'keyword' },
        dport: { type: 'keyword' },
        mark: { type: 'keyword' },
        output: { type: 'keyword' },
        use: { type: 'keyword' },
        step: { type: 'keyword' },
        epoch: { type: 'keyword' },
        batch: { type: 'keyword' },
        slot: { type: 'keyword' },
        blade: { type: 'keyword' },
        rank: { type: 'keyword' }
      }
    },
    'value-format': { type: 'keyword' },
    values: {
      properties: {
        pass: { type: 'keyword' },
        fail: { type: 'keyword' }
      }
    }
  }
};
indexDefs['v9dev']['metric_desc'] = JSON.parse(JSON.stringify(indexDefs['v8dev']['metric_desc']));

// TODO: add new names for cdmv9

// ONLY for cdmv9 and newer: A metric_def mapping provides a definition of specific breakout.
// Documents for this index are optional but encouraged.  Tools and benchmarks define these documents
// when post-processing their collected data.
//
// For example, for mpstat:
// <doc1>
// mestric_desc-uuid:  same uuid in metric_desc doc that has source: mpstat, type: Busy-CPU
// name: cpu
// definition: A logical CPU ID, as found in lscpu, /proc/cpu, and /sys/devices/system/cpu/cpu*
//
// <doc2>
// mestric_desc-uuid:  same uuid in metric_desc doc that has source: mpstat, type: Busy-CPU
// name: package
// definition: The ID of a physical grouping of CPU cores on a single chip.  Often the same as a NUMA node ID.
indexDefs['v9dev']['metric_def'] = JSON.parse(JSON.stringify(indexDefs['v9dev']['period']));
indexDefs['v9dev']['metric_def']['mappings']['properties']['metric_def'] = {
  properties: {
    'metric_desc-uuid': { type: 'keyword' },
    name: { type: 'keyword' },
    definition: { type: 'text' }
  }
};

indexDefs['v8dev']['metric_data'] = JSON.parse(JSON.stringify(indexDefs['v8dev']['run_micro']));
indexDefs['v8dev']['metric_data']['mappings']['properties']['metric_desc'] = {
  properties: {
    'metric_desc-uuid': { type: 'keyword' }
  }
};
indexDefs['v8dev']['metric_data']['mappings']['properties']['metric_data'] = {
  properties: {
    value: { type: 'double' },
    begin: { type: 'date' },
    end: { type: 'date' },
    duration: { type: 'long' }
  }
};
indexDefs['v9dev']['metric_data'] = JSON.parse(JSON.stringify(indexDefs['v8dev']['metric_data']));

function memUsage() {
  const memUsage = process.memoryUsage();
  debuglog({
    rss: `${Math.round(memUsage.rss / 1024 / 1024)} MB`, // Resident Set Size
    heapTotal: `${Math.round(memUsage.heapTotal / 1024 / 1024)} MB`, // Total heap size
    heapUsed: `${Math.round(memUsage.heapUsed / 1024 / 1024)} MB`, // Used heap size
    external: `${Math.round(memUsage.external / 1024 / 1024)} MB` // External memory
  });
}
exports.memUsage = memUsage;

function numMBytes(a, str) {
  var totalBytes = 0;
  a.forEach((element) => {
    totalBytes += JSON.stringify(element).length;
  });
  var mb = totalBytes / 1024 / 1024;
  return mb;
}

function getCdmVer(instance) {
  return instance['ver'];
}

debuglog = function (str) {
  if (debugOut != 0) {
    console.log(str);
  }
};
exports.debuglog = debuglog;

getCdmVerFromIndex = function (index) {
  var regExp = /^cdm-*v([\d+])dev-(.+)/;
  var matches = regExp.exec(index);
  if (matches) {
    cdmVer = 'v' + matches[1] + 'dev';
  } else {
    console.log('ERROR: index [' + index + '] is not recognized');
    process.exit(1);
  }
  return cdmVer;
};

// cdmv9 adds indices dynamically based on year and month, so
// we need a way to check for existence of the index and create
// if not found.  Since this moves toward a auto-create index
// policy, might as well do the same for earlier CDM versions.
// This will eliminate the need for other projects (like Crucible)
// to maintain the indices.
checkCreateIndex = function (instance, index) {
  const cdmVer = getCdmVerFromIndex(index);
  //if (!Object.keys(instance).includes('indices')) {
  //instance['indices'] = {};
  //}

  //debuglog('instance:\n' + JSON.stringify(instance, null, 2));
  //debuglog('index: ' + index);
  if (Object.keys(instance['indices']).includes(cdmVer)) {
    //debuglog('found cdmver: ' + cdmVer);
    if (instance['indices'][cdmVer].includes(index)) {
      //debuglog('found index: ' + index);
      return;
    }
  }

  var regExp = /\*$/;
  var matches = regExp.exec(index);
  if (matches) {
    //debuglog('Not going to create index because it includes a wildcard: [' + index + ']');
    return;
  }

  const docType = getDocType(index);
  debuglog('checkCreateIndex(): got docType: [' + docType + ']');
  var url = 'http://' + instance['host'] + '/' + index;
  var resp = request('PUT', url, { headers: instance['header'], body: JSON.stringify(indexDefs[cdmVer][docType]) });
  var data = JSON.parse(resp.getBody());
  debuglog('response:::\n' + JSON.stringify(data, null, 2));
  console.log("instance:\n" + JSON.stringify(instance, null, 2));
  if (!Object.keys(instance['indices']).includes(cdmVer)) {
    instance['indices'][cdmVer] = [];
  }
  instance['indices'][cdmVer].push(index);
  //TODO: query opensearch to verify index is present
  return;
};
exports.checkCreateIndex = checkCreateIndex;

function getDocType(index) {
  const cdmVer = getCdmVerFromIndex(index);

  //debuglog('cdmver: [' + cdmVer + ']');
  if (cdmVer == 'v7dev' || cdmVer == 'v8dev') {
    var regExp = /^cdmv[7|8]dev-(.+)/;
    var matches = regExp.exec(index);
    if (matches) {
      docType = matches[1];
      if (docTypes[cdmVer].includes(docType)) {
        return docType;
      } else {
        console.log('ERROR: index [' + index + '] does not match a docType: ' + docTypes[cdmVer]);
        process.exit(1);
      }
    } else {
      console.log('ERROR: index name [' + index + '] does not match cdmv7/8 format');
      process.exit(1);
    }
  }

  if (cdmVer == 'v9dev') {
    var regExp = /^cdm-v9dev-([^@]+)(@\d\d\d\d\.\d\d|\*)/;
    var matches = regExp.exec(index);
    if (matches) {
      docType = matches[1];
      if (docTypes[cdmVer].includes(docType)) {
        return docType;
      } else {
        console.log('ERROR: index [' + index + '] does not match a docType: ' + docTypes[cdmVer]);
        process.exit(1);
      }
    } else {
      console.log('ERROR: index name [' + index + '] does not match cdmv9 format');
      process.exit(1);
    }
  }

  console.log('ERROR: the cdmVer provided [' + cdmVer + '] is not supported');
  process.exit(1);
}

function getIndexBaseName(instance) {
  // CDM version support is effectively determined here
  cdmVer = getCdmVer(instance);
  //debuglog('cdmver: [' + cdmVer + ']');
  if (cdmVer == 'v7dev' || cdmVer == 'v8dev') {
    return 'cdm' + cdmVer + '-';
  } else if (cdmVer == 'v9dev') {
    // v9dev adds a '-' after 'cdm' because of a [lab admin] naming convention
    // used for shared opensearch.  Therefore, you will find that v7dev
    // and v8dev cannot be used for some [lab managed] opensearch instances with
    // same naming requirement.
    return 'cdm-v9dev-';
  } else {
    console.log('CDM version [' + instance['ver'] + '] is not supported, exiting');
  }
}

function getIndexName(docType, instance, yearDotMonth) {
  const baseName = getIndexBaseName(instance);
  cdmVer = getCdmVer(instance);
  if (cdmVer == 'v7dev' || cdmVer == 'v8dev') {
    name = docType;
  } else {
    name = docType + yearDotMonth;
  }
  fullName = baseName + name;
  //debuglog('getIndexName() fullName: [' + fullName + ']');
  // necessary?
  checkCreateIndex(instance, fullName);
  return fullName;
}

// Return subtraction of two 1-dimensional arrays
subtractTwoArrays = function (a1, a2) {
  const a3 = [];
  a1.forEach((element) => {
    if (!a2.includes(element)) {
      a3.push(element);
    }
  });
  return a3;
};
exports.subtractTwoArrays = subtractTwoArrays;

// Return consolidation (non-repeated values) from value for key 'k' found in array of objects
function getObjVals(a, k) {
  const c = [];
  a.forEach((b) => {
    if (typeof b[k] !== 'undefined' && !c.includes(b[k])) c.push(b[k]);
  });
  return c;
}

// Return consolidation (non-repeated values) of a 2-dimensional array
consolidateAllArrays = function (a) {
  const c = [];
  a.forEach((b) => {
    b.forEach((e) => {
      if (!c.includes(e)) c.push(e);
    });
  });
  return c;
};
exports.consolidateAllArrays = consolidateAllArrays;

// Return intersection of two 1-dimensional arrays
intersectTwoArrays = function (a1, a2) {
  const a3 = [];
  a1.forEach((element) => {
    if (!a2.includes(element)) {
      return;
    }
    a3.push(element);
  });
  return a3;
};
exports.intersectTwoArrays = intersectTwoArrays;

// Return intersection of many 1-dimensional arrays found in 2-dimensional array
intersectAllArrays = function (a2D) {
  var intersectArray = a2D[0];
  a2D.forEach((a1D) => {
    intersectArray = intersectTwoArrays(intersectArray, a1D);
  });
  return intersectArray;
};
exports.intersectAllArrays = intersectAllArrays;

async function fetchBatchedData(instance, reqs, batchSize = 16) {
  //debuglog('fetchBatchedData() begin');
  //debuglog('fetchBatchedData() reqs.length: ' + reqs.length);
  const responses = [];
  const batches = [];

  for (let i = 0; i < reqs.length; i += batchSize) {
    batches.push(reqs.slice(i, i + batchSize));
  }

  for (const batch of batches) {
    //debuglog('fetchBatchedData() processing batch');
    const promises = batch.map(async (req) => {
      try {
        // thenRequest will abolutely *not* work unless this header is converted to string and back
        const headerStr = JSON.stringify(instance['header']);
        const hdrs = JSON.parse(headerStr);
        //debuglog('fetchBatchedData() calling thenRequest()');
        const response = await thenRequest('POST', req.url, { body: req.body, headers: hdrs });
        //debuglog('fetchBatchedData() returned from thenRequest()');
        if (response.statusCode >= 200 && response.statusCode < 300) {
          try {
            //debuglog('fetchBatchedData() about to return with JSON.parse');
            return JSON.parse(response.getBody('utf8')); // Attempt JSON parsing
          } catch (jsonError) {
            //debuglog('fetchBatchedData() about to return with JSON(no-parse)');
            return response.getBody('utf8'); // return text if JSON parsing fails
          }
        } else {
          throw new Error(`HTTP error! status: ${response.statusCode}`);
        }
      } catch (error) {
        console.error(`Error fetching ${req}:`, error);
        return null; // Or handle the error as needed
      }
    });

    const batchResults = await Promise.all(promises);
    //debuglog('fetchBatchedData() batchResults.length:' + batchResults.length);
    for (const batchResult of batchResults) {
      const keys = Object.keys(batchResult);
      // items present if this is document creation
      if (keys.includes('items')) {
        responses.push(...batchResult.items);
      } else {
        // responses present if this is a query (_msearch)
        if (keys.includes('responses')) {
          responses.push(...batchResult.responses);
        }
      }
      //debuglog('fetchBatchedData() responses.length so far:' + responses.length);
      //debuglog('fetchBatchedData() responses[-1]:\n' + JSON.stringify(responses[responses.length - 1], null, 2));
    }
  }

  //debuglog('fetchBatchedData() responses.length:' + responses.length);
  return responses;
}

esJsonArrRequest = async function (instance, docType, action, jsonArr, yearDotMonth) {
  //debuglog('esJsonArrRequest begin: yearDotMonth: ' + yearDotMonth);
  debuglog('esJsonArrRequest jsonArr.length: ' + jsonArr.length);
  memUsage();
  debuglog('jsonArr MB at beginning of esJsonArrRequest: ' + numMBytes(jsonArr));

  //if (jsonArr.length > 500) {
  //process.exit(1);
  //}

  var thisJson = '';
  var url = '';
  if (docType == '') {
    // Expect to have the Index and action in the jsonArr itself
    url = 'http://' + instance['host'] + '/_bulk';
  } else {
    url = 'http://' + instance['host'] + '/' + getIndexName(docType, instance, yearDotMonth) + action;
  }
  var max = 16384;
  var idx = 0;
  var req_count = 0;
  var q_count = 0;
  var ndjson = '';
  var reqs = [];
  var theseResps = [];
  var allResponses = [];
  // Process queries in chunks no larger than 'max' chars
  while (idx < jsonArr.length) {
    while (reqs.length < 64 && idx < jsonArr.length) {
      // Limit size in order to not exhaust heap
      // Add the first request (2 lines) even if it exceeds our limit (we don't really have a choice)
      // The limit we have is likely much lower than what can be handled, but if this becomes a
      // problem, we'll have to look at either an alternate way to submit a huge request, or we will
      // have to break up the request into mulitple requests with fewer metric_id's, then sum the
      // responses.
      q_count++;
      ndjson += jsonArr[idx] + '\n' + jsonArr[idx + 1] + '\n';
      idx += 2;
      // Add more requests if are any left and the max will not be exceeded
      if (idx + 2 < jsonArr.length && ndjson.length + jsonArr[idx].length + jsonArr[idx + 1].length < max) {
        q_count++;
        ndjson += jsonArr[idx] + '\n' + jsonArr[idx + 1] + '\n';
        // Remove data from jsonArr to conserve memory (should be cleaned up by GC)
        delete jsonArr[idx];
        delete jsonArr[idx + 1];
        idx += 2;
      } else {
        req_count++;
        q_count = 0;
        const req = { url: url, body: ndjson };
        reqs.push(req);
        ndjson = '';
      }
    } //while
    // Max was not exceeded but there are some requests that have not been submitted
    if (ndjson != '') {
      req_count++;
      q_count = 0;
      const req = { url: url, body: ndjson };
      reqs.push(req);
    }

    debuglog('esJsonArrRequest reqs.length:\n' + reqs.length);
    var responses = await fetchBatchedData(instance, reqs);
    reqs = [];

    debuglog('esJsonArrRequest jsonArr MB: ' + numMBytes(jsonArr));
    debuglog('esJsonArrRequest responses MB: ' + numMBytes(responses));
    memUsage();

    allResponses.push(...responses);
    responses = [];

    debuglog('esJsonArrRequest responses MB after clearing: ' + numMBytes(responses));
    memUsage();
  }

  debuglog('iesJsonArrRequest allResponses ' + numMBytes(allResponses));

  debuglog('esJsonArrRequest end');
  return allResponses;
};
exports.esJsonArrRequest = esJsonArrRequest;

function esRequest(instance, docType, action, q, yearDotMonth) {
  // This is the only http request remainig that still uses a sync-request
  var url = 'http://' + instance['host'] + '/' + getIndexName(docType, instance, yearDotMonth) + action;
  debuglog('esRequest() url: ' + url);
  // The var q can be an object or a string.  If you are submitting NDJSON
  // for a _msearch, it must be a [multi-line] string.
  if (typeof q === 'object') {
    q = JSON.stringify(q);
  }
  var resp = request('POST', url, {
    body: q,
    headers: instance['header']
  });
  return resp;
}

// mSearch: take a several serach requests and create a ES _msearch
// mSearch should be used whenever possible, instead of requesting
// many single search requests separately.  Significant performance
// improvements are generally possible when reducing the actual number
// of http requests.
// Note: termKeys is a 1D array, while values is a 2D array.
// termKeys[x] uses list of values from values[x]
mSearch = async function (instance, index, yearDotMonth, termKeys, values, source, aggs, size, sort) {
  if (typeof termKeys !== typeof []) return;
  if (typeof values !== typeof []) return;
  var jsonArr = [];
  for (var i = 0; i < values[0].length; i++) {
    var req = { query: { bool: { filter: [] } } };
    if (source !== '' && source !== null) {
      req._source = source;
    }
    for (var x = 0; x < termKeys.length; x++) {
      var termStr = '{ "term": { "' + termKeys[x] + '": "' + values[x][i] + '"}}';
      req['query']['bool']['filter'].push(JSON.parse(termStr));

      if (typeof size !== 'undefined') {
        req.size = size;
      } else {
        req.size = bigQuerySize;
      }

      if (typeof sort !== 'undefined') req.sort = sort;
    }
    // aggs is not an array, and is used the same for all queries
    if (aggs !== null) {
      req['aggs'] = aggs;
    }
    jsonArr.push('{}');
    jsonArr.push(JSON.stringify(req));
  }
  debuglog('mSearch(): calling esJsonArrRequest()');
  var responses = await esJsonArrRequest(instance, index, '/_msearch', jsonArr, yearDotMonth);
  //debuglog('mSearch(): returned from calling esJsonArrRequest(), responses:\n' + JSON.stringify(responses, null, 2));
  memUsage();

  // Unpack response and organize in array of arrays
  var retData = [];
  for (var i = 0; i < responses.length; i++) {
    // For queries with aggregation
    if (typeof responses[i].aggregations !== 'undefined' && Array.isArray(responses[i].aggregations.source.buckets)) {
      if (responses[i].aggregations.source.sum_other_doc_count > 0) {
        console.log(
          'WARNING! msearch aggregation returned sum_other_doc_count > 0, which means not all terms were returned.  This query needs a larger "size"'
        );
      }
      // Assemble the keys from the bucket for this query (i)
      var keys = [];
      responses[i].aggregations.source.buckets.forEach((element) => {
        keys.push(element.key);
      });
      retData[i] = keys;

      // For queries without aggregation
    } else {
      debuglog('hits:\n' + JSON.stringify(responses[i], null, 2));
      if (responses[i].hits == null) {
        console.log('WARNING! msearch returned data.responses[' + i + '].hits is NULL');
        console.log(JSON.stringify(responses[i], null, 2));
        return;
      }
      if (Array.isArray(responses[i].hits.hits) && responses[i].hits.hits.length > 0) {
        if (
          responses[i].hits.total.value !== responses[i].hits.hits.length &&
          req.size != responses[i].hits.hits.length
        ) {
          console.log(
            'WARNING! msearch(size: ' +
              size +
              ') responses[' +
              i +
              '].hits.total.value (' +
              responses[i].hits.total.value +
              ') and responses[' +
              i +
              '].hits.hits.length (' +
              responses[i].hits.hits.length +
              ') are not equal, which means the retured data is probably incomplete'
          );
        }
        var ids = [];
        responses[i].hits.hits.forEach((element) => {
          // A source of "x.y" <string> must be converted to reference the object
          // For example, a source (string) of "metric_desc.metric_desc-uuid" needs to reference metric_desc[id]
          var obj = element._source;
          if (source !== '' && source !== null) {
            // a blank source assumes you want everything returned
            source.split('.').forEach((thisObj) => {
              if (typeof obj[thisObj] == 'undefined') {
                console.log(
                  'WARNING: the requested source for this query [' + source + '] does not exist in the returned data:\n'
                );
                console.log(JSON.stringify(obj.null, 2));
                return;
              }
              obj = obj[thisObj];
            });
          }
          ids.push(obj);
        });
        retData[i] = ids;
      } else {
        retData[i] = [];
      }
    }
  }
  debuglog('mSearch(): about to return');
  return retData;
};
exports.mSearch = mSearch;

// Functions starting with mget use msearch, and require 1D array of values and return a 1D array or results
// Functions starting with get are just for legacy support, where caller expects to provide a single value,
// but these functions just wrap the value in 2D array for msearch (and a key in a 1D array).  Effectively
// all query functions use msearch, even if there is a single query.

mgetPrimaryMetric = async function (instance, iterations, yearDotMonth) {
  var metrics = await mSearch(
    instance,
    'iteration',
    yearDotMonth,
    ['iteration.iteration-uuid'],
    [iterations],
    'iteration.primary-metric'
  );
  // mSearch returns a list of values for each query, so 2D array.  We only have exactly 1 primary-metric
  // for each iteration, so collapse the 2D array into a 1D array, 1 element per iteration.
  var primaryMetrics = [];
  for (var i = 0; i < metrics.length; i++) {
    primaryMetrics[i] = metrics[i][0];
  }
  return primaryMetrics;
};
exports.mgetPrimaryMetric = mgetPrimaryMetric;

getPrimaryMetric = async function (instance, iteration, yearDotMonth) {
  var primaryMetrics = await mgetPrimaryMetric(instance, [iteration], yearDotMonth);
  return primaryMetrics[0][0];
};
exports.getPrimaryMetric = getPrimaryMetric;

mgetPrimaryPeriodName = async function (instance, iterations, yearDotMonth) {
  var data = await mSearch(
    instance,
    'iteration',
    yearDotMonth,
    ['iteration.iteration-uuid'],
    [iterations],
    'iteration.primary-period'
  );
  // There can be only 1 period-name er iteration, therefore no need for a period name per period [of the same iteration]
  // Therefore, we do not need to return a 2D array
  var periodNames = [];
  for (var i = 0; i < data.length; i++) {
    periodNames[i] = data[i][0];
  }
  return periodNames;
};
exports.mgetPrimaryPeriodName = mgetPrimaryPeriodName;

getPrimaryPeriodName = async function (instance, iteration, yearDotMonth) {
  var primaryPeriodNames = await mgetPrimaryPeriodName(instance, [iteration], yearDotMonth);
  return primaryPeriodNames[0][0];
};
exports.getPrimaryPeriodName = getPrimaryPeriodName;

mgetSamples = async function (instance, iters, yearDotMonth) {
  return await mSearch(
    instance,
    'sample',
    yearDotMonth,
    ['iteration.iteration-uuid'],
    [iters],
    'sample.sample-uuid',
    null,
    1000,
    [{ 'sample.num': { order: 'asc', numeric_type: 'long' } }]
  );
};
exports.mgetSamples = mgetSamples;

getSamples = async function (instance, iter, yearDotMonth) {
  var samples = await mgetSamples(instance, [iter], yearDotMonth);
  return samples[0];
};
exports.getSamples = getSamples;

// For a specific metric-source and metric-type,
// find all the metadata names shared among all
// found metric docs.  These names are what can be
// used for "breakouts".
mgetMetricNames = async function (instance, runIds, sources, types, yearDotMonth) {
  return await mSearch(
    instance,
    'metric_desc',
    yearDotMonth,
    ['run.run-uuid', 'metric_desc.source', 'metric_desc.type'],
    [runIds, sources, types],
    '',
    { source: { terms: { field: 'metric_desc.names-list', size: bigQuerySize } } }
  );
};
exports.mgetMetricNames = mgetMetricNames;

getMetricNames = async function (instance, runId, source, type, yearDotMonth) {
  var metricNames = await mgetMetricNames(instance, [runId], [source], [type], yearDotMonth);
  return metricNames[0];
};

mgetSampleNums = async function (instance, Ids, yearDotMonth) {
  var sampleIds = [];
  var idx = 0;
  for (var i = 0; i < Ids.length; i++) {
    for (j = 0; j < Ids[i].length; j++) {
      sampleIds[idx] = Ids[i][j];
      idx++;
    }
  }

  var data = await mSearch(
    instance,
    'sample',
    yearDotMonth,
    ['sample.sample-uuid'],
    [sampleIds],
    'sample.num',
    null,
    1
  );
  var sampleNums = []; // Will be 2D array of [iter][sampIds];
  idx = 0;
  for (var i = 0; i < Ids.length; i++) {
    for (j = 0; j < Ids[i].length; j++) {
      if (typeof sampleNums[i] == 'undefined') {
        sampleNums[i] = [];
      }
      sampleNums[i][j] = data[idx][0];
      idx++;
    }
  }
  return sampleNums;
};
exports.mgetSampleNums = mgetSampleNums;

getSampleNum = async function (instance, sampId, yearDotMonth) {
  var sampleNum = await mgetSampleNums(instance, [sampId], yearDotMonth);
  return sampleNums[0][0];
};
exports.getSampleNum = getSampleNum;

mgetSampleStatuses = async function (instance, Ids, yearDotMonth) {
  var sampleIds = [];
  var idx = 0;
  for (var i = 0; i < Ids.length; i++) {
    for (j = 0; j < Ids[i].length; j++) {
      sampleIds[idx] = Ids[i][j];
      idx++;
    }
  }

  var data = await mSearch(
    instance,
    'sample',
    yearDotMonth,
    ['sample.sample-uuid'],
    [sampleIds],
    'sample.status',
    null,
    1
  );
  var sampleStatus = []; // Will be 2D array of [iter][sampIds];
  idx = 0;
  for (var i = 0; i < Ids.length; i++) {
    for (j = 0; j < Ids[i].length; j++) {
      if (typeof sampleStatus[i] == 'undefined') {
        sampleStatus[i] = [];
      }
      sampleStatus[i][j] = data[idx][0];
      idx++;
    }
  }
  return sampleStatus;
};
exports.mgetSampleStatuses = mgetSampleStatuses;

getSampleStatus = async function (instance, sampId) {
  var sampleStatuses = await mgetSampleStatuses(instance, [sampId]);
  return sampleStatuses[0][0];
};
exports.getSampleStatus = getSampleStatus;

mgetPrimaryPeriodId = async function (instance, sampIds, periNames, yearDotMonth) {
  // needs 2D array iterSampleIds: [iter][samp] and 1D array iterPrimaryPeriodNames [iter]
  // returns 2D array [iter][samp]
  if (periNames.length == 1) {
    // Only 1 primary-period-name provided, so assume all sample IDs have same primary-period-name
    for (i = 1; i < sampIds.length; i++) periNames[i] = periNames[0];
  }
  // Need to convert to 1D array for sampleIds, with 1 periName for each, in order to call mSearch()
  var sampleIds = [];
  var perSamplePeriNames = [];
  var idx = 0;
  for (var i = 0; i < sampIds.length; i++) {
    for (j = 0; j < sampIds[i].length; j++) {
      sampleIds[idx] = sampIds[i][j];
      perSamplePeriNames[idx] = periNames[i];
      idx++;
    }
  }
  var data = await mSearch(
    instance,
    'period',
    yearDotMonth,
    ['sample.sample-uuid', 'period.name'],
    [sampleIds, perSamplePeriNames],
    'period.period-uuid',
    null,
    1
  );
  // mSearch returns a 2D array, in other words, a list of values (inner array) for each query (outer array)
  // In this case, the queries are 1 per sampleId/periodName (for all iterations ordered), and the list of values
  // happens to be exactly 1 value, the primaryPeriodId.
  var periodIds = []; // Will be 2D array of [iter][periIds];
  idx = 0;
  for (var i = 0; i < sampIds.length; i++) {
    for (j = 0; j < sampIds[i].length; j++) {
      if (typeof periodIds[i] == 'undefined') {
        periodIds[i] = [];
      }
      periodIds[i][j] = data[idx][0];
      idx++;
    }
  }
  return periodIds;
};
exports.mgetPrimaryPeriodId = mgetPrimaryPeriodId;

getPrimaryPeriodId = async function (instance, sampId, periName) {
  var primaryPeriodIds = await mgetPrimaryPeriodId(instance, [sampId], [periName]);
  return primaryPeriodIds[0][0];
};
exports.getPrimaryPeriodId = getPrimaryPeriodId;

mgetPeriodRange = async function (instance, periodIds, yearDotMonth) {
  // needs 2D array periodIds: [iter][peri]
  // returns 2D array [iter][samp] of { "begin": x, "end": y }

  // Need to collapse [iter][sample] to 1D array of periodIds, in order to call mSearch()
  var Ids = [];
  var idx = 0;
  for (var i = 0; i < periodIds.length; i++) {
    for (j = 0; j < periodIds[i].length; j++) {
      Ids[idx] = periodIds[i][j];
      idx++;
    }
  }
  var data = await mSearch(instance, 'period', yearDotMonth, ['period.period-uuid'], [Ids], 'period', null, 1);
  // mSearch returns a 2D array, in other words, a list of values (inner array) for each query (outer array)
  // In this case, the queries are 1 per sampleId/periodName (for all iterations ordered), and the list of values
  // happens to be exactly 1 value, the primaryPeriodId.
  var ranges = []; // Will be 2D array of [iter][periIds];
  idx = 0;
  for (var i = 0; i < periodIds.length; i++) {
    if (typeof ranges[i] == 'undefined') {
      ranges[i] = [];
    }
    for (j = 0; j < periodIds[i].length; j++) {
      if (typeof ranges[i][j] == 'undefined') {
        ranges[i][j] = {};
      }
      ranges[i][j]['begin'] = data[idx][0]['begin'];
      ranges[i][j]['end'] = data[idx][0]['end'];
      idx++;
    }
  }
  return ranges;
};
exports.mgetPeriodRange = mgetPeriodRange;

getPeriodRange = async function (instance, periId, yearDotMonth) {
  var periodRanges = await mgetPeriodRange(instance, [[periId]], yearDotMonth);
  return periodRanges[0][0];
};
exports.getPeriodRange = getPeriodRange;

mgetMetricDescs = async function (instance, runIds, yearDotMonth) {
  return await mSearch(
    instance,
    'metric_desc',
    yearDotMonth,
    ['run.run-uuid'],
    [runIds],
    'metric_desc.metric_desc-uuid',
    null,
    bigQuerySize
  );
};

getMetricDescs = async function (instance, runId) {
  var metricDescs = await mgetMetricDescs(instance, [runId]);
  return metricDescs[0];
};
exports.getMetricDescs = getMetricDescs;

mgetMetricDataDocs = async function (instance, metricIds, yearDotMonth) {
  return await mSearch(
    instance,
    'metric_data',
    yearDotMonth,
    ['metric_desc.metric_desc-uuid'],
    [metricIds],
    '',
    null,
    bigQuerySize
  );
};

getMetricDataDocs = async function (instance, metricId) {
  var metricDataDocs = await mgetMetricDataDocs(instance, [metricId]);
  return await mgetMetricDataDocs(instance, [metricId])[0];
};
exports.getMetricDataDocs = getMetricDataDocs;

mgetMetricTypes = async function (instance, runIds, metricSources, yearDotMonth) {
  return await mSearch(
    instance,
    'metric_desc',
    yearDotMonth,
    ['run.run-uuid', 'metric_desc.source'],
    [runIds, metricSources],
    null,
    { source: { terms: { field: 'metric_desc.type', size: 10000 } } },
    0
  );
};
exports.mgetMetricTypes = mgetMetricTypes;

getMetricTypes = async function (instance, runId, metricSource) {
  var metricTypes = await mgetMetricTypes(instance, [runId], [metricSources]);
  return await metricTypes[0];
};
exports.getMetricTypes = getMetricTypes;

mgetIterations = async function (instance, runIds, yearDotMonth) {
  return await mSearch(
    instance,
    'iteration',
    yearDotMonth,
    ['run.run-uuid'],
    [runIds],
    'iteration.iteration-uuid',
    null,
    1000,
    [{ 'iteration.num': { order: 'asc', numeric_type: 'long' } }]
  );
};

getIterations = async function (instance, runId, yearDotMonth) {
  var iterations = await mgetIterations(instance, [runId], yearDotMonth);
  return iterations[0];
};
exports.getIterations = getIterations;

mgetTags = async function (instance, runIds, yearDotMonth) {
  return await mSearch(instance, 'tag', yearDotMonth, ['run.run-uuid'], [runIds], 'tag', null, 1000);
};

getTags = async function (instance, runId, yearDotMonth) {
  var tags = await mgetTags(instance, [runId], yearDotMonth);
  return tags[0];
};
exports.getTags = getTags;

mgetRunFromIter = async function (instance, iterIds, yearDotMonth) {
  return await mSearch(
    instance,
    'iteration',
    yearDotMonth,
    ['iteration.iteration-uuid'],
    [iterIds],
    'run.run-uuid',
    null,
    1000
  );
};

getRunFromIter = async function (instance, iterId, yearDotMonth) {
  var runsFromIter = await mgetRunFromIter(instance, [iterId], yearDotMonth);
  return runsFromIter[0][0];
};
exports.getRunFromIter = getRunFromIter;

mgetRunFromPeriod = async function (instance, periIds, yearDotMonth) {
  return await mSearch(instance, 'period', yearDotMonth, ['period.period-uuid'], [periIds], 'run.run-uuid', null, 1);
};

getRunFromPeriod = async function (instance, periId, yearDotMonth) {
  var runFromPeriods = await mgetRunFromPeriod(instance, [periId], yearDotMonth);
  return runFromPeriods[0][0];
};
exports.getRunFromPeriod = getRunFromPeriod;

// For each opensearch instance, get the cdm versions they contain and their indices
getInstancesInfo = function (instances) {
  for (inst_idx = 0; inst_idx < instances.length; inst_idx++) {
    var url = 'http://' + instances[inst_idx]['host'] + '/_cat/indices?format=json';
    var resp;
    instances[inst_idx]['online'] = true;
    try {
      resp = request('GET', url, { headers: instances[inst_idx]['header'] });
    } catch (error) {
      console.log(
        'getInstancesInfo(): Failed to reach the host ' + instances[inst_idx]['host'] + ', so not including it'
      );
      instances[inst_idx]['online'] = false;
      continue;
    }
    if (typeof resp != 'undefined') {
      var indices = JSON.parse(resp.getBody());
      instances[inst_idx]['indices'] = {};
      for (const index of indices) {
        var name = index['index'];
        if (/^cdm/.exec(name)) {
          debuglog('index:\n' + JSON.stringify(index, null, 2));
          const match = name.match(/^cdm[-]{0,1}(v[\d+]dev)/);
          const cdmver = match[1];
          if (!Object.keys(instances[inst_idx]['indices']).includes(cdmver)) {
            instances[inst_idx]['indices'][cdmver] = [];
          }
          instances[inst_idx]['indices'][cdmver].push(name);
        }
      }
    }
    if (Object.keys(instances[inst_idx]).includes('ver')) {
      // User has already requested a specific cdm version
      continue;
    }
    if (Object.keys(instances[inst_idx]['indices']).length != 0) {
      // If mulitple versions of indices exist, default to the latest version
      // (this can be overridden with --ver <v7dev|v8dev|v9dev> after --host)
      // Note: if you index a new data into a newer CDM version, that will
      // create those indices, and a subsequent query (without --ver) will now
      // default to the newer cdm version.
      var cdmvers = Object.keys(instances[inst_idx]['indices']).sort();
      instances[inst_idx]['ver'] = cdmvers[cdmvers.length - 1];
      continue;
    }
    // There are no cdm indices at all, so we have to pick a default cdm version.
    instances[inst_idx]['ver'] = 'v9dev';
  }
};

invalidInstance = function (instance) {
  if (!instance['online']) {
    debuglog('invalidInstance(): Not using instance ' + instance['host'] + ' becasue it cannot be reached');
    return true;
  }
  if (!Object.keys(instance).includes('indices') || Object.keys(instance['indices']).length == 0) {
    debuglog(
      'invalidInstance(): Not using instance ' +
        instance['host'] +
        ' becasue it does not have indices for any cdm version'
    );
    return true;
  }
  if (!Object.keys(instance['indices']).includes(instance['ver'])) {
    debuglog(
      'invalidInstance(): Not using instance ' +
        instance['host'] +
        ' becasue the cdm version requested [' +
        instance['ver'] +
        '] is not one of the cdm versions it contains [' +
        Object.keys(instance['indices']) +
        ']'
    );
    return true;
  }
  return false;
};

findYearDotMonthFromRun = async function (instance, runId) {
  // Versions older than v9dev don't have a year and month
  if (instance['ver'] == 'v7dev' || instance['ver'] == 'v8dev') {
    return '';
  }

  var q = { query: { bool: { filter: [{ term: { 'run.run-uuid': runId } }] } } };
  var resp = esRequest(instance, 'run', '/_search', q, '@*');
  var data = JSON.parse(resp.getBody());
  if (data['hits']['hits'].length == 0) {
    return null;
  }
  var index = data['hits']['hits'][0]['_index'];
  var regExp = /(@\d\d\d\d\.\d\d)$/;
  var matches = regExp.exec(index);
  if (matches) {
    return matches[1];
  }
};
exports.findYearDotMonthFromRun = findYearDotMonthFromRun;

findInstanceFromRun = async function (instances, runId) {
  var foundInstance;
  for (const instance of instances) {
    if (invalidInstance(instance)) {
      console.log('not a valid instance: ' + JSON.stringify(instance, null, 2));
      continue;
    }
    // Use any function which searches by run id and always returns something if the run id is present
    debuglog('findInstanceFromRun(): about to call getIterations()');
    var result = await getIterations(instance, runId, '@*');
    debuglog('findInstanceFromRun(): returned from calling getIterations()');
    if (typeof result != 'undefined' && result.length > 0) {
      debuglog('found valid instance: ' + JSON.stringify(instance, null, 2));
      foundInstance = instance;
      break;
    }
  }
  debuglog('findInstanceFromRun(): about to return');
  return foundInstance;
};
exports.findInstanceFromRun = findInstanceFromRun;

findInstanceFromPeriod = async function (instances, periId) {
  var foundInstance;
  for (const instance of instances) {
    if (invalidInstance(instance)) {
      continue;
    }
    //var result = await mgetRunFromPeriod(instance, [periId])[0][0];
    var result = await getRunFromPeriod(instance, [periId], '@*');
    if (typeof result != 'undefined') {
      foundInstance = instance;
      break;
    }
  }
  return foundInstance;
};
exports.findInstanceFromPeriod = findInstanceFromPeriod;

mgetParams = async function (instance, iterIds, yearDotMonth) {
  return await mSearch(instance, 'param', yearDotMonth, ['iteration.iteration-uuid'], [iterIds], 'param', null, 1000);
};
exports.mgetParams = mgetParams;

getParams = async function (instance, iterId, yearDotMonth) {
  return await mgetParams(instance, [iterId], yearDotMonth)[0];
};
exports.getParams = getParams;

mgetIterationDoc = async function (instance, iterIds, yearDotMonth) {
  return await mSearch(instance, 'iteration', yearDotMonth, ['iteration.iteration-uuid'], [iterIds], '', null, 1000);
};

getIterationDoc = async function (instance, iterId, yearDotMonth) {
  var iterationDocs = await mgetIterationDoc(instance, [iterId], yearDotMonth);
  return iterationDocs[0][0];
};
exports.getIterationDoc = getIterationDoc;

mgetBenchmarkNameFromIter = async function (instance, Ids, yearDotMonth) {
  return await mSearch(
    instance,
    'iteration',
    yearDotMonth,
    ['iteration.iteration-uuid'],
    [Ids],
    'run.benchmark',
    null,
    1
  );
};

getBenchmarkNameFromIter = async function (instance, Id, yearDotMonth) {
  var benchmarkNameforIters = await mgetBenchmarkNameFromIter(instance, [Id], yearDotMonth);
  return benchmarkNameFromIters[0][0];
};
exports.getBenchmarkNameFromIter = getBenchmarkNameFromIter;

mgetBenchmarkName = async function (instance, runIds, yearDotMonth) {
  return await mSearch(instance, 'run', yearDotMonth, ['run.run-uuid'], [runIds], 'run.benchmark', null, 1);
};

getBenchmarkName = async function (instance, runId, yearDotMonth) {
  var benchmarkNames = await mgetBenchmarkName(instance, [runId], yearDotMonth);
  return benchmarkNames[0][0];
};
exports.getBenchmarkName = getBenchmarkName;

mgetRunData = async function (instance, runIds, yearDotMonth) {
  return await mSearch(instance, 'run', yearDotMonth, ['run.run-uuid'], [runIds], '', null, 1000);
};

getRunData = async function (instance, runId, yearDotMonth) {
  var runData = await mgetRunData(instance, [runId], yearDotMonth);
  return runData[0];
};
exports.getRunData = getRunData;

calcIterMetrics = function (vals) {
  var count = vals.length;
  if (count == 0) return -1;
  var total = vals.reduce((a, b) => a + b, 0);
  var mean = total / count;
  var diff = 0;
  vals.forEach((val) => {
    diff += (mean - val) * (mean - val);
  });
  diff /= count - 1;
  var mstddev = Math.sqrt(diff);
  var mstddevpct = (100 * mstddev) / mean;
  return {
    mean: mean,
    min: Math.min(...vals),
    max: Math.max(...vals),
    stddev: mstddev,
    stddevpct: mstddevpct
  };
};

mgetIterMetrics = async function (instance, iterationIds) {
  var results = {};
  var benchmarkNames = consolidateAllArrays(await mgetBenchmarkNameFromIter(instance, iterationIds));
  if (benchmarkNames.length !== 1) {
    console.log('ERROR: The benchmark-name for all iterations was not the same, includes: ' + benchmarkNames);
    process.exit(1);
  }
  var primaryMetrics = consolidateAllArrays(await mgetPrimaryMetric(instance, iterationIds));
  if (primaryMetrics.length !== 1) {
    console.log('ERROR: The primary-metric for all iterations was not the same, includes: ' + primaryMetrics);
    process.exit(1);
  }
  var primaryPeriodNames = consolidateAllArrays(await mgetPrimaryPeriodName(instance, iterationIds));
  if (primaryPeriodNames.length !== 1) {
    console.log('ERROR: The primary-period-name for all iterations was not the same, includes: ' + primaryPeriodNames);
    process.exit(1);
  }
  // Find all of the passing samples, then all of the primary-periods, then get the metric for all of them in one request
  var samples = await mgetSamples(instance, iterationIds); // Samples organized in 2D array, first dimension matching iterationIds
  var samplesByIterId = {};
  var iterIdFromSample = {};
  for (i = 0; i < iterationIds.length; i++) {
    var iterId = iterationIds[i];
    var thisIterSamples = samples[i]; // Array
    samplesByIterId[iterId] = thisIterSamples;
    thisIterSamples.forEach((s) => {
      iterIdFromSample[s] = iterId;
    });
  }
  var consSamples = consolidateAllArrays(samples); // All sample IDs flattened into 1 array
  var consSamplesStatus = await mgetSampleStatus(instance, consSamples);
  var consPassingSamples = []; // Only passing samples in flattened array
  for (i = 0; i < consSamplesStatus.length; i++) {
    if (consSamplesStatus[i] == 'pass') consPassingSamples.push(consSamples[i]);
  }
  var primaryPeriodIds = await mgetPrimaryPeriodId(instance, consPassingSamples, primaryPeriodNames);
  var periodsBySample = {};
  var sampleIdFromPeriod = {};
  for (i = 0; i < consPassingSamples.length; i++) {
    var sampId = consPassingSamples[i];
    var thisSamplePeriods = primaryPeriodIds[i]; // Array
    periodsBySample[sampId] = thisSamplePeriods;
    thisSamplePeriods.forEach((p) => {
      sampleIdFromPeriod[p] = sampId;
    });
  }
  var consPrimaryPeriodIds = consolidateAllArrays(primaryPeriodIds);
  var periodRanges = await mgetPeriodRange(instance, consPrimaryPeriodIds);
  // Create the sets for getMetricDataSets
  var sets = [];
  var periodsByIteration = {};
  for (i = 0; i < consPrimaryPeriodIds.length; i++) {
    periodId = consPrimaryPeriodIds[i];
    var p = {
      period: periodId,
      source: benchmarkNames[0],
      type: primaryMetrics[0],
      begin: periodRanges[i][0].begin,
      end: periodRanges[i][0].end,
      resolution: 1,
      breakout: []
    };
    sets.push(p);
    periodsByIteration[iterIdFromSample[sampleIdFromPeriod[periodId]]] = p;
  }
  // Returned data should be in same order as consPrimaryPeriodIds
  var metricDataSets = await getMetricDataSets(instance, sets);
  // Build per-iteration results
  var period = consPrimaryPeriodIds[0];
  var sample = sampleIdFromPeriod[period];
  var iter = iterIdFromSample[sample];
  var vals = [];
  // Below relies on the expectation that periods for the same sample are stored contiguously in consPrimaryPeriodIds array
  for (i = 0; i < consPrimaryPeriodIds.length; i++) {
    period = consPrimaryPeriodIds[i];
    sample = sampleIdFromPeriod[period];
    nextIter = iterIdFromSample[sample];
    //if (iter !== iterIdFromSample[sample]) {
    if (iter !== nextIter) {
      // detected next iteration, calc current iteration's metrics
      var thisResult = calcIterMetrics(vals);
      results[iter] = thisResult;
      // now switch to new iteration
      iter = nextIter;
      vals = [];
    }
    // metricDataSets can return metrics with multiple labels, and for each of those, multiple data-samples.
    // In this case, we are expecting a blank label since there is no metric-breakout, and exactly 1 data-sample.
    vals.push(metricDataSets[i][''][0].value);
  }
  var thisResult = calcIterMetrics(vals);
  results[iter] = thisResult;
  return results;
};
exports.mgetIterMetrics = mgetIterMetrics;

getIterMetrics = async function (instance, iterId) {
  return await mgetIterMetrics(instance, [iterId]);
};

deleteDocs = function (instance, docTypes, q, yearDotMonth) {
  docTypes.forEach((docType) => {
    debuglog('deleteDocs() q: ' + JSON.stringify(q, null, 2));
    var resp = esRequest(instance, docType, '/_delete_by_query?wait_for_completion=false', q, yearDotMonth);
    var responses = JSON.parse(resp.getBody());
    debuglog(JSON.stringify(responses, null, 2));
  });
};
exports.deleteDocs = deleteDocs;

// For comparing N iterations across 1 or more runs.
buildIterTree = function (
  instance,
  results,
  params,
  tags,
  paramValueByIterAndArg,
  tagValueByIterAndName,
  iterIds,
  dontBreakoutTags,
  dontBreakoutParams,
  omitParams,
  breakoutOrderTags,
  breakoutOrderParams,
  indent
) {
  // params: 2-d hash, {arg}{val}, value = [list of iteration IDs that has this val]
  // tags: 2-d hash, {name}{val}, value = [list of iteration IDs that has this val]

  if (typeof indent == 'undefined') {
    indent = '';
  }

  var iterNode = {};
  var newParamsJsonStr = JSON.stringify(params);
  var newParams = JSON.parse(newParamsJsonStr);
  var newTagsJsonStr = JSON.stringify(tags);
  var newTags = JSON.parse(newTagsJsonStr);

  // Move any params which have only 1 value to current iterNode
  Object.keys(newParams).forEach((arg) => {
    if (Object.keys(newParams[arg]).length == 1) {
      if (typeof iterNode['params'] == 'undefined') {
        iterNode['params'] = [];
      }
      var val = Object.keys(newParams[arg])[0]; // the one and only value
      var thisParam = { arg: arg, val: val };
      iterNode.params.push(thisParam);
      delete newParams[arg]; // delete all possible values for this arg
    }
  });

  // Move any tags which have only 1 value to current iterNode
  Object.keys(newTags).forEach((name) => {
    if (Object.keys(newTags[name]).length == 1) {
      if (typeof iterNode['tags'] == 'undefined') {
        iterNode['tags'] = [];
      }
      var val = Object.keys(newTags[name])[0]; // the one and only value
      var thisTag = { name: name, val: val };
      iterNode.tags.push(thisTag);
      delete newTags[name]; // delete all possible values for this arg
    }
  });

  if (iterIds.length == 0) {
    console.log(indent + 'FYI, iterIds.length is 0');
  }

  // The child nodes can only be from breaking out one param or one tag
  // The current implementation checks for a param first, and only if there
  // are none, does it try a tag.  The opposite order could also work.

  var args = Object.keys(newParams).filter((x) => !dontBreakoutParams.includes(x));
  if (args.length > 0) {
    // There are multi-val params, so breakout one of them
    var nextArg;
    for (i = 0; i < breakoutOrderParams.length; i++) {
      if (args.includes(breakoutOrderParams[i])) {
        nextArg = breakoutOrderParams[i];
        break;
      }
    }
    if (typeof nextArg == 'undefined') {
      nextArg = args[0];
    }
    var intersectedIterCount = 0;
    Object.keys(newParams[nextArg]).forEach((val) => {
      const intersectedIterIds = intersectTwoArrays(iterIds, newParams[nextArg][val]);
      const intersectedIterLength = intersectedIterIds.length;
      if (intersectedIterLength == 0) {
      } else {
        intersectedIterCount += intersectedIterLength;
        var newIter;
        var newNewParamsJsonStr = JSON.stringify(newParams);
        var newNewParams = JSON.parse(newNewParamsJsonStr);
        delete newNewParams[nextArg]; // delete all possible values for this arg
        newNewParams[nextArg] = {};
        newNewParams[nextArg][val] = newParams[nextArg][val];
        newIter = buildIterTree(
          instance,
          results,
          newNewParams,
          newTags,
          paramValueByIterAndArg,
          tagValueByIterAndName,
          intersectedIterIds,
          dontBreakoutTags,
          dontBreakoutParams,
          omitParams,
          breakoutOrderTags,
          breakoutOrderParams,
          indent + '  '
        );
        if (typeof newIter !== 'undefined' && Object.keys(newIter).length > 0) {
          if (typeof iterNode['breakout'] == 'undefined') {
            iterNode['breakout'] = [];
          }
          iterNode['breakout'].push(newIter);
        } else {
          console.log(indent + 'warning: newIter undefined or empty:\n' + JSON.stringify(newIter, null, 2));
        }
      }
    });
    if (iterIds.length !== intersectedIterCount) {
      console.log(
        'ERROR: iterIds.length (' +
          iterIds.length +
          ') and intersectedIterCount (' +
          intersectedIterCount +
          ') do not match for arg: ' +
          nextArg
      );
    }
    return iterNode;
  }

  var names = Object.keys(newTags).filter((x) => !dontBreakoutTags.includes(x));
  if (names.length > 0) {
    // No multi-val params, but have multi-val tags, to breakout one of them
    var nextName;
    for (i = 0; i < breakoutOrderTags.length; i++) {
      if (names.includes(breakoutOrderTags[i])) {
        nextName = breakoutOrderTags[i];
        break;
      }
    }
    if (typeof nextName == 'undefined') {
      nextName = names[0];
    }
    var intersectedIterCount = 0;
    Object.keys(newTags[nextName]).forEach((val) => {
      const intersectedIterIds = intersectTwoArrays(iterIds, newTags[nextName][val]);
      const intersectedIterLength = intersectedIterIds.length;
      if (intersectedIterLength == 0) {
      } else {
        intersectedIterCount += intersectedIterLength;
        var newIter;
        var newNewTagsJsonStr = JSON.stringify(newTags);
        var newNewTags = JSON.parse(newNewTagsJsonStr);
        delete newNewTags[nextName]; // delete all possible values for this arg
        newNewTags[nextName] = {};
        newNewTags[nextName][val] = newTags[nextName][val];
        newIter = buildIterTree(
          instance,
          results,
          newParams,
          newNewTags,
          paramValueByIterAndArg,
          tagValueByIterAndName,
          intersectedIterIds,
          dontBreakoutTags,
          dontBreakoutParams,
          omitParams,
          breakoutOrderTags,
          breakoutOrderParams,
          indent + '  '
        );
        if (typeof newIter !== 'undefined' && Object.keys(newIter).length > 0) {
          if (typeof iterNode['breakout'] == 'undefined') {
            iterNode['breakout'] = [];
          }
          iterNode['breakout'].push(newIter);
        } else {
          console.log(indent + 'warning: newIter undefined or empty:\n' + JSON.stringify(newIter, null, 2));
        }
      }
    });
    if (iterIds.length !== intersectedIterCount) {
      console.log(
        indent +
          'ERROR: iterIds.length (' +
          iterIds.length +
          ') and intersectedIterCount (' +
          intersectedIterCount +
          ') do not match for name: ' +
          nextName
      );
    }
    return iterNode;
  }

  // There are no breakouts to create, so we should be at the leaf.  Create the iteration with labels, metrics, etc.
  var iterations = [];
  iterIds.forEach((id) => {
    //var result = getIterMetrics(instance, id);
    //return { "mean": mean, "min": Math.min(...vals), "max": Math.max(...vals), "stddev": mstddev, "stddevpct": mstddevpct };
    var thisIter = {
      id: id,
      labels: '',
      mean: results[id]['mean'],
      stddevpct: results[id]['stddevpct'],
      min: results[id]['min'],
      max: results[id]['max']
    };
    Object.keys(newTags).forEach((name) => {
      if (typeof tagValueByIterAndName[id][name] !== 'undefined') {
        thisIter['labels'] += ' ' + name + ':' + tagValueByIterAndName[id][name];
      }
    });
    Object.keys(newParams).forEach((arg) => {
      if (typeof paramValueByIterAndArg[id][arg] !== 'undefined') {
        thisIter['labels'] += ' ' + arg + ':' + paramValueByIterAndArg[id][arg];
      }
    });
    iterations.push(thisIter);
  });
  iterNode['iterations'] = iterations;
  return iterNode;
};

// Generate a txt report for iteration compareisons (uses data from buildIterTree)
reportIters = function (iterTree, indent, count) {
  //if (typeof(indent) == "undefined" || indent == "") {
  //}
  if (typeof count == 'undefined') {
    count = 0;
  }

  var midPoint = 70;
  var len = 0;

  // Print the params and tags for this subsection
  var tagStr = '';
  if (typeof iterTree.tags != 'undefined') {
    if (iterTree.tags.length == 1) {
      tagStr += iterTree.tags[0].name + ':' + iterTree.tags[0].val;
    } else {
      var separator;
      if (typeof indent == 'undefined' || indent == '') {
        indent = '';
        tagStr = 'All common tags:';
        separator = ' '; // params common to all results at top full width
      } else {
        separator = '\n';
      }
      iterTree.tags.forEach((tag) => {
        tagStr += separator + tag.name + ':' + tag.val;
      });
    }
    tagStr = sprintf('%-' + midPoint + 's', indent + tagStr);
    if (len < tagStr.length) {
      len = tagStr.length;
    }
    process.stdout.write(tagStr + '\n');
    if (typeof indent == 'undefined' || indent == '') {
      console.log('');
    }
  }
  var paramStr = '';
  if (typeof iterTree.params != 'undefined') {
    if (iterTree.params.length == 1) {
      paramStr += iterTree.params[0].arg + ':' + iterTree.params[0].val;
    } else {
      var separator;
      if (typeof indent == 'undefined' || indent == '') {
        indent = '';
        paramStr = 'All common params:';
        separator = ' '; // params common to all results at top full width
      } else {
        separator = '\n';
      }
      iterTree.params.forEach((param) => {
        paramStr += separator + param.arg + ':' + param.val;
      });
    }
    paramStr = sprintf('%-' + midPoint + 's', indent + paramStr);
    if (len < paramStr.length) {
      len = paramStr.length;
    }
    process.stdout.write(paramStr + '\n');
    if (typeof indent == 'undefined' || indent == '') {
      console.log('');
    }
  }

  // Print the headers if this is the first call to reportIters
  if (typeof indent == 'undefined' || indent == '') {
    // print the row names after all common tags/params are printed
    var header = sprintf('\n%' + midPoint + 's' + ' %10s %10s %36s', 'label', 'mean', 'stddevpct', 'iter-id');
    console.log(header);
    indent = '';
  }

  if (typeof iterTree.iterations == 'undefined') {
    // We are not at the leaf, need to go deeper
    if (typeof iterTree.breakout != 'undefined' && iterTree.breakout.length > 0) {
      iterTree.breakout.forEach((iter) => {
        var retCount = reportIters(iter, '  ' + indent, 0);
        count = count + retCount;
      });
      return count;
    } else {
      return count;
    }
  } else {
    // We should be at a leaf of the tree.  Anything in breakout[] should be params or tags which were reqsuested to not break-out
    const sorted = iterTree.iterations.sort((a, b) =>
      a.labels.localeCompare(b.labels, undefined, {
        numeric: true,
        sensitivity: 'base'
      })
    );
    sorted.forEach((i) => {
      count++;
      var metrics = sprintf(
        '%' + midPoint + 's' + ' %10.4f %10.4f %36s',
        i['labels'],
        i['mean'],
        i['stddevpct'],
        i['id']
      );
      console.log(metrics);
    });
    return count;
  }

  return;
};

// getIters(): filter and group interations, typically for generating comparisons (clustered bar graphs)
getIters = async function (
  instance,
  filterByAge,
  filterByTags,
  filterByParams,
  dontBreakoutTags,
  omitTags,
  dontBreakoutParams,
  omitParams,
  breakoutOrderTags,
  breakoutOrderParams,
  addRuns,
  addIterations
) {
  // Process:
  // 1) Get run.run-uuids from age + benchmark + tag filters
  // 2) From run.run-uuids, get iteration.iteration-uuids
  // 3) Get iteration.iteratoin-uuids from age + benchmark + param filters
  // 4) Intersect iters from #2 and #3
  // 5) Build iteration lookup tables by param and by tag

  const now = Date.now();
  var intersectedRunIds = [];
  var jsonArr = [];
  var jsonArr2 = '';
  var indexjson = '';
  var qjson = '';
  var newestDay = now - 1000 * 3600 * 24 * filterByAge.split('-')[0];
  var oldestDay = now - 1000 * 3600 * 24 * filterByAge.split('-')[1];

  var base_q = {
    query: {
      bool: {
        filter: [{ range: { 'run.end': { lte: newestDay } } }, { range: { 'run.begin': { gte: oldestDay } } }]
      }
    },
    _source: 'run.run-uuid',
    size: bigQuerySize
  };
  var base_q_json = JSON.stringify(base_q);

  // Each filter of tagName:tagVal must be a separate query.
  // However, all of these queries can be submitted together via msearch.
  // The responses (a list of run.run-uuids for each query) must be intersected
  // to have only the run.run-uuids that match *all* tag filters.
  console.log('Get all iterations from ' + filterByTags.length + ' tag filters');
  filterByTags.forEach((nameval) => {
    var tag_query = JSON.parse(base_q_json);
    var name = nameval.split(':')[0];
    var val = nameval.split(':')[1];
    var tagNameTerm = { term: { 'tag.name': name } };
    tag_query.query.bool.filter.push(tagNameTerm);
    if (val != 'tag-not-used') {
      var tagValTerm = { term: { 'tag.val': val } };
      tag_query.query.bool.filter.push(tagValTerm);
      jsonArr.push('{"index": "' + getIndexBaseName(instance) + 'tag' + '" }');
      jsonArr.push(JSON.stringify(tag_query));
    } else {
      // Find the run IDs which have this tag name present (value does not matter)
      jsonArr2 += '{"index": "' + getIndexBaseName(instance) + 'tag' + '" }\n';
      jsonArr2 += JSON.stringify(tag_query) + '\n';
    }
  });

  if (jsonArr.length > 0) {
    var responses = esJsonArrRequest(instance, 'tag', '/_msearch', jsonArr);
    var runIds = [];
    responses.forEach((response) => {
      var theseRunIds = [];
      response.hits.hits.forEach((run) => {
        theseRunIds.push(run._source.run['run-uuid']);
      });
      runIds.push(theseRunIds);
    });
    var intersectedRunIds = intersectAllArrays(runIds);

    if (jsonArr2.length > 0) {
      var responses2 = esJsonArrRequest(instance, 'tag', '/_msearch', jsonArr2);
      responses2.forEach((response) => {
        response.hits.hits.forEach((run) => {
          if (intersectedRunIds.includes(run._source.run['run-uuid'])) {
            var index = intersectedRunIds.indexOf(run._source.run['run-uuid']);
            if (index != -1) {
              intersectedRunIds.splice(index, 1);
            }
          }
        });
      });
    }
    if (intersectedRunIds.length == 0) {
      console.log(
        'ERROR: The combination of filters used for --filter-by-age and --filter-by-tags yielded 0 iterations.  Try using less restrictive filters'
      );
      process.exit(1);
    }
  }
  // Now we can get all of the iterations for these run.run-uuids
  var iterIdsFromRun = await getIterations(instance, intersectedRunIds);

  // Next, we must find the iterations that match the params filters.
  // We are trying to find iterations that have *all* params filters matching, not just one.
  // Each filter of paramArg:paramVal must be a separate query.
  // However, all of these queries can be submitted together via msearch.
  // The responses (a list of iteration.iteration-uuids for each query) must be intersected
  // to have only the iteration.iteration-uuids that match all param filters.
  console.log('Get all iterations from ' + filterByParams.length + ' param filters');
  jsonArr = [];
  filterByParams.forEach((argval) => {
    var param_query = JSON.parse(base_q_json);
    var arg = argval.split(':')[0];
    var val = argval.split(':')[1];
    param_query._source = 'iteration.iteration-uuid';
    var paramArg = { term: { 'param.arg': arg } };
    param_query.query.bool.filter.push(paramArg);
    if (val != 'param-not-used') {
      var paramVal = { term: { 'param.val': val } };
      param_query.query.bool.filter.push(paramVal);
      jsonArr.push('{"index": "' + getIndexBaseName(instance) + 'param' + '" }');
      jsonArr.push(JSON.stringify(param_query));
    } else {
      // Find the run IDs which have this param name present (value does not matter).
      // Later, we will subtract these iteration IDs from the ones found with ndjson query.
      jsonArr2 += '{"index": "' + getIndexBaseName(instance) + 'param' + '" }\n';
      jsonArr2 += JSON.stringify(param_query) + '\n';
    }
  });

  var iterIdsFromParam = [];
  if (jsonArr.length > 0) {
    var resp = esJsonArrRequest(instance, 'param', '/_msearch', jsonArr);
    var responses = JSON.parse(resp.getBody());
    var iterationIds = [];
    responses.forEach((response) => {
      var theseIterationIds = [];
      response.hits.hits.forEach((iteration) => {
        theseIterationIds.push(iteration._source.iteration['iteration-uuid']);
      });
      iterationIds.push(theseIterationIds);
    });
    iterIdsFromParam = intersectAllArrays(iterationIds);

    if (jsonArr2 != '') {
      var resp2 = esJsonArrRequest(instance, 'tag', '/_msearch', jsonArr2);
      var responses2 = JSON.parse(resp2.getBody());
      responses2.forEach((response) => {
        response.hits.hits.forEach((hit) => {
          if (iterIdsFromParam.includes(hit._source.iteration['iteration-uuid'])) {
            var index = iterIdsFromParam.indexOf(hit._source.iteration['iteration-uuid']);
            if (index !== -1) {
              iterIdsFromParam.splice(index, 1);
            }
          }
        });
      });
    }
    if (iterIdsFromParam.length == 0) {
      console.log(
        'ERROR: The combination of filters used for --filter-by-age and --filter-by-params yielded 0 iterations.  Try using less restrictive filters'
      );
      process.exit(1);
    }
  }

  // Get the iteration IDs that are common from both tag and param filters
  var allFilterIterIds = [];
  if (iterIdsFromRun.length > 0 && iterIdsFromParam.length > 0) {
    var iterIds = [];
    iterIds.push(iterIdsFromRun);
    iterIds.push(iterIdsFromParam);
    allFilterIterIds = intersectAllArrays(iterIds);
  } else if (iterIdsFromRun.length > 0) {
    allFilterIterIds = iterIdsFromRun;
  } else {
    allFilterIterIds = iterIdsFromParam;
  }

  var allIterIds = allFilterIterIds;

  // Now we can add any iterations from --add-runs and --add-iterations.
  // These options are not subject to the tags and params filters.
  if (typeof addRuns != 'undefined' && addRuns != []) {
    var ids = await getIterations(instance, addRuns);
    ids.forEach((id) => {
      if (!allIterIds.includes(id)) {
        allIterIds.push(id);
      }
    });
  }
  if (typeof addIterations != 'undefined' && addRuns != []) {
    addIterations.forEach((id) => {
      if (!allIterIds.includes(id)) {
        allIterIds.push(id);
      }
    });
  }

  if (allIterIds.length == 0) {
    console.log(
      'ERROR: The combination of filters used for --filter-by-age --filter-by-params and --filter-by-tags yielded 0 iterations.  Try using less restrictive filters'
    );
    process.exit(1);
  }

  console.log('Total iterations: ' + allIterIds.length);

  console.log('Finding all tag names');
  var iterRunIds = await mgetRunFromIter(instance, allIterIds);
  var iterTags = await mgetTags(instance, iterRunIds);
  var allTagNames = getObjVals(consolidateAllArrays(iterTags), 'name');
  console.log('allTagNames:\n' + JSON.stringify(allTagNames, null, 2));
  console.log('Finding all param args');
  var iterParams = await mgetParams(instance, allIterIds);
  var allParamArgs = getObjVals(consolidateAllArrays(iterParams), 'arg');
  console.log('allParamArgs:\n' + JSON.stringify(allParamArgs, null, 2));

  // Build look-up tables [iterId][param-arg] = param-value and [iterId][tag-name] = tag-value
  console.log('Building param and tag look-up tables');
  var paramValueByIterAndArg = {};
  var tagValueByIterAndName = {};
  var iterations = [];

  //allIterIds.forEach(iter => {
  for (j = 0; j < allIterIds.length; j++) {
    var iter = allIterIds[j];
    //var params = await getParams(instance, iter);
    var params = iterParams[j];
    // Need to consolidate multiple params with same arg but different values
    var paramIdx = {};
    var l = params.length;
    for (var i = 0; i < l; i++) {
      var arg = params[i].arg;
      if (typeof paramIdx[arg] !== 'undefined') {
        // This param arg was already found, combine this value with exiting param
        var existing_arg_idx = paramIdx[arg];
        params[existing_arg_idx]['val'] += '_' + params[i]['val'];
        params.splice(i, 1);
        l--;
        i--;
      } else {
        paramIdx[arg] = i;
      }
    }
    var tags = iterTags[j];
    var thisIter = { iterId: iter, tags: tags, params: params };
    var loggedParams = [];
    params.forEach((thisParam) => {
      if (typeof paramValueByIterAndArg[iter] == 'undefined') {
        paramValueByIterAndArg[iter] = {};
      }
      if (loggedParams.includes(thisParam['arg'])) {
        console.log(
          'WARNING: param arg ' +
            thisParam['arg'] +
            ' (new value: ' +
            thisParam['val'] +
            ') already processed for iteration ' +
            iter +
            '(old value: ' +
            paramValueByIterAndArg[iter][thisParam['arg']] +
            ')'
        );
        paramValueByIterAndArg[iter][thisParam['arg']] += '_' + thisParam['val'];
        console.log(JSON.stringify(thisParam));
        console.log('WARNING: param value is now ' + paramValueByIterAndArg[iter][thisParam['arg']]);
      } else {
        paramValueByIterAndArg[iter][thisParam['arg']] = thisParam['val'];
        loggedParams.push(thisParam['arg']);
      }
    });
    tags.forEach((thisTag) => {
      if (typeof tagValueByIterAndName[iter] == 'undefined') {
        tagValueByIterAndName[iter] = {};
      }
      tagValueByIterAndName[iter][thisTag['name']] = thisTag['val'];
    });
    iterations.push(thisIter);
  }

  // Find the tag names which are present in every single iteration
  // We can only do "breakouts" if the tag is used everywhere
  console.log('Finding only the tag names which are present in all iterations');
  var notCommonTagNames = [];
  var notCommonParamArgs = [];
  for (j = 0; j < allIterIds.length; j++) {
    var iter = allIterIds[j];
    for (i = 0; i < allTagNames.length; i++) {
      var name = allTagNames[i];
      if (typeof tagValueByIterAndName[iter][name] == 'undefined') {
        if (!notCommonTagNames.includes(name)) {
          notCommonTagNames.push(name);
        }
        var index = allTagNames.indexOf(name);
        if (index !== -1) {
          allTagNames.splice(index, 1);
          i--;
        }
      }
    }
    for (i = 0; i < allParamArgs.length; i++) {
      var arg = allParamArgs[i];
      if (typeof paramValueByIterAndArg[iter][arg] == 'undefined') {
        if (!notCommonParamArgs.includes(arg)) {
          notCommonParamArgs.push(arg);
        }
        var index = allParamArgs.indexOf(arg);
        if (index !== -1) {
          allParamArgs.splice(index, 1);
          i--;
        }
      }
    }
  }

  var commonTagNames = [...allTagNames];
  var commonParamArgs = [...allParamArgs];

  // For the notCommonTagNames, add this tag with a value of "tag-not-used"
  // to any iteration which has this tag missing
  notCommonTagNames.forEach((name) => {
    for (var i = 0; i < iterations.length; i++) {
      var iterId = iterations[i]['iterId'];
      var foundTag = false;
      for (var j = 0; j < iterations[i]['tags'].length; j++) {
        if (iterations[i]['tags'][j]['name'] == name) {
          foundTag = true;
        }
      }
      if (foundTag == false) {
        var newTag = { name: name, val: 'tag-not-used' };
        iterations[i]['tags'].push(newTag);
      }
    }
  });

  // For the notCommonParamArgs, add this param with a value of "param-not-used"
  // to any iteration which has this param missing
  notCommonParamArgs.forEach((arg) => {
    for (var i = 0; i < iterations.length; i++) {
      var iterId = iterations[i]['iterId'];
      var foundParam = false;
      for (var j = 0; j < iterations[i]['params'].length; j++) {
        if (iterations[i]['params'][j]['arg'] == arg) {
          foundParam = true;
        }
      }
      if (foundParam == false) {
        var newParam = { arg: arg, val: 'param-not-used' };
        iterations[i]['params'].push(newParam);
      }
    }
  });

  // Scan iterations to find all different values for each tag and param
  console.log('Finding all different values for each tag and param');
  var tags = {};
  var params = {};
  iterations.forEach((thisIter) => {
    thisIter['tags'].forEach((tag) => {
      if (!omitTags.includes(tag.name)) {
        if (typeof tags[tag.name] == 'undefined') {
          tags[tag.name] = {};
        }
        if (typeof tags[tag.name][tag.val] == 'undefined') {
          tags[tag.name][tag.val] = [];
        }
        tags[tag.name][tag.val].push(thisIter.iterId);
      }
    });

    thisIter.params.forEach((param) => {
      if (!omitParams.includes(param.arg)) {
        if (typeof params[param.arg] == 'undefined') {
          params[param.arg] = {};
        }
        if (typeof params[param.arg][param.val] == 'undefined') {
          params[param.arg][param.val] = [];
        }
        params[param.arg][param.val].push(thisIter.iterId);
      }
    });
  });

  var sortedTagNames = Object.keys(tags).sort((a, b) =>
    a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' })
  );

  sortedTagNames.forEach((name) => {
    var sortedTagValues = Object.keys(tags[name]).sort((a, b) =>
      a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' })
    );
  });

  var iterTree = {};
  console.log('allIterIds.length: ' + allIterIds.length);

  // Build a lookup table of iterId->metrics
  console.log('mgetIterMetrics');
  var results = await mgetIterMetrics(instance, allIterIds);

  iterTree = buildIterTree(
    instance,
    results,
    params,
    tags,
    paramValueByIterAndArg,
    tagValueByIterAndName,
    allIterIds,
    dontBreakoutTags,
    dontBreakoutParams,
    omitParams,
    breakoutOrderTags,
    breakoutOrderParams
  );
  return iterTree;
};
exports.getIters = getIters;

exports.getMetricSources = function (instance, runId, yearDotMonth) {
  var q = {
    query: { bool: { filter: [{ term: { 'run.run-uuid': runId } }] } },
    aggs: {
      source: { terms: { field: 'metric_desc.source', size: bigQuerySize } }
    },
    size: 0
  };
  var resp = esRequest(instance, 'metric_desc', '/_search', q, yearDotMonth);
  var data = JSON.parse(resp.getBody());
  if (Array.isArray(data.aggregations.source.buckets)) {
    var sources = [];
    data.aggregations.source.buckets.forEach((element) => {
      sources.push(element.key);
    });
    return sources;
  }
};

getDocCount = function (instance, runId, docType, yearDotMonth) {
  var q = { query: { bool: { filter: [{ term: { 'run.run-uuid': runId } }] } } };
  var resp = esRequest(instance, docType, '/_count', q, yearDotMonth);
  var data = JSON.parse(resp.getBody());
  return data.count;
};
exports.getDocCount = getDocCount;

// Traverse a response from a nested aggregation to generate a set of filter terms
// for each metric group.
getMetricGroupTermsFromAgg = function (agg, terms) {
  var value;
  if (typeof terms == 'undefined') {
    terms = '';
  }
  if (typeof agg.key != 'undefined') {
    value = agg.key;
    terms += '"' + value + '"}}';
  }
  var count = 0;
  var metricGroupTerms = new Array();
  Object.keys(agg).forEach((field) => {
    if (/^metric_desc/.exec(field)) {
      count++;
      if (typeof agg[field].buckets != 'undefined') {
        agg[field].buckets.forEach((bucket) => {
          metricGroupTerms = metricGroupTerms.concat(
            getMetricGroupTermsFromAgg(bucket, terms + ',' + '{"term": {"' + field + '": ')
          );
        });
      }
    }
  });
  if (count > 0) {
    return metricGroupTerms;
  } else {
    metricGroupTerms.push(terms.replace(/^,/, ''));
    return metricGroupTerms;
  }
};
exports.getMetricGroupTermsFromAgg = getMetricGroupTermsFromAgg;

getBreakoutAggregation = function (source, type, breakout) {
  var agg_str = '{';
  agg_str += '"metric_desc.source": { "terms": { "field": "metric_desc.source"}';
  agg_str += ',"aggs": { "metric_desc.type": { "terms": { "field": "metric_desc.type"}';
  // More nested aggregations are added, one per field found in the broeakout
  var field_count = 0;
  var regExp = /([^\=]+)\=([^\=]+)/;
  //var matches = regExp.exec("");

  if (Array.isArray(breakout)) {
    breakout.forEach((field) => {
      //if (/([^\=]+)\=([^\=]+)/.exec(field)) {
      var matches = regExp.exec(field);
      if (matches) {
        //field = $1;
        field = matches[1];
      }
      agg_str +=
        ',"aggs": { "metric_desc.names.' +
        field +
        '": { "terms": ' +
        '{ "show_term_doc_count_error": true, "size": ' +
        bigQuerySize +
        ',' +
        '"field": "metric_desc.names.' +
        field +
        '" }';
      field_count++;
    });
    while (field_count > 0) {
      agg_str += '}}';
      field_count--;
    }
    //agg_str += '}}}}';
    //return agg_str;
  }
  // add:
  agg_str += '}}}}';
  return agg_str;
};
exports.getBreakoutAggregation = getBreakoutAggregation;

getMetricGroupTermsByLabel = function (metricGroupTerms) {
  var metricGroupTermsByLabel = {};
  metricGroupTerms.forEach((term) => {
    var terms = JSON.parse('[' + term + ']');
    var label = '';
    terms.forEach((thisTerm) => {
      Object.keys(thisTerm.term).forEach((field) => {
        // The true label does not actually include the source/type
        // but the query does have those in the filter terms, so we
        // need to excluse it when forming the label.
        if (field == 'metric_desc.source' || field == 'metric_desc.type') {
          return;
        }
        label += '-' + '<' + thisTerm.term[field] + '>';
      });
    });
    label = label.replace(/^-/, '');
    metricGroupTermsByLabel[label] = term;
  });
  return metricGroupTermsByLabel;
};

mgetMetricIdsFromTerms = async function (instance, termsSets, yearDotMonth) {
  // termsSets is an array of:
  // { 'period': x, 'run': y, 'termsByLabel': {} }
  // termsByLabel is a dict/hash of:
  // { <label>: sring }
  var jsonArr = [];
  var totalReqs = 0;
  for (i = 0; i < termsSets.length; i++) {
    var periId = termsSets[i].period;
    var runId = termsSets[i].run;
    Object.keys(termsSets[i].termsByLabel)
      .sort()
      .forEach((label) => {
        var terms_string = termsSets[i].termsByLabel[label];
        var q = {
          query: { bool: { filter: JSON.parse('[' + terms_string + ']') } },
          _source: 'metric_desc.metric_desc-uuid',
          size: bigQuerySize
        };
        if (periId == null && runId == null) {
          console.log('ERROR: mgetMetricIdsFromTerms(), terms[' + i + ']  must have either a period-id or run-id\n');
          return;
        }
        if (periId != null) {
          q.query.bool.filter.push(JSON.parse('{"term": {"period.period-uuid": "' + periId + '"}}'));
        }
        if (runId != null) {
          q.query.bool.filter.push(JSON.parse('{"term": {"run.run-uuid": "' + runId + '"}}'));
        }
        jsonArr.push('{}');
        jsonArr.push(JSON.stringify(q));
        totalReqs++;
      });
  }
  var responses = await esJsonArrRequest(instance, 'metric_desc', '/_msearch', jsonArr, yearDotMonth);
  if (totalReqs != responses.length) {
    console.log(
      'mgetMetricIdsFromTerms(): ERROR, number of _msearch responses (' +
        responses.length +
        ') did not match number of requests (' +
        totalReqs +
        ')'
    );
    return;
  }
  if (responses == null) {
    console.log('ERROR: responses is null');
    return;
  }

  // Process the responses and assemble metric IDs into array
  var metricIdsSets = []; // eventual length = termsSets
  var count = 0;
  for (i = 0; i < termsSets.length; i++) {
    var thisMetricIds = {};
    Object.keys(termsSets[i].termsByLabel)
      .sort()
      .forEach((label) => {
        thisMetricIds[label] = [];
        if (responses[i] == null) {
          console.log('ERROR: responses[' + i + '] is null');
          console.log('responses.length:' + responses.length);
          console.log('responses:\n' + JSON.stringify(responses, null, 2));
          console.log('termsSets.length: ' + termsSets.length);
          console.log('totalReqs: ' + totalReqs);
          console.log('query:\n' + jsonArr);
          process.exit(1);
        }
        if (responses[i].hits == null) {
          console.log('ERROR: responses[' + i + '].hits is null');
          console.log('responses[' + i + ']:\n' + JSON.stringify(responses[i], null, 2));
          console.log('termsSets.length: ' + termsSets.length);
          console.log('totalReqs: ' + totalReqs);
          console.log('query:\n' + jsonArr);
          process.exit(1);
        }
        if (responses[i].hits.total.value >= bigQuerySize || responses[i].hits.hits.length >= bigQuerySize) {
          console.log('ERROR: hits from returned query exceeded max size of ' + bigQuerySize);
          process.exit(1);
        }
        for (j = 0; j < responses[count].hits.hits.length; j++) {
          thisMetricIds[label].push(responses[count].hits.hits[j]._source.metric_desc['metric_desc-uuid']);
        }
        count++;
      });
    metricIdsSets.push(thisMetricIds);
  }
  return metricIdsSets;
};
exports.mgetMetricIdsFromTerms = mgetMetricIdsFromTerms;

// Before querying for metric data, we must first find out which metric IDs we need
// to query.  There may be one or more groups of these IDs, depending if the user
// wants to "break-out" the metric (by some metadatam like cpu-id, devtype, etc).
// Find the number of groups needed based on the --breakout options, then find out
// what metric IDs belong in each group.
getMetricGroupsFromBreakouts = async function (instance, sets, yearDotMonth) {
  var metricGroupIdsByLabel = [];
  var indexjson = '{}\n';
  var index = JSON.parse(indexjson);
  var jsonArr = [];

  sets.forEach((set) => {
    var result = getBreakoutAggregation(set.source, set.type, set.breakout);
    var aggs = JSON.parse(result);
    var q = {
      query: {
        bool: {
          filter: [{ term: { 'metric_desc.source': set.source } }, { term: { 'metric_desc.type': set.type } }]
        }
      },
      size: 0
    };

    if (set.period != null) {
      q.query.bool.filter.push(JSON.parse('{"term": {"period.period-uuid": "' + set.period + '"}}'));
    }
    if (set.run != null) {
      q.query.bool.filter.push(JSON.parse('{"term": {"run.run-uuid": "' + set.run + '"}}'));
    }
    // If the breakout contains a match requirement (something like "host=myhost"), then we must add a term filter for it.
    // Eventually it would be nice to have something other than a match, like a regex: host=/^client/.
    var regExp = /([^\=]+)\=([^\=]+)/;
    set.breakout.forEach((field) => {
      var matches = regExp.exec(field);
      if (matches) {
        field = matches[1];
        value = matches[2];
        q.query.bool.filter.push(JSON.parse('{"term": {"metric_desc.names.' + field + '": "' + value + '"}}'));
      }
    });
    q.aggs = aggs;
    jsonArr.push(JSON.stringify(index));
    jsonArr.push(JSON.stringify(q));
  });
  var responses = await esJsonArrRequest(instance, 'metric_desc', '/_msearch', jsonArr, yearDotMonth);

  var metricGroupIdsByLabelSets = [];
  var metricGroupTermsSets = [];
  var metricGroupTermsByLabelSets = [];
  var termsSets = [];
  for (var idx = 0; idx < sets.length; idx++) {
    // The response includes a result from a nested aggregation, which will be parsed to produce
    // query terms for each of the metric groups
    var metricGroupTerms = getMetricGroupTermsFromAgg(responses[idx].aggregations);
    // Derive the label from each group and organize into a dict, key = label, value = the filter terms
    var metricGroupTermsByLabel = getMetricGroupTermsByLabel(metricGroupTerms);
    var thisLabelSet = {
      run: sets[idx].run,
      period: sets[idx].period,
      termsByLabel: metricGroupTermsByLabel
    };
    termsSets.push(thisLabelSet);
  }
  metricGroupIdsByLabelSets = await mgetMetricIdsFromTerms(instance, termsSets, yearDotMonth);
  return metricGroupIdsByLabelSets;
};
exports.getMetricGroupsFromBreakouts = getMetricGroupsFromBreakouts;

getMetricGroupsFromBreakout = async function (instance, runId, periId, source, type, breakout) {
  var thisSet = {
    run: runId,
    period: periId,
    source: source,
    type: type,
    breakout: breakout
  };
  var metricGroupIdsByLabelSets = await getMetricGroupsFromBreakouts(instance, [thisSet]);
  return metricGroupIdsByLabelSets[0];
};
exports.getMetricGroupsFromBreakout = getMetricGroupsFromBreakout;

sendMetricReq = async function (
  jsonArr,
  jsonArrTracker,
  jsonArrIdx,
  responses,
  valueSets,
  set,
  label,
  lastPass,
  instance,
  begin,
  end,
  resolution,
  metricIds,
  yearDotMonth
) {
  debuglog('sendMetricReq begin');
  debuglog(
    'sendMetricReq, jsonArr MB [' +
      numMBytes(jsonArr) +
      ']  responses MB [' +
      numMBytes(responses) +
      ']  jsonArrIdx: [' +
      jsonArrIdx +
      ']'
  );

  // When jsonArr goes over this, submit the requests we have so far, so we can
  // get responses and delete these reqs from jsonArr and process and delete the
  // matching responses.
  var chunkMBytes = 32;

  // Create a query for each data-point in a line graph.  Resolution = number of data-points
  // These vars are used for defining the requests and are altered in each loop cycle below (while)
  // These are not used for processing responses, as response processing is triggered on demand,
  // and these vars won't have the correct info.
  //
  // The resolution determines how many times we compute a value, each value for a
  // different "slice" in the original begin-to-end time domain.
  var duration = Math.floor((end - begin) / resolution);
  var thisBegin = begin;
  var thisEnd = begin + duration;

  // To have the correct info for processing responses, it is stoted in this array, where the array index*2
  // corresponds to the index in the jsonArr request.
  //var info = { 'label': label, 'set': set, 'begin': thisBegin, 'end': thisEnd, 'numMetricIds': metricIds.length };

  while (true) {
    // Calculating a single value representing an average for thisBegin - thisEnd
    // relies on an [weighted average] aggregation, plus a few other queries.  An
    // alternative method would involve querying all documents for the orignal
    // begin - end time range, then [locally] computing a weighted average per
    // thisBegin - thisEnd slice. Each method has pros/cons depending on the
    // resolution and the total number of metric_data documents.
    //
    // This first request is for the weighted average, but does not include the
    // documents which are partially outside the time range we need.
    indexjson = '{"index": "' + getIndexName('metric_data', instance, yearDotMonth) + '" }\n';
    reqjson = '{';
    reqjson += '  "size": 0,';
    reqjson += '  "query": {';
    reqjson += '    "bool": {';
    reqjson += '      "filter": [';
    reqjson += '        {"range": {"metric_data.end": { "lte": "' + thisEnd + '"}}},';
    reqjson += '        {"range": {"metric_data.begin": { "gte": "' + thisBegin + '"}}},';
    reqjson += '        {"terms": {"metric_desc.metric_desc-uuid": ' + JSON.stringify(metricIds) + '}}';
    reqjson += '      ]';
    reqjson += '    }';
    reqjson += '  },';
    reqjson += '  "aggs": {';
    reqjson += '    "metric_avg": {';
    reqjson += '      "weighted_avg": {';
    reqjson += '        "value": {';
    reqjson += '          "field": "metric_data.value"';
    reqjson += '        },';
    reqjson += '        "weight": {';
    reqjson += '          "field": "metric_data.duration"';
    reqjson += '        }';
    reqjson += '      }';
    reqjson += '    }';
    reqjson += '  }';
    reqjson += '}';
    var index = JSON.parse(indexjson);
    var req = JSON.parse(reqjson);
    jsonArr.push(JSON.stringify(index));
    jsonArr.push(JSON.stringify(req));
    jsonArrTracker.push({ label: label, set: set, begin: thisBegin, end: thisEnd, numMetricIds: metricIds.length });
    // This second request is for the total weight of the previous weighted average request.
    // We need this because we are going to recompute the weighted average by adding
    // a few more documents that are partially outside the time domain.
    indexjson = '{"index": "' + getIndexName('metric_data', instance, yearDotMonth) + '" }\n';
    reqjson = '{';
    reqjson += '  "size": 0,';
    reqjson += '  "query": {';
    reqjson += '    "bool": {';
    reqjson += '      "filter": [';
    reqjson += '        {"range": {"metric_data.end": { "lte": "' + thisEnd + '"}}},';
    reqjson += '        {"range": {"metric_data.begin": { "gte": "' + thisBegin + '"}}},';
    reqjson += '        {"terms": {"metric_desc.metric_desc-uuid": ' + JSON.stringify(metricIds) + '}}';
    reqjson += '      ]';
    reqjson += '    }';
    reqjson += '  },';
    reqjson += '  "aggs": {';
    reqjson += '    "total_weight": {';
    reqjson += '      "sum": {"field": "metric_data.duration"}';
    reqjson += '    }';
    reqjson += '  }';
    reqjson += '}\n';
    index = JSON.parse(indexjson);
    req = JSON.parse(reqjson);
    jsonArr.push(JSON.stringify(index));
    jsonArr.push(JSON.stringify(req));
    jsonArrTracker.push({});
    // This third request is for documents that had its begin during or before the time range, but
    // its end was after the time range.
    //
    // Due to some limitations in how many documents can be returned from a query,
    // (in spite of using size:<huge number>)
    // the number of metricIds inlcuded in the search is limited to 10,000.  If there
    // are more than 10,000 metric IDs to query for, use more queries.
    const chunkSize = 10000;
    for (let i = 0; i < metricIds.length; i += chunkSize) {
      indexjson = '{"index": "' + getIndexName('metric_data', instance, yearDotMonth) + '" }\n';
      reqjson = '{';
      reqjson += '  "size": ' + bigQuerySize + ',';
      reqjson += '  "_source": ["metric_data.begin", "metric_data.end", "metric_data.value"],';
      reqjson += '  "query": {';
      reqjson += '    "bool": {';
      reqjson += '      "filter": [';
      reqjson += '        {"range": {"metric_data.end": { "gt": "' + thisEnd + '"}}},';
      reqjson += '        {"range": {"metric_data.begin": { "lte": "' + thisEnd + '"}}},';
      reqjson +=
        '        {"terms": {"metric_desc.metric_desc-uuid": ' +
        JSON.stringify(metricIds.slice(i, i + chunkSize)) +
        '}}\n';
      reqjson += '      ]';
      reqjson += '    }';
      reqjson += '  }';
      reqjson += '}';
      index = JSON.parse(indexjson);
      req = JSON.parse(reqjson);
      jsonArr.push(JSON.stringify(index));
      jsonArr.push(JSON.stringify(req));
      jsonArrTracker.push({});
      // This fourth request is for documents that had its begin before the time range, but
      //  its end was during or after the time range
      var indexjson = '{"index": "' + getIndexName('metric_data', instance, yearDotMonth) + '" }\n';
      var reqjson = '';
      reqjson += '{';
      reqjson += '  "size": ' + bigQuerySize + ',';
      reqjson += '  "_source": ["metric_data.begin", "metric_data.end", "metric_data.value"],';
      reqjson += '  "query": {';
      reqjson += '    "bool": {';
      reqjson += '      "filter": [';
      reqjson += '        {"range": {"metric_data.end": { "gte": ' + thisBegin + '}}},';
      reqjson += '        {"range": {"metric_data.begin": { "lt": ' + thisBegin + '}}},';
      reqjson +=
        '        {"terms": {"metric_desc.metric_desc-uuid": ' +
        JSON.stringify(metricIds.slice(i, i + chunkSize)) +
        '}}\n';
      reqjson += '      ]';
      reqjson += '    }';
      reqjson += '  }';
      reqjson += '}\n';
      index = JSON.parse(indexjson);
      req = JSON.parse(reqjson);
      jsonArr.push(JSON.stringify(index));
      jsonArr.push(JSON.stringify(req));
      jsonArrTracker.push({});
    }

    debuglog('jsonArrTracker.length: ' + jsonArrTracker.length);
    debuglog('jsonArrTracker right before begin and end are updated: ' + JSON.stringify(jsonArrTracker, null, 2));

    // Cycle through every "slice" of the time domain, adding the requests for the entire time domain
    thisBegin = thisEnd + 1;
    thisEnd += duration + 1;
    if (thisEnd > end) {
      thisEnd = end;
    }

    // We can't let the jsonArr or the responses arrays to grow too big, so
    // we have to work them down as we submit requests.
    // Two things can trigger submitting the request:
    // 1) The jsonArr has grown too large
    // 2) The jsonArr may does not exceed the size threshold, but
    //    This is the final call to sendMetricReq() and on the final data-point,
    //    so this is the last opportunity to submit the request.
    if (numMBytes(jsonArr) > chunkMBytes || (thisBegin > thisEnd && lastPass)) {
      debuglog('sendMetricReq jsonArr size MB: ' + numMBytes(jsonArr));
      debuglog('sendMetricReq responses size MB: ' + numMBytes(responses));
      const theseResponses = await esJsonArrRequest(instance, 'metric_data', '/_msearch', jsonArr, yearDotMonth);
      responses.push(...theseResponses);
      jsonArr.length = 0; // No longer needed since we have the response; Deleting to save memory.

      // Now that there are some responses available, we process those so we can also delete the data
      // in the reeponses array.  The elements in the responses array can get very big relative
      // to the data calculated and stored in the valueSets.
      // Note: the number of elements on responses should be exaclty half the number of
      // elements in jsonArr, because each request in the jsonArr uses one entry for the index
      // and another entry for the query, which generates a single entry in the responses array.
      debuglog('sendMetricReq jsonArrTracker.length:' + jsonArrTracker.length);
      debuglog('sendMetricReq jsonArrTracker:' + JSON.stringify(jsonArrTracker, null, 2));
      debuglog('jsonArrIdx:' + jsonArrIdx);
      while (jsonArrIdx < responses.length * 2) {
        const setIdx = jsonArrTracker[jsonArrIdx / 2]['set'];
        const label = jsonArrTracker[jsonArrIdx / 2]['label'];
        debuglog('sendMetricReq setIdx: [' + setIdx + ']  label: [' + label + ']');
        if (typeof valueSets[setIdx] == 'undefined') {
          valueSets[setIdx] = {};
        }
        if (typeof valueSets[setIdx][label] == 'undefined') {
          valueSets[setIdx][label] = [];
        }
        jsonArrIdx = calcAvg(
          jsonArrTracker[jsonArrIdx / 2]['begin'],
          jsonArrTracker[jsonArrIdx / 2]['end'],
          responses,
          jsonArrIdx,
          jsonArrTracker,
          jsonArrTracker[jsonArrIdx / 2]['numMetricIds'],
          valueSets[setIdx][label]
        );
      }
    }

    if (thisBegin > thisEnd) {
      // why not thisBegin > end ?
      break;
    }
  }

  debuglog('sendMetricReq end, jsonArr MB [' + numMBytes(jsonArr) + ']  responses MB [' + numMBytes(responses) + ']');
  debuglog('sendMetricReq end, lastPass: ' + lastPass);
};

calcAvg = function (thisBegin, thisEnd, responses, jsonArrIdx, jsonArrTracker, numMetricIds, values) {
  debuglog('calcAvg start');
  debuglog(
    'calcAvg jsonArrIdx: [' +
      jsonArrIdx +
      ']  thisBegin: [' +
      thisBegin +
      ']  thisEnd: [' +
      thisEnd +
      ']  numMetricIds: [' +
      numMetricIds +
      ']'
  );

  var timeWindowDuration = thisEnd - thisBegin + 1;
  var totalWeightTimesMetrics = timeWindowDuration * numMetricIds;
  var aggAvg;
  var aggWeight;
  var aggAvgTimesWeight;
  var newWeight;
  debuglog('calcAvg responses[' + jsonArrIdx / 2 + ']:' + JSON.stringify(responses[jsonArrIdx / 2], null, 2));
  aggAvg = responses[jsonArrIdx / 2].aggregations.metric_avg.value;
  if (typeof aggAvg != 'undefined') {
    // We have the weighted average for documents that don't overlap the time range,
    // but we need to combine that with the documents that are partially outside
    // the time range.  We need to know the total weight from the documents we
    // just finished in order to add the new documents and recompute the new weighted
    // average.
    aggWeight = responses[jsonArrIdx / 2 + 1].aggregations.total_weight.value;
    aggAvgTimesWeight = aggAvg * aggWeight;
  } else {
    // It is possible that the aggregation returned no results because all of the documents
    // were partially outside the time domain.  This can happen when
    //  1) A  metric does not change during the entire test, and therefore only 1 document
    //  is created with a huge duration with begin before the time range and after after the
    //  time range.
    //  2) The time domain we have is really small because the resolution we are using is
    //  very big.
    //
    //  In eithr case, we have to set the average and total_weight to 0, and then the
    //  recompuation of the weighted average [with the last two requests in this set, finding
    //  all of th docs that are partially in the time domain] will work.
    aggAvg = 0;
    aggWeight = 0;
    aggAvgTimesWeight = 0;
  }

  // Process the remaining responses in the 'set'.  These are typically 2 or more documents.
  // Since these docs have a time range partially outside the time range we want,
  // we have to get a new, reduced duration and use that to agment our weighted average.
  var sumValueTimesWeight = 0;
  var sumWeight = 0;
  // It is possible to have the same document returned from these remaining queries.
  // This can happen when the document's begin is before $this_begin *and* the document's end
  // if after $this_end.
  // You must not process the document twice.  Perform a consolidation by organizing by the
  //  returned document's '_id'
  var partialDocs = {};
  var k;
  delete responses[jsonArrIdx / 2];
  delete responses[jsonArrIdx / 2 + 1];
  delete jsonArrTracker[jsonArrIdx / 2];
  delete jsonArrTracker[jsonArrIdx / 2 + 1];
  jsonArrIdx += 4; //advance to the non-aggreation responses
  // There can be 1 to many multiples of 2 of these types of responses here.
  // We know these type of responses have ended when the next response does
  // have an aggregation in it.
  while (jsonArrIdx / 2 < responses.length && !Object.keys(responses[jsonArrIdx / 2]).includes('aggregations')) {
    if (responses[jsonArrIdx / 2].hits.total.value !== responses[jsonArrIdx / 2].hits.hits.length) {
      console.log(
        'WARNING! getMetricDataFromIdsSets() responses[' +
          (jsonArrIdx / 2 + k) +
          '].hits.total.value (' +
          responses[jsonArrIdx / 2].hits.total.value +
          ') and responses[' +
          jsonArrIdx / 2 +
          '].hits.hits.length (' +
          responses[jsonArrIdx / 2].hits.hits.length +
          ') are not equal, which means the retured data is probably incomplete'
      );
    }
    responses[jsonArrIdx / 2].hits.hits.forEach((element) => {
      partialDocs[element._id] = {};
      Object.keys(element._source.metric_data).forEach((key) => {
        partialDocs[element._id][key] = element._source.metric_data[key];
      });
    });
    delete responses[jsonArrIdx / 2];
    delete jsonArrTracker[jsonArrIdx / 2];
    jsonArrIdx += 2;
  }
  // Now we can process the partialDocs
  Object.keys(partialDocs).forEach((id) => {
    //var docDuration = partialDocs[id].duration;
    var docDuration = partialDocs[id].end - partialDocs[id].begin;
    if (partialDocs[id].begin < thisBegin) {
      docDuration -= thisBegin - partialDocs[id].begin;
    }
    if (partialDocs[id].end > thisEnd) {
      docDuration -= partialDocs[id].end - thisEnd;
    }
    var valueTimesWeight = partialDocs[id].value * docDuration;
    sumValueTimesWeight += valueTimesWeight;
    sumWeight += docDuration;
  });
  var result = (aggAvgTimesWeight + sumValueTimesWeight) / totalWeightTimesMetrics;
  result *= numMetricIds;
  var dataSample = {};
  dataSample.begin = thisBegin;
  dataSample.end = thisEnd;
  dataSample.value = result;
  values.push(dataSample);

  return jsonArrIdx;
};

// From a set of metric_desc ID's, return 1 or more values depending on resolution.
// For each metric ID, there should be exactly 1 metric_desc doc and at least 1 metric_data docs.
// A metric_data doc has a 'value', a 'begin' timestamp, and and 'end' timestamp (also a
// 'duration' to make weighted avgerage queries easier).
// The begin-end time range represented in a metric_data doc are inclusive, and the
// granularity is 1 millisecond.
// For any ID, there should be enough metric_data docs with that ID that have the function's
// 'begin' and 'end' time domain represented with no gap or overlaps.  For example, if this
// function is called with begin=5 and end=1005, and there are 2 metric_data documents [having the same
// metric_id in metricIds], and their respective (begin,end) are (0,500) and (501,2000),
// then there are enough metric_data documents to compute the results.
getMetricDataFromIdsSets = async function (instance, sets, metricGroupIdsByLabelSets, yearDotMonth) {
  var jsonArr = []; // What is used to submit metric query requests in bulk
  var jsonArrTracker = []; // Detailed iInfo (set, label, begin, end) about each element in jsonArr
  var jsonArrIdx = 0; // Index of next element in jsonArr that needs its response processed
  var responses = []; // Ordered responses for jsonArr
  var valueSets = [];
  var reqSize = 0;
  var count = 0;
  for (var idx = 0; idx < metricGroupIdsByLabelSets.length; idx++) {
    const sortedKeys = Object.keys(metricGroupIdsByLabelSets[idx]).sort();
    for (var k = 0; k < sortedKeys.length; k++) {
      const label = sortedKeys[k];
      debuglog('label: [' + label + ']');
      var metricIds = metricGroupIdsByLabelSets[idx][label];
      if (typeof sets[idx].begin == 'undefined') {
        console.log('ERROR: sets.[' + idx + '].begin is not defined:\n' + JSON.stringify(sets[idx]), null, 2);
        process.exit(1);
      }
      var begin = Number(sets[idx].begin);
      if (isNaN(begin)) {
        console.log('ERROR: begin is not defined');
        process.exit(1);
      }
      if (typeof sets[idx].end == 'undefined') {
        console.log('ERROR: sets.[' + idx + '].end is not defined');
        process.exit(1);
      }
      var end = Number(sets[idx].end);
      var resolution = Number(sets[idx].resolution);
      var duration = Math.floor((end - begin) / resolution);

      const lastPass = idx + 1 >= metricGroupIdsByLabelSets.length && k + 1 >= sortedKeys.length;
      await sendMetricReq(
        jsonArr,
        jsonArrTracker,
        jsonArrIdx,
        responses,
        valueSets,
        idx,
        label,
        lastPass,
        instance,
        begin,
        end,
        resolution,
        metricIds,
        yearDotMonth
      );
    }
  }
  return valueSets;
};

exports.getMetricDataFromIdsSets = getMetricDataFromIdsSets;

getMetricData = async function (
  instance,
  runId,
  periId,
  source,
  type,
  begin,
  end,
  resolution,
  breakout,
  filter,
  yearDotMonth
) {
  var sets = [];
  var thisSet = {
    run: runId,
    period: periId,
    source: source,
    type: type,
    begin: begin,
    end: end,
    resolution: resolution,
    breakout: breakout,
    filter: filter
  };
  sets.push(thisSet);
  var dataSets = await getMetricDataSets(instance, sets, yearDotMonth);
  return dataSets[0];
};
exports.getMetricData = getMetricData;

// Generates 1 or more values for 1 or more groups for a metric of a particular source
// (tool or benchmark) and type (iops, l2-Gbps, ints/sec, etc).
// - The breakout determines if the metric is broken out into groups -if it is empty,
//   there is only 1 group.
// - The resolution determines the number of values for each group.  If you just need
//   a single average for the metric group, the resolution should be 1.
// - perId is optional, but should be used for benchmark metrics, as those metrics are
//   attributed to a period
// - The begin and end control the time domain, and must be within the time domain
//   from this [benchmark-iteration-sample-]period (from doc which contains the periId)
//   *if* the metric is from a benchmark.  If you want to query for corresponding
//   tool data, use the same begin and end as the benchmark-iteration-sample-period.
getMetricDataSets = async function (instance, sets, yearDotMonth) {
  for (var i = 0; i < sets.length; i++) {
    // If a begin and end are not defined, get it from the period.begin & period.end.
    // If a begin and/or end are not defined, and the period is not defined, error out.
    // If a run is not defined, get it from the period.
    // If a run and period are not defined, error out.
    if (typeof sets[i].run == 'undefined') {
      if (typeof sets[i].period != 'undefined') {
        console.log('getRunFromPeriod');
        sets[i].run = await getRunFromPeriod(instance, sets[i].period, yearDotMonth);
      } else {
        console.log('ERROR: run and period was not defined');
      }
    }
    var periodRange;
    if (typeof sets[i].begin == 'undefined') {
      if (typeof sets[i].period != 'undefined') {
        periodRange = await getPeriodRange(instance, sets[i].period, yearDotMonth);
        sets[i]['begin'] = periodRange['begin'];
      } else {
        console.log('ERROR: begin is not defined and a period was not defined');
      }
    }
    if (typeof sets[i].end == 'undefined') {
      if (typeof sets[i].period != 'undefined') {
        if (typeof periodRange == 'undefined') {
          periodRange = await getPeriodRange(instance, sets[i].period, yearDotMonth);
        }
        sets[i].end = periodRange.end;
      } else {
        console.log('ERROR: end is not defined and a period was not defined');
      }
    }
    // In order for all metric queries to work, we must remove the period ID.
    // Not all metrics have a period associated with them.  Benchmark metrics do,
    // because they have mulitple periods (one of them being the primaryPeriod) in
    // which the data is collected.  Since tools run across all benchmark samples,
    // their data is not attributed to a specific period.
    //
    // Note that users often include a period when querying for a tool metric.
    // This is not because the metric has this period attributed to it.  It is simply
    // a convenience to limit the metric data to a specific time period.  So, we
    // will get calls to this function where a period is provided, but the metric desired
    // will *not* be found if the period ID is used on the query.  Therefore we must
    // always remove the period ID from each element in the input set.
    //
    // Perhaps eventually we can detect if the metric source & type are for a benchmark,
    // and if so, allow the period ID to remain in the query.
    delete sets[i].period;
  }

  var metricGroupIdsByLabelSets = await getMetricGroupsFromBreakouts(instance, sets, yearDotMonth);
  var dataSets = await getMetricDataFromIdsSets(instance, sets, metricGroupIdsByLabelSets, yearDotMonth);

  if (dataSets.length != sets.length) {
    console.log(
      'ERROR: number of generated data sets (' +
        dataSets.length +
        ') does not match the number of metric query sets (' +
        sets.length +
        ')'
    );
    return;
  }

  // Rearrange data to call getMetricNames
  var runIds = [];
  var sources = [];
  var types = [];
  for (var i = 0; i < sets.length; i++) {
    runIds[i] = sets[i].run;
    sources[i] = sets[i].source;
    types[i] = sets[i].type;
  }
  var setBreakouts = await mgetMetricNames(instance, runIds, sources, types, yearDotMonth);

  for (var i = 0; i < sets.length; i++) {
    // Rearrange the actual data into 'values' section
    Object.keys(dataSets[i]).forEach((label) => {
      if (typeof dataSets[i].values == 'undefined') {
        dataSets[i].values = {};
      }
      dataSets[i].values[label] = dataSets[i][label];
      delete dataSets[i][label];
    });
    // Build the label-decoder and the remaining breakouts
    dataSets[i].usedBreakouts = sets[i].breakout;
    dataSets[i].valueSeriesLabelDecoder = '';
    var regExp = /([^\=]+)\=([^\=]+)/;
    dataSets[i].usedBreakouts.forEach((field) => {
      var matches = regExp.exec(field);
      if (matches) {
        field = matches[1];
        value = matches[2];
      }
      dataSets[i].valueSeriesLabelDecoder += '-' + '<' + field + '>';
      //TODO: validate if user's breakouts are available by checking against data.breakouts
    });
    dataSets[i].valueSeriesLabelDecoder = dataSets[i].valueSeriesLabelDecoder.replace('-', '');
    // Breakouts already used should not show up in the list of avauilable breakouts
    dataSets[i].remainingBreakouts = setBreakouts[i].filter((n) => !dataSets[i].usedBreakouts.includes(n));
  }

  for (var i = 0; i < sets.length; i++) {
    var reg = /(\w+)\:([-+]?[0-9]*\.?[0-9]+)/;
    var m = reg.exec(sets[i].filter);
    if (sets[i].filter != null && m) {
      Object.keys(dataSets[i].values).forEach((metric) => {
        var metricValue = 1.0 * dataSets[i].values[metric][0].value;
        var condition = m[1];
        var value = m[2];
        if (
          !(
            (condition == 'gt' && metricValue > value) ||
            (condition == 'ge' && metricValue >= value) ||
            (condition == 'lt' && metricValue < value) ||
            (condition == 'le' && metricValue <= value)
          )
        ) {
          delete dataSets[i].values[metric];
        }
      });
    }
  }
  return dataSets;
};
exports.getMetricDataSets = getMetricDataSets;

async function waitForDeletedDocs(instance, runId, docTypes, yearDotMonth) {
  var numAttempts = 1;
  var maxAttempts = 30;
  var remainingDocTypes = docTypes;
  var totalDocCount = 0;
  var previousTotalDocCount = 0;
  var interval = 3;
  while (numAttempts <= maxAttempts && docTypes.length > 0) {
    let promise = new Promise((resolve, reject) => {
      setTimeout(() => resolve('done!'), interval * 1000);
    });
    let result = await promise;

    totalDocCount = 0;
    docWaitStr = '';
    for (let i = 0; i < docTypes.length; i++) {
      var thisNumDocs = getDocCount(instance, runId, docTypes[i], yearDotMonth);
      if (thisNumDocs > 0) {
        //console.log('  ' + docTypes[i] + ': doc count: ' + thisNumDocs);
        docWaitStr += '  ' + docTypes[i] + ': doc count: ' + thisNumDocs + '\n';
        totalDocCount += thisNumDocs;
      }
      if (thisNumDocs == 0) {
        remainingDocTypes = remainingDocTypes.filter((val) => val !== docTypes[i]);
      }
    }
    if (previousTotalDocCount != 0) {
      console.log('Document deletion rate: ' + (previousTotalDocCount - totalDocCount) / interval + ' documents/sec');
    }
    if (totalDocCount > 0) {
      console.log('\nWaiting for the following documents to be deleted in OpenSearch (attempt #' + numAttempts + ')');
      console.log(docWaitStr);
    }
    docTypes = remainingDocTypes;
    numAttempts++;

    previousTotalDocCount = totalDocCount;
  }
  console.log('');
  return docTypes.lenth;
}
exports.waitForDeletedDocs = waitForDeletedDocs;
