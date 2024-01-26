//# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript
var request = require('sync-request');
var bigQuerySize = 262144;

function getIndexBaseName() {
  return 'cdmv7dev-';
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
function consolidateAllArrays(a) {
  const c = [];
  a.forEach((b) => {
    b.forEach((e) => {
      if (!c.includes(e)) c.push(e);
    });
  });
  return c;
}

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

function esRequest(host, idx, q) {
  var url = 'http://' + host + '/' + getIndexBaseName() + idx;
  // The var q can be an object or a string.  If you are submitting NDJSON
  // for a _msearch, it must be a [multi-line] string.
  if (typeof q === 'object') {
    q = JSON.stringify(q);
  }
  var resp = request('POST', url, {
    body: q,
    headers: { 'Content-Type': 'application/json' }
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
mSearch = function (url, index, termKeys, values, source, aggs, size, sort) {
  if (typeof termKeys !== typeof []) return;
  if (typeof values !== typeof []) return;
  var ndjson = '';
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
    ndjson += '{}\n' + JSON.stringify(req) + '\n';
  }
  var resp = esRequest(url, index + '/_msearch', ndjson);
  var data = JSON.parse(resp.getBody());

  // Unpack response and organize in array of arrays
  var retData = [];
  for (var i = 0; i < data.responses.length; i++) {
    // For queries with aggregation
    if (
      typeof data.responses[i].aggregations !== 'undefined' &&
      Array.isArray(data.responses[i].aggregations.source.buckets)
    ) {
      if (data.responses[i].aggregations.source.sum_other_doc_count > 0) {
        console.log(
          'WARNING! msearch aggregation returned sum_other_doc_count > 0, which means not all terms were returned.  This query needs a larger "size"'
        );
      }
      // Assemble the keys from the bucket for this query (i)
      var keys = [];
      data.responses[i].aggregations.source.buckets.forEach((element) => {
        keys.push(element.key);
      });
      retData[i] = keys;

      // For queries without aggregation
    } else {
      if (data.responses[i].hits == null) {
        console.log('WARNING! msearch returned data.responses[' + i + '].hits is NULL');
        console.log(JSON.stringify(data.responses[i], null, 2));
        return;
      }
      if (Array.isArray(data.responses[i].hits.hits) && data.responses[i].hits.hits.length > 0) {
        if (
          data.responses[i].hits.total.value !== data.responses[i].hits.hits.length &&
          req.size != data.responses[i].hits.hits.length
        ) {
          console.log(
            'WARNING! msearch(size: ' +
              size +
              ') data.responses[' +
              i +
              '].hits.total.value (' +
              data.responses[i].hits.total.value +
              ') and data.responses[' +
              i +
              '].hits.hits.length (' +
              data.responses[i].hits.hits.length +
              ') are not equal, which means the retured data is probably incomplete'
          );
        }
        var ids = [];
        data.responses[i].hits.hits.forEach((element) => {
          // A source of "x.y" <string> must be converted to reference the object
          // For example, a source (string) of "metric_desc.id" needs to reference metric_desc[id]
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
        //console.log("WARNING: no hits for request:\nquery:\n" + ndjson + "\nresponse:\n" + JSON.stringify(data));
      }
    }
  }
  return retData;
};
exports.mSearch = mSearch;

// Functions starting with mget use msearch, and require 1D array of values and return a 1D array or results
// Functions starting with get are just for legacy support, where caller expects to provide a single value,
// but these functions just wrap the value in 2D array for msearch (and a key in a 1D array).  Effectively
// all query functions use msearch, even if there is a single query.

mgetPrimaryMetric = function (url, iterations) {
  var metrics = mSearch(url, 'iteration', ['iteration.id'], [iterations], 'iteration.primary-metric');
  // mSearch returns a list of values for each query, so 2D array.  We only have exactly 1 primary-metric
  // for each iteration, so collapse the 2D array into a 1D array, 1 element per iteration.
  var primaryMetrics = [];
  for (var i = 0; i < metrics.length; i++) {
    primaryMetrics[i] = metrics[i][0];
  }
  return primaryMetrics;
};
exports.mgetPrimaryMetric = mgetPrimaryMetric;
getPrimaryMetric = function (url, iteration) {
  return mgetPrimaryMetric(url, [iteration])[0][0];
};
exports.getPrimaryMetric = getPrimaryMetric;

mgetPrimaryPeriodName = function (url, iterations) {
  var data = mSearch(url, 'iteration', ['iteration.id'], [iterations], 'iteration.primary-period');
  // There can be only 1 period-name er iteration, therefore no need for a period name per period [of the same iteration]
  // Therefore, we do not need to return a 2D array
  var periodNames = [];
  for (var i = 0; i < data.length; i++) {
    periodNames[i] = data[i][0];
  }
  return periodNames;
};
exports.mgetPrimaryPeriodName = mgetPrimaryPeriodName;
getPrimaryPeriodName = function (url, iteration) {
  return mgetPrimaryPeriodName(url, [iteration])[0][0];
};
exports.getPrimaryPeriodName = getPrimaryPeriodName;

mgetSamples = function (url, iters) {
  return mSearch(url, 'sample', ['iteration.id'], [iters], 'sample.id');
};
exports.mgetSamples = mgetSamples;
getSamples = function (url, iter) {
  return mgetSamples(url, [iter])[0];
};
exports.getSamples = getSamples;

// For a specific metric-source and metric-type,
// find all the metadata names shared among all
// found metric docs.  These names are what can be
// used for "breakouts".
mgetMetricNames = function (url, runIds, sources, types) {
  return mSearch(
    url,
    'metric_desc',
    ['run.id', 'metric_desc.source', 'metric_desc.type'],
    [runIds, sources, types],
    '',
    { source: { terms: { field: 'metric_desc.names-list', size: bigQuerySize } } },
    0
  );
};
exports.mgetMetricNames = mgetMetricNames;
getMetricNames = function (url, runId, source, type) {
  return mgetMetricNames(url, [runId], [source], [type]);
};

mgetSampleStatus = function (url, Ids) {
  var sampleIds = [];
  var perSamplePeriNames = [];
  var idx = 0;
  for (var i = 0; i < Ids.length; i++) {
    for (j = 0; j < Ids[i].length; j++) {
      sampleIds[idx] = Ids[i][j];
      idx++;
    }
  }

  var data = mSearch(url, 'sample', ['sample.id'], [sampleIds], 'sample.status', null, 1);

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
exports.mgetSampleStatus = mgetSampleStatus;
getSampleStatus = function (url, sampId) {
  return mgetSampleStatus(url, [sampId])[0][0];
};
exports.getSampleStatus = getSampleStatus;

mgetPrimaryPeriodId = function (url, sampIds, periNames) {
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
  var data = mSearch(
    url,
    'period',
    ['sample.id', 'period.name'],
    [sampleIds, perSamplePeriNames],
    'period.id',
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
getPrimaryPeriodId = function (url, sampId, periName) {
  return mgetPrimaryPeriodId(url, [sampId], [periName])[0][0];
};
exports.getPrimaryPeriodId = getPrimaryPeriodId;

mgetPeriodRange = function (url, periodIds) {
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
  //console.log("Ids:\n" + JSON.stringify(Ids, null, 2));
  var data = mSearch(url, 'period', ['period.id'], [Ids], 'period', null, 1);
  //console.log("data:\n" + JSON.stringify(data, null, 2));
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
getPeriodRange = function (url, periId) {
  return mgetPeriodRange(url, [[periId]])[0][0];
};
exports.getPeriodRange = getPeriodRange;

mgetMetricDescs = function (url, runIds) {
  return mSearch(url, 'metric_desc', ['run.id'], [runIds], 'metric_desc.id', null, bigQuerySize);
};
getMetricDescs = function (url, runId) {
  return mgetMetricDescs(url, [runId])[0];
};
exports.getMetricDescs = getMetricDescs;

mgetMetricDataDocs = function (url, metricIds) {
  return mSearch(url, 'metric_data', ['metric_desc.id'], [metricIds], '', null, bigQuerySize);
};
getMetricDataDocs = function (url, metricId) {
  return mgetMetricDataDocs(url, [metricId])[0];
};
exports.getMetricDataDocs = getMetricDataDocs;

mgetMetricTypes = function (url, runIds, metricSources) {
  return mSearch(
    url,
    'metric_desc',
    ['run.id', 'metric_desc.source'],
    [runIds, metricSources],
    null,
    { source: { terms: { field: 'metric_desc.type', size: 10000 } } },
    0
  );
};
exports.mgetMetricTypes = mgetMetricTypes;
getMetricTypes = function (url, runId, metricSource) {
  return mgetMetricTypes(url, [runId], [metricSources])[0];
};
exports.getMetricTypes = getMetricTypes;

mgetIterations = function (url, runIds) {
  return mSearch(url, 'iteration', ['run.id'], [runIds], 'iteration.id', null, 1000, [
    { 'iteration.num': { order: 'asc', numeric_type: 'long' } }
  ]);
};
getIterations = function (url, runId) {
  return mgetIterations(url, [runId])[0];
};
exports.getIterations = getIterations;

mgetTags = function (url, runIds) {
  return mSearch(url, 'tag', ['run.id'], [runIds], 'tag', null, 1000);
};
getTags = function (url, runId) {
  return mgetTags(url, [runId])[0];
};
exports.getTags = getTags;

mgetRunFromIter = function (url, iterIds) {
  return mSearch(url, 'iteration', ['iteration.id'], [iterIds], 'run.id', null, 1000);
};
getRunFromIter = function (url, iterId) {
  return mgetRunFromIter(url, [iterId])[0][0];
};
exports.getRunFromIter = getRunFromIter;

mgetRunFromPeriod = function (url, periIds) {
  return mSearch(url, 'period', ['period.id'], [periIds], 'run.id', null, 1);
};
getRunFromPeriod = function (url, periId) {
  return mgetRunFromPeriod(url, [periId])[0][0];
};
exports.getRunFromPeriod = getRunFromPeriod;

mgetParams = function (url, iterIds) {
  return mSearch(url, 'param', ['iteration.id'], [iterIds], 'param', null, 1000);
};
exports.mgetParams = mgetParams;
getParams = function (url, iterId) {
  return mgetParams(url, [iterId])[0];
};
exports.getParams = getParams;

mgetIterationDoc = function (url, iterIds) {
  return mSearch(url, 'iteration', ['iteration.id'], [iterIds], '', null, 1000);
};
getIterationDoc = function (url, iterId) {
  return mgetIterationDoc(url, [iterId])[0][0];
};
exports.getIterationDoc = getIterationDoc;

mgetBenchmarkNameFromIter = function (url, Ids) {
  return mSearch(url, 'iteration', ['iteration.id'], [Ids], 'run.benchmark', null, 1);
};
getBenchmarkNameFromIter = function (url, Id) {
  return mgetBenchmarkNameFromIter(url, [Id])[0][0];
};
exports.getBenchmarkNameFromIter = getBenchmarkNameFromIter;

mgetBenchmarkName = function (url, runIds) {
  return mSearch(url, 'run', ['run.id'], [runIds], 'run.benchmark', null, 1);
};
getBenchmarkName = function (url, runId) {
  return mgetBenchmarkName(url, [runId])[0][0];
};
exports.getBenchmarkName = getBenchmarkName;

mgetRunData = function (url, runIds) {
  return mSearch(url, 'run', ['run.id'], [runIds], '', null, 1000);
};
getRunData = function (url, runId) {
  return mgetRunData(url, [runId]);
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

mgetIterMetrics = function (url, iterationIds) {
  var results = {};
  var benchmarkNames = consolidateAllArrays(mgetBenchmarkNameFromIter(url, iterationIds));
  if (benchmarkNames.length !== 1) {
    console.log('ERROR: The benchmark-name for all iterations was not the same, includes: ' + benchmarkNames);
    process.exit(1);
  }
  var primaryMetrics = consolidateAllArrays(mgetPrimaryMetric(url, iterationIds));
  if (primaryMetrics.length !== 1) {
    console.log('ERROR: The primary-metric for all iterations was not the same, includes: ' + primaryMetrics);
    process.exit(1);
  }
  var primaryPeriodNames = consolidateAllArrays(mgetPrimaryPeriodName(url, iterationIds));
  if (primaryPeriodNames.length !== 1) {
    console.log('ERROR: The primary-period-name for all iterations was not the same, includes: ' + primaryPeriodNames);
    process.exit(1);
  }
  // Find all of the passing samples, then all of the primary-periods, then get the metric for all of them in one request
  //console.log("mgetSamples");
  var samples = mgetSamples(url, iterationIds); // Samples organized in 2D array, first dimension matching iterationIds
  //console.log("\nsamples:\n" + JSON.stringify(samples, null, 2));
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
  //console.log("iterIdFromSample:\n" + JSON.stringify(iterIdFromSample, null, 2));
  var consSamples = consolidateAllArrays(samples); // All sample IDs flattened into 1 array
  var consSamplesStatus = mgetSampleStatus(url, consSamples);
  var consPassingSamples = []; // Only passing samples in flattened array
  for (i = 0; i < consSamplesStatus.length; i++) {
    if (consSamplesStatus[i] == 'pass') consPassingSamples.push(consSamples[i]);
  }
  //console.log("\nconsPassingSamples: " + consPassingSamples);
  //console.log("mgetPrimaryperiodId");
  var primaryPeriodIds = mgetPrimaryPeriodId(url, consPassingSamples, primaryPeriodNames);
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
  //console.log("\nconsPrimaryPeriodIds: " + JSON.stringify(consPrimaryPeriodIds, null , 2));
  //console.log("mgetPeriodRange");
  var periodRanges = mgetPeriodRange(url, consPrimaryPeriodIds);
  //console.log("\nperiodRanges: " + JSON.stringify(periodRanges, null, 2));
  // Create the sets for getMetricDataSets
  var sets = [];
  var periodsByIteration = {};
  for (i = 0; i < consPrimaryPeriodIds.length; i++) {
    periodId = consPrimaryPeriodIds[i];
    //console.log("period: " + periodId + "  periodRanges[" + i + "]: " + JSON.stringify(periodRanges[i]));
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
  //console.log("\ngetMetricDataSets for " + sets.length + " sets");
  // Returned data should be in same order as consPrimaryPeriodIds
  //console.log("getMetricDataSets");
  var metricDataSets = getMetricDataSets(url, sets);
  //console.log("calcIterMetrics");
  //console.log("\nMetricDataSets.length: " + metricDataSets.length);
  // Build per-iteration results
  var period = consPrimaryPeriodIds[0];
  var sample = sampleIdFromPeriod[period];
  var iter = iterIdFromSample[sample];
  var vals = [];
  // Below relies on the expectation that periods for the same sample are stored contiguously in consPrimaryPeriodIds array
  for (i = 0; i < consPrimaryPeriodIds.length; i++) {
    //console.log("i: " + i);
    period = consPrimaryPeriodIds[i];
    //console.log("period: " + period);
    sample = sampleIdFromPeriod[period];
    //console.log("sample: " + sample);
    //console.log("                    iter: " + iter);
    //console.log("iterIdFromSample[sample]: " + iterIdFromSample[sample]);
    nextIter = iterIdFromSample[sample];
    //if (iter !== iterIdFromSample[sample]) {
    if (iter !== nextIter) {
      // detected next iteration, calc current iteration's metrics
      //console.log("vals:\n" + JSON.stringify(vals));
      var thisResult = calcIterMetrics(vals);
      //console.log(JSON.stringify(thisResult));
      results[iter] = thisResult;
      //console.log("results length is now: " + Object.keys(results).length);
      // now switch to new iteration
      //console.log("switching from iteration " + iter + " to iteration " + nextIter);
      iter = nextIter;
      vals = [];
    }
    //console.log("Getting val from metricDataSets[" + i + "]:\n" + JSON.stringify(metricDataSets[i], null, 2));
    // metricDataSets can return metrics with multiple labels, and for each of those, multiple data-samples.
    // In this case, we are expecting a blank label since there is no metric-breakout, and exactly 1 data-sample.
    //console.log("value: [" + metricDataSets[i][''][0].value + "]");
    vals.push(metricDataSets[i][''][0].value);
  }
  //console.log("vals:\n" + JSON.stringify(vals));
  var thisResult = calcIterMetrics(vals);
  //console.log(JSON.stringify(thisResult));
  results[iter] = thisResult;
  //console.log("results length is now: " + Object.keys(results).length);
  //console.log("mgetIterMetrics complete");
  return results;
};
exports.mgetIterMetrics = mgetIterMetrics;

getIterMetrics = function (url, iterId) {
  mgetIterMetrics(url, [iterId]);
};

deleteDocs = function (url, docTypes, q) {
  docTypes.forEach((docType) => {
    //console.log("deleteDocs() query:\n" + JSON.stringify(q, null, 2));
    var resp = esRequest(url, docType + '/_delete_by_query', q);
    var data = JSON.parse(resp.getBody());
  });
};
exports.deleteDocs = deleteDocs;

// Delete all the metric (metric_desc and metric_data) for a run
// TODO: probably should implment mDeleteMetrics with array of runIds for input
deleteMetrics = function (url, runId) {
  var ids = getMetricDescs(url, runId);
  //console.log("There are " + ids.length + " metric_desc docs");
  var q = { query: { bool: { filter: { terms: { 'metric_desc.id': [] } } } } };
  ids.forEach((element) => {
    var term = { 'metric_desc.id': element };
    q['query']['bool']['filter']['terms']['metric_desc.id'].push(element);
    if (q['query']['bool']['filter']['terms']['metric_desc.id'].length >= 1000) {
      //console.log("deleting " + q['query']['bool']['filter']['terms']["metric_desc.id"].length + " metrics");
      deleteDocs(url, ['metric_data', 'metric_desc'], q);
      q['query']['bool']['filter']['terms']['metric_desc.id'] = [];
    }
  });
  var remaining = q['query']['bool']['filter']['terms']['metric_desc.id'].length;
  if (remaining > 0) {
    //console.log("deleting " + q['query']['bool']['filter']['terms']["metric_desc.id"].length + " metrics");
    deleteDocs(url, ['metric_data', 'metric_desc'], q);
  }
};
exports.deleteMetrics = deleteMetrics;

// For comparing N iterations across 1 or more runs.
buildIterTree = function (
  url,
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
          url,
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
          url,
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
    //var result = getIterMetrics(url, id);
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
getIters = function (
  url,
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
  // 1) Get run.ids from age + benchmark + tag filters
  // 2) From run.ids, get iteration.ids
  // 3) Get iteration.ids from age + benchmark + param filters
  // 4) Intersect iters from #2 and #3
  // 5) Build iteration lookup tables by param and by tag

  const now = Date.now();
  var intersectedRunIds = [];
  var ndjson = '';
  var ndjson2 = '';
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
    _source: 'run.id',
    size: bigQuerySize
  };
  var base_q_json = JSON.stringify(base_q);

  // Each filter of tagName:tagVal must be a separate query.
  // However, all of these queries can be submitted together via msearch.
  // The responses (a list of run.ids for each query) must be intersected
  // to have only the run.ids that match *all* tag filters.
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
      ndjson += '{"index": "' + getIndexBaseName() + 'tag' + '" }\n';
      ndjson += JSON.stringify(tag_query) + '\n';
    } else {
      // Find the run IDs which have this tag name present (value does not matter)
      ndjson2 += '{"index": "' + getIndexBaseName() + 'tag' + '" }\n';
      ndjson2 += JSON.stringify(tag_query) + '\n';
    }
  });

  if (ndjson != '') {
    var resp = esRequest(url, 'tag/_msearch', ndjson);
    var data = JSON.parse(resp.getBody());
    var runIds = [];
    data.responses.forEach((response) => {
      var theseRunIds = [];
      response.hits.hits.forEach((run) => {
        theseRunIds.push(run._source.run.id);
      });
      runIds.push(theseRunIds);
    });
    var intersectedRunIds = intersectAllArrays(runIds);

    if (ndjson2 != '') {
      var resp2 = esRequest(url, 'tag/_msearch', ndjson2);
      var data2 = JSON.parse(resp2.getBody());
      data2.responses.forEach((response) => {
        response.hits.hits.forEach((run) => {
          if (intersectedRunIds.includes(run._source.run.id)) {
            var index = intersectedRunIds.indexOf(run._source.run.id);
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
  // Now we can get all of the iterations for these run.ids
  var iterIdsFromRun = getIterations(url, intersectedRunIds);

  // Next, we must find the iterations that match the params filters.
  // We are trying to find iterations that have *all* params filters matching, not just one.
  // Each filter of paramArg:paramVal must be a separate query.
  // However, all of these queries can be submitted together via msearch.
  // The responses (a list of iteration.ids for each query) must be intersected
  // to have only the iteration.ids that match all param filters.
  console.log('Get all iterations from ' + filterByParams.length + ' param filters');
  ndjson = '';
  filterByParams.forEach((argval) => {
    var param_query = JSON.parse(base_q_json);
    var arg = argval.split(':')[0];
    var val = argval.split(':')[1];
    param_query._source = 'iteration.id';
    var paramArg = { term: { 'param.arg': arg } };
    param_query.query.bool.filter.push(paramArg);
    if (val != 'param-not-used') {
      var paramVal = { term: { 'param.val': val } };
      param_query.query.bool.filter.push(paramVal);
      ndjson += '{"index": "' + getIndexBaseName() + 'param' + '" }\n';
      ndjson += JSON.stringify(param_query) + '\n';
    } else {
      // Find the run IDs which have this param name present (value does not matter).
      // Later, we will subtract these iteration IDs from the ones found with ndjson query.
      ndjson2 += '{"index": "' + getIndexBaseName() + 'param' + '" }\n';
      ndjson2 += JSON.stringify(param_query) + '\n';
    }
  });

  var iterIdsFromParam = [];
  if (ndjson != '') {
    var resp = esRequest(url, 'param/_msearch', ndjson);
    var data = JSON.parse(resp.getBody());
    var iterationIds = [];
    data.responses.forEach((response) => {
      var theseIterationIds = [];
      response.hits.hits.forEach((iteration) => {
        theseIterationIds.push(iteration._source.iteration.id);
      });
      iterationIds.push(theseIterationIds);
    });
    iterIdsFromParam = intersectAllArrays(iterationIds);

    if (ndjson2 != '') {
      var resp2 = esRequest(url, 'tag/_msearch', ndjson2);
      var data2 = JSON.parse(resp2.getBody());
      data2.responses.forEach((response) => {
        response.hits.hits.forEach((hit) => {
          if (iterIdsFromParam.includes(hit._source.iteration.id)) {
            var index = iterIdsFromParam.indexOf(hit._source.iteration.id);
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
    //var ids = getIterations(url, [{ "terms": { "run.id": addRuns }}]);
    var ids = getIterations(url, addRuns);
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
  var iterRunIds = mgetRunFromIter(url, allIterIds);
  //console.log("runIds from Iters:\n" + JSON.stringify(runIds, null, 2));
  var iterTags = mgetTags(url, iterRunIds);
  var allTagNames = getObjVals(consolidateAllArrays(iterTags), 'name');
  console.log('allTagNames:\n' + JSON.stringify(allTagNames, null, 2));
  console.log('Finding all param args');
  var iterParams = mgetParams(url, allIterIds);
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
    //console.log("\niterId: " + iter);
    //var params = getParams(url, iter);
    var params = iterParams[j];
    //console.log("params:\n" + JSON.stringify(params, null, 2));
    // Need to consolidate multiple params with same arg but different values
    var paramIdx = {};
    var l = params.length;
    for (var i = 0; i < l; i++) {
      var arg = params[i].arg;
      if (typeof paramIdx[arg] !== 'undefined') {
        // This param arg was already found, combine this value with exiting param
        var existing_arg_idx = paramIdx[arg];
        //console.log("i: " + i + "  This param arg (" + arg + ") was already found (idx: " + existing_arg_idx + "), combine this value: (" + JSON.stringify(params[i]) + "), with existing one (" + JSON.stringify(params[existing_arg_idx]) + ")");
        params[existing_arg_idx]['val'] += '_' + params[i]['val'];
        params.splice(i, 1);
        l--;
        i--;
      } else {
        //console.log("Adding arg: " + arg + " to paramIdx[" + arg + "]:" + i);
        paramIdx[arg] = i;
      }
    }
    //console.log("updated params:\n" + JSON.stringify(params, null, 2));
    //var runId = getRunFromIter(url, iter);
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
          //console.log("Removing " + name + " from allTagNames");
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
          //console.log("Removing " + arg + " from allParamArgs");
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
    //console.log("checking all iterations for tag " + name);
    for (var i = 0; i < iterations.length; i++) {
      var iterId = iterations[i]['iterId'];
      var foundTag = false;
      for (var j = 0; j < iterations[i]['tags'].length; j++) {
        if (iterations[i]['tags'][j]['name'] == name) {
          //console.log("Found tag " + name);
          foundTag = true;
        }
      }
      if (foundTag == false) {
        var newTag = { name: name, val: 'tag-not-used' };
        //console.log("Did not find tag " + name + ", so adding with val: tag-not-used");
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
          //console.log("Found param " + arg);
          foundParam = true;
        }
      }
      if (foundParam == false) {
        var newParam = { arg: arg, val: 'param-not-used' };
        //console.log("Did not find param " + arg + ", so adding with val: param-not-used");
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
  var results = mgetIterMetrics(url, allIterIds);
  //console.log("results:\n" + JSON.stringify(results, null, 2));

  //console.log("Build iterTree");
  iterTree = buildIterTree(
    url,
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

exports.getMetricSources = function (url, runId) {
  var q = {
    query: { bool: { filter: [{ term: { 'run.id': runId } }] } },
    aggs: {
      source: { terms: { field: 'metric_desc.source', size: bigQuerySize } }
    },
    size: 0
  };
  //console.log("Q:\n" + JSON.stringify(q, null, 2));
  var resp = esRequest(url, 'metric_desc/_search', q);
  var data = JSON.parse(resp.getBody());
  if (Array.isArray(data.aggregations.source.buckets)) {
    var sources = [];
    data.aggregations.source.buckets.forEach((element) => {
      sources.push(element.key);
    });
    return sources;
  }
};

exports.getDocCount = function (url, runId, docType) {
  var q = { query: { bool: { filter: [{ term: { 'run.id': runId } }] } } };
  var resp = esRequest(url, docType + '/_count', q);
  var data = JSON.parse(resp.getBody());
  return data.count;
};

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

mgetMetricIdsFromTerms = function (url, termsSets) {
  // termsSets is an array of:
  // { 'period': x, 'run': y, 'termsByLabel': {} }
  // termsByLabel is a dict/hash of:
  // { <label>: sring }
  var ndjson = '';
  var totalReqs = 0;
  for (i = 0; i < termsSets.length; i++) {
    //console.log("mgetMetricIdsFromTerms():  termsSets[" + i + "]:\n" + JSON.stringify(termsSets[i], null, 2));
    var periId = termsSets[i].period;
    var runId = termsSets[i].run;
    Object.keys(termsSets[i].termsByLabel)
      .sort()
      .forEach((label) => {
        //console.log("mgetMetricIdsFromTerms():  label: '" + label + "'  terms string: '" + termsSets[i].termsByLabel[label] + "'");
        var terms_string = termsSets[i].termsByLabel[label];
        var q = {
          query: { bool: { filter: JSON.parse('[' + terms_string + ']') } },
          _source: 'metric_desc.id',
          size: bigQuerySize
        };
        if (periId == null && runId == null) {
          console.log('ERROR: mgetMetricIdsFromTerms(), terms[' + i + ']  must have either a period-id or run-id\n');
          return;
        }
        if (periId != null) {
          q.query.bool.filter.push(JSON.parse('{"term": {"period.id": "' + periId + '"}}'));
        }
        if (runId != null) {
          q.query.bool.filter.push(JSON.parse('{"term": {"run.id": "' + runId + '"}}'));
        }
        ndjson += '{}\n' + JSON.stringify(q) + '\n';
        totalReqs++;
      });
  }
  //console.log("mgetMetricIdsFromTerms(): ndjson:\n" + ndjson + "\n");
  var resp = esRequest(url, 'metric_desc/_msearch', ndjson);
  var data = JSON.parse(resp.getBody());
  if (totalReqs != data.responses.length) {
    console.log('mgetMetricIdsFromTerms(): ERROR, number of _msearch responses did not match number of requests');
    return;
  }
  if (data.responses == null) {
    console.log('ERROR: data.responses is null');
    return;
  }

  //console.log("data:\n" + JSON.stringify(data, null, 2));
  // Process the responses and assemble metric IDs into array
  //console.log("\nmgetMetricIdsFromTerms():  termsSets.length: " + termsSets.length);
  var metricIdsSets = []; // eventual length = termsSets
  var count = 0;
  for (i = 0; i < termsSets.length; i++) {
    //console.log("\nmgetMetricIdsFromTerms():  i: " + i);
    var thisMetricIds = {};
    Object.keys(termsSets[i].termsByLabel)
      .sort()
      .forEach((label) => {
        //console.log("mgetMetricIdsFromTerms():  label: " + label);
        //console.log("mgetMetricIdsFromTerms():  count: " + count);
        thisMetricIds[label] = [];
        if (data.responses[i] == null) {
          console.log('ERROR: data.responses[' + i + '] is null');
          console.log('data.responses.length:' + data.responses.length);
          console.log('data.responses:\n' + JSON.stringify(data.responses, null, 2));
          console.log('termsSets.length: ' + termsSets.length);
          console.log('totalReqs: ' + totalReqs);
          console.log('query:\n' + ndjson);
          process.exit(1);
        }
        if (data.responses[i].hits == null) {
          console.log('ERROR: data.responses[' + i + '].hits is null');
          console.log('data.responses[' + i + ']:\n' + JSON.stringify(data.responses[i], null, 2));
          console.log('termsSets.length: ' + termsSets.length);
          console.log('totalReqs: ' + totalReqs);
          console.log('query:\n' + ndjson);
          process.exit(1);
        }
        if (data.responses[i].hits.total.value >= bigQuerySize || data.responses[i].hits.hits.length >= bigQuerySize) {
          console.log('ERROR: hits from returned query exceeded max size of ' + bigQuerySize);
          process.exit(1);
        }
        //console.log("mgetMetricIdsFromTerms():  data.responses[" + count + "]:\n" + JSON.stringify(data.responses[count], null, 2));
        //console.log("mgetMetricIdsFromTerms():  data.responses[" + count + "].hits.hits.length: " + data.responses[count].hits.hits.length);
        for (j = 0; j < data.responses[count].hits.hits.length; j++) {
          //console.log("mgetMetricIdsFromTerms():  data.responses[" + count + "].hits.hits[" + j + "]:\n" + JSON.stringify(data.responses[count].hits.hits[j], null, 2));
          //console.log("mgetMetricIdsFromTerms():  adding " + data.responses[count].hits.hits[j]._source.metric_desc.id);
          thisMetricIds[label].push(data.responses[count].hits.hits[j]._source.metric_desc.id);
        }
        count++;
      });
    metricIdsSets.push(thisMetricIds);
  }
  //console.log("mgetMetricIdsFromTerms:  metricIdsSets:\n" + JSON.stringify(metricIdsSets));
  return metricIdsSets;
};
exports.mgetMetricIdsFromTerms = mgetMetricIdsFromTerms;

// Before querying for metric data, we must first find out which metric IDs we need
// to query.  There may be one or more groups of these IDs, depending if the user
// wants to "break-out" the metric (by some metadatam like cpu-id, devtype, etc).
// Find the number of groups needed based on the --breakout options, then find out
// what metric IDs belong in each group.
getMetricGroupsFromBreakouts = function (url, sets) {
  var metricGroupIdsByLabel = [];
  var indexjson = '{}\n';
  var index = JSON.parse(indexjson);
  var ndjson = '';

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
      q.query.bool.filter.push(JSON.parse('{"term": {"period.id": "' + set.period + '"}}'));
    }
    if (set.run != null) {
      q.query.bool.filter.push(JSON.parse('{"term": {"run.id": "' + set.run + '"}}'));
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
    ndjson += JSON.stringify(index) + '\n';
    ndjson += JSON.stringify(q) + '\n';
  });
  var resp = esRequest(url, 'metric_desc/_msearch', ndjson);
  var data = JSON.parse(resp.getBody());

  var metricGroupIdsByLabelSets = [];
  var metricGroupTermsSets = [];
  var metricGroupTermsByLabelSets = [];
  var termsSets = [];
  for (var idx = 0; idx < sets.length; idx++) {
    // The response includes a result from a nested aggregation, which will be parsed to produce
    // query terms for each of the metric groups
    var metricGroupTerms = getMetricGroupTermsFromAgg(data.responses[idx].aggregations);
    // Derive the label from each group and organize into a dict, key = label, value = the filter terms
    var metricGroupTermsByLabel = getMetricGroupTermsByLabel(metricGroupTerms);
    var thisLabelSet = {
      run: sets[idx].run,
      period: sets[idx].period,
      termsByLabel: metricGroupTermsByLabel
    };
    termsSets.push(thisLabelSet);
  }
  metricGroupIdsByLabelSets = mgetMetricIdsFromTerms(url, termsSets);
  return metricGroupIdsByLabelSets;
};
exports.getMetricGroupsFromBreakouts = getMetricGroupsFromBreakouts;

getMetricGroupsFromBreakout = function (url, runId, periId, source, type, breakout) {
  var thisSet = {
    run: runId,
    period: periId,
    source: source,
    type: type,
    breakout: breakout
  };
  var metricGroupIdsByLabelSets = getMetricGroupsFromBreakouts(url, [thisSet]);
  return metricGroupIdsByLabelSets[0];
};
exports.getMetricGroupsFromBreakout = getMetricGroupsFromBreakout;

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
getMetricDataFromIdsSets = function (url, sets, metricGroupIdsByLabelSets) {
  //console.log("metricGroupIdsByLabelSets:\n" + JSON.stringufy(metricGroupIdsByLabelSets, null, 2));
  var ndjson = '';
  for (var idx = 0; idx < metricGroupIdsByLabelSets.length; idx++) {
    Object.keys(metricGroupIdsByLabelSets[idx])
      .sort()
      .forEach(function (label) {
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
        var thisBegin = begin;
        var thisEnd = begin + duration;
        // The resolution determines how many times we compute a value, each value for a
        // different "slice" in the original begin-to-end time domain.
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
          indexjson = '{"index": "' + getIndexBaseName() + 'metric_data' + '" }\n';
          reqjson = '{';
          reqjson += '  "size": 0,';
          reqjson += '  "query": {';
          reqjson += '    "bool": {';
          reqjson += '      "filter": [';
          reqjson += '        {"range": {"metric_data.end": { "lte": "' + thisEnd + '"}}},';
          reqjson += '        {"range": {"metric_data.begin": { "gte": "' + thisBegin + '"}}},';
          reqjson += '        {"terms": {"metric_desc.id": ' + JSON.stringify(metricIds) + '}}';
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
          ndjson += JSON.stringify(index) + '\n';
          ndjson += JSON.stringify(req) + '\n';
          // This second request is for the total weight of the previous weighted average request.
          // We need this because we are going to recompute the weighted average by adding
          // a few more documents that are partially outside the time domain.
          indexjson = '{"index": "' + getIndexBaseName() + 'metric_data' + '" }\n';
          reqjson = '{';
          reqjson += '  "size": 0,';
          reqjson += '  "query": {';
          reqjson += '    "bool": {';
          reqjson += '      "filter": [';
          reqjson += '        {"range": {"metric_data.end": { "lte": "' + thisEnd + '"}}},';
          reqjson += '        {"range": {"metric_data.begin": { "gte": "' + thisBegin + '"}}},';
          reqjson += '        {"terms": {"metric_desc.id": ' + JSON.stringify(metricIds) + '}}';
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
          ndjson += JSON.stringify(index) + '\n';
          ndjson += JSON.stringify(req) + '\n';
          // This third request is for documents that had its begin during or before the time range, but
          // its end was after the time range.
          indexjson = '{"index": "' + getIndexBaseName() + 'metric_data' + '" }\n';
          reqjson = '{';
          reqjson += '  "size": ' + bigQuerySize + ',';
          reqjson += '  "query": {';
          reqjson += '    "bool": {';
          reqjson += '      "filter": [';
          reqjson += '        {"range": {"metric_data.end": { "gt": "' + thisEnd + '"}}},';
          reqjson += '        {"range": {"metric_data.begin": { "lte": "' + thisEnd + '"}}},';
          reqjson += '        {"terms": {"metric_desc.id": ' + JSON.stringify(metricIds) + '}}\n';
          reqjson += '      ]';
          reqjson += '    }';
          reqjson += '  }';
          reqjson += '}';
          index = JSON.parse(indexjson);
          req = JSON.parse(reqjson);
          ndjson += JSON.stringify(index) + '\n';
          ndjson += JSON.stringify(req) + '\n';
          // This fourth request is for documents that had its begin before the time range, but
          //  its end was during or after the time range
          var indexjson = '{"index": "' + getIndexBaseName() + 'metric_data' + '" }\n';
          var reqjson = '';
          reqjson += '{';
          reqjson += '  "size": ' + bigQuerySize + ',';
          reqjson += '  "query": {';
          reqjson += '    "bool": {';
          reqjson += '      "filter": [';
          reqjson += '        {"range": {"metric_data.end": { "gte": ' + thisBegin + '}}},';
          reqjson += '        {"range": {"metric_data.begin": { "lt": ' + thisBegin + '}}},';
          reqjson += '        {"terms": {"metric_desc.id": ' + JSON.stringify(metricIds) + '}}\n';
          reqjson += '      ]';
          reqjson += '    }';
          reqjson += '  }';
          reqjson += '}\n';
          index = JSON.parse(indexjson);
          req = JSON.parse(reqjson);
          ndjson += JSON.stringify(index) + '\n'; //ensures JSON is exactly 1 line
          ndjson += JSON.stringify(req) + '\n'; //ensures JSON is exactly 1 line

          // Cycle through every "slice" of the time domain, adding the requests for the entire time domain
          thisBegin = thisEnd + 1;
          thisEnd += duration + 1;
          if (thisEnd > end) {
            thisEnd = end;
          }
          if (thisBegin > thisEnd) {
            break;
          }
        }
      });
  }

  var resp = esRequest(url, 'metric_data/_msearch', ndjson);
  var data = JSON.parse(resp.getBody());
  var elements = data.responses.length;

  var valueSets = [];
  var count = 0;
  for (var idx = 0; idx < metricGroupIdsByLabelSets.length; idx++) {
    thisSetElements = elements / metricGroupIdsByLabelSets.length;
    var valuesByLabel = {};
    Object.keys(metricGroupIdsByLabelSets[idx])
      .sort()
      .forEach(function (label) {
        valuesByLabel[label] = [];
        thisLabelElements = metricGroupIdsByLabelSets[idx][label].length;
        var metricIds = metricGroupIdsByLabelSets[idx][label];
        var values = [];
        var begin = Number(sets[idx].begin);
        var end = Number(sets[idx].end);
        var resolution = Number(sets[idx].resolution);
        var duration = Math.floor((end - begin) / resolution);
        var thisBegin = begin;
        var thisEnd = begin + duration;
        var subCount = 0;
        //var elements = data.responses.length / metricGroupIdsByLabelSets.length;
        var numMetricIds = metricIds.length;
        while (true) {
          var timeWindowDuration = thisEnd - thisBegin + 1;
          var totalWeightTimesMetrics = timeWindowDuration * numMetricIds;
          subCount++;
          var aggAvg;
          var aggWeight;
          var aggAvgTimesWeight;
          var newWeight;
          aggAvg = data.responses[count].aggregations.metric_avg.value; //$$resp_ref{'responses'}[$count]{'aggregations'}{'metric_avg'}{'value'};
          if (typeof aggAvg != 'undefined') {
            // We have the weighted average for documents that don't overlap the time range,
            // but we need to combine that with the documents that are partially outside
            // the time range.  We need to know the total weight from the documents we
            // just finished in order to add the new documents and recompute the new weighted
            // average.
            aggWeight = data.responses[count + 1].aggregations.total_weight.value;
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

          // Process last 2 of the 4 responses in the 'set'
          // Since these docs have a time range partially outside the time range we want,
          // we have to get a new, reduced duration and use that to agment our weighted average.
          var sumValueTimesWeight = 0;
          var sumWeight = 0;
          // It is possible to have the same document returned from the last two queries in this set of 4.
          // This can happen when the document's begin is before $this_begin *and* the document's end
          // if after $this_end.
          // You must not process the document twice.  Perform a consolidation by organizing by the
          //  returned document's '_id'
          var partialDocs = {};
          var k;
          for (k = 2; k < 4; k++) {
            //for my $j (@{ $$resp_ref{'responses'}[$count + $k]{'hits'}{'hits'} }) {
            if (data.responses[count + k].hits.total.value !== data.responses[count + k].hits.hits.length) {
              console.log(
                'WARNING! getMetricDataFromIdsSets() data.responses[' +
                  (count + k) +
                  '].hits.total.value (' +
                  data.responses[count + k].hits.total.value +
                  ') and data.responses[' +
                  (count + k) +
                  '].hits.hits.length (' +
                  data.responses[count + k].hits.hits.length +
                  ') are not equal, which means the retured data is probably incomplete'
              );
            }
            data.responses[count + k].hits.hits.forEach((element) => {
              //for my $key (keys %{ $$j{'_source'}{'metric_data'} }) {
              partialDocs[element._id] = {};
              Object.keys(element._source.metric_data).forEach((key) => {
                //partial_docs[{$$j{'_id'}}{$key} = $$j{'_source'}{'metric_data'}{$key};
                partialDocs[element._id][key] = element._source.metric_data[key];
              });
            });
          }
          // Now we can process the partialDocs
          Object.keys(partialDocs).forEach((id) => {
            var docDuration = partialDocs[id].duration;
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
          //result = Number.parseFloat(result).toPrecision(4);
          var dataSample = {};
          dataSample.begin = thisBegin;
          dataSample.end = thisEnd;
          dataSample.value = result;
          values.push(dataSample);

          count += 4; // Bumps count to the next set of responses

          // Cycle through every "slice" of the time domain, adding the requests for the entire time domain
          thisBegin = thisEnd + 1;
          thisEnd += duration + 1;
          if (thisEnd > end) {
            thisEnd = end;
          }
          if (thisBegin > thisEnd) {
            break;
          }
        }
        valuesByLabel[label] = values;
      });
    valueSets[idx] = valuesByLabel;
  }
  return valueSets;
};
exports.getMetricDataFromIdsSets = getMetricDataFromIdsSets;

getMetricData = function (url, runId, periId, source, type, begin, end, resolution, breakout, filter) {
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
  var dataSets = getMetricDataSets(url, sets);
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
getMetricDataSets = function (url, sets) {
  for (var i = 0; i < sets.length; i++) {
    // If a begin and end are not defined, get it from the period.begin & period.end.
    // If a begin and/or end are not defined, and the period is not defined, error out.
    // If a run is not defined, get it from the period.
    // If a run and period are not defined, error out.
    if (typeof sets[i].run == 'undefined') {
      if (typeof sets[i].period != 'undefined') {
        sets[i].run = getRunFromPeriod(url, sets[i].period);
      } else {
        console.log('ERROR: run and period was not defined');
      }
    }
    var periodRange;
    if (typeof sets[i].begin == 'undefined') {
      if (typeof sets[i].period != 'undefined') {
        periodRange = getPeriodRange(url, sets[i].period);
        sets[i]['begin'] = periodRange['begin'];
      } else {
        console.log('ERROR: begin is not defined and a period was not defined');
      }
    }
    if (typeof sets[i].end == 'undefined') {
      if (typeof sets[i].period != 'undefined') {
        if (typeof periodRange == 'undefined') {
          periodRange = getPeriodRange(url, sets[i].period);
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

  var metricGroupIdsByLabelSets = getMetricGroupsFromBreakouts(url, sets);
  var dataSets = getMetricDataFromIdsSets(url, sets, metricGroupIdsByLabelSets);

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
  var setBreakouts = mgetMetricNames(url, runIds, sources, types);

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
