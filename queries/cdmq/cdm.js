//# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript
var request = require('sync-request');


function getIndexBaseName() {
  return 'cdmv4dev-';
}

function esQuery(host, idx, q) {
  var url = 'http://' + host + '/' + getIndexBaseName() + idx;
  // The var q can be an object or a string.  If you are submitting NDJSON
  // for a _msearch, it must be a [multi-line] string.
  if (typeof(q) === "object") {
    q = JSON.stringify(q);
  }
  //console.log("query:\n/" + q + "/");
  var resp = request('POST', url, { body: q, headers: {"Content-Type": "application/json" } });
  return resp;
}

exports.getPrimaryMetric = function (url, iterId) {
  var q = { 'query': { 'bool': { 'filter': [ {"term": {"iteration.id": iterId}} ] }},
            'aggs': { 'source': { 'terms': { 'field': 'iteration.primary_metric'}}},
      // aggregation is used to confirm exactly 1 primary metric among samples
            'size': 0 };
  var resp = esQuery(url, "sample/sample/_search", q);
  var data = JSON.parse(resp.getBody());
  if (Array.isArray(data.aggregations.source.buckets) && data.aggregations.source.buckets.length == 1) {
    return data.aggregations.source.buckets[0].key;
  }
};

exports.getIterations = function (url, searchTerms) {
  var q = { 'query': { 'bool': { 'filter': [] }},
      '_source': "iteration.id",
            'size': 1000 };
  if (searchTerms.length === 0) {
    console.log("Found no search terms\n");
    return;
  }
  searchTerms.forEach(element => {
    var myTerm = {};
    myTerm[element.term] = element.value;
    q.query.bool.filter.push({"term": myTerm});
  });
  var resp = esQuery(url, "iteration/iteration/_search", q);
  var data = JSON.parse(resp.getBody());
  var ids = [];
  if (Array.isArray(data.hits.hits) && data.hits.hits.length > 0) {
    data.hits.hits.forEach(element => {
      ids.push(element._source.iteration.id);
    });
  }
  return { ids };
};

exports.getSamples = function (url, searchTerms) {
  var q = { 'query': { 'bool': { 'filter': [] }},
      '_source': "sample.id",
            'size': 1000 };
  if (searchTerms.length === 0) {
    return;
  }
  searchTerms.forEach(element => {
    var myTerm = {};
    myTerm[element.term] = element.value;
    q.query.bool.filter.push({"term": myTerm});
  });
  var resp = esQuery(url, "sample/sample/_search", q);
  var data = JSON.parse(resp.getBody());
  var ids = [];
  if (Array.isArray(data.hits.hits) && data.hits.hits.length > 0) {
    data.hits.hits.forEach(element => {
      ids.push(element._source.sample.id);
    });
  }
  return { ids };
};

exports.getPrimaryPeriodName = function (url, sampId) {
  var q = { 'query': { 'bool': { 'filter': [ {"term": {"sample.id": sampId}} ] }},
            '_source': 'iteration.primary_period',
            'size': 1 };
  var resp = esQuery(url, "sample/sample/_search", q);
  var data = JSON.parse(resp.getBody());
  if (data.hits.hits[0]._source.iteration.primary_period) {
    return data.hits.hits[0]._source.iteration.primary_period;
  }
};

exports.getPeriodRange = function (url, periId) {
  var q = { 'query': { 'bool': { 'filter': [ {"term": {"period.id": periId}} ] }},
            '_source': [ 'period.begin', 'period.end' ],
            'size': 1 };
  var resp = esQuery(url, "period/period/_search", q);
  var data = JSON.parse(resp.getBody());
  if (data.hits.hits[0]._source.period.begin && data.hits.hits[0]._source.period.end) {
    return { "begin": data.hits.hits[0]._source.period.begin, "end": data.hits.hits[0]._source.period.end };
  }
};

exports.getPrimaryPeriodId = function (url, sampId, periName) {
  var q = { 'query': { 'bool': { 'filter': [
                                       {"term": {"sample.id": sampId}},
                                       {"term": {"period.name": periName}}
                                           ] }},
      '_source': 'period.id',
            'size': 1 };
  var resp = esQuery(url, "period/period/_search", q);
  var data = JSON.parse(resp.getBody());
  if (data.hits.total > 0 && Array.isArray(data.hits.hits) && data.hits.hits[0]._source.period.id) {
    return data.hits.hits[0]._source.period.id;
  } else {
    console.log("primary period id not found\n");
  }
};

exports.getNameFormat = function (url, periId, source, type) {
  var q = { 'query': { 'bool': { 'filter': [ 
                                             {"term": {"period.id": periId}},
                                             {"term": {"metric_desc.source": source}},
                                             {"term": {"metric_desc.type": type}} ]
                               }},
            'aggs': { 'source': { 'terms': { 'field': 'metric_desc.name_format'}}},
            'size': 0 };
  var resp = esQuery(url, "metric_desc/metric_desc/_search", q);
  var data = JSON.parse(resp.getBody());
  if (Array.isArray(data.aggregations.source.buckets) && data.aggregations.source.buckets.length == 1) {
    return data.aggregations.source.buckets[0].key;
  }
};

exports.getBenchmarkName = function (url, runId) {
  var q = { 'query': { 'bool': { 'filter': [ {"term": {"run.id": runId}} ] }},
      '_source': "run.bench.name",
            'size': 1 };
  var resp = esQuery(url, "run/run/_search", q);
  var data = JSON.parse(resp.getBody());
  if (data.hits.hits[0]._source.run.bench.name) {
    return data.hits.hits[0]._source.run.bench.name;
  }
};

exports.getRuns = function (url, searchTerms) {
  var q = { 'query': { 'bool': { 'filter': [] }},
            'aggs': { 'source': { 'terms': { 'field': 'run.id'}}},
      // it's possible to have multiple run docs with same ID, so use aggregation
            'size': 0 };
  if (searchTerms.length === 0) {
    return;
  }
  searchTerms.forEach(element => {
    var myTerm = {};
    myTerm[element.term] = element.value;
    q.query.bool.filter.push({"term": myTerm});
  });
  var resp = esQuery(url, "run/run/_search", q);
  var data = JSON.parse(resp.getBody());
  var ids = [];
  if (Array.isArray(data.aggregations.source.buckets) && data.aggregations.source.buckets.length > 0) {
    data.aggregations.source.buckets.forEach(element => {
      ids.push(element.key);
    });
    return { ids };
  }
};

// Traverse a response from a nested aggregation to generate a set of filter terms
// for each metric group.
getMetricGroupTermsFromAgg = function (agg, terms) {
  var value;
  if (typeof(terms) == "undefined") {
    terms = "";
  }
  if (typeof(agg.key) != "undefined") {
    value = agg.key;
    terms += '"' + value + '"}}';
  }
  var count = 0;
  var metricGroupTerms = new Array();
  Object.keys(agg).forEach(field => {
    if (/^metric_desc/.exec(field)) {
      count++;
      if (typeof(agg[field].buckets) != "undefined") {
        agg[field].buckets.forEach(bucket => {
          metricGroupTerms = metricGroupTerms.concat(getMetricGroupTermsFromAgg(bucket, terms + ',' + '{"term": {"' + field + '": '));
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
  if (Array.isArray(breakout)) {
    breakout.forEach(field => {
      if (/([^\=]+)\=([^\=]+)/.exec(field)) {
        field = $1
      }
      agg_str += ',"aggs": { "metric_desc.names.' + field + '": { "terms": ' +
                  '{ "show_term_doc_count_error": true, "size": 1000,' + 
                  '"field": "metric_desc.names.' + field + '" }';
      field_count++;
    });
    while (field_count > 0) {
      agg_str += '}}';
      field_count--;
    }
    agg_str += '}}}}';
    return agg_str;
  }
};
exports.getBreakoutAggregation = getBreakoutAggregation;

getMetricGroupTermsByLabel = function (metricGroupTerms) {
  var metricGroupTermsByLabel = {};
  metricGroupTerms.forEach(term => {
    var terms = JSON.parse("[" + term + "]");
    var label = "";
    terms.forEach(thisTerm => {
      Object.keys(thisTerm.term).forEach(field => {
        // The true label does not actually include the source/type
        // but the query does have those in the filter terms, so we
        // need to excluse it when forming the label.
        if (field == "metric_desc.source" || field == "metric_desc.type") {
          return;
        }
        label += '-' + thisTerm.term[field];
      });
    });
    label = label.replace(/^-/, '');
    metricGroupTermsByLabel[label] = term;
  });
  return metricGroupTermsByLabel;
}

getMetricIdsFromTerms = function (url, periId, terms_string) {
  var filter = JSON.parse("[" + terms_string + "]");
  var q = { 'query': { 'bool': { 'filter': JSON.parse("[" + terms_string + "]") }},
            '_source': 'metric_desc.id',
            'size': 10000 };
            // Need alternatives when exceeding 10,000.  This issue is detected below.
            // Most tools/benchmarks probably would not exceed 10,000, but some could,
            // if this was a very large test.  For example, pidstat per-PID cpu usage,
            // if you had 250 hosts and each host has 400 PIDs, that could produce
            // 10,000 metric IDs.  It could happen much quicker if we just added the 
            // metric to each PID's CPU usage per cpu-mode.  This problem is not
            // wthout a solution.  One can:
            // 1) Use the the "scroll" function in ES
            // 2) Query with finer-grain terms (break-down the query by
            //    the compnents in the metric's label, like "host") then do multiple
            //    queries and aggregate the metric IDs.
            // 3) Simply adjust index.max_result_window to > 10,000, but test this,
            //    as the size is dependent on the Java heap size.
  q.query.bool.filter.push(JSON.parse('{"term": {"period.id": "' + periId + '"}}'));
  var resp = esQuery(url, "metric_desc/metric_desc/_search", q);
  var data = JSON.parse(resp.getBody());
  if (data.hits.total > data.size) {
    console.log("The number of documents matched in this search exceeds the 'size' parameter: " + data.hits.total + ", " + data.size + "\n");
    return;
  }
  var metricIds = [];
  data.hits.hits.forEach(element => {
    metricIds.push(element._source.metric_desc.id);
  });
  return metricIds;
}
exports.getMetricIdsFromTerms = getMetricIdsFromTerms;

// If metrics are "broken-out" by some part of the name_format, we must organize those into
// groups, each group a collection of 1 or more metric IDs.
getMetricGroupsFromBreakout = function (url, periId, source, type, breakout) {
  var metricGroupIdsByLabel = {};
  var q = { 'query': { 'bool': { 'filter': [ 
                                             {"term": {"metric_desc.source": source}},
                                             {"term": {"metric_desc.type": type}},
                                             {"term": {"period.id": periId}}
                                            ]
                               }},
            'size': 0 };
  q.aggs = JSON.parse(getBreakoutAggregation(source, type, breakout));
  // If the breaout contains a match requirement (host=myhost), then we must add a term filter for it.
  // Eventually it would be nice to have something other than a match, like a regex: host=/^client/.
  breakout.forEach(field => {
    if (/([^\=]+)\=([^\=]+)/.exec(field)) {
      field = $1;
      value = $2;
      q.query.bool.filter.push(JSON.parse('{"term": {"metric_desc.names."' + field + '": "' + value + '"}}'));
    }
  });
  var resp = esQuery(url, "metric_desc/metric_desc/_search", q);
  var data = JSON.parse(resp.getBody());
  // The response includes a result from a nested aggregation, which will be parsed to produce
  // query terms for each of the metric groups
  //var metricGroupTerms = getMetricGroupTermsFromAgg(data.aggregations, 0, "");
  var metricGroupTerms = getMetricGroupTermsFromAgg(data.aggregations);
  // Derive the label from each group and organize into a dict, key = label, value = the filter terms 
  var metricGroupTermsByLabel = getMetricGroupTermsByLabel(metricGroupTerms);
  // Now iterate over these labels and query with the label's search terms to get the metric IDs
  Object.keys(metricGroupTermsByLabel).forEach(label => {
    metricGroupIdsByLabel[label] = getMetricIdsFromTerms(url, periId, metricGroupTermsByLabel[label]);
  });
  return metricGroupIdsByLabel;
};
exports.getMetricGroupsFromBreakout = getMetricGroupsFromBreakout;

// From a set of metric ID's, return 1 or more values depending on resolution.
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
getMetricDataFromIds = function (url, begin, end, resolution, metricIds) {
  begin = Number(begin);
  end = Number(end);
  resolution = Number(resolution);
  var duration = Math.floor((end - begin) / resolution);
  var thisBegin = begin;
  var thisEnd = begin + duration;
  var values = [];
  var ndjson = "";
  // The resolution determines how many times we compute a value, each value for a
  // different "slice" in the original begin-to-end time domain.
  while (true) {
    //console.log("thisBegin: " + thisBegin + "  thisEnd: " + thisEnd + "\n");
    //console.log("building ndjson\n");
    // Calculating a single value representing an average for thisBegin - thisEnd
    // relies on an [weighted average] aggregation, plus a few other queries.  An
    // alternative method would involve querying all documents for the orignal
    // begin - end time range, then [locally] computing a weighted average per
    // thisBegin - thisEnd slice. Each method has pros/cons depending on the
    // resolution and the total number of metric_data documents. 

    // This first request is for the weighted average, but does not include the
    // documents which are partially outside the time range we need.

    indexjson = '{"index": "' + getIndexBaseName() + 'metric_data' + '" }\n';

    reqjson  = '{';
    reqjson += '  "size": 0,';
    reqjson += '  "query": {';
    reqjson += '    "bool": {';
    reqjson += '      "filter": [';
    reqjson += '        {"range": {"metric_data.end": { "lte": "' + thisEnd + '"}}},';
    reqjson += '        {"range": {"metric_data.begin": { "gte": "' + thisBegin + '"}}},';
    reqjson += '        {"terms": {"metric_data.id": ' + JSON.stringify(metricIds) + '}}';
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
    //console.log("first request:\n" + JSON.stringify(index, null, 2) + "\n" + JSON.stringify(req, null, 2));
    ndjson += JSON.stringify(index) + "\n";
    ndjson += JSON.stringify(req) + "\n";

    // This second request is for the total weight of the previous weighted average request.
    // We need this because we are going to recompute the weighted average by adding
    // a few more documents that are partially outside the time domain.

    indexjson = '{"index": "' + getIndexBaseName() + 'metric_data' + '" }\n';

    reqjson  = '{';
    reqjson += '  "size": 0,';
    reqjson += '  "query": {';
    reqjson += '    "bool": {';
    reqjson += '      "filter": [';
    reqjson += '        {"range": {"metric_data.end": { "lte": "' + thisEnd + '"}}},';
    reqjson += '        {"range": {"metric_data.begin": { "gte": "' + thisBegin + '"}}},';
    reqjson += '        {"terms": {"metric_data.id": ' + JSON.stringify(metricIds) + '}}';
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
    //console.log("second request:\n" + JSON.stringify(index, null, 2) + "\n" + JSON.stringify(req, null, 2));
    ndjson += JSON.stringify(index) + "\n";
    ndjson += JSON.stringify(req) + "\n";

    // This third request is for documents that had its begin during or before the time range, but
    // its end was after the time range.

    indexjson = '{"index": "' + getIndexBaseName() + 'metric_data' + '" }\n';

    reqjson  = '{';
    reqjson += '  "query": {';
    reqjson += '    "bool": {';
    reqjson += '      "filter": [';
    reqjson += '        {"range": {"metric_data.end": { "gt": "' + thisEnd + '"}}},';
    reqjson += '        {"range": {"metric_data.begin": { "lte": "' + thisEnd + '"}}},';
    reqjson += '        {"terms": {"metric_data.id": ' + JSON.stringify(metricIds) + '}}\n';
    reqjson += '      ]';
    reqjson += '    }';
    reqjson += '  }';
    reqjson += '}';
    index = JSON.parse(indexjson);
    req = JSON.parse(reqjson);
    //console.log("third request:\n" + JSON.stringify(index, null, 2) + "\n" + JSON.stringify(req, null, 2));
    ndjson += JSON.stringify(index) + "\n";
    ndjson += JSON.stringify(req) + "\n";

    // This fourth request is for documents that had its begin before the time range, but
    //  its end was during or after the time range

    var indexjson = '{"index": "' + getIndexBaseName() + 'metric_data' + '" }\n';
    var reqjson = '';
    reqjson += '{';
    reqjson += '  "query": {';
    reqjson += '    "bool": {';
    reqjson += '      "filter": [';
    reqjson += '        {"range": {"metric_data.end": { "gte": ' + thisBegin + '}}},';
    reqjson += '        {"range": {"metric_data.begin": { "lt": ' + thisBegin + '}}},';
    reqjson += '        {"terms": {"metric_data.id": ["' + metricIds + '"]}}\n';
    reqjson += '      ]';
    reqjson += '    }';
    reqjson += '  }';
    reqjson += '}\n';
    //console.log("reqjson:\n" + reqjson);
    index = JSON.parse(indexjson);
    req = JSON.parse(reqjson);
    //console.log("fourth request:\n" + JSON.stringify(index, null, 2) + "\n" + JSON.stringify(req, null, 2));
    ndjson += JSON.stringify(index) + "\n";
    ndjson += JSON.stringify(req) + "\n";

    // Cycle through every "slice" of the time domain, adding the requests for the entire time domain
    thisBegin = thisEnd + 1;
    thisEnd += duration + 1;
    if (thisEnd > end) {
      thisEnd = end;
    }
    //console.log("thisBegin: " + thisBegin + "  thisEnd: " + thisEnd + "  begin: " + begin + "  end: " + end + "\n");
    if (thisBegin > thisEnd) {
      break;
    }
  }
  //console.log("submitting request\n" + ndjson);
  var resp = esQuery(url, "metric_data/metric_data/_msearch", ndjson);
  var data = JSON.parse(resp.getBody());
  thisBegin = begin;
  thisEnd = begin + duration;
  var count = 0;
  var subCount = 0;
  var elements = data.responses.length;
  var numMetricIds = metricIds.length;
  while (count < elements) {
    var timeWindowDuration = thisEnd - thisBegin + 1;
    var totalWeightTimesMetrics = timeWindowDuration * numMetricIds;
    subCount++;
    var aggAvg;
    var aggWeight;
    var aggAvgTimesWeight;
    var newWeight;
    aggAvg = data.responses[count].aggregations.metric_avg.value; //$$resp_ref{'responses'}[$count]{'aggregations'}{'metric_avg'}{'value'};
    if (typeof aggAvg != "undefined") {
      // We have the weighted average for documents that don't overlap the time range,
      // but we need to combine that with the documents that are partially outside
      // the time range.  We need to know the total weight from the documents we
      // just finished in order to add the new documents and recompute the new weighted
      // average.
      aggWeight = data.responses[count+1].aggregations.total_weight.value;
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
    //console.log("weighted avg before processing docs partially in the time range: " + aggAvg);
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
      data.responses[count + k].hits.hits.forEach(element => {
        //console.log("element: " + JSON.stringify(element) + "\n");
        //for my $key (keys %{ $$j{'_source'}{'metric_data'} }) {
        partialDocs[element._id] = {};
        Object.keys(element._source.metric_data).forEach(key => {
          //partial_docs[{$$j{'_id'}}{$key} = $$j{'_source'}{'metric_data'}{$key};
          partialDocs[element._id][key] = element._source.metric_data[key];
        });
      });
    }
    // Now we can process the partialDocs
    //console.log("docs partially in the time range:\n");
    Object.keys(partialDocs).forEach(id => {
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
      //console.log("_id: " + id + "  thisBegin / begin: " + thisBegin + " / " + partialDocs[id].begin + "  thisEnd / end: " + thisEnd + " / " + partialDocs[id].end + "  duration: " + docDuration + "\n");
    });
    var result = (aggAvgTimesWeight + sumValueTimesWeight) / totalWeightTimesMetrics;
    result *= numMetricIds;
    result = Number.parseFloat(result).toPrecision(4);
    var dataSample = {};
    dataSample.begin = thisBegin;
    dataSample.end = thisEnd;
    dataSample.value = result;
    values.push(dataSample);

    count += 4;
    thisBegin = thisEnd + 1;
    thisEnd += duration + 1;
    if (thisEnd > end) {
      thisEnd = end;
    }
    //thisBegin = thisEnd;
    //thisEnd += thisEnd + duration + 1;
    //if (thisEnd > end) {
      //thisEnd = end;
    //}
  }
  return values;
};
exports.getMetricDataFromIds = getMetricDataFromIds;

// Generates 1 or more values for 1 or more groups for a metric of a particular source
// (tool or benchmark) and type (iops, l2-Gbps, ints/sec, etc).
// The breakout determines if the metric is broken out into groups -if it is empty,
// there is only 1 group.
// The resolution determines the number of values for each group -if you just need
// a single average for the metric group, the resolution should be 1.
// The begin and end control the time domain, andmust be within the time domain
// from this [benchmark-iteration-sample-]period (from doc which contains the periId).
exports.getMetricDataFromPeriod = function(url, periId, source, type, begin, end, resolution, breakout) {
  var data = { "breakouts": [], "values": {} };
  var metricGroupIdsByLabel = getMetricGroupsFromBreakout(url, periId, source, type, breakout);
  Object.keys(metricGroupIdsByLabel).forEach(function(label) {
    data.values[label] = getMetricDataFromIds(url, begin, end, resolution, metricGroupIdsByLabel[label]);
  });
  return data;
};
