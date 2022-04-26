//# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript
var request = require('sync-request');
var printf = require('printf');
var bigQuerySize = 262144;


function getIndexBaseName() {
  return 'cdmv6dev-';
}

// Return intersection of two 1-dimensional arrays
function intersectTwoArrays(a1, a2) {
   const a3 = [];
   a1.forEach(element => {
      if(!a2.includes(element)){
         return;
      };
      a3.push(element);
   });
   return a3;
};

// Return intersection of many 1-dimensional arrays found in 2-dimensional array
function intersectAllArrays(a2D) {
   var intersectArray = a2D[0];
   a2D.forEach(a1D => {
      intersectArray = intersectTwoArrays(intersectArray, a1D);
   });
   return intersectArray;
};

function esRequest(host, idx, q) {
  var url = 'http://' + host + '/' + getIndexBaseName() + idx;
  // The var q can be an object or a string.  If you are submitting NDJSON
  // for a _msearch, it must be a [multi-line] string.
  if (typeof(q) === "object") {
    q = JSON.stringify(q);
  }
  var resp = request('POST', url, { body: q, headers: {"Content-Type": "application/json" } });
  //console.log("esRequest complete");
  return resp;
}

deleteDocs = function (url, docTypes, q) {
  docTypes.forEach(docType => {
    var resp = esRequest(url, docType + "/_doc/_delete_by_query", q);
    var data = JSON.parse(resp.getBody());
  });
};
exports.deleteDocs = deleteDocs;

getPrimaryMetric = function (url, iterId) {
  var q = { 'query': { 'bool': { 'filter': [ {"term": {"iteration.id": iterId}} ] }}};
  var resp = esRequest(url, "iteration/_doc/_search", q);
  var data = JSON.parse(resp.getBody());
  return data.hits.hits[0]._source.iteration['primary-metric'];
};
exports.getPrimaryMetric = getPrimaryMetric;

getMetricDescs = function (url, runId) {
  var q = { 'query': { 'bool': { 'filter': [{ "term": {"run.id": runId }}] }},
            '_source': "metric_desc.id",
            'size': bigQuerySize };
  var resp = esRequest(url, "metric_desc/_doc/_search", q);
  var data = JSON.parse(resp.getBody());
  var ids = [];
  if (Array.isArray(data.hits.hits) && data.hits.hits.length > 0) {
    data.hits.hits.forEach(element => {
      if (ids.find(value => value == element._source.metric_desc.id)) {
        console.log("Odd, metric ID " + element._source.metric_desc.id + "found more than once");
      } else {
        ids.push(element._source.metric_desc.id);
      }
    });
  }
  return ids;
};
exports.getMetricDescs = getMetricDescs;

getMetricDataDocs = function (url, metricId) {
  var q = { 'query': { 'bool': { 'filter': [ {"term": {"metric_desc.id": metricId}} ] }},
            'size': 10000 };
  var resp = esRequest(url, "metric_data/_doc/_search", q);
  var data = JSON.parse(resp.getBody());
  return data;
};
exports.getMetricDataDocs = getMetricDataDocs;

// Delete all the metric (metric_desc and metric_data) for a run
deleteMetrics = function (url, runId) {
  var ids = getMetricDescs(url, runId);
  console.log("There are " + ids.length + " metric_desc docs");
  var q = { 'query': { 'bool': { 'filter': { "terms": { "metric_desc.id": [] }}}}};
  ids.forEach(element => {
    var term = {"metric_desc.id": element };
    q['query']['bool']['filter']['terms']["metric_desc.id"].push(element);
    if (q['query']['bool']['filter']['terms']["metric_desc.id"].length >= 1000) {
      console.log("deleting " + q['query']['bool']['filter']['terms']["metric_desc.id"].length + " metrics");
      deleteDocs(url, ['metric_data', 'metric_desc'], q);
      q['query']['bool']['filter']['terms']["metric_desc.id"] = [];
    }
  });
  var remaining = q['query']['bool']['filter']['terms']["metric_desc.id"].length;
  if (remaining > 0) {
    console.log("deleting " + q['query']['bool']['filter']['terms']["metric_desc.id"].length + " metrics");
    deleteDocs(url, ['metric_data', 'metric_desc'], q);
  }
};
exports.deleteMetrics = deleteMetrics;

buildIterTree = function (url, params, tags, paramValueByIterAndArg, tagValueByIterAndName, iterIds, dontBreakoutTags, dontBreakoutParams, omitParams, breakoutOrderTags, breakoutOrderParams) {

  // params: 2-d hash, {arg}{val}, value = [list of iteration IDs that has this val]
  // tags: 2-d hash, {name}{val}, value = [list of iteration IDs that has this val]

  //console.log("buildIterTree():\nparams:\n" + JSON.stringify(params) + "\ntags:\n" + JSON.stringify(tags) + "\nparamValueByIterAndArg:\n" + JSON.stringify(paramValueByIterAndArg) + "\ntagValueByIterAndName:\n" + JSON.stringify(tagValueByIterAndName) + "\niterIds:\n" + JSON.stringify(iterIds));

  var iterNode = {};
  if (typeof(iterIds) == "undefined" || iterIds.length == 0) {
    return iterNode;
  };

  // Move any params which have only 1 value to current iterNode
  Object.keys(params).forEach(arg =>{
    if (Object.keys(params[arg]).length == 1) {
      if (typeof(iterNode["params"]) == "undefined") {
        iterNode["params"] = [];
      }
      var val = Object.keys(params[arg])[0]; // the one and only value
      var thisParam = { "arg": arg, "val": val };
      iterNode.params.push(thisParam);
      iterIds = intersectTwoArrays(iterIds, params[arg][val]);
      delete params[arg]; // delete all possible values for this arg
    }
  });

  // Move any tags which have only 1 value to current iterNode
  Object.keys(tags).forEach(name =>{
    if (Object.keys(tags[name]).length == 1) {
      if (typeof(iterNode["tags"]) == "undefined") {
        iterNode["tags"] = [];
      }
      var val = Object.keys(tags[name])[0]; // the one and only value
      var thisTag = { "name": name, "val": val };
      iterNode.tags.push(thisTag);
      iterIds = intersectTwoArrays(iterIds, tags[name][val]);
      delete tags[name]; // delete all possible values for this arg
    }
  });

  // The child nodes can only be from breaking out one param or one tag
  // The current implementation checks for a param first, and only if there
  // are none, does it try a tag.  The opposite order could also work.
  
  if (Object.keys(params).length > 0) {
    var args = Object.keys(params).filter(x => !dontBreakoutParams.includes(x));
    //console.log("Params:\n" + JSON.stringify(args));
    if (args.length > 0) {
      var nextArg;
      for (i = 0; i < breakoutOrderParams.length; i++) {
        if (args.includes(breakoutOrderParams[i])) {
          //console.log("Assigning nextArg to breakoutOrderParams[" + i + "]: " + breakoutOrderParams[i]);
          nextArg = breakoutOrderParams[i];
          break;
        }
      }
      if (typeof(nextArg) == "undefined") {
        nextArg = args[0];
      }
      Object.keys(params[nextArg]).forEach(val => {
        const intersectedIterIds = intersectTwoArrays(iterIds, params[nextArg][val]);
        var newIter;
        // deep-copy params, then modify copy such that newParams{nextArg} only has 1 value
        var newParamsJsonStr = JSON.stringify(params);
        var newParams = JSON.parse(newParamsJsonStr);
        newParams[nextArg] = {}; // delete all possible values for this arg
        newParams[nextArg][val] = params[nextArg][val]; 
        newIter = buildIterTree(url, newParams, tags, paramValueByIterAndArg, tagValueByIterAndName, intersectedIterIds, dontBreakoutTags, dontBreakoutParams, omitParams, breakoutOrderTags, breakoutOrderParams);
        if (typeof(newIter) != "undefined" && Object.keys(newIter).length > 0) {
          if (typeof(iterNode["breakout"]) == "undefined") {
            iterNode["breakout"] = [];
          }
          iterNode["breakout"].push(newIter);
        } else {
          console.log("warning: newIter undefined or empty:\n" + JSON.stringify(newIter, null, 2));
        }
      });
    }
  }

  if (typeof(iterNode["breakout"]) == "undefined") {
    // skipping any requested "don't break out tags" below
    var names = Object.keys(tags).filter(x => !dontBreakoutTags.includes(x));
    if (names.length > 0) {
      var nextName;
      for (i = 0; i < breakoutOrderTags.length; i++) {
        if (names.includes(breakoutOrderTags[i])) {
          nextName = breakoutOrderTags[i];
          break;
        }
      }
      if (typeof(nextName) == "undefined") {
        nextName = names[0];
      }

      Object.keys(tags[nextName]).forEach(val => {
        const intersectedIterIds = intersectTwoArrays(iterIds, tags[nextName][val]);
        var newIter;
        // deep-copy tags, then modify copy such that newTags{nextName} only has 1 value
        var newTagsJsonStr = JSON.stringify(tags);
        var newTags = JSON.parse(newTagsJsonStr);
        newTags[nextName] = {}; // delete all possible values for this arg
        newTags[nextName][val] = tags[nextName][val]; 
        newIter = buildIterTree(url, params, newTags, paramValueByIterAndArg, tagValueByIterAndName, intersectedIterIds, dontBreakoutTags, dontBreakoutParams, omitParams, breakoutOrderTags, breakoutOrderParams);
        if (typeof(newIter) != "undefined" && Object.keys(newIter).length > 0) {
          if (typeof(iterNode["breakout"]) == "undefined") {
            iterNode["breakout"] = [];
          }
          iterNode["breakout"].push(newIter);
        } else {
          console.log("warning: newIter undefined or empty:\n" + JSON.stringify(newIter, null, 2));
        }
      });
    }
  }

  // Only if there are no more breakouts, should we create the iteration
  if (typeof(iterNode["breakout"]) == "undefined") {
    var iterations = [];
    iterIds.forEach(id => {
      var result = getIterMetrics(url, id);
      var thisIter = { "id": id, "labels": "", "mean": result["mean"], "stddevpct": result["stddevpct"], "min": result["min"], "max": result["max"] }
      Object.keys(tags).forEach(name => {
        thisIter["labels"]+= " " + name + ":" + tagValueByIterAndName[id][name];
      });
      Object.keys(params).forEach(arg => {
        thisIter["labels"]+= " " + arg + ":" + paramValueByIterAndArg[id][arg];
      });
      iterations.push(thisIter);
    });
    iterNode["iterations"] = iterations;
  }

  return iterNode;
};

reportIters = function(iterTree, indent) {

  if (typeof(indent) == "undefined" || indent == "") {
  }

  var midPoint = 70;
  var len = 0;

  var tagStr = "";
  if (typeof(iterTree.tags) != "undefined") {
    if (iterTree.tags.length == 1) {
      tagStr += iterTree.tags[0].name + ":" + iterTree.tags[0].val;
    } else {
      var separator;
      if (typeof(indent) == "undefined" || indent == "") {
        indent = "";
        tagStr = "\nAll common tags:"
        separator = " "; // params common to all results at top full width
      } else {
        separator = "\n";
      }
      iterTree.tags.forEach(tag => {
        tagStr += separator + tag.name + ":" + tag.val;
      });
    }
    tagStr = printf("%-" + midPoint + "s", indent + tagStr);
    if (len < tagStr.length) {
      len = tagStr.length;
    }
    process.stdout.write(tagStr + "\n");
  }

  var paramStr = "";
  if (typeof(iterTree.params) != "undefined") {
    if (iterTree.params.length == 1) {
      paramStr += iterTree.params[0].arg + ":" + iterTree.params[0].val;
    } else {
      var separator;
      if (typeof(indent) == "undefined" || indent == "") {
        indent = "";
        paramStr = "\nAll common params:";
        separator = " "; // params common to all results at top full width
      } else {
        separator = "\n";
      }
      iterTree.params.forEach(param => {
        paramStr += separator + param.arg + ":" + param.val;
      });
    }
    paramStr = printf("%-" + midPoint + "s", indent + paramStr);
    if (len < paramStr.length) {
      len = paramStr.length;
    }
    process.stdout.write(paramStr + "\n");
  }


  if (typeof(indent) == "undefined" || indent == "") {  // print the row names after all common tags/params are printed
    var header = printf("\n%" + midPoint + "s" + " %10s %10s", "label", "mean", "stddevpct");
    console.log(header);
    indent = "";
  }

  if (typeof(iterTree.breakout) != "undefined") {
    iterTree.breakout.forEach(iter => {
      reportIters(iter, "  " + indent);
    });
  }
  if (typeof(iterTree.iterations) != "undefined") {

    const sorted = iterTree.iterations.sort((a, b) => (a.labels.localeCompare(b.labels , undefined, {numeric: true, sensitivity: 'base' })));

    sorted.forEach(i => {
      var metrics = printf("%" + midPoint+ "s" + " %10.4f %10.4f", i["labels"], i["mean"], i["stddevpct"]);
      console.log(metrics);
    });
  }

}

// getIters(): filter and group interations, typically for generating comparisons (clustered bar graphs)
getIters = function (url, filterByAge, filterByTags, filterByParams, dontBreakoutTags, omitTags, dontBreakoutParams, omitParams, breakoutOrderTags, breakoutOrderParams) {

  // Process:
  // 1) Get run.ids from age, benchmark, and tag filters
  // 2) From run.ids, get iteration.ids from param filters
  // 3) From iteration.ids, organize into groups from tag and param grouping preferences

  const now = Date.now();
  var ndjson = '';
  var ndjson2 = '';
  var indexjson = '';
  var qjson = '';
  var newestDay = now - (1000 * 3600 * 24 * filterByAge.split('-')[0]);
  var oldestDay = now - (1000 * 3600 * 24 * filterByAge.split('-')[1]);
  
  var base_q =
      { "query":
        { "bool": 
          { "filter": 
            [ 
              {"range": {"run.end": { "lte": newestDay}}},
              {"range": {"run.begin": { "gte": oldestDay}}}
            ]
          }
        },
        "_source": "run.id",
        "size": 1000
      };
  var base_q_json = JSON.stringify(base_q);

  // Each filter of tagName:tagVal must be a separate query.
  // However, all of these queries can be submitted together via msearch.
  // The responses (a list of run.ids for each query) must be intersected
  // to have only the run.ids that match *all* tag filters.
  filterByTags.forEach(nameval => {
    var tag_query = JSON.parse(base_q_json);
    var name = nameval.split(':')[0];
    var val = nameval.split(':')[1];
    var tagNameTerm = { "term": { "tag.name": name}};
    tag_query.query.bool.filter.push(tagNameTerm);
    if (val != "tag-not-used") {
      var tagValTerm = { "term": { "tag.val": val}};
      tag_query.query.bool.filter.push(tagValTerm);
      ndjson += '{"index": "' + getIndexBaseName() + 'tag' + '" }\n';
      ndjson += JSON.stringify(tag_query) + "\n"; 
    } else {
      // Find the run IDs which have this tag name present (value does not matter)
      ndjson2 += '{"index": "' + getIndexBaseName() + 'tag' + '" }\n';
      ndjson2 += JSON.stringify(tag_query) + "\n"; 
    }
  }); 

  var resp = esRequest(url, "tag/_doc/_msearch", ndjson);
  var data = JSON.parse(resp.getBody());
  var runIds = [];
  data.responses.forEach(response => {
    var theseRunIds = [];
    response.hits.hits.forEach(run => {
      theseRunIds.push(run._source.run.id);
    });
    runIds.push(theseRunIds);
  });
  var intersectedRunIds = intersectAllArrays(runIds);

  if (ndjson2 != "") {
    //console.log("query:\n" + ndjson2);
    var resp2 = esRequest(url, "tag/_doc/_msearch", ndjson2);
    var data2 = JSON.parse(resp2.getBody());
    data2.responses.forEach(response => {
      response.hits.hits.forEach(run => {
        if (intersectedRunIds.includes(run._source.run.id)) {
          var index = intersectedRunIds.indexOf(run._source.run.id);
          if (index !== -1) {
            intersectedRunIds.splice(index, 1);
          }
        }
      });
    });
  }


  // Now we can get all of the iterations for these run.ids
  var q = { 'query': { 'bool': { 'filter': [ { "terms": { "run.id": intersectedRunIds }} ] }}, 'size': 1000};
  resp = esRequest(url, "iteration/_doc/_search", q);
  data = JSON.parse(resp.getBody());
  var iterIdsFromRun = [];
  data.hits.hits.forEach(element => {
    iterIdsFromRun.push(element._source.iteration.id);
  });

  // Each filter of paramArg:paramVal must be a separate query.
  // However, all of these queries can be submitted together via msearch.
  // The responses (a list of iteration.ids for each query) must be intersected
  // to have only the iteration.ids that match all param filters.
  ndjson = '';
  filterByParams.forEach(nameval => {
    var param_query = JSON.parse(base_q_json);
    param_query._source = "iteration.id";
    //console.log("\n\nparam_query: " + JSON.stringify(param_query));
    var paramArg = { "term": { "param.arg": nameval.split(':')[0]}};
    //console.log("paramArg: " + JSON.stringify(paramArg));
    param_query.query.bool.filter.push(paramArg);
    //console.log("param_query: " + JSON.stringify(param_query));
    var paramVal = { "term": { "param.val": nameval.split(':')[1]}};
    //console.log("paramVal: " + JSON.stringify(paramVal));
    param_query.query.bool.filter.push(paramVal);
    //console.log("param_query: " + JSON.stringify(param_query));
    ndjson += '{"index": "' + getIndexBaseName() + 'param' + '" }\n';
    ndjson += JSON.stringify(param_query) + "\n"; 
  }); 

  var resp = esRequest(url, "param/_doc/_msearch", ndjson);
  var data = JSON.parse(resp.getBody());
  var iterationIds = [];
  data.responses.forEach(response => {
    var theseIterationIds = [];
    response.hits.hits.forEach(iteration => {
      theseIterationIds.push(iteration._source.iteration.id);
    });
    iterationIds.push(theseIterationIds);
  });
  var iterIdsFromParam = intersectAllArrays(iterationIds);

  // Get the iteration IDs that are common from both tag and param filters
  var iterIds = [];
  iterIds.push(iterIdsFromRun);
  iterIds.push(iterIdsFromParam);
  var allFilterIterIds = intersectAllArrays(iterIds);

  // Get all possible tag names
  console.log("Finding all tag names");
  var allTagNames = [];
  allFilterIterIds.forEach(iter => {
    var runId = getRunFromIter(url, iter);
    var tags = getTags(url, runId);
    tags.forEach(thisTag => {
      if (!allTagNames.includes(thisTag["name"])) {
        allTagNames.push(thisTag["name"]);
      }
    });
  });

  // Get all possible param args
  console.log("Finding all param args");
  var allParamArgs = [];
  allFilterIterIds.forEach(iter => {
    var params = getParams(url, [{ "term": "iteration.id", "match": "eq", "value": iter }]);
    params.forEach(thisParam => {
      if (!allParamArgs.includes(thisParam["arg"])) {
        allParamArgs.push(thisParam["arg"]);
      }
    });
  });

  // Build look-up tables [iterId][param-arg] = param-value and [iterId][tag-name] = tag-value
  console.log("Building param and tag look-up tables");
  var paramValueByIterAndArg = {};
  var tagValueByIterAndName = {};
  var iterations = [];
  allFilterIterIds.forEach(iter => {
    var params = getParams(url, [{ "term": "iteration.id", "match": "eq", "value": iter }]);
    var runId = getRunFromIter(url, iter);
    var tags = getTags(url, runId);
    var thisIter = { "iterId": iter, "tags": tags, "params": params };
    params.forEach(thisParam => {
      if (typeof(paramValueByIterAndArg[iter]) == "undefined") {
        paramValueByIterAndArg[iter] = {};
      }
      paramValueByIterAndArg[iter][thisParam["arg"]] = thisParam["val"];
    });
    tags.forEach(thisTag => {
      if (typeof(tagValueByIterAndName[iter]) == "undefined") {
        tagValueByIterAndName[iter] = {};
      }
      tagValueByIterAndName[iter][thisTag["name"]] = thisTag["val"];
    });
    iterations.push(thisIter);
  });

  // Find the tag names which are present in every single iteration
  // We can only do "breakouts" if the tag is used everywhere
  console.log("Finding only the tag names which are present in all iterations");
  var notCommonTagNames = [];
  var commonTagNames = allTagNames;
  allTagNames.forEach(name => {
    allFilterIterIds.forEach(iter => {
      if (typeof(tagValueByIterAndName[iter][name]) == "undefined") {
        if (!notCommonTagNames.includes(name)) {
          notCommonTagNames.push(name);
        }
        var index = commonTagNames.indexOf(name);
          if (index !== -1) {
            commonTagNames.splice(index, 1);
          }
        return;
      }
    });
  });

  // Find the param names which are present in every single iteration
  // We can only do "breakouts" if the param is used everywhere
  console.log("Finding only the param args which are present in all iterations");
  var notCommonParamArgs = [];
  var commonParamArgs = allParamArgs;
  allParamArgs.forEach(arg => {
    allFilterIterIds.forEach(iter => {
      if (typeof(paramValueByIterAndArg[iter][arg]) == "undefined") {
        if (!notCommonParamArgs.includes(arg)) {
          notCommonParamArgs.push(arg);
        }
        var index = commonParamArgs.indexOf(arg);
          if (index !== -1) {
            commonParamArgs.splice(index, 1);
          }
        return;
      }
    });
  });

  // For the notCommonTagNames, add this tag with a value of "unused"
  // to any iteration which has this tag missing
  notCommonTagNames.forEach(name => {
    for (var i=0; i<iterations.length; i++){
      var iterId = iterations[i]["iterId"];
      var foundTag = false;
      for (var j=0; j<iterations[i]["tags"].length; j++) {
        if (iterations[i]["tags"][j]["name"] == name) {
          console.log("Found tag " + name);
          foundTag = true;
        }
      }
      if (foundTag == false) {
        var newTag = { "name": name, "val": "<tag-not-used>" };
        console.log("Did not find tag " + name + ", so adding with val: <tag-not-used>");
        iterations[i]["tags"].push(newTag);
      }
    }
  });

  // For the notCommonParamArgs, add this param with a value of "unused"
  // to any iteration which has this param missing
  notCommonParamArgs.forEach(arg => {
    for (var i=0; i<iterations.length; i++){
      var iterId = iterations[i]["iterId"];
      var foundParam = false;
      for (var j=0; j<iterations[i]["params"].length; j++) {
        if (iterations[i]["params"][j]["arg"] == arg) {
          console.log("Found param " + arg);
          foundParam = true;
        }
      }
      if (foundParam == false) {
        var newParam = { "arg": arg, "val": "<param-not-used>" };
        console.log("Did not find param " + arg + ", so adding with val: <param-not-used>");
        iterations[i]["params"].push(newParam);
      }
    }
  });


  // Scan iterations to find all different values for each tag and param
  console.log("Finding all different values for each tag and param");
  var tags = {};
  var params = {};
  iterations.forEach(thisIter => {
    thisIter["tags"].forEach(tag => {
      if (!omitTags.includes(tag.name)) {
        if (typeof(tags[tag.name]) == "undefined") {
          tags[tag.name] = {};
        }
        if (typeof(tags[tag.name][tag.val]) == "undefined") {
          tags[tag.name][tag.val] = [];
        }
        //console.log("Adding tag name: " + tag.name + " val: " + tag.val + " to tags hash");
        tags[tag.name][tag.val].push(thisIter.iterId);
      }
    });

    thisIter.params.forEach(param => {
      if (!omitParams.includes(param.arg)) {
        if (typeof(params[param.arg]) == "undefined") {
          params[param.arg] = {};
        }
        if (typeof(params[param.arg][param.val]) == "undefined") {
          params[param.arg][param.val] = [];
        }
        params[param.arg][param.val].push(thisIter.iterId);
      }
    });

  });

  var iterTree = {};
  console.log("Build iterTree");
  iterTree = buildIterTree(url, params, tags, paramValueByIterAndArg, tagValueByIterAndName, allFilterIterIds, dontBreakoutTags, dontBreakoutParams, omitParams, breakoutOrderTags, breakoutOrderParams);
  return iterTree;

}
exports.getIters = getIters;

exports.getIterationDoc = function (url, id) {
  var q = { 'query': { 'bool': { 'filter': [ { "term": { "iteration.id": id }} ] }}};
  var resp = esRequest(url, "iteration/_doc/_search", q);
  var data = JSON.parse(resp.getBody());
  return data;
};

exports.getIterations = function (url, searchTerms) {
  var q = { 'query': { 'bool': { 'filter': [] }},
            '_source': "iteration.id", 'size': 1000,
            'sort': [ { "iteration.num": { "order": "asc", "numeric_type": "long" }} ] };
  if (searchTerms.length === 0) {
    console.log("Found no search terms\n");
    return;
  }
  searchTerms.forEach(element => {
    var myTerm = {};
    myTerm[element.term] = element.value;
    q.query.bool.filter.push({"term": myTerm});
  });
  var resp = esRequest(url, "iteration/_doc/_search", q);
  var data = JSON.parse(resp.getBody());
  var ids = [];
  if (Array.isArray(data.hits.hits) && data.hits.hits.length > 0) {
    data.hits.hits.forEach(element => {
      ids.push(element._source.iteration.id);
    });
  }
  return ids;
};

getParams = function (url, searchTerms) {
  var q = { 'query': { 'bool': { 'filter': [] }},
      '_source': "param",
            'size': 1000 };
  if (searchTerms.length === 0) {
    return;
  }
  searchTerms.forEach(element => {
    var myTerm = {};
    myTerm[element.term] = element.value;
    q.query.bool.filter.push({"term": myTerm});
  });
  var resp = esRequest(url, "param/_doc/_search", q);
  var data = JSON.parse(resp.getBody());
  var params = [];
  if (Array.isArray(data.hits.hits) && data.hits.hits.length > 0) {
    data.hits.hits.forEach(element => {
      params.push({"arg": element._source.param.arg, "val": element._source.param.val});
    });
  }
  return params;
};
exports.getParams = getParams;


getSamples = function (url, searchTerms) {
  var q = { 'query': { 'bool': { 'filter': [] }},
            '_source': "sample.id", 'size': 1000,
            'sort': [ { "sample.num": { "order": "asc"}} ] };
  if (searchTerms.length === 0) {
    return;
  }
  searchTerms.forEach(element => {
    var myTerm = {};
    myTerm[element.term] = element.value;
    q.query.bool.filter.push({"term": myTerm});
  });
  var resp = esRequest(url, "sample/_doc/_search", q);
  var data = JSON.parse(resp.getBody());
  var ids = [];
  if (Array.isArray(data.hits.hits) && data.hits.hits.length > 0) {
    data.hits.hits.forEach(element => {
      ids.push(element._source.sample.id);
    });
  }
  return ids;
};
exports.getSamples = getSamples;

getPrimaryPeriodName = function (url, iterId) {
  var q = { 'query': { 'bool': { 'filter': [ {"term": {"iteration.id": iterId}} ] }},
            '_source': 'iteration.primary-period',
            'size': 1 };
  var resp = esRequest(url, "iteration/_doc/_search", q);
  var data = JSON.parse(resp.getBody());
  if (data.hits.hits[0]._source.iteration['primary-period']) {
    return data.hits.hits[0]._source.iteration['primary-period'];
  }
};
exports.getPrimaryPeriodName = getPrimaryPeriodName;

getRunFromIter = function (url, iterId) {
  var q = { 'query': { 'bool': { 'filter': [ {"term": {"iteration.id": iterId}} ] }},
            '_source': [ 'run.id' ],
            'size': 1 };
  var resp = esRequest(url, "iteration/_doc/_search", q);
  var data = JSON.parse(resp.getBody());
  if (data.hits.hits[0] && data.hits.hits[0]._source &&
      data.hits.hits[0]._source.run &&
      data.hits.hits[0]._source.run.id) {
    return data.hits.hits[0]._source.run.id;
  }
};
exports.getRunFromIter = getRunFromIter;

getRunFromPeriod = function (url, periId) {
  var q = { 'query': { 'bool': { 'filter': [ {"term": {"period.id": periId}} ] }},
            '_source': [ 'run.id' ],
            'size': 1 };
  var resp = esRequest(url, "period/_doc/_search", q);
  var data = JSON.parse(resp.getBody());
  if (data.hits.hits[0] && data.hits.hits[0]._source &&
      data.hits.hits[0]._source.run &&
      data.hits.hits[0]._source.run.id) {
    return data.hits.hits[0]._source.run.id;
  }
};
exports.getRunFromPeriod = getRunFromPeriod;

getPeriodRange = function (url, periId) {
  var q = { 'query': { 'bool': { 'filter': [ {"term": {"period.id": periId}} ] }},
            '_source': [ 'period.begin', 'period.end' ],
            'size': 1 };
  var resp = esRequest(url, "period/_doc/_search", q);
  var data = JSON.parse(resp.getBody());
  if (data.hits.hits[0] && data.hits.hits[0]._source &&
      data.hits.hits[0]._source.period &&
      data.hits.hits[0]._source.period.begin &&
      data.hits.hits[0]._source.period.end) {
    return { "begin": data.hits.hits[0]._source.period.begin, "end": data.hits.hits[0]._source.period.end };
  }
};
exports.getPeriodRange = getPeriodRange;

getSampleStatus = function (url, sampId) {
  var q = { 'query': { 'bool': { 'filter': [ {"term": {"sample.id": sampId}} ] }},
            '_source': 'sample.status',
            'size': 1 };
  var resp = esRequest(url, "sample/_doc/_search", q);
  var data = JSON.parse(resp.getBody());
  if (data.hits.total.value > 0 && Array.isArray(data.hits.hits) && data.hits.hits[0]._source.sample.status) {
    return data.hits.hits[0]._source.sample.status;
  } else {
    console.log("sample status not found\n");
  }
};
exports.getSampleStatus = getSampleStatus;

getPrimaryPeriodId = function (url, sampId, periName) {
  var q = { 'query': { 'bool': { 'filter': [ {"term": {"sample.id": sampId}}, {"term": {"period.name": periName}} ] }},
            '_source': 'period.id',
            'size': 1 };
  var resp = esRequest(url, "period/_doc/_search", q);
  var data = JSON.parse(resp.getBody());
  if (data.hits.total.value > 0 && Array.isArray(data.hits.hits) && data.hits.hits[0]._source.period.id) {
    return data.hits.hits[0]._source.period.id;
  } else {
    return null;
  }
};
exports.getPrimaryPeriodId = getPrimaryPeriodId;

exports.getMetricSources = function (url, runId) {
  var q = { 'query': { 'bool': { 'filter': [ {"term": {"run.id": runId}} ] }},
            'aggs': { 'source': { 'terms': { 'field': 'metric_desc.source', "size": 10000 }}},
            'size': 0 };
  var resp = esRequest(url, "metric_desc/_doc/_search", q);
  var data = JSON.parse(resp.getBody());
  if (Array.isArray(data.aggregations.source.buckets)) {
    var sources = [];
    data.aggregations.source.buckets.forEach(element => {
      sources.push(element.key);
    });
    return sources;
  }
};

exports.getMetricTypes = function (url, runId, source) {
  var q = { 'query': { 'bool': { 'filter': [ {"term": {"run.id": runId}}, {"term": {"metric_desc.source": source}} ] }},
            'aggs': { 'source': { 'terms': { 'field': 'metric_desc.type', "size": 10000 }}},
            'size': 0 };
  var resp = esRequest(url, "metric_desc/_doc/_search", q);
  var data = JSON.parse(resp.getBody());
  if (Array.isArray(data.aggregations.source.buckets)) {
    var types = [];
    data.aggregations.source.buckets.forEach(element => {
      types.push(element.key);
    });
    return types;
  }
};

// For a specific metric-source and metric-type,
// find all the metadata names shared among all
// found metric docs.  These names are what can be
// used for "breakouts".
getMetricNames = function (url, runId, periId, source, type) {
  var q = { 'query': { 'bool': { 'filter': [ 
                                             {"term": {"metric_desc.source": source}},
                                             {"term": {"metric_desc.type": type}} ]
                               }},
            'aggs': { 'source': { 'terms': { 'field': 'metric_desc.names-list'}}},
            'size': 0 };
  if (periId != null) {
    q.query.bool.filter.push(JSON.parse('{"term": {"period.id": "' + periId + '"}}'));
  }
  if (runId != null) {
    q.query.bool.filter.push(JSON.parse('{"term": {"run.id": "' + runId + '"}}'));
  }
  var resp = esRequest(url, "metric_desc/_doc/_search", q);
  var data = JSON.parse(resp.getBody());
  var names = [];
  if (Array.isArray(data.aggregations.source.buckets)) {
    data.aggregations.source.buckets.forEach(element => {
      names.push(element.key);
    });
  }
  return names;
};
exports.getMetricNames = getMetricNames;

getTags = function (url, runId) {
  var q = { 'query': { 'bool': { 'filter': [ {"term": {"run.id": runId}} ] }},
            //'_source': "tag", 'size': 1000};
            'size': 1000};
  var resp = esRequest(url, "tag/_doc/_search", q);
  var data = JSON.parse(resp.getBody());
  //console.log(JSON.stringify(data, null,2));
  var tags = [];
  if (Array.isArray(data.hits.hits) && data.hits.hits.length > 0) {
    data.hits.hits.forEach(element => {
      tags.push({"name": element._source.tag.name, "val": element._source.tag.val});
    });
  }
  return tags;
};
exports.getTags = getTags;

exports.getBenchmarkName = function (url, runId) {
  var q = { 'query': { 'bool': { 'filter': [ {"term": {"run.id": runId}} ] }},
            '_source': "run.benchmark",
            'size': 1 };
  var resp = esRequest(url, "run/_doc/_search", q);
  var data = JSON.parse(resp.getBody());
  if (data.hits.hits[0]._source.run.benchmark) {
    return data.hits.hits[0]._source.run.benchmark;
  }
};

exports.getRunData = function (url, searchTerms) {
  var q = { 'query': { 'bool': { 'filter': [] }},
            'size': 1000 };
  if (searchTerms.length === 0) {
    return;
  }
  searchTerms.forEach(element => {
    var myTerm = {};
    myTerm[element.term] = element.value;
    q.query.bool.filter.push({"term": myTerm});
  });
  var resp = esRequest(url, "run/_doc/_search", q);
  var data = JSON.parse(resp.getBody());
  return data;
};

exports.getRuns = function (url, searchTerms) {
  var q = { 'query': { 'bool': { 'filter': [] }},
            'aggs': { 'source': { 'terms': { 'field': 'run.id', 'size': 10000}}},
            // it's possible to have multiple run docs with same ID, so use aggregation
            'size': 0 };
  searchTerms.forEach(element => {
    var myTerm = {};
    myTerm[element.term] = element.value;
    q.query.bool.filter.push({"term": myTerm});
  });
  var resp = esRequest(url, "run/_doc/_search", q);
  var data = JSON.parse(resp.getBody());
  var ids = [];
  if (Array.isArray(data.aggregations.source.buckets) && data.aggregations.source.buckets.length > 0) {
    data.aggregations.source.buckets.forEach(element => {
      ids.push(element.key);
    });
    return ids;
  }
};

exports.getDocCountByMetricId = function (url, Id, docType) {
  var q = { 'query': { 'bool': { 'filter': [ {"term": {"metric_desc.id": Id}} ] }}};
  var resp = esRequest(url, docType + "/_doc/_count", q);
  var data = JSON.parse(resp.getBody());
  return data.count;
};

exports.getDocCount = function (url, runId, docType) {
  var q = { 'query': { 'bool': { 'filter': [ {"term": {"run.id": runId}} ] }}};
  var resp = esRequest(url, docType + "/_doc/_count", q);
  var data = JSON.parse(resp.getBody());
  return data.count;
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
  var regExp = /([^\=]+)\=([^\=]+)/;
  //var matches = regExp.exec("");

  if (Array.isArray(breakout)) {
    breakout.forEach(field => {
      //if (/([^\=]+)\=([^\=]+)/.exec(field)) {
      var matches = regExp.exec(field);
      if (matches) {
        //field = $1;
        field = matches[1];
      }
      agg_str += ',"aggs": { "metric_desc.names.' + field + '": { "terms": ' +
                  '{ "show_term_doc_count_error": true, "size": ' + bigQuerySize + ',' + 
                  '"field": "metric_desc.names.' + field + '" }';
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
        label += '-' + '<' + thisTerm.term[field] + '>';
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
            'size': bigQuerySize };
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
  if (periId != null) {
    q.query.bool.filter.push(JSON.parse('{"term": {"period.id": "' + periId + '"}}'));
  }
  var resp = esRequest(url, "metric_desc/_doc/_search", q);
  var data = JSON.parse(resp.getBody());
  if (data.hits.total.value >= bigQuerySize) {
    return;
  }
  var metricIds = [];
  data.hits.hits.forEach(element => {
    metricIds.push(element._source.metric_desc.id);
  });
  return metricIds;
}
exports.getMetricIdsFromTerms = getMetricIdsFromTerms;

// Before querying for metric data, we must first find out which metric IDs we need
// to query.  There may be one or more groups of these IDs, depending if the user
// wants to "break-out" the metric (by some metadatam like cpu-id, devtype, etc).
// Find the number of groups needed based on the --breakout options, then find out
// what metric IDs belong in each group.
getMetricGroupsFromBreakout = function (url, runId, periId, source, type, breakout) {
  // First build the groups that we eneventually be populated with metric IDs.
  var metricGroupIdsByLabel = {};
  var q = { 'query': { 'bool': { 'filter': [ 
                                             {"term": {"metric_desc.source": source}},
                                             {"term": {"metric_desc.type": type}},
                                             {"term": {"run.id": runId}}
                                            ]
                               }},
            'size': 0 };

  if (periId != null) {
    q.query.bool.filter.push(JSON.parse('{"term": {"period.id": "' + periId + '"}}'));
  }
  q.aggs = JSON.parse(getBreakoutAggregation(source, type, breakout));

  // If the breaout contains a match requirement (host=myhost), then we must add a term filter for it.
  // Eventually it would be nice to have something other than a match, like a regex: host=/^client/.
  var regExp = /([^\=]+)\=([^\=]+)/;
  breakout.forEach(field => {
    var matches = regExp.exec(field);
    //if (/([^\=]+)\=([^\=]+)/.exec(field)) {
    if (matches) {
      //field = $1;
      //value = $2;
      field = matches[1];
      value = matches[2];
      q.query.bool.filter.push(JSON.parse('{"term": {"metric_desc.names.' + field + '": "' + value + '"}}'));
    }
  });
  var resp = esRequest(url, "metric_desc/_doc/_search", q);
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

// Like above but get the metric groups for multiple sets
getMetricGroupsFromBreakouts = function (url, sets) {
  var metricGroupIdsByLabel = [];
  //var indexjson = '{"index": "' + getIndexBaseName() + 'metric_data' + '" }\n';
  var indexjson = '{}\n';
  var index = JSON.parse(indexjson);
  var ndjson = "";

  sets.forEach(period => {
    var result = getBreakoutAggregation(period.source, period.type, period.breakout);
    var aggs = JSON.parse(result);
    var q = { 'query': { 'bool': { 'filter': [ 
                                              {"term": {"metric_desc.source": period.source}},
                                              {"term": {"metric_desc.type": period.type}}
                                             ]
                                }},
             'size': 0 };
  
    if (period.period != null) {
      q.query.bool.filter.push(JSON.parse('{"term": {"period.id": "' + period.period + '"}}'));
    }
    if (period.run != null) {
      q.query.bool.filter.push(JSON.parse('{"term": {"run.id": "' + period.run + '"}}'));
    }
    // If the breaout contains a match requirement (something like "host=myhost"), then we must add a term filter for it.
    // Eventually it would be nice to have something other than a match, like a regex: host=/^client/.
    var regExp = /([^\=]+)\=([^\=]+)/;
    period.breakout.forEach(field => {
      var matches = regExp.exec(field);
      if (matches) {
        field = matches[1];
        value = matches[2];
        q.query.bool.filter.push(JSON.parse('{"term": {"metric_desc.names.' + field + '": "' + value + '"}}'));
      }
    });
    q.aggs = aggs;
    ndjson += JSON.stringify(index) + "\n";
    ndjson += JSON.stringify(q) + "\n";
  });
  //console.log("request:\n" + ndjson);
  var resp = esRequest(url, "metric_desc/_doc/_msearch", ndjson);
  var data = JSON.parse(resp.getBody());

  // The response includes a result from a nested aggregation, which will be parsed to produce
  // query terms for each of the metric groups
  //var metricGroupTerms = getMetricGroupTermsFromAgg(data.aggregations, 0, "");
  //var metricGroupTerms = getMetricGroupTermsFromAgg(data.aggregations);
  var metricGroupIdsByLabelSets = [];
  for (var idx = 0; idx < data.responses.length; idx++) {
  //data.responses.forEach(response => {
    var metricGroupTerms = getMetricGroupTermsFromAgg(data.responses[idx].aggregations);
    // Derive the label from each group and organize into a dict, key = label, value = the filter terms 
    var metricGroupTermsByLabel = getMetricGroupTermsByLabel(metricGroupTerms);
    // Now iterate over these labels and query with the label's search terms to get the metric IDs
    Object.keys(metricGroupTermsByLabel).forEach(label => {
      metricGroupIdsByLabel[label] = getMetricIdsFromTerms(url, sets[idx].period, metricGroupTermsByLabel[label]);
      metricGroupIdsByLabelSets[idx] = {};
      metricGroupIdsByLabelSets[idx][label] = metricGroupIdsByLabel[label];
    });
  //});
  }
  return metricGroupIdsByLabelSets;
};
exports.getMetricGroupsFromBreakouts = getMetricGroupsFromBreakouts;

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
    reqjson  = '{';
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
    ndjson += JSON.stringify(index) + "\n";
    ndjson += JSON.stringify(req) + "\n";
    // This third request is for documents that had its begin during or before the time range, but
    // its end was after the time range.
    indexjson = '{"index": "' + getIndexBaseName() + 'metric_data' + '" }\n';
    reqjson  = '{';
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
    ndjson += JSON.stringify(index) + "\n";
    ndjson += JSON.stringify(req) + "\n";
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
    ndjson += JSON.stringify(index) + "\n"; //ensures JSON is exactly 1 line
    ndjson += JSON.stringify(req) + "\n"; //ensures JSON is exactly 1 line
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
  var resp = esRequest(url, "metric_data/_doc/_msearch", ndjson);
  var data = JSON.parse(resp.getBody());
  thisBegin = begin;
  thisEnd = begin + duration;
  var count = 0;
  //var subCount = 0;
  var elements = data.responses.length;
  var numMetricIds = metricIds.length;
  while (count < elements) {
    var timeWindowDuration = thisEnd - thisBegin + 1;
    var totalWeightTimesMetrics = timeWindowDuration * numMetricIds;
    //subCount++;
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
      if (data.responses[count +k].hits.total.value > data.responses[count +k].hits.hits.length) {
        console.log("ERROR: not all documents arer present in hits.hits[]\n");
      }
      data.responses[count + k].hits.hits.forEach(element => {
        partialDocs[element._id] = {};
        Object.keys(element._source.metric_data).forEach(key => {
          partialDocs[element._id][key] = element._source.metric_data[key];
        });
      });
    }
    // Now we can process the partialDocs
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
  }
  return values;
};
exports.getMetricDataFromIds = getMetricDataFromIds;

// Like above but queries for all sets of Metric IDs and for all labels
getMetricDataFromIdsSets = function (url, sets, metricGroupIdsByLabelSets) {
  var ndjson = "";
  for (var idx = 0; idx < metricGroupIdsByLabelSets.length; idx++) {
    Object.keys(metricGroupIdsByLabelSets[idx]).forEach(function(label) {
    //(metricGroupIdsByLabelSets[idx]).forEach(label => {
      var metricIds = metricGroupIdsByLabelSets[idx][label];
      var begin = Number(sets[idx].begin);
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
        reqjson  = '{';
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
        reqjson += '        {"terms": {"metric_desc.id": ' + JSON.stringify(metricIds) + '}}\n';
        reqjson += '      ]';
        reqjson += '    }';
        reqjson += '  }';
        reqjson += '}';
        index = JSON.parse(indexjson);
        req = JSON.parse(reqjson);
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
        reqjson += '        {"terms": {"metric_desc.id": ' + JSON.stringify(metricIds) + '}}\n';
        reqjson += '      ]';
        reqjson += '    }';
        reqjson += '  }';
        reqjson += '}\n';
        index = JSON.parse(indexjson);
        req = JSON.parse(reqjson);
        ndjson += JSON.stringify(index) + "\n"; //ensures JSON is exactly 1 line
        ndjson += JSON.stringify(req) + "\n"; //ensures JSON is exactly 1 line

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


  var resp = esRequest(url, "metric_data/_doc/_msearch", ndjson);
  var data = JSON.parse(resp.getBody());
  var elements = data.responses.length;

  var valueSets = [];
  var count = 0;
  for (var idx = 0; idx < metricGroupIdsByLabelSets.length; idx++) {
    thisSetElements = elements / metricGroupIdsByLabelSets.length;
    var valuesByLabel = {};
    Object.keys(metricGroupIdsByLabelSets[idx]).forEach(function(label) {
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
            //for my $key (keys %{ $$j{'_source'}{'metric_data'} }) {
            partialDocs[element._id] = {};
            Object.keys(element._source.metric_data).forEach(key => {
              //partial_docs[{$$j{'_id'}}{$key} = $$j{'_source'}{'metric_data'}{$key};
              partialDocs[element._id][key] = element._source.metric_data[key];
            });
          });
        }
        // Now we can process the partialDocs
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
        });
        var result = (aggAvgTimesWeight + sumValueTimesWeight) / totalWeightTimesMetrics;
        result *= numMetricIds;
        result = Number.parseFloat(result).toPrecision(4);
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
        //thisBegin = thisEnd;
        //thisEnd += thisEnd + duration + 1;
        //if (thisEnd > end) {
          //thisEnd = end;
        //}
      }
      valuesByLabel[label] = values;
    });
    valueSets[idx] = valuesByLabel;
  }
  return valueSets;
};
exports.getMetricDataFromIds = getMetricDataFromIds;

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
exports.getMetricData = function(url, runId, periId, source, type, begin, end, resolution, breakout, filter) {
  var data = { "name": source, "type": type, "label": "", "values": {},
               "breakouts": getMetricNames(url, runId, null, source, type) };
  if (runId == undefined) {
    if (periId != undefined) {
      runId = getRunFromPeriod(url, periId);
    } else {
      console.log("You must define either periId or the runId");
      return;
    }
  }
  if (begin == undefined || end == undefined) {
    if (periId == undefined) {
      console.log("You must define either periId or begin and end");
      return;
    }
    var range = getPeriodRange(url, periId);
    if (begin == undefined) {
      begin = range.begin;
    }
    if (end == undefined) {
      end = range.end;
    }
  }
  // At this point the period ID is not needed because we have a runId, begin, and end
  periId = undefined;
  var usedBreakouts = [];
  var regExp = /([^\=]+)\=([^\=]+)/;
  breakout.forEach(field => {
    var matches = regExp.exec(field);
    if (matches) {
      field = matches[1];
      value = matches[2];
    }
    data.label += "-" + "<" + field + ">";
    //TODO: validate is user's breakouts are available by checking against data.breakouts
    usedBreakouts.push(field);
  });
  data.label = data.label.replace('-', '');
  data.breakouts = data.breakouts.filter(n => !usedBreakouts.includes(n));
  var metricGroupIdsByLabel = getMetricGroupsFromBreakout(url, runId, periId, source, type, breakout);
  Object.keys(metricGroupIdsByLabel).forEach(function(label) {
    data.values[label] = getMetricDataFromIds(url, begin, end, resolution, metricGroupIdsByLabel[label]);
  });
  var reg = /(\w+)\:([-+]?[0-9]*\.?[0-9]+)/;
  var m = reg.exec(filter);
  if (filter != null && m) {
    Object.keys(data.values).forEach(metric => {
      var metricValue = 1.0 * data.values[metric][0].value;
      var condition = m[1];
      var value = m[2];
      if ( !(
                 (condition == "gt" && metricValue > value)
              || (condition == "ge" && metricValue >= value)
              || (condition == "lt" && metricValue < value)
              || (condition == "le" && metricValue <= value)
                                                            )) {
        delete data.values[metric];
      }
    });
  }
  return data;
};

getMetricDataSets = function(url, sets) {
  var metricGroupIdsByLabelSets = getMetricGroupsFromBreakouts(url, sets);
  var dataSets = getMetricDataFromIdsSets(url, sets, metricGroupIdsByLabelSets);
  return dataSets;
}
exports.getMetricDataSets = getMetricDataSets;


getIterMetrics = function(url, iterationId) {
  var result = {};
  var primaryMetric = getPrimaryMetric(url, iterationId);
  var primaryPeriodName = getPrimaryPeriodName(url, iterationId);
  if (primaryPeriodName == undefined) {
    console.log("      the primary period-name for this iteration is undefined, exiting\n");
    process.exit(1);
  }
  var samples = getSamples(url, [{ "term": "iteration.id", "match": "eq", "value": iterationId }]);
  var sampleTotal = 0;
  var sampleCount = 0;
  var sampleVals = [];
  var sampleList = "";
  var periods = [];
  samples.forEach(sample => {
    if (getSampleStatus(url, sample) == "pass") {
      var primaryPeriodId = getPrimaryPeriodId(url, sample, primaryPeriodName);
      if (primaryPeriodId == undefined || primaryPeriodId == null) {
        console.log("          the primary perdiod-id for this sample is not valid, exiting\n");
        process.exit(1);
      }
      //console.log("          primary period-id: %s", primaryPeriodId);
      var range = getPeriodRange(url, primaryPeriodId);
      if (range == undefined || range == null) {
        console.log("          the range for the primary period is undefined, exiting");
        process.exit(1);
      }
      var breakout = []; // By default we do not break-out a benchmark metric, so this is empty
      // Needed for getMetricDataSets further below:
      var period = { "period": primaryPeriodId, "source": "uperf", "type": primaryMetric, "begin": range.begin, "end": range.end, "resolution": 1, "breakout": [] };
      periods.push(period);
    }
  });

  if (periods.length > 0) {
    var metricDataSets = getMetricDataSets(url, periods);
    var msampleCount = 0;
    var msampleVals = [];
    var msampleTotal = 0;
    var msampleList = "";
    metricDataSets.forEach(metricData => {
      var msampleVal = metricData[""];
      if (msampleVal && msampleVal[0] && msampleVal[0].value) {
        msampleVal = parseFloat(msampleVal[0].value);
        msampleVals.push(msampleVal);
        msampleTotal += msampleVal;
        var msampleFixed = msampleVal.toFixed(2);
        msampleList += " " + msampleFixed;
        msampleCount++;
      }
    });
    if (msampleCount > 0) {
      var mean = msampleTotal / msampleCount;
      var diff = 0;
      msampleVals.forEach(val => {
        diff += (mean - val) * (mean - val);
      });
      diff /= (msampleCount - 1);
      var mstddev = Math.sqrt(diff);
      var mstddevpct = 100 * mstddev / mean;
      result = { "mean": parseFloat(mean).toFixed(2), "min": parseFloat(Math.min(...msampleVals)).toFixed(2), "max": parseFloat(Math.max(...msampleVals)).toFixed(2), "stddev": parseFloat(mstddev).toFixed(2), "stddevpct": parseFloat(mstddevpct).toFixed(2) };
    }
  }
  return result;
}
exports.getIterMetrics = getIterMetrics;
