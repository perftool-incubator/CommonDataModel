//# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript

var cdm = require('./cdm');
var program = require('commander');

program
  .version('0.1.0')
  .option('--user <"full user name">')
  .option('--email <email address>')
  .option('--run <run-ID>')
  .option('--harness <harness name>')
  .option('--url <host:port>')
  .parse(process.argv);

//console.log(JSON.stringify(program));

var searchTerms = [];
if (program.user) {
  searchTerms.push({ "term": "run.name", "match": "eq", "value": program.user });
}
if (program.email) {
  searchTerms.push({ "term": "run.email", "match": "eq", "value": program.email });
}
if (program.run) {
  searchTerms.push({ "term": "run.id", "match": "eq", "value": program.run });
}
if (program.harness) {
  searchTerms.push({ "term": "run.harness", "match": "eq", "value": program.harness });
}
if (!program.url) {
  program.url = "localhost:9200";
}
var runIds = cdm.getRuns(program.url, searchTerms);
if (runIds == undefined) {
  console.log("The run ID could not be found, exiting");
  process.exit(1);
}
runIds.forEach(runId => {
  console.log("\nrun-id: " + runId);
  var tags = cdm.getTags(program.url, runId);
  tags.sort((a, b) => a.name < b.name ? -1 : 1)
  var tagList = "  tags: ";
  tags.forEach(tag => {
    tagList += tag.name + "=" + tag.val + " ";
  });
  console.log(tagList);
  var benchName = cdm.getBenchmarkName(program.url, runId);
  console.log("  metrics:");
  var metricSources = cdm.getMetricSources(program.url, runId);
  metricSources.forEach(metricSource => {
    var metricTypes = cdm.getMetricTypes(program.url, runId, metricSource);
    console.log("    source: %s", metricSource);
    var typeList = "      types: ";
    metricTypes.forEach(type => {
      typeList += type + " ";
    });
    console.log(typeList);
  });
  console.log("  iterations:");
  var benchIterations = cdm.getIterations(program.url, [{ "term": { "run.id": runId }}]);
  if (benchIterations.length == 0) {
    console.log("There were no iterations found, exiting");
    process.exit(1);
  }
  var allParams = [];
  var allParamsCounts = [];
  benchIterations.forEach(iterationId => {
    var params = cdm.getParams(program.url, [{ "term": "iteration.id", "match": "eq", "value": iterationId }]);
    params.forEach(param => {
      newParam = param.arg + "=" + param.val;
      idx = allParams.indexOf(newParam)
      if (idx == -1) {
        allParams.push(newParam);
        allParamsCounts.push(1);
      } else {
        allParamsCounts[idx] += 1
      }
    });
  });
  var commonParams = [];
  for (var idx=0; idx<allParams.length; idx++) {
    if (allParamsCounts[idx] == benchIterations.length) {
      commonParams.push(allParams[idx]);
    }
  }
  commonParams.sort()
  var commonParamsStr = "    common params: ";
  commonParams.forEach(param => {
    commonParamsStr += param + " ";
  });
  console.log(commonParamsStr);
  benchIterations.forEach(iterationId => {
    console.log("    iteration-id: %s", iterationId);
    //d = Date.now();
    //console.log(d + " call:getParams");
    var params = cdm.getParams(program.url, [{ "term": "iteration.id", "match": "eq", "value": iterationId }]);
    params.sort((a, b) => a.arg < b.arg ? -1 : 1);
    var paramList = "      unique params: ";
    params.forEach(param => {
      paramStr = param.arg + "=" + param.val;
      if (commonParams.indexOf(paramStr) == -1) {
        paramList += param.arg + "=" + param.val + " ";
      }
    });
    console.log(paramList);
    //dPrev = d;
    //d = Date.now();
    //console.log(d + " return:getParams, call:getPrimaryMetric +" + (d - dPrev));
    var primaryMetric = cdm.getPrimaryMetric(program.url, iterationId);
    //dPrev = d;
    //d = Date.now();
    //console.log(d + " return:getPrimaryMetric, call:getPrimaryPeriodName +" + (d - dPrev));
    var primaryPeriodName = cdm.getPrimaryPeriodName(program.url, iterationId);
    //dPrev = d;
    //d = Date.now();
    //console.log(d + " return:getPrimaryPeriodName, call:getSamples +" + (d - dPrev));
    if (primaryPeriodName == undefined) {
      console.log("      the primary period-name for this iteration is undefined, exiting\n");
      process.exit(1);
    }
    console.log("      primary-period name: " + primaryPeriodName);
    var samples = cdm.getSamples(program.url, [ iterationId ]);
    //dPrev = d;
    //d = Date.now();
    //console.log(d + " return:getSamples +" + (d - dPrev));
    var sampleTotal = 0;
    var sampleCount = 0;
    var sampleVals = [];
    var sampleList = "";
    var periods = [];
    console.log("      samples:");
    samples[0].forEach(sample => {
      if (cdm.getSampleStatus(program.url, sample) == "pass") {
        //d = Date.now();
        console.log("        sample-id: " + sample);
        var primaryPeriodId = cdm.getPrimaryPeriodId(program.url, sample, primaryPeriodName);
        if (primaryPeriodId == undefined || primaryPeriodId == null) {
          console.log("          the primary perdiod-id for this sample is not valid, exiting\n");
          process.exit(1);
        }
        console.log("          primary period-id: %s", primaryPeriodId);
        var range = cdm.getPeriodRange(program.url, primaryPeriodId);
        if (range == undefined || range == null) {
          console.log("          the range for the primary period is undefined, exiting");
          process.exit(1);
        }
        console.log("          period range: begin: " + range.begin + " end: " + range.end);
        //dPrev = d;
        //d = Date.now();
        //console.log(d + " return:getPeriodRange +" + (d - dPrev));
        var breakout = []; // By default we do not break-out a benchmark metric, so this is empty
        // Needed for getMetricDataSets further below:
        var period = { "run": runId, "period": primaryPeriodId, "source": benchName, "type": primaryMetric, "begin": range.begin, "end": range.end, "resolution": 1, "breakout": [] };
        periods.push(period);
      }
    });
 
    if (periods.length > 0) {
      var metricDataSets = cdm.getMetricDataSets(program.url, periods);
      //dPrev = d;
      //d = Date.now();
      //console.log(d + " return:getMetricDataSets +" + (d - dPrev));
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
        console.log("        result: (" + primaryMetric + ") samples:" + msampleList +
                    " mean: " + parseFloat(mean).toFixed(2) +
                    " min: " + parseFloat(Math.min(...msampleVals)).toFixed(2) +
                    " max: " + parseFloat(Math.max(...msampleVals)).toFixed(2) +
                    " stddev: " + parseFloat(mstddev).toFixed(2) +
                    " stddevpct: " + parseFloat(mstddevpct).toFixed(2));
      }
    }
  });
});

