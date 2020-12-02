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
  var benchIterations = cdm.getIterations(program.url, [{ "term": "run.id", "match": "eq", "value": runId }]);
  benchIterations.forEach(iterationId => {
    console.log("    iteration-id: %s", iterationId);
    //d = Date.now();
    //console.log(d + " call:getParams");
    var params = cdm.getParams(program.url, [{ "term": "iteration.id", "match": "eq", "value": iterationId }]);
    params.sort((a, b) => a.arg < b.arg ? -1 : 1)
    var paramList = "      params: ";
    params.forEach(param => {
      paramList += param.arg + "=" + param.val + " ";
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
    var samples = cdm.getSamples(program.url, [{ "term": "iteration.id", "match": "eq", "value": iterationId }]);
    //dPrev = d;
    //d = Date.now();
    //console.log(d + " return:getSamples +" + (d - dPrev));
    var sampleTotal = 0;
    var sampleCount = 0;
    var sampleVals = [];
    var sampleList = "";
    var periods = [];
    samples.forEach(sample => {
      if (cdm.getSampleStatus(program.url, sample) == "pass") {
        //d = Date.now();
        //console.log(d + " call:getPrimaryPeriodId");
        var primaryPeriodId = cdm.getPrimaryPeriodId(program.url, sample, primaryPeriodName);
	console.log("period-id: %s", primaryPeriodId);
        //dPrev = d;
        //d = Date.now();
        //console.log(d + " return:getPrimaryPeriodId, call:getNameFormat +" + (d - dPrev));
        var nameFormat = cdm.getNameFormat(program.url, primaryPeriodId, benchName, primaryMetric);
        //dPrev = d;
        //d = Date.now();
        //console.log(d + " return:getNameFormat, call:getPeriodRange +" + (d - dPrev));
        var range = cdm.getPeriodRange(program.url, primaryPeriodId);
        console.log("periodRange: " + JSON.stringify(range));
        //dPrev = d;
        //d = Date.now();
        //console.log(d + " return:getPeriodRange +" + (d - dPrev));
        var breakout = []; // By default we do not break-out a benchmark metric, so this is empty
        // Needed for getMetricDataFromPeriods further below:
        var period = { "run": runId, "period": primaryPeriodId, "source": benchName, "type": primaryMetric, "begin": range.begin, "end": range.end, "resolution": 1, "breakout": [] };
        periods.push(period);
      }
    });
 
    //d = Date.now();
    //console.log(d + " call:getMetricDataFromPeriods");
    var metricDataSets = cdm.getMetricDataFromPeriods(program.url, periods);
    //dPrev = d;
    //d = Date.now();
    //console.log(d + " return:getMetricDataFromPeriods +" + (d - dPrev));
    var msampleCount = 0;
    var msampleVals = [];
    var msampleTotal = 0;
    var msampleList = "";
    metricDataSets.forEach(metricData => {
      //console.log(JSON.stringify(metricData));
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
      diff /= msampleTotal;
      var mstddev = Math.sqrt(diff);
      var mstddevpct = 100 * mstddev / mean;
      console.log("      result: (" + primaryMetric + ") samples:" + msampleList +
                  " mean: " + parseFloat(mean).toFixed(2) + " stddev: " +
                  parseFloat(mstddev).toFixed(2) + " stddevpct: " +
                  parseFloat(mstddevpct).toFixed(2));
    }
  });
});

