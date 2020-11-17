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
  console.log("tags: " + tags);
  var benchName = cdm.getBenchmarkName(program.url, runId);
  var benchTags = cdm.getTags(program.url, runId);
  var metricSources = cdm.getMetricSources(program.url, runId);
  metricSources.forEach(metricSource => {
    var metricTypes = cdm.getMetricTypes(program.url, runId, metricSource);
    console.log("metric-source: %s  (metric-types: %s)", metricSource, metricTypes);
  });
  var benchIterations = cdm.getIterations(program.url, [{ "term": "run.id", "match": "eq", "value": runId }]);
  benchIterations.forEach(iterationId => {
    console.log("  iteration-id: %s", iterationId);
    var params = cdm.getParams(program.url, [{ "term": "iteration.id", "match": "eq", "value": iterationId }]);
    params.sort((a, b) => a.arg < b.arg ? -1 : 1)
    var paramList = "    params: ";
    params.forEach(param => {
      paramList += param.arg + "=" + param.val + " ";
    });
    console.log(paramList);
    //d = Date.now();
    //console.log(d + " call:getPrimaryMetric");
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
        //dPrev = d;
        //d = Date.now();
        //console.log(d + " return:getPrimaryPeriodId, call:getNameFormat +" + (d - dPrev));
        var nameFormat = cdm.getNameFormat(program.url, primaryPeriodId, benchName, primaryMetric);
        //dPrev = d;
        //d = Date.now();
        //console.log(d + " return:getNameFormat, call:getPeriodRange +" + (d - dPrev));
        var range = cdm.getPeriodRange(program.url, primaryPeriodId);
        //dPrev = d;
        //d = Date.now();
        //console.log(d + " return:getPeriodRange, call:getMetricDataFromPeriod +" + (d - dPrev));
        var breakout = [];
        var period = { "run": runId, "period": primaryPeriodId, "source": benchName, "type": primaryMetric, "begin": range.begin, "end": range.end, "resolution": 1, "breakout": [] };
        periods.push(period);
        //dPrev = d;
        //d = Date.now();
        var metricData = cdm.getMetricDataFromPeriod(program.url, runId, primaryPeriodId, benchName, primaryMetric, range.begin, range.end, 1, breakout);
        //console.log(d + " return:getMetricDataFromPeriod +" + (d - dPrev));
        //console.log(JSON.stringify(metricData));
        var sampleVal = metricData.values[""];
        if (sampleVal && sampleVal[0] && sampleVal[0].value) {
          sampleVal = parseFloat(sampleVal[0].value);
          sampleVals.push(sampleVal);
          sampleTotal += sampleVal;
          var sampleFixed = sampleVal.toFixed(2);
          sampleList += " " + sampleFixed;
          sampleCount++;
        }
      }
    });
    //d = Date.now();
    //console.log(d + " call:getMetricDataFromPeriods");
    //var metricDataSets = cdm.getMetricDataFromPeriods(program.url, periods);
    //dPrev = d;
    //d = Date.now();
    //console.log(d + " return:getMetricDataFromPeriods +" + (d - dPrev));
    //console.log(JSON.stringify(metricDataSets));
    /*
    metricDataSets.forEach(metricData => {
      var sampleVal = metricData.values[""];
      if (sampleVal && sampleVal[0] && sampleVal[0].value) {
        sampleVal = parseFloat(sampleVal[0].value);
        sampleVals.push(sampleVal);
        sampleTotal += sampleVal;
        var sampleFixed = sampleVal.toFixed(2);
        sampleList += " " + sampleFixed;
        sampleCount++;
      }
    });
    */
    if (sampleCount > 0) {
      var mean = sampleTotal / sampleCount;
      var diff = 0;
      sampleVals.forEach(val => {
        diff += (mean - val) * (mean - val);
      });
      diff /= sampleTotal;
      var stddev = Math.sqrt(diff);
      var stddevpct = 100 * stddev / mean;
      console.log("    result: (" + primaryMetric + ") samples:" + sampleList +
                  " mean: " + parseFloat(mean).toFixed(2) + " stddev: " +
                  parseFloat(stddev).toFixed(2) + " stddevpct: " +
                  parseFloat(stddevpct).toFixed(2));
    }
  });
});

