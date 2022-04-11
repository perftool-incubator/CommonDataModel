//# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript

var cdm = require('./cdm');
const file = require('fs');
var program = require('commander');

program
  .version('0.1.0')
  .option('--user <"full user name">')
  .option('--email <email address>')
  .option('--tag <tag:pair>')
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
if (program.tag) {
  searchTerms.push({ "term": "tag", "match": "eq", "value": program.tag });
}
if (program.harness) {
  searchTerms.push({ "term": "run.harness", "match": "eq", "value": program.harness });
}
if (!program.url) {
  program.url = "localhost:9200";
}
var runIds = cdm.getRunsFromTag(program.url, program.tag);
console.log(runIds)
if (runIds.length == 0) {
  console.log("The tag could not be found, exiting");
}

runIds.forEach(runId => {
  var benchName = cdm.getBenchmarkName(program.url, runId);
  var benchIterations = cdm.getIterations(program.url, [{ "term": "run.id", "match": "eq", "value": runId }]);
  if (benchIterations.length == 0) {
    console.log("There were no iterations found, exiting");
    return;
  }
  var prevDescriptionCSV = '';
  benchIterations.forEach(iterationId => {
    var file_name = '/tmp/' + runId + program.tag.replace(':','_') + '-data.csv';
    var descriptionCSV = 'runId,';
    var dataCSV = runId + ',';
    descriptionCSV += 'iterationId,';
    dataCSV += iterationId + ',';

    var tags = cdm.getTags(program.url, runId);
    tags.sort((a, b) => a.name < b.name ? -1 : 1)
    tags.forEach(tag => {
      descriptionCSV += tag.name + ',';
      dataCSV += tag.val + ',';
    });

    var params = cdm.getParams(program.url, [{ "term": "iteration.id", "match": "eq", "value": iterationId }]);
    params.sort((a, b) => a.arg < b.arg ? -1 : 1);
    params.forEach(param => {
      descriptionCSV += param.arg + ',';
      dataCSV += param.val + ',';
    });
    var primaryMetric = cdm.getPrimaryMetric(program.url, iterationId);
    var primaryPeriodName = cdm.getPrimaryPeriodName(program.url, iterationId);
    if (primaryPeriodName == undefined) {
      console.log("      the primary period-name for this iteration is undefined, exiting\n");
      process.exit(1);
    }
    var samples = cdm.getSamples(program.url, [{ "term": "iteration.id", "match": "eq", "value": iterationId }]);
    var sampleTotal = 0;
    var sampleCount = 0;
    var sampleVals = [];
    var sampleList = "";
    var workers_cpu = {};
    var periods = [];
    samples.forEach(sample => {
        if (cdm.getSampleStatus(program.url, sample) == "pass") {
        var primaryPeriodId = cdm.getPrimaryPeriodId(program.url, sample, primaryPeriodName);
        if (primaryPeriodId == undefined || primaryPeriodId == null) {
        console.log("          the primary perdiod-id for this sample is not valid, exiting\n");
        process.exit(1);
        }
        var range = cdm.getPeriodRange(program.url, primaryPeriodId);
        if (range == undefined || range == null) {
        console.log("          the range for the primary period is undefined, exiting");
        process.exit(1);
        }
        var breakout = []; // By default we do not break-out a benchmark metric, so this is empty
        var period = { "run": runId, "period": primaryPeriodId, "source": benchName, "type": primaryMetric, "begin": range.begin, "end": range.end, "resolution": 1, "breakout": [] };
        periods.push(period);
        var cpu_usage = cdm.getMetricData(program.url, period.run, period.period, "mpstat","Busy-CPU",period.begin,period.end,period.resolution,["cstype","csid"]);
        var workers = Object.keys(cpu_usage.values);
        workers.forEach((key, index) => {
            if( !Array.isArray(workers_cpu[key])){
              workers_cpu[key] = [];
              }
            workers_cpu[key].push(parseFloat(cpu_usage.values[key][0].value));

            });
        }
    });

    /*
     * Calculations on the samples to get the primary metric
     */
    if (periods.length > 0) {
      var metricDataSets = cdm.getMetricDataSets(program.url, periods);
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
        var mean = parseFloat(mean).toFixed(2);
        var min = parseFloat(Math.min(...msampleVals)).toFixed(2);
        var max = parseFloat(Math.max(...msampleVals)).toFixed(2);
        var stddev = parseFloat(mstddev).toFixed(2);
        var stddevpct = parseFloat(mstddevpct).toFixed(2);
        descriptionCSV += primaryMetric +  '-mean,';
        descriptionCSV += primaryMetric +  '-min,';
        descriptionCSV += primaryMetric +  '-max,';
        descriptionCSV += primaryMetric +  '-stddev,';
        descriptionCSV += primaryMetric +  '-stddevpct,';
        dataCSV += mean + ',' + min + ','+ max + ',' + stddev + ','+stddevpct + ',';

      }
      /*
       * stats on worker-cpu
       */
      Object.keys(workers_cpu).forEach(worker => {
        //javascript array reduction
        var initVal = 0;
        var total_cpu = workers_cpu[worker].reduce((initVal,sumVal) => initVal + sumVal, initVal);
        var mean_cpu = parseFloat(total_cpu / workers_cpu[worker].length).toFixed(2);
        descriptionCSV += worker + '-mean_cpu,';
        dataCSV+= mean_cpu + ',';
      });

    }// periods

    /*
     * Samples MATH is over, put it all out to CSV
     */

    //console.log(descriptionCSV);
    //var unifiedCSV = descriptionCSV + dataCSV;

    if(descriptionCSV != prevDescriptionCSV){
      console.log('New description string - ' + descriptionCSV);
      //TODO: remove the trailing ',' in the line, but sed can get rid of it for us
      file.appendFile(file_name, descriptionCSV +'\n',err => {
        if(err) {
          console.log(err);
          console.log(file_name + ' error in description to file');
        }
      });
      prevDescriptionCSV = descriptionCSV;
    };

      //TODO: remove the trailing ',' in the line
    file.appendFile(file_name, dataCSV +'\n',err => {
      if(err) {
        console.log(err);
        console.log(file_name + ' error in writing data to file');
      }
    });
  });// iteration


}); //runID

