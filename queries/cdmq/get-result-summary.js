//# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript

var cdm = require('./cdm');
var program = require('commander');

function list(val) {
  return val.split(',');
}

program
  .version('0.1.0')
  .option('--user <"full user name">')
  .option('--email <email address>')
  .option('--run <run-ID>')
  .option('--harness <harness name>')
  .option('--url <host:port>')
  .option('--output-dir <path>, if not used, output is to console only')
  .option('--output-format <fmt>, fmta[,fmtb]', 'one or more output formats: txt html', list, [])
  .parse(process.argv);

var termKeys = [];
var values = [];

if (program.user) {
  termKeys.push("run.name");
  values.push([ program.user ]);
}
if (program.email) {
  termKeys.push("run.email");
  values.push([ program.email ]);
}
if (program.run) {
  termKeys.push("run.id");
  values.push([ program.run ]);
}
if (program.harness) {
  termKeys.push("run.harness");
  values.push([ program.harness ]);
}
if (!program.url) {
  program.url = "localhost:9200";
}

if (!program.outputDir) {
  program.outputDir = "";
}
if (!program.outputFormat) {
  program.outputFormat = [""];
}
var noHtml = subtractTwoArrays(program.outputFormat, ['html']);
var txt_summary = '';
var html_summary = '<pre>';

function logOutput(str, formats) {
  txt_summary += str + '\n';
  if (formats.includes('html')) {
    html_summary += str + '\n';
  }
}

var runIds = cdm.mSearch(program.url, "run", termKeys, values, "run.id", 1000)[0];
if (runIds == undefined) {
  console.log("The run ID could not be found, exiting");
  process.exit(1);
}
runIds.forEach(runId => {
  logOutput("\nrun-id: " + runId, program.outputFormat);
  var tags = cdm.getTags(program.url, runId);
  tags.sort((a, b) => a.name < b.name ? -1 : 1)
  var tagList = "  tags: ";
  tags.forEach(tag => {
    tagList += tag.name + "=" + tag.val + " ";
  });
  logOutput(tagList, program.outputFormat);
  var benchName = cdm.getBenchmarkName(program.url, runId);
  logOutput("  benchmark: " + benchName, program.outputFormat);
  var benchIterations = cdm.getIterations(program.url, runId);
  if (benchIterations.length == 0) {
    console.log("There were no iterations found, exiting");
    process.exit(1);
  }
  var allParams = [];
  var allParamsCounts = [];

  var iterParams = cdm.mgetParams(program.url, benchIterations);
  var iterPrimaryMetrics = cdm.mgetPrimaryMetric(program.url, benchIterations);
  var iterPrimaryPeriodNames = cdm.mgetPrimaryPeriodName(program.url, benchIterations);
  var iterSamples = cdm.mgetSamples(program.url, benchIterations);
  var iterPrimaryPeriodIds = cdm.mgetPrimaryPeriodId(program.url, iterSamples, iterPrimaryPeriodNames);
  var iterPrimaryPeriodRanges = cdm.mgetPeriodRange(program.url, iterPrimaryPeriodIds);

  var commonParams = intersectAllArrays(iterParams);
  commonParams.sort()
  var commonParamsStr = "  common params: ";
  commonParams.forEach(param => {
    commonParamsStr += param + " ";
  });
  logOutput(commonParamsStr, program.outputFormat);

  logOutput("  metrics:", program.outputFormat);
  var metricSources = cdm.getMetricSources(program.url, runId);
  var metricTypes = cdm.mgetMetricTypes(program.url, metricSources);
  for (var i=0; i<metricSources.length; i++) {
    logOutput("    source: " + metricSources[i], program.outputFormat);
    var typeList = "      types: ";
    for (var j=0; j<metricTypes[i].length; j++) {
      typeList += type + " ";
    }
    logOutput(typeList, program.outputFormat);
  }

  // build the sets for the mega-query
  var sets = [];
  for (var i=0; i<benchIterations.length; i++) {
    for (var j=0; j<iterSamples[i].length; j++) {
      var set = { "run": runId
                  "period": iterPrimaryPeriodIds[i][j],
                  "source": benchName,
                  "type": iterPrimaryMetric[i][j],
                  "begin": iterPrimaryPeriodRanges[i][j].begin,
                  "end": iterPrimaryPeriodRanges[i][j].end,
                  "resolution": 1,
                  "breakout": [] };
      sets.push(set);
    }
  }

  logOutput("  iterations:", noHtml);
  var data = {};
  var numIter = {};
  var iterations = [];
  benchIterations.forEach(iterationId => {
    var series = {};
    var thisIter = {};
    thisIter['id'] = iterationId;
    logOutput("    iteration-id: " + iterationId, noHtml);
    var params = cdm.getParams(program.url, [{ "term": "iteration.id", "match": "eq", "value": iterationId }]);
    params.sort((a, b) => a.arg < b.arg ? -1 : 1);
    var paramList = "      unique params: ";
    series['label'] = "";
    params.forEach(param => {
      paramStr = param.arg + "=" + param.val;
      if (commonParams.indexOf(paramStr) == -1) {
        paramList += param.arg + "=" + param.val + " ";
        if (series['label'] == "") {
          series['label'] = param.arg + "=" + param.val;
        } else {
          series['label'] += "," + param.arg + "=" + param.val;
        }
      }
    });
    thisIter['paramList'] = paramList;
    logOutput(paramList, noHtml);
    var primaryMetric = cdm.getPrimaryMetric(program.url, iterationId);
    thisIter['primary-metric'] = primaryMetric;
    if ( typeof data[primaryMetric] == "undefined" ) {
      data[primaryMetric] = [];
      numIter[primaryMetric] = 0;
    }
    numIter[primaryMetric]++;
    var primaryPeriodName = cdm.getPrimaryPeriodName(program.url, iterationId);
    if (primaryPeriodName == undefined) {
      console.log("      the primary period-name for this iteration is undefined, exiting\n");
      process.exit(1);
    }
    logOutput("      primary-period name: " + primaryPeriodName, noHtml);
    var samples = cdm.getSamples(program.url, iterationId);
    var sampleTotal = 0;
    var sampleCount = 0;
    var sampleVals = [];
    var sampleList = "";
    var sets = [];
    logOutput("      samples:", noHtml);
    samples.forEach(sample => {
      if (cdm.getSampleStatus(program.url, sample) == "pass") {
        logOutput("        sample-id: " + sample, noHtml);
        var primaryPeriodId = cdm.getPrimaryPeriodId(program.url, sample, primaryPeriodName);
        if (primaryPeriodId == undefined || primaryPeriodId == null) {
          logOutput("          the primary perdiod-id for this sample is not valid, exiting\n", noHtml);
          process.exit(1);
        }
        logOutput("          primary period-id: " + primaryPeriodId, noHtml);
        var range = cdm.getPeriodRange(program.url, primaryPeriodId);
        if (range == undefined || range == null) {
          logOutput("          the range for the primary period is undefined, exiting", noHtml);
          process.exit(1);
        }
        logOutput("          period range: begin: " + range.begin + " end: " + range.end, noHtml);
        var breakout = []; // By default we do not break-out a benchmark metric, so this is empty
        // Needed for getMetricDataSets further below:
        var set = { "run": runId, "period": primaryPeriodId, "source": benchName, "type": primaryMetric, "begin": range.begin, "end": range.end, "resolution": 1, "breakout": [] };
        sets.push(set);
      }
    });
 
    if (sets.length > 0) {
      var metricDataSets = cdm.getMetricDataSets(program.url, sets);
      //console.log("metricDataSets:\n" + JSON.stringify(metricDataSets, null, 2));
      var msampleCount = 0;
      var msampleVals = [];
      var msampleTotal = 0;
      var msampleList = "";
      metricDataSets.forEach(metricData => {
        var msampleVal = metricData.values[""];
        if (msampleVal && msampleVal[0] && msampleVal[0].value) {
          msampleVal = parseFloat(msampleVal[0].value);
          msampleVals.push(msampleVal);
          msampleTotal += msampleVal;
          var msampleFixed = msampleVal.toFixed(6);
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
        logOutput("        result: (" + primaryMetric + ") samples:" + msampleList +
                    " mean: " + parseFloat(mean).toFixed(6) +
                    " min: " + parseFloat(Math.min(...msampleVals)).toFixed(6) +
                    " max: " + parseFloat(Math.max(...msampleVals)).toFixed(6) +
                    " stddev: " + parseFloat(mstddev).toFixed(6) +
                    " stddevpct: " + parseFloat(mstddevpct).toFixed(6), noHtml);
        series['mean'] = mean;
        series['min'] = Math.min(...msampleVals);
        series['max'] = Math.max(...msampleVals);
      }
      data[primaryMetric].push(series);
    }
  });
  html_summary += '</pre>\n';
  var html_resources = '<!-- Resources -->\n' +
                       '<script src="https://cdn.amcharts.com/lib/5/index.js"></script>\n' +
                       '<script src="https://cdn.amcharts.com/lib/5/xy.js"></script>\n' +
                       '<script src="https://cdn.amcharts.com/lib/5/themes/Animated.js"></script>\n' +
                       '<script src="data.js"></script>\n' +
                       '<script src="chart.js"></script>\n';
  var html_styles = '<!-- Styles -->\n' + 
                    '<style>\n';;
  var html_div = '';
  Object.keys(numIter).forEach(pri =>{
    html_div += '<div id="' + pri + '"></div>\n';
    html_styles += '#' + pri + ' {\n' +
                   '  width: 1000px;\n' +
                   '  height: ' + (120 + 25*numIter[pri]) + 'px;\n' +
                   '}\n';
  });
  html_styles += '</style>\n';
  var html = html_styles + html_resources + html_summary + html_div;

  // Maintain default behavior of sending to stdout
  console.log(txt_summary);

  const fs = require('fs');
  if (program.outputFormat.includes('txt')) {
    try {
      fs.writeFileSync(program.outputDir + "/" + 'result-summary.txt', txt_summary);
    } catch (err) {
      console.error(err);
    }
  }
  if (program.outputFormat.includes('html')) {
    try {
      fs.writeFileSync(program.outputDir + "/" + 'data.js', 'var data = ' + JSON.stringify(data, null, 2));
    } catch (err) {
      console.error(err);
    }
    try {
      fs.writeFileSync(program.outputDir + "/" + 'result-summary.html', html);
    } catch (err) {
      console.error(err);
    }
    try {
      fs.copyFileSync("chart.js", program.outputDir + "/" + 'chart.js');
    } catch (err) {
      console.log(err);
    }
  }
});

