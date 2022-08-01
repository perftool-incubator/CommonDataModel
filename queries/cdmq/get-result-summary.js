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

//console.log("program.args:\n" + JSON.stringify(program, null, 2));
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

var runIds = cdm.mSearch(program.url, "run", termKeys, values, "run.id", null, 1000)[0];
if (runIds == undefined || runIds.length == 0) {
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
  //console.log("benchIterations:\n" + JSON.stringify(benchIterations, null, 2));
  if (benchIterations.length == 0) {
    console.log("There were no iterations found, exiting");
    process.exit(1);
  }



  var iterParams = cdm.mgetParams(program.url, benchIterations);
  var iterPrimaryMetrics = cdm.mgetPrimaryMetric(program.url, benchIterations);
  //returns 1D array [iter]
  var iterPrimaryPeriodNames = cdm.mgetPrimaryPeriodName(program.url, benchIterations);
  //returns 2D array [iter][samp]
  var iterSampleIds = cdm.mgetSamples(program.url, benchIterations);
  //needs 2D array iterSampleIds: [iter][samp] and 1D array iterPrimaryPeriodNames [iter]
  //returns 2D array [iter][samp]
  var iterPrimaryPeriodIds = cdm.mgetPrimaryPeriodId(program.url, iterSampleIds, iterPrimaryPeriodNames);
  var iterPrimaryPeriodRanges = cdm.mgetPeriodRange(program.url, iterPrimaryPeriodIds);

  // Find the params which are the same in every iteration
  var allParams = [];
  var allParamsCounts = [];
  iterParams.forEach(params => {
  params.forEach(param => {
      var newParam = param.arg + "=" + param.val;
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
  var commonParamsStr = "  common params: ";
  commonParams.forEach(param => {
    commonParamsStr += param + " ";
  });
  logOutput(commonParamsStr, program.outputFormat);

  logOutput("  metrics:", program.outputFormat);
  var metricSources = cdm.getMetricSources(program.url, runId);
  var runIds = [];
  for (var i=0; i<metricSources.length; i++) {
    runIds[i] = runId;
  }
  var metricTypes = cdm.mgetMetricTypes(program.url, runIds, metricSources);

  for (var i=0; i<metricSources.length; i++) {
    logOutput("    source: " + metricSources[i], program.outputFormat);
    var typeList = "      types: ";
    for (var j=0; j<metricTypes[i].length; j++) {
      typeList += metricTypes[i][j] + " ";
    }
    logOutput(typeList, program.outputFormat);
  }

  // build the sets for the mega-query
  var sets = [];
  for (var i=0; i<benchIterations.length; i++) {
    for (var j=0; j<iterSampleIds[i].length; j++) {
      var set = { "run": runId,
                  "period": iterPrimaryPeriodIds[i][j],
                  "source": benchName,
                  "type": iterPrimaryMetrics[i],
                  "begin": iterPrimaryPeriodRanges[i][j].begin,
                  "end": iterPrimaryPeriodRanges[i][j].end,
                  "resolution": 1,
                  "breakout": [] };
      sets.push(set);
    }
  }

  // do the mega-query
  var metricDataSets = cdm.getMetricDataSets(program.url, sets);


  // output the results
  var data = {};
  var numIter = {};
  var idx = 0;
  for (var i=0; i<benchIterations.length; i++) {
    var series = {};
    logOutput("    iteration-id: " + benchIterations[i], noHtml);
    var paramList = "      unique params: ";
    series['label'] = "";
    iterParams[i].sort((a, b) => a.arg < b.arg ? -1 : 1).forEach(param => {
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
    logOutput(paramList, noHtml);
    logOutput("      primary-period name: " + iterPrimaryPeriodNames[i], noHtml);
    var primaryMetric = iterPrimaryMetrics[i];
    if ( typeof data[primaryMetric] == "undefined" ) {
      data[primaryMetric] = [];
      numIter[primaryMetric] = 0;
    }
    numIter[primaryMetric]++;
    logOutput("      samples:", noHtml);
    var msampleCount = 0;
    var msampleTotal = 0;
    var msampleVals = [];
    var msampleList = "";
    for (var j=0; j<iterSampleIds[i].length; j++) {
      logOutput("        sample-id: " + iterSampleIds[i][j], noHtml);
      logOutput("          primary period-id: " + iterPrimaryPeriodIds[i][j], noHtml);
      logOutput("          period range: begin: " + iterPrimaryPeriodRanges[i][j].begin + " end: " + iterPrimaryPeriodRanges[i][j].end, noHtml);
      msampleVal = parseFloat(metricDataSets[idx].values[""][0].value);
      msampleVals.push(msampleVal);
      msampleTotal += msampleVal;
      var msampleFixed = msampleVal.toFixed(6);
      msampleList += " " + msampleFixed;
      msampleCount++;
      idx++;
    }
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

