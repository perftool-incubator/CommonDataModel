//# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript
var cdm = require('./cdm');
var program = require('commander');
var instances = [];

function list(val) {
  return val.split(',');
}

function save_host(host) {
    var host_info = { 'host': host, 'header': { 'Content-Type': 'application/json' } };
    instances.push(host_info);
}

function save_userpass(userpass) {
    if (instances.length == 0) {
        console.log("You must specify a --host before a --userpass");
        process.exit(1);
    }
    instances[instances.length - 1]['header'] = { 'Content-Type': 'application/json', 'Authorization' : 'Basic ' + btoa(userpass) };
}

function save_ver(ver) {
    if (instances.length == 0) {
        console.log("You must specify a --host before a --ver");
        process.exit(1);
    }
    if (/^v[7|8|9]dev$/.exec(ver)) {
      instances[instances.length - 1]['ver'] = ver;
    } else {
      console.log("The version must be v7dev, v8dev, or v9dev, not: " + ver);
      process.exit(1);
    }
}

program
  .version('0.1.0')
  .option('--run <run-ID>')
  .option('--host <host[:port]>', 'The host and optional port of the OpenSearch instance', save_host)
  .option('--userpass <user:pass>', 'The user and password for the most recent --host', save_userpass)
  .option('--ver <v7dev|v8dev|v9dev>', 'The Common Data Model version to use for the most recent --host', save_ver)
  .option('--output-dir <path>, if not used, output is to console only')
  .option('--output-format <fmt>, fmta[,fmtb]', 'one or more output formats: txt html', list, [])
  .parse(process.argv);

var termKeys = [];
var values = [];

if (program.user) {
  termKeys.push('run.name');
  values.push([program.user]);
}
if (program.email) {
  termKeys.push('run.email');
  values.push([program.email]);
}
if (program.run) {
  termKeys.push('run.run-uuid');
  values.push([program.run]);
}
if (program.harness) {
  termKeys.push('run.harness');
  values.push([program.harness]);
}

if (!program.outputDir) {
  program.outputDir = '';
}
if (!program.outputFormat) {
  program.outputFormat = [''];
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

async function main() {

  // If the user does not specify any hosts, assume localhost:9200 is used
  if (instances.length == 0) {
    save_host("localhost:9200")
  }

  getInstancesInfo(instances);
  console.log(JSON.stringify(instances, null, 2));

  // Since this query is looking for run ids (and may not inlcude run-uuid as a search term), we
  // need to check all instances.
  var allInstanceRunIds = [];
  for (const instance of instances) {
    if (invalidInstance(instance)) {
      continue;
    }
    console.log("main(): calling cdm.mSearch()");
    var instanceRunIds = await cdm.mSearch(instance, 'run', termKeys, values, 'run.run-uuid', null, 1000);
    console.log("main(): returned from cdm.mSearch()");
    console.log("instanceRunIds:\n" + JSON.stringify(instanceRunIds, null, 2));
    if (typeof instanceRunIds[0] != 'undefined') {
      allInstanceRunIds.push(instanceRunIds[0]);
    }
  }
  console.log("allInstanceRunIds:\n" + JSON.stringify(allInstanceRunIds, null, 2));

  var runIds = cdm.consolidateAllArrays(allInstanceRunIds);
  console.log("(consolidated)allInstanceRunIds:\n" + JSON.stringify(runIds, null, 2));

  if (typeof runIds == 'undefined' || runIds.length == 0) {
    console.log('The run ID could not be found, exiting');
    process.exit(1);
  }

  //runIds.forEach((runId) => {
  console.log("runIds:\n" + JSON.stringify(runIds, null, 2));
  for (runIdx = 0; runIdx < runIds.length; runIdx++) {
    console.log("runIdx:\n" + runIdx);
    const runId = runIds[runIdx];
    //console.log("instances:\n" + JSON.stringify(instances, null, 2));
    var instance = await findInstanceFromRun(instances, runId);
    //console.log("instance: " + JSON.stringify(instance, null, 2));
    logOutput('\nrun-id: ' + runId, program.outputFormat);
    var tags = await cdm.getTags(instance, runId);
    tags.sort((a, b) => (a.name < b.name ? -1 : 1));
    var tagList = '  tags: ';
    tags.forEach((tag) => {
      tagList += tag.name + '=' + tag.val + ' ';
    });
    logOutput(tagList, program.outputFormat);
    var benchName = await cdm.getBenchmarkName(instance, runId);
    var benchmarks = list(benchName);
    logOutput('  benchmark: ' + benchName, program.outputFormat);
    var benchIterations = await cdm.getIterations(instance, runId);
    if (benchIterations.length == 0) {
      console.log('There were no iterations found, exiting');
      process.exit(1);
    }

    var iterParams = await cdm.mgetParams(instance, benchIterations);
    //returns 1D array [iter]
    var iterPrimaryPeriodNames = await cdm.mgetPrimaryPeriodName(instance, benchIterations);
    //input: 1D array
    //output: 2D array [iter][samp]
    var iterSampleIds = await cdm.mgetSamples(instance, benchIterations);
    //input: 2D array iterSampleIds: [iter][samp]
    //output: 2D array [iter][samp]
    var iterSampleStatus = await cdm.mgetSampleStatus(instance, iterSampleIds);
    //console.log("sampleStatus:\n" + JSON.stringify(iterSampleStatus, null, 2));
    //needs 2D array iterSampleIds: [iter][samp] and 1D array iterPrimaryPeriodNames [iter]
    //returns 2D array [iter][samp]
    var iterPrimaryPeriodIds = await cdm.mgetPrimaryPeriodId(instance, iterSampleIds, iterPrimaryPeriodNames);
    var iterPrimaryPeriodRanges = await cdm.mgetPeriodRange(instance, iterPrimaryPeriodIds);

    // Find the params which are the same in every iteration
    var iterPrimaryMetrics = await cdm.mgetPrimaryMetric(instance, benchIterations);
    var primaryMetrics = list(iterPrimaryMetrics[0]);
    // For now only dump params when 1 primary metric is used
    if (primaryMetrics.length == 1) {
      var allParams = [];
      var allParamsCounts = [];
      iterParams.forEach((params) => {
        params.forEach((param) => {
          var newParam = param.arg + '=' + param.val;
          idx = allParams.indexOf(newParam);
          if (idx == -1) {
            allParams.push(newParam);
            allParamsCounts.push(1);
          } else {
            allParamsCounts[idx] += 1;
          }
        });
      });
      var commonParams = [];
      for (var idx = 0; idx < allParams.length; idx++) {
        if (allParamsCounts[idx] == benchIterations.length) {
          commonParams.push(allParams[idx]);
        }
      }
      commonParams.sort();
      var commonParamsStr = '  common params: ';
      commonParams.forEach((param) => {
        commonParamsStr += param + ' ';
      });
      logOutput(commonParamsStr, program.outputFormat);
    }

    logOutput('  metrics:', program.outputFormat);
    var metricSources = await cdm.getMetricSources(instance, runId);
    var theseRunIds = [];
    for (var i = 0; i < metricSources.length; i++) {
      theseRunIds[i] = runId;
    }
    var metricTypes = await cdm.mgetMetricTypes(instance, theseRunIds, metricSources);

    for (var i = 0; i < metricSources.length; i++) {
      logOutput('    source: ' + metricSources[i], program.outputFormat);
      var typeList = '      types: ';
      for (var j = 0; j < metricTypes[i].length; j++) {
        typeList += metricTypes[i][j] + ' ';
      }
      logOutput(typeList, program.outputFormat);
    }

    // build the sets for the mega-query
    var metricDataSetsChunks = [];
    var batchedQuerySize = 10;
    var benchmarks = benchName.split(',');
    var sets = [];
    var chunkNum = 0;
    for (var i = 0; i < benchIterations.length; i++) {
      for (var j = 0; j < iterSampleIds[i].length; j++) {
        var primaryMetrics = list(iterPrimaryMetrics[i]);
        for (var k = 0; k < primaryMetrics.length; k++) {
          var source = '';
          var type = '';
          var sourceType = primaryMetrics[k].split('::');
          if (sourceType.length == 1) {
            // Older runs have only 1 benchmark and only have "type" in primaryMetrics
            source = benchmarks[0];
            type = primaryMetrics[k];
          } else if (sourceType.length == 2) {
            // Newer run data embeds source and type for primaryMetric
            source = sourceType[0];
            type = sourceType[1];
          } else {
            console.log('sourceType array is an unexpected length, ' + sourceType.length);
            process.exit(1);
          }
          var set = {
            run: runId,
            period: iterPrimaryPeriodIds[i][j],
            source: source,
            type: type,
            begin: iterPrimaryPeriodRanges[i][j].begin,
            end: iterPrimaryPeriodRanges[i][j].end,
            resolution: 1,
            breakout: []
          };
          sets.push(set);
          if (sets.length == batchedQuerySize) {
            // Submit a chunk of the query and save the result
            metricDataSetsChunks[chunkNum] = await cdm.getMetricDataSets(instance, sets);
            chunkNum++;
            sets = [];
          }
        }
      }
    }
    if (sets.length > 0) {
      // Submit a chunk of the query and save the result
      metricDataSetsChunks[chunkNum] = await cdm.getMetricDataSets(instance, sets);
      chunkNum++;
      sets = [];
    }

    // output the results
    var data = {};
    var numIter = {};
    var idx = 0;
    for (var i = 0; i < benchIterations.length; i++) {
      var primaryMetrics = list(iterPrimaryMetrics[i]);
      var series = {};
      logOutput('    iteration-id: ' + benchIterations[i], noHtml);

      if (primaryMetrics.length == 1) {
        var paramList = '      unique params: ';
        series['label'] = '';
        iterParams[i]
          .sort((a, b) => (a.arg < b.arg ? -1 : 1))
          .forEach((param) => {
            paramStr = param.arg + '=' + param.val;
            if (commonParams.indexOf(paramStr) == -1) {
              paramList += param.arg + '=' + param.val + ' ';
              if (series['label'] == '') {
                series['label'] = param.arg + '=' + param.val;
              } else {
                series['label'] += ',' + param.arg + '=' + param.val;
              }
            }
          });
        logOutput(paramList, noHtml);
      }

      logOutput('      primary-period name: ' + iterPrimaryPeriodNames[i], noHtml);
      var primaryMetric = iterPrimaryMetrics[i];
      if (typeof data[primaryMetric] == 'undefined') {
        data[primaryMetric] = [];
        numIter[primaryMetric] = 0;
      }
      numIter[primaryMetric]++;
      logOutput('      samples:', noHtml);
      var msampleCount = 0;
      var msampleTotal = 0;
      var msampleVals = [];
      var msampleList = '';

      /*
      samples.forEach(sample => {
        if (cdm.getSampleStatus(instance, sample) == "pass") {
          logOutput("        sample-id: " + sample, noHtml);
          var primaryPeriodId = cdm.getPrimaryPeriodId(instance, sample, primaryPeriodName);
          if (primaryPeriodId == undefined || primaryPeriodId == null) {
            logOutput("          the primary perdiod-id for this sample is not valid, exiting\n", noHtml);
            process.exit(1);
          }
          logOutput("          primary period-id: " + primaryPeriodId, noHtml);
          var range = cdm.getPeriodRange(instance, primaryPeriodId);
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
  */

      var allBenchMsampleVals = [];
      var allBenchMsampleTotal = [];
      var allBenchMsampleFixedList = [];
      var allBenchMsampleCount = [];
      for (var j = 0; j < iterSampleIds[i].length; j++) {
        if (
          iterSampleStatus[i][j] == 'pass' &&
          iterPrimaryPeriodRanges[i][j].begin !== undefined &&
          iterPrimaryPeriodRanges[i][j].end !== undefined
        ) {
          logOutput('        sample-id: ' + iterSampleIds[i][j], noHtml);
          logOutput('          primary period-id: ' + iterPrimaryPeriodIds[i][j], noHtml);
          logOutput(
            '          period range: begin: ' +
              iterPrimaryPeriodRanges[i][j].begin +
              ' end: ' +
              iterPrimaryPeriodRanges[i][j].end,
            noHtml
          );
          logOutput(
            '          period length: ' +
              (iterPrimaryPeriodRanges[i][j].end - iterPrimaryPeriodRanges[i][j].begin) / 1000 +
              ' seconds',
            noHtml
          );
          //for (var k=0; k<benchmarks.length; k++) {
          var primaryMetrics = list(iterPrimaryMetrics[i]);
          for (var k = 0; k < primaryMetrics.length; k++) {
            var sourceType = primaryMetrics[k].split('::');
            var thisChunk = Math.floor(idx / batchedQuerySize);
            var thisIdx = idx % batchedQuerySize;
            msampleVal = parseFloat(metricDataSetsChunks[thisChunk][thisIdx].values[''][0].value);
            if (allBenchMsampleVals[k] == null) {
              allBenchMsampleVals[k] = [];
            }
            allBenchMsampleVals[k].push(msampleVal);

            if (allBenchMsampleTotal[k] == null) {
              allBenchMsampleTotal[k] = 0;
            }
            allBenchMsampleTotal[k] += msampleVal;

            msampleFixed = msampleVal.toFixed(6);

            if (allBenchMsampleFixedList[k] == null) {
              allBenchMsampleFixedList[k] = '';
            }
            allBenchMsampleFixedList[k] += ' ' + msampleFixed;

            if (allBenchMsampleCount[k] == null) {
              allBenchMsampleCount[k] = 0;
            }
            allBenchMsampleCount[k]++;
            idx++;
          }
        }
      }
      for (var k = 0; k < primaryMetrics.length; k++) {
        var sourceType = primaryMetrics[k].split('::');
        if (allBenchMsampleCount[k] > 0) {
          var mean = allBenchMsampleTotal[k] / allBenchMsampleCount[k];
          var diff = 0;
          allBenchMsampleVals[k].forEach((val) => {
            diff += (mean - val) * (mean - val);
          });
          diff /= allBenchMsampleCount[k] - 1;
          var mstddev = Math.sqrt(diff);
          var mstddevpct = (100 * mstddev) / mean;
          logOutput(
            '            result: (' +
              sourceType[0] +
              '::' +
              sourceType[1] +
              ') samples:' +
              allBenchMsampleFixedList[k] +
              ' mean: ' +
              parseFloat(mean).toFixed(6) +
              ' min: ' +
              parseFloat(Math.min(...allBenchMsampleVals[k])).toFixed(6) +
              ' max: ' +
              parseFloat(Math.max(...allBenchMsampleVals[k])).toFixed(6) +
              ' stddev: ' +
              parseFloat(mstddev).toFixed(6) +
              ' stddevpct: ' +
              parseFloat(mstddevpct).toFixed(6),
            noHtml
          );
          series['mean'] = mean;
          series['min'] = Math.min(...allBenchMsampleVals[k]);
          series['max'] = Math.max(...allBenchMsampleVals[k]);
        }
      }
      data[primaryMetric].push(series);
    }

    html_summary += '</pre>\n';
    var html_resources =
      '<!-- Resources -->\n' +
      '<script src="https://cdn.amcharts.com/lib/5/index.js"></script>\n' +
      '<script src="https://cdn.amcharts.com/lib/5/xy.js"></script>\n' +
      '<script src="https://cdn.amcharts.com/lib/5/themes/Animated.js"></script>\n' +
      '<script src="data.js"></script>\n' +
      '<script src="chart.js"></script>\n';
    var html_styles = '<!-- Styles -->\n' + '<style>\n';
    var html_div = '';
    Object.keys(numIter).forEach((pri) => {
      html_div += '<div id="' + pri + '"></div>\n';
      html_styles +=
        '#' + pri + ' {\n' + '  width: 1000px;\n' + '  height: ' + (120 + 25 * numIter[pri]) + 'px;\n' + '}\n';
    });
    html_styles += '</style>\n';
    var html = html_styles + html_resources + html_summary + html_div;

    // Maintain default behavior of sending to stdout
    console.log(txt_summary);

    const fs = require('fs');
    if (program.outputFormat.includes('txt')) {
      try {
        fs.writeFileSync(program.outputDir + '/' + 'result-summary.txt', txt_summary);
      } catch (err) {
        console.error(err);
      }
    }
    if (program.outputFormat.includes('html')) {
      try {
        fs.writeFileSync(program.outputDir + '/' + 'data.js', 'var data = ' + JSON.stringify(data, null, 2));
      } catch (err) {
        console.error(err);
      }
      try {
        fs.writeFileSync(program.outputDir + '/' + 'result-summary.html', html);
      } catch (err) {
        console.error(err);
      }
      try {
        fs.copyFileSync('chart.js', program.outputDir + '/' + 'chart.js');
      } catch (err) {
        console.log(err);
      }
    }
  //});
  }
}

main();
