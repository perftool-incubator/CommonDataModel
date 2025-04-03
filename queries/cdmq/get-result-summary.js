//# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript

var cdm = require('./cdm');
var program = require('commander');

const DEBUG = false; // Set to `false` to disable debug logs
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
  .option('--output-format <fmt>, fmta[,fmtb]', 'one or more output formats: json txt html', list, [])
  .parse(process.argv);

//console.log("program.args:\n" + JSON.stringify(program, null, 2));
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
if (!program.url) {
  program.url = 'localhost:9200';
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

let json_summary = {
      "run-id": "",
      "tags": {},
      "benchmark": "",
      "common_params": {},
      "metrics": [],
      "iteration-array": [],  // Array of iteration objects
};

function logOutput(str, formats) {
  txt_summary += str + '\n';
  if (formats.includes('html')) {
    html_summary += str + '\n';
  }

  if (DEBUG) console.log("DEBUG: Received log line:", JSON.stringify(str));

  if (formats.includes('json')) {
    try {
      let match;

      // Ensure metrics exist
      if (!json_summary.metrics) json_summary.metrics = [];

      // Run ID
      if ((match = str.match(/^\s*run-id:\s*(.+)/))) {
        json_summary["run-id"] = match[1].trim();
        if (DEBUG) console.log("DEBUG: Set run-id:", json_summary["run-id"]);
      }
      // Tags (Handling spaces before 'tags:')
      else if ((match = str.match(/^\s*tags:\s*(.+)/))) {
        json_summary.tags = match[1].split(' ').reduce((acc, pair) => {
          let [key, value] = pair.split('=');
          if (key && value) acc[key] = value;
          return acc;
        }, {});
        if (DEBUG) console.log("DEBUG: Set tags:", JSON.stringify(json_summary.tags));
      }
      // Benchmark
      else if ((match = str.match(/^\s*benchmark:\s*(.+)/))) {
        json_summary.benchmark = match[1].trim();
        if (DEBUG) console.log("DEBUG: Set benchmark:", json_summary.benchmark);
      }
      // Common Params
      else if ((match = str.match(/^\s*common params:\s*(.+)/))) {
        json_summary.common_params = match[1].split(' ').reduce((acc, pair) => {
          let [key, value] = pair.split('=');
          if (key && value) acc[key] = value;
          return acc;
        }, {});
        if (DEBUG) console.log("DEBUG: Set common_params:", JSON.stringify(json_summary.common_params));
      }
      // Metrics Source
      else if ((match = str.match(/^\s*source:\s*([\w-]+)/))) {
        let source = match[1].trim();
        json_summary.metrics.push({ source, types: [] });
        if (DEBUG) console.log("DEBUG: Added metric source:", source);
      }
      // Metric Types
      else if ((match = str.match(/^\s*types:\s*(.+)/))) {
        let types = match[1].split(' ').map(type => type.trim());
        if (json_summary.metrics.length > 0) {
          json_summary.metrics[json_summary.metrics.length - 1].types = types;
          if (DEBUG) console.log("DEBUG: Updated metric types:", types);
        }
      }

      // Iteration ID (Start a new iteration)
      else if ((match = str.match(/^\s*iteration-id:\s*([\w-]{36})/))) {
        let iterationId = match[1].trim();
        json_summary["iteration-array"].push({
          "iteration-id": iterationId,
          "unique_params": {},  // Initialize unique params
          "primary-period-name": "",
          "samples": [],
          "results": []  // Add the results object for each iteration
        });
        if (DEBUG) console.log("DEBUG: Set iteration-id:", iterationId);
      }

      // Unique Params (Captured for each iteration)
      else if ((match = str.match(/^\s*unique params:\s*(.+)/))) {
        let uniqueParams = match[1].split(' ').reduce((acc, pair) => {
          let [key, value] = pair.split('=');
          if (key && value) acc[key] = value;
          return acc;
        }, {});

        // Find the last iteration-id and assign unique params to it
        if (json_summary["iteration-array"].length > 0) {
          json_summary["iteration-array"][json_summary["iteration-array"].length - 1]["unique_params"] = uniqueParams;
          if (DEBUG) console.log("DEBUG: Set unique params for iteration:", JSON.stringify(uniqueParams));
        }
      }

      // Primary Period Name (Captured for each iteration)
      else if ((match = str.match(/^\s*primary-period name:\s*(.+)/))) {
        let periodName = match[1].trim();
        if (json_summary["iteration-array"].length > 0) {
          json_summary["iteration-array"][json_summary["iteration-array"].length - 1]["primary-period-name"] = periodName;
          if (DEBUG) console.log("DEBUG: Set primary-period name for iteration:", periodName);
        }
      }

      // Sample ID (Start a new sample object)
      else if ((match = str.match(/^\s*sample-id:\s*([\w-]{36})/))) {
        let sampleId = match[1].trim();
        let newSample = {
          "sample-id": sampleId,
          "primary-period-id": "",
          "period-range": { "begin": null, "end": null },
          "period-length": null
        };
        json_summary["iteration-array"][json_summary["iteration-array"].length - 1].samples.push(newSample);
        if (DEBUG) console.log("DEBUG: Added new sample:", JSON.stringify(newSample));
      }

      // Primary Period ID
      else if ((match = str.match(/^\s*primary period-id:\s*([\w-]{36})/))) {
        let periodId = match[1].trim();
        if (json_summary["iteration-array"].length > 0 && json_summary["iteration-array"][json_summary["iteration-array"].length - 1].samples.length > 0) {
          json_summary["iteration-array"][json_summary["iteration-array"].length - 1].samples[json_summary["iteration-array"][json_summary["iteration-array"].length - 1].samples.length - 1]["primary-period-id"] = periodId;
          if (DEBUG) console.log("DEBUG: Set primary-period-id:", periodId);
        }
      }

      // Period Range
      else if ((match = str.match(/^\s*period range:\s*begin:\s*(\d+)\s*end:\s*(\d+)/))) {
        let begin = parseInt(match[1]);
        let end = parseInt(match[2]);
        if (json_summary["iteration-array"].length > 0 && json_summary["iteration-array"][json_summary["iteration-array"].length - 1].samples.length > 0) {
          json_summary["iteration-array"][json_summary["iteration-array"].length - 1].samples[json_summary["iteration-array"][json_summary["iteration-array"].length - 1].samples.length - 1]["period-range"] = { begin, end };
          if (DEBUG) console.log("DEBUG: Set period range:", { begin, end });
        }
      }

      // Period Length
      else if ((match = str.match(/^\s*period length:\s*([\d.]+)\s*seconds/))) {
        let length = parseFloat(match[1]);
        if (json_summary["iteration-array"].length > 0 && json_summary["iteration-array"][json_summary["iteration-array"].length - 1].samples.length > 0) {
          json_summary["iteration-array"][json_summary["iteration-array"].length - 1].samples[json_summary["iteration-array"][json_summary["iteration-array"].length - 1].samples.length - 1]["period-length"] = length;
          if (DEBUG) console.log("DEBUG: Set period length:", length);
        }
      }

      // Iteration Result (Aggregated metrics for iteration)
      else if ((match = str.match(/^\s*result:\s*\(([\w:.-]+)\)\s*samples:\s*([\d.\s]+)mean:\s*([\d.]+)\s+min:\s*([\d.]+)\s+max:\s*([\d.]+)\s+stddev:\s*([\d.]+|NaN)\s+(?:stddev\s*%|stddevpct):\s*([\d.]+|NaN)/))) {

        let metric = match[1];
        let samples = match[2].trim().split(/\s+/).map(Number);  // Split into array of numbers
        let mean = parseFloat(match[3]);
        let min = parseFloat(match[4]);
        let max = parseFloat(match[5]);
        let stddev = isNaN(parseFloat(match[6])) ? null : parseFloat(match[6]);
        let stddevPercent = isNaN(parseFloat(match[7])) ? null : parseFloat(match[7]);

        // Add the result at iteration level
        if (json_summary["iteration-array"].length > 0) {
          json_summary["iteration-array"][json_summary["iteration-array"].length - 1].results.push({
            "metric": metric,
            "samples": samples,  // Dynamically includes all sample values
            "mean": mean,
            "min": min,
            "max": max,
            "stddev": stddev,
            "stddevpct": stddevPercent
          });
        }

        if (DEBUG) console.log("DEBUG: Added iteration result:", {
            "metric": metric,
            "samples": samples,  // Log dynamically captured samples
            "mean": mean,
            "min": min,
            "max": max,
            "stddev": stddev,
            "stddev %": stddevPercent
        });
      }

      // Print full JSON at the end
      if (DEBUG) console.log("DEBUG: Current JSON Summary:", JSON.stringify(json_summary, null, 2));

    } catch (error) {
      console.error("ERROR processing JSON output:", error);
    }
  }
}

var runIds = cdm.mSearch(program.url, 'run', termKeys, values, 'run.run-uuid', null, 1000)[0];
if (runIds == undefined || runIds.length == 0) {
  console.log('The run ID could not be found, exiting');
  process.exit(1);
}

runIds.forEach((runId) => {
  logOutput('\nrun-id: ' + runId, program.outputFormat);
  var tags = cdm.getTags(program.url, runId);
  tags.sort((a, b) => (a.name < b.name ? -1 : 1));
  var tagList = '  tags: ';
  tags.forEach((tag) => {
    tagList += tag.name + '=' + tag.val + ' ';
  });
  logOutput(tagList, program.outputFormat);
  var benchName = cdm.getBenchmarkName(program.url, runId);
  var benchmarks = list(benchName);
  logOutput('  benchmark: ' + benchName, program.outputFormat);
  var benchIterations = cdm.getIterations(program.url, runId);
  if (benchIterations.length == 0) {
    console.log('There were no iterations found, exiting');
    process.exit(1);
  }

  var iterParams = cdm.mgetParams(program.url, benchIterations);
  //returns 1D array [iter]
  var iterPrimaryPeriodNames = cdm.mgetPrimaryPeriodName(program.url, benchIterations);
  //input: 1D array
  //output: 2D array [iter][samp]
  var iterSampleIds = cdm.mgetSamples(program.url, benchIterations);
  //input: 2D array iterSampleIds: [iter][samp]
  //output: 2D array [iter][samp]
  var iterSampleStatus = cdm.mgetSampleStatus(program.url, iterSampleIds);
  //console.log("sampleStatus:\n" + JSON.stringify(iterSampleStatus, null, 2));
  //needs 2D array iterSampleIds: [iter][samp] and 1D array iterPrimaryPeriodNames [iter]
  //returns 2D array [iter][samp]
  var iterPrimaryPeriodIds = cdm.mgetPrimaryPeriodId(program.url, iterSampleIds, iterPrimaryPeriodNames);
  var iterPrimaryPeriodRanges = cdm.mgetPeriodRange(program.url, iterPrimaryPeriodIds);

  // Find the params which are the same in every iteration
  var iterPrimaryMetrics = cdm.mgetPrimaryMetric(program.url, benchIterations);
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
  var metricSources = cdm.getMetricSources(program.url, runId);
  var runIds = [];
  for (var i = 0; i < metricSources.length; i++) {
    runIds[i] = runId;
  }
  var metricTypes = cdm.mgetMetricTypes(program.url, runIds, metricSources);

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
          metricDataSetsChunks[chunkNum] = cdm.getMetricDataSets(program.url, sets);
          chunkNum++;
          sets = [];
        }
      }
    }
  }
  if (sets.length > 0) {
    // Submit a chunk of the query and save the result
    metricDataSetsChunks[chunkNum] = cdm.getMetricDataSets(program.url, sets);
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
  if (program.outputFormat.includes('json')) {
    try {
      fs.writeFileSync(program.outputDir + '/' + 'result-summary.json', JSON.stringify(json_summary, null, 2));
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
});
