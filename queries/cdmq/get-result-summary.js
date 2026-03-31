//# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript
var yaml = require('js-yaml');
var program = require('commander');
const http = require('http');
const https = require('https');
var summary = {};

function list(val) {
  return val.split(',');
}

// --------------------------------------------------------------------------------------------------------------
// HTTP client for API requests
// --------------------------------------------------------------------------------------------------------------
function debuglog(msg) {
  if (program.debug) {
    console.error('[DEBUG] ' + msg);
  }
}

async function apiRequest(serverUrl, method, path, body) {
  debuglog(method + ' ' + path + (body ? ' body=' + JSON.stringify(body) : ''));
  return new Promise((resolve, reject) => {
    const url = new URL(serverUrl);
    const isHttps = url.protocol === 'https:';
    const client = isHttps ? https : http;

    const postData = body ? JSON.stringify(body) : null;

    const options = {
      hostname: url.hostname,
      port: url.port || (isHttps ? 443 : 80),
      path: path,
      method: method,
      headers: {
        'Content-Type': 'application/json'
      }
    };

    if (postData) {
      options.headers['Content-Length'] = Buffer.byteLength(postData);
    }

    const req = client.request(options, (res) => {
      let data = '';

      res.on('data', (chunk) => {
        data += chunk;
      });

      res.on('end', () => {
        debuglog(method + ' ' + path + ' status=' + res.statusCode + ' response=' + data.substring(0, 500));

        let parsed;
        try {
          parsed = JSON.parse(data);
        } catch (e) {
          reject(
            new Error(
              'Failed to parse response from ' +
                method +
                ' ' +
                path +
                ': ' +
                e.message +
                '\n  Raw response (' +
                data.length +
                ' bytes): ' +
                JSON.stringify(data.substring(0, 200))
            )
          );
          return;
        }

        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve(parsed);
        } else {
          const code = parsed.code || 'HTTP_' + res.statusCode;
          const msg = parsed.error || data;
          reject(new Error('[' + code + '] ' + msg));
        }
      });
    });

    req.on('error', (error) => {
      reject(new Error('Failed to connect to server at ' + serverUrl + ': ' + error.message));
    });

    if (postData) {
      req.write(postData);
    }
    req.end();
  });
}

async function apiGet(baseUrl, path) {
  return await apiRequest(baseUrl, 'GET', path, null);
}

async function apiPost(baseUrl, path, body) {
  return await apiRequest(baseUrl, 'POST', path, body);
}

program
  .version('0.1.0')
  .option('--run <run-ID>')
  .option(
    '--server-url <url>',
    'The base URL of the CDM query server (e.g., http://localhost:3000)',
    'http://localhost:3000'
  )
  .option('--host <host[:port]>', 'Ignored (accepted for backward compatibility)')
  .option('--userpass <user:pass>', 'Ignored (accepted for backward compatibility)')
  .option('--ver <v7dev|v8dev|v9dev>', 'Ignored (accepted for backward compatibility)')
  .option('--user <name>', 'Filter by run name')
  .option('--email <email>', 'Filter by email')
  .option('--harness <harness>', 'Filter by harness')
  .option('--debug', 'Enable debug logging of API requests and responses')
  .option('--output-dir <path>, if not used, output is to console only')
  .option('--output-format <fmt>, fmta[,fmtb]', 'one or more output formats: txt, json, yaml', list, [])
  .parse(process.argv);

if (!program.outputDir) {
  program.outputDir = '';
}
if (!program.outputFormat) {
  program.outputFormat = [''];
}
var txt_summary = '';

function logOutput(str, formats) {
  txt_summary += str + '\n';
}

async function main() {
  var baseUrl = program.serverUrl;

  // Build query params for the runs search
  var queryParts = [];
  if (program.run) {
    queryParts.push('run=' + encodeURIComponent(program.run));
  }
  if (program.user) {
    queryParts.push('name=' + encodeURIComponent(program.user));
  }
  if (program.email) {
    queryParts.push('email=' + encodeURIComponent(program.email));
  }
  if (program.harness) {
    queryParts.push('harness=' + encodeURIComponent(program.harness));
  }
  var queryString = queryParts.length > 0 ? '?' + queryParts.join('&') : '';

  // Find matching runs
  var runsResp;
  try {
    runsResp = await apiGet(baseUrl, '/api/v1/runs' + queryString);
  } catch (error) {
    console.log('Error searching for runs: ' + error.message);
    process.exit(1);
  }

  var runIds = runsResp.runIds;
  if (typeof runIds == 'undefined' || runIds.length == 0) {
    console.log('The run ID could not be found, exiting');
    process.exit(1);
  }

  summary['runs'] = [];
  for (runIdx = 0; runIdx < runIds.length; runIdx++) {
    var thisRun = {};
    const runId = runIds[runIdx];
    var runPrefix = '/api/v1/run/' + runId;

    logOutput('\nrun-id: ' + runId, program.outputFormat);
    thisRun['run-id'] = runId;
    thisRun['iterations'] = [];

    // Fetch tags
    var tagsResp;
    try {
      tagsResp = await apiGet(baseUrl, runPrefix + '/tags');
    } catch (error) {
      console.log('Error fetching tags for run ' + runId + ': ' + error.message);
      process.exit(1);
    }
    var tags = tagsResp.tags;
    tags.sort((a, b) => (a.name < b.name ? -1 : 1));
    thisRun['tags'] = tags;
    var tagList = '  tags: ';
    tags.forEach((tag) => {
      tagList += tag.name + '=' + tag.val + ' ';
    });
    logOutput(tagList, program.outputFormat);

    // Fetch benchmark name
    var benchResp;
    try {
      benchResp = await apiGet(baseUrl, runPrefix + '/benchmark');
    } catch (error) {
      console.log('Error fetching benchmark for run ' + runId + ': ' + error.message);
      process.exit(1);
    }
    var benchName = benchResp.benchmark;
    var benchmarks = list(benchName);
    logOutput('  benchmark: ' + benchName, program.outputFormat);

    // Fetch iterations
    var iterResp;
    try {
      iterResp = await apiGet(baseUrl, runPrefix + '/iterations');
    } catch (error) {
      console.log('Error fetching iterations for run ' + runId + ': ' + error.message);
      process.exit(1);
    }
    var benchIterations = iterResp.iterations;
    if (benchIterations.length == 0) {
      console.log('There were no iterations found, exiting');
      process.exit(1);
    }

    // Fetch iteration-level data in parallel: params, primary-period-name, samples, primary-metric
    var iterBody = { iterations: benchIterations };
    var paramsResp, periodNamesResp, samplesResp, primaryMetricsResp;
    try {
      [paramsResp, periodNamesResp, samplesResp, primaryMetricsResp] = await Promise.all([
        apiPost(baseUrl, runPrefix + '/iterations/params', iterBody),
        apiPost(baseUrl, runPrefix + '/iterations/primary-period-name', iterBody),
        apiPost(baseUrl, runPrefix + '/iterations/samples', iterBody),
        apiPost(baseUrl, runPrefix + '/iterations/primary-metric', iterBody)
      ]);
    } catch (error) {
      console.log('Error fetching iteration data for run ' + runId + ': ' + error.message);
      process.exit(1);
    }
    var iterParams = paramsResp.params;
    var iterPrimaryPeriodNames = periodNamesResp.periodNames;
    var iterSampleIds = samplesResp.samples;
    var iterPrimaryMetrics = primaryMetricsResp.primaryMetrics;

    // Fetch sample-level data: statuses and primary period IDs
    var statusesResp, periodIdsResp;
    try {
      [statusesResp, periodIdsResp] = await Promise.all([
        apiPost(baseUrl, runPrefix + '/samples/statuses', { sampleIds: iterSampleIds }),
        apiPost(baseUrl, runPrefix + '/samples/primary-period-id', {
          sampleIds: iterSampleIds,
          periodNames: iterPrimaryPeriodNames
        })
      ]);
    } catch (error) {
      console.log('Error fetching sample data for run ' + runId + ': ' + error.message);
      process.exit(1);
    }
    var iterSampleStatuses = statusesResp.statuses;
    var iterPrimaryPeriodIds = periodIdsResp.periodIds;

    // Fetch period ranges
    var rangesResp;
    try {
      rangesResp = await apiPost(baseUrl, runPrefix + '/periods/range', { periodIds: iterPrimaryPeriodIds });
    } catch (error) {
      console.log('Error fetching period ranges for run ' + runId + ': ' + error.message);
      process.exit(1);
    }
    var iterPrimaryPeriodRanges = rangesResp.ranges;

    // Find the params which are the same in every iteration
    var primaryMetrics = list(iterPrimaryMetrics[0]);
    // For now only dump params when 1 primary metric is used
    var commonParams = [];
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
      for (var idx = 0; idx < allParams.length; idx++) {
        if (allParamsCounts[idx] == benchIterations.length) {
          commonParams.push(allParams[idx]);
        }
      }
      commonParams.sort();
      thisRun['common-params'] = commonParams;
      var commonParamsStr = '  common params: ';
      commonParams.forEach((param) => {
        commonParamsStr += param + ' ';
      });
      logOutput(commonParamsStr, program.outputFormat);
    }

    // Fetch and display metric sources and types
    logOutput('  metrics:', program.outputFormat);
    var sourcesResp;
    try {
      sourcesResp = await apiGet(baseUrl, runPrefix + '/metric-sources');
    } catch (error) {
      console.log('Error fetching metric sources for run ' + runId + ': ' + error.message);
      process.exit(1);
    }
    var metricSources = sourcesResp.sources;

    var typesResp;
    try {
      typesResp = await apiPost(baseUrl, runPrefix + '/metric-types', { sources: metricSources });
    } catch (error) {
      console.log('Error fetching metric types for run ' + runId + ': ' + error.message);
      process.exit(1);
    }
    var metricTypes = typesResp.types;

    thisRun['metrics'] = [];
    for (var i = 0; i < metricSources.length; i++) {
      logOutput('    source: ' + metricSources[i], program.outputFormat);
      var typeList = '      types: ';
      for (var j = 0; j < metricTypes[i].length; j++) {
        typeList += metricTypes[i][j] + ' ';
      }
      logOutput(typeList, program.outputFormat);
      var thisMetric = { source: metricSources[i], types: metricTypes[i] };
      thisRun['metrics'].push(thisMetric);
    }

    // Build the sets for the metric data queries
    var benchmarks = benchName.split(',');
    var sets = [];
    for (var i = 0; i < benchIterations.length; i++) {
      for (var j = 0; j < iterSampleIds[i].length; j++) {
        var primaryMetrics = list(iterPrimaryMetrics[i]);
        for (var k = 0; k < primaryMetrics.length; k++) {
          var source = '';
          var type = '';
          var sourceType = primaryMetrics[k].split('::');
          if (sourceType.length == 1) {
            source = benchmarks[0];
            type = primaryMetrics[k];
          } else if (sourceType.length == 2) {
            source = sourceType[0];
            type = sourceType[1];
          } else {
            console.log('ERROR: sourceType array is an unexpected length, ' + sourceType.length);
            process.exit(1);
          }
          sets.push({
            run: runId,
            period: iterPrimaryPeriodIds[i][j],
            source: source,
            type: type,
            begin: iterPrimaryPeriodRanges[i][j].begin,
            end: iterPrimaryPeriodRanges[i][j].end,
            resolution: 1,
            breakout: [],
            iterIdx: i,
            sampIdx: j,
            metricIdx: k
          });
        }
      }
    }

    // Fetch metric data in batches
    var batchedQuerySize = 10;
    var metricDataResults = new Array(sets.length);
    for (var batchStart = 0; batchStart < sets.length; batchStart += batchedQuerySize) {
      var batchEnd = Math.min(batchStart + batchedQuerySize, sets.length);
      var batchPromises = [];
      for (var b = batchStart; b < batchEnd; b++) {
        var s = sets[b];
        batchPromises.push(
          apiPost(baseUrl, '/api/v1/metric-data', {
            run: s.run,
            period: s.period,
            source: s.source,
            type: s.type,
            begin: s.begin,
            end: s.end,
            resolution: s.resolution,
            breakout: s.breakout
          })
        );
      }
      var batchResults;
      try {
        batchResults = await Promise.all(batchPromises);
      } catch (error) {
        console.log('Error fetching metric data for run ' + runId + ': ' + error.message);
        process.exit(1);
      }
      for (var b = 0; b < batchResults.length; b++) {
        metricDataResults[batchStart + b] = batchResults[b];
      }
    }

    // Output the results
    var data = {};
    var numIter = {};
    var idx = 0;
    for (var i = 0; i < benchIterations.length; i++) {
      var primaryMetrics = list(iterPrimaryMetrics[i]);
      var series = {};
      logOutput('    iteration-id: ' + benchIterations[i]);
      var thisIteration = {};
      thisIteration['iteration-id'] = benchIterations[i];
      thisIteration['unique-params'] = [];
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
        logOutput(paramList);
        thisIteration['unique-params'] = iterParams[i].sort((a, b) => (a.arg < b.arg ? -1 : 1));
      }

      logOutput('      primary-period name: ' + iterPrimaryPeriodNames[i]);
      thisIteration['primary-period'] = iterPrimaryPeriodNames[i];
      var primaryMetric = iterPrimaryMetrics[i];
      thisIteration['primary-metric'] = iterPrimaryMetrics[i];
      if (typeof data[primaryMetric] == 'undefined') {
        data[primaryMetric] = [];
        numIter[primaryMetric] = 0;
      }
      numIter[primaryMetric]++;
      logOutput('      samples:');
      thisIteration['samples'] = [];
      var thisSample = {};
      var msampleCount = 0;
      var msampleTotal = 0;
      var msampleVals = [];
      var msampleList = '';
      var allBenchMsampleVals = [];
      var allBenchMsampleTotal = [];
      var allBenchMsampleFixedList = [];
      var allBenchMsampleCount = [];
      for (var j = 0; j < iterSampleIds[i].length; j++) {
        if (
          iterSampleStatuses[i][j] == 'pass' &&
          iterPrimaryPeriodRanges[i][j].begin !== undefined &&
          iterPrimaryPeriodRanges[i][j].end !== undefined
        ) {
          var thisSample = {};
          logOutput('        sample-id: ' + iterSampleIds[i][j]);
          thisSample['sample-id'] = iterSampleIds[i][j];
          logOutput('          primary period-id: ' + iterPrimaryPeriodIds[i][j]);
          thisSample['primary-period-id'] = iterPrimaryPeriodIds[i][j];
          logOutput(
            '          period range: begin: ' +
              iterPrimaryPeriodRanges[i][j].begin +
              ' end: ' +
              iterPrimaryPeriodRanges[i][j].end
          );
          thisSample['begin'] = iterPrimaryPeriodRanges[i][j].begin;
          thisSample['end'] = iterPrimaryPeriodRanges[i][j].end;

          logOutput(
            '          period length: ' +
              (iterPrimaryPeriodRanges[i][j].end - iterPrimaryPeriodRanges[i][j].begin) / 1000 +
              ' seconds'
          );
          thisSample['length'] = iterPrimaryPeriodRanges[i][j].length;
          var primaryMetrics = list(iterPrimaryMetrics[i]);
          thisSample['values'] = {};
          for (var k = 0; k < primaryMetrics.length; k++) {
            var sourceType = primaryMetrics[k].split('::');
            var metric_data = metricDataResults[idx];
            msampleVal = parseFloat(metric_data.values[''][0].value);
            thisSample['values'][primaryMetrics[k]] = msampleVal;
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
          thisIteration['samples'].push(thisSample);
        }
      }
      thisIteration['results'] = [];
      for (var k = 0; k < primaryMetrics.length; k++) {
        var sourceType = primaryMetrics[k].split('::');
        var thisValue = {};
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
              parseFloat(mstddevpct).toFixed(6)
          );
          thisValue['primary-metric'] = primaryMetrics[k];
          thisValue['mean'] = mean;
          thisValue['min'] = Math.min(...allBenchMsampleVals[k]);
          thisValue['max'] = Math.max(...allBenchMsampleVals[k]);
          thisValue['stddev'] = mstddev;
          thisValue['stddevpct'] = mstddevpct;
          thisValue['max'] = Math.max(...allBenchMsampleVals[k]);
          thisIteration['results'].push(thisValue);
          series['mean'] = mean;
          series['min'] = Math.min(...allBenchMsampleVals[k]);
          series['max'] = Math.max(...allBenchMsampleVals[k]);
        }
      }
      data[primaryMetric].push(series);
      thisRun['iterations'].push(thisIteration);
    }
    summary['runs'].push(thisRun);

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
        fs.writeFileSync(program.outputDir + '/' + 'result-summary.json', JSON.stringify(summary, null, 2));
      } catch (err) {
        console.error(err);
      }
    }
    if (program.outputFormat.includes('yaml')) {
      try {
        fs.writeFileSync(program.outputDir + '/' + 'result-summary.yaml', yaml.dump(summary, { 'sort-keys': true }));
      } catch (err) {
        console.error(err);
      }
    }
  }
}

main();
