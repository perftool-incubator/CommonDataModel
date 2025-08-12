//# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript
var cdm = require('./cdm');
var yaml = require('js-yaml');
var program = require('commander');
var instances = [];
var summary = {};

function list(val) {
  return val.split(',');
}

function save_host(host) {
  var host_info = { host: host, header: { 'Content-Type': 'application/json' } };
  instances.push(host_info);
}

function save_userpass(userpass) {
  if (instances.length == 0) {
    console.log('You must specify a --host before a --userpass');
    process.exit(1);
  }
  instances[instances.length - 1]['header'] = {
    'Content-Type': 'application/json',
    Authorization: 'Basic ' + btoa(userpass)
  };
}

function save_ver(ver) {
  if (instances.length == 0) {
    console.log('You must specify a --host before a --ver');
    process.exit(1);
  }
  if (/^v[7|8|9]dev$/.exec(ver)) {
    instances[instances.length - 1]['ver'] = ver;
  } else {
    console.log('The version must be v7dev, v8dev, or v9dev, not: ' + ver);
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
  .option('--output-format <fmt>, fmta[,fmtb]', 'one or more output formats: txt, json, yaml', list, [])
  .parse(process.argv);

var termKeys = [];
var values = [];

if (program.run) {
  termKeys.push('run.run-uuid');
  values.push([program.run]);
}

async function main() {
  // If the user does not specify any hosts, assume localhost:9200 is used
  if (instances.length == 0) {
    save_host('localhost:9200');
  }

  getInstancesInfo(instances);
  cdm.debuglog(JSON.stringify(instances, null, 2));

  // Since this query is looking for run ids (and may not inlcude run-uuid as a search term), we
  // need to check all instances.
  var allInstanceRunIds = [];
  for (const instance of instances) {
    if (invalidInstance(instance)) {
      continue;
    }
    cdm.debuglog('main(): calling cdm.mSearch()');
    var instanceRunIds = await cdm.mSearch(instance, 'run', '@*', termKeys, values, 'run.run-uuid', null, 1000);
    cdm.debuglog('main(): returned from cdm.mSearch()');
    cdm.debuglog('instanceRunIds:\n' + JSON.stringify(instanceRunIds, null, 2));
    if (typeof instanceRunIds[0] != 'undefined') {
      allInstanceRunIds.push(instanceRunIds[0]);
    }
  }
  cdm.debuglog('allInstanceRunIds:\n' + JSON.stringify(allInstanceRunIds, null, 2));

  var runIds = cdm.consolidateAllArrays(allInstanceRunIds);
  cdm.debuglog('(consolidated)allInstanceRunIds:\n' + JSON.stringify(runIds, null, 2));

  if (typeof runIds == 'undefined' || runIds.length == 0) {
    console.log('The run ID could not be found, exiting');
    process.exit(1);
  }

  cdm.debuglog('runIds:\n' + JSON.stringify(runIds, null, 2));
  summary['runs'] = [];
  for (runIdx = 0; runIdx < runIds.length; runIdx++) {
    cdm.debuglog('runIdx:\n' + runIdx);
    var thisRun = {};
    const runId = runIds[runIdx];
    var instance = await findInstanceFromRun(instances, runId);
    var yearDotMonth = await findYearDotMonthFromRun(instance, runId);
    thisRun['run-id'] = runId;
    thisRun['iterations'] = [];
    var benchName = await cdm.getBenchmarkName(instance, runId, yearDotMonth);
    var benchmarks = list(benchName);
    var benchIterations = await cdm.getIterations(instance, runId, yearDotMonth);
    if (benchIterations.length == 0) {
      cdm.debuglog('There were no iterations found, exiting');
      process.exit(1);
    }

    var iterPrimaryMetrics = await cdm.mgetPrimaryMetric(instance, benchIterations, yearDotMonth);
    //returns 1D array [iter]
    var iterPrimaryPeriodNames = await cdm.mgetPrimaryPeriodName(instance, benchIterations, yearDotMonth);
    //input: 1D array
    //output: 2D array [iter][samp]
    var iterSampleIds = await cdm.mgetSamples(instance, benchIterations, yearDotMonth);
    //input: 2D array iterSampleIds: [iter][samp]
    //output: 2D array [iter][samp]
    var iterSampleStatuses = await cdm.mgetSampleStatuses(instance, iterSampleIds, yearDotMonth);
    //needs 2D array iterSampleIds: [iter][samp] and 1D array iterPrimaryPeriodNames [iter]
    //returns 2D array [iter][samp]
    var iterPrimaryPeriodIds = await cdm.mgetPrimaryPeriodId(
      instance,
      iterSampleIds,
      iterPrimaryPeriodNames,
      yearDotMonth
    );

    var data = {};
    var numIter = {};
    var idx = 0;
    for (var i = 0; i < benchIterations.length; i++) {
      var primaryMetrics = list(iterPrimaryMetrics[i]);
      for (var j = 0; j < iterSampleIds[i].length; j++) {
        if (iterSampleStatuses[i][j] == 'pass') {
          console.log(iterPrimaryPeriodIds[i][j] + '   Iteration: ' + (i + 1) + '   Sample: ' + (j + 1));
        }
      }
    }
  }
}

main();
