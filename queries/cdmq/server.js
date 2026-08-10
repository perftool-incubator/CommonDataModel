const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const app = express();
const cdm = require('./cdm');
const PORT = process.env.PORT || 3000;
const { Command } = require('commander');
const program = new Command();

var instances = [];

// Server log file — written to /var/lib/crucible/logs/ if it exists, otherwise cwd
var logDir = '/var/lib/crucible/logs';
try {
  fs.mkdirSync(logDir, { recursive: true });
} catch (e) {
  logDir = '.';
}
var logFile = logDir + '/cdm-server.log';
var logStream = fs.createWriteStream(logFile, { flags: 'a' });

function serverLog(msg, reqId) {
  var prefix = '[' + new Date().toISOString() + ']';
  if (reqId) prefix += ' [' + reqId + ']';
  var line = prefix + ' ' + msg;
  console.log(line);
  logStream.write(line + '\n');
}

function serverError(msg, reqId) {
  var prefix = '[' + new Date().toISOString() + ']';
  if (reqId) prefix += ' [' + reqId + ']';
  var line = prefix + ' ERROR: ' + msg;
  console.error(line);
  logStream.write(line + '\n');
}

// Validate that a request body contains only known fields.
// Returns null if valid, or an error message string if unknown fields are found.
function validateBodyFields(body, knownFields) {
  if (!body || typeof body !== 'object') return null;
  var unknown = Object.keys(body).filter(function (k) {
    return !knownFields.includes(k);
  });
  if (unknown.length === 0) return null;
  var hints = unknown.map(function (u) {
    // suggest a known field if it differs only by a trailing 's' or missing trailing 's'
    var candidate = u.endsWith('s') ? u.slice(0, -1) : u + 's';
    if (knownFields.includes(candidate)) return u + ' (did you mean: ' + candidate + '?)';
    return u;
  });
  return 'Unknown field(s) in request body: ' + hints.join(', ') + '. Known fields: ' + knownFields.join(', ');
}

// Per-client request counter for generating short session-like IDs
var clientCounters = {};
function generateReqId(req) {
  var ip = req.ip || req.connection.remoteAddress || 'unknown';
  var shortIp = ip.replace(/^.*:/, ''); // last part of IPv6 or IPv4
  if (!clientCounters[shortIp]) clientCounters[shortIp] = 0;
  clientCounters[shortIp]++;
  return shortIp + '-' + clientCounters[shortIp];
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
  if (cdm.isValidCdmVersion(ver)) {
    instances[instances.length - 1]['ver'] = ver;
  } else {
    console.log('The version must be one of: ' + cdm.supportedCdmVersions.join(', ') + ', not: ' + ver);
    process.exit(1);
  }
}

program
  .version('1.0.0')
  .option('--host <host[:port]>', 'The host and optional port of the OpenSearch instance', save_host)
  .option('--userpass <user:pass>', 'The user and password for the most recent --host', save_userpass)
  .option(cdm.cdmVersionOptionDesc(), 'The Common Data Model version to use for the most recent --host', save_ver)
  .parse(process.argv);

const options = program.opts();

// If the user does not specify any hosts, assume localhost:9200 is used
if (instances.length == 0) {
  save_host('localhost:9200');
}

serverLog(
  'Starting CDM server with ' + instances.length + ' instance(s): ' + JSON.stringify(instances.map((i) => i.host))
);
serverLog('Log file: ' + logFile);

getInstancesInfo(instances);

serverLog('Instance info after discovery: ' + JSON.stringify(instances, null, 2));

app.use(cors());
app.use(express.json());

// Assign a request ID to each request for log correlation
app.use(function (req, res, next) {
  req.reqId = generateReqId(req);
  next();
});

// --------------------------------------------------------------------------------------------------------------
// Middleware: resolve a run ID to an OpenSearch instance and yearDotMonth
// Attaches req.cdm = { instance, yearDotMonth, runId } on success
// --------------------------------------------------------------------------------------------------------------
async function resolveRun(req, res, next) {
  try {
    const runId = req.params.id;
    if (!runId) {
      return res.status(400).json({
        code: 'MISSING_RUN_ID',
        error: 'A run ID is required'
      });
    }

    if (!instances || instances.length === 0) {
      return res.status(503).json({
        code: 'NO_INSTANCES',
        error: 'No OpenSearch instances configured'
      });
    }

    // Refresh instance index info before querying
    getInstancesInfo(instances);

    const instance = await findInstanceFromRun(instances, runId);
    if (instance == null) {
      return res.status(404).json({
        code: 'RUN_NOT_FOUND',
        error: 'Could not find run ID ' + runId + ' in any OpenSearch instance'
      });
    }

    const yearDotMonth = await findYearDotMonthFromRun(instance, runId);

    req.cdm = { instance, yearDotMonth, runId };
    next();
  } catch (error) {
    serverError('Error in resolveRun middleware:', error);
    res.status(500).json({
      code: 'INTERNAL_ERROR',
      error: 'Failed to resolve run: ' + error.message
    });
  }
}

// --------------------------------------------------------------------------------------------------------------
// GET /api/v1/runs — search for runs by filters
// Query params: name, email, harness, run (all optional)
// --------------------------------------------------------------------------------------------------------------
app.get('/api/v1/runs', async (req, res) => {
  try {
    var termKeys = [];
    var values = [];

    if (req.query.name) {
      termKeys.push('run.name');
      values.push([req.query.name]);
    }
    if (req.query.email) {
      termKeys.push('run.email');
      values.push([req.query.email]);
    }
    // Run ID filter: supports comma-separated values for multi-select
    var runIdFilter = null;
    if (req.query.run) {
      var runIds = req.query.run.split(',').filter(Boolean);
      if (runIds.length === 1) {
        termKeys.push('run.run-uuid');
        values.push([runIds[0]]);
      } else {
        // Multiple run IDs: filter results after the query
        runIdFilter = new Set(runIds);
      }
    }
    if (req.query.harness) {
      termKeys.push('run.harness');
      values.push([req.query.harness]);
    }
    if (req.query.benchmark) {
      termKeys.push('run.benchmark');
      values.push([req.query.benchmark]);
    }

    if (!instances || instances.length === 0) {
      return res.status(503).json({
        code: 'NO_INSTANCES',
        error: 'No OpenSearch instances configured'
      });
    }

    // Refresh instance index info before querying
    getInstancesInfo(instances);

    var allInstanceRunIds = [];
    for (const instance of instances) {
      if (invalidInstance(instance)) {
        continue;
      }
      var ydm = getYdm(instance, 'run', req);
      var instanceRunIds = await cdm.mSearch(instance, 'run', ydm, termKeys, values, 'run.run-uuid', null, 1000);
      if (typeof instanceRunIds[0] != 'undefined') {
        allInstanceRunIds.push(instanceRunIds[0]);
      }
    }

    var runIds = cdm.consolidateAllArrays(allInstanceRunIds);
    if (typeof runIds == 'undefined') {
      runIds = [];
    }

    // Apply multi-run-ID filter if specified
    if (runIdFilter) {
      runIds = runIds.filter(function (id) {
        return runIdFilter.has(id);
      });
    }

    // Helper: get run IDs from a cross-index aggregation and intersect with current set
    async function intersectRunIds(runIdSet, fetchFn) {
      var crossIds = [];
      for (const instance of instances) {
        if (invalidInstance(instance)) continue;
        var result = await fetchFn(instance);
        if (result && result[0]) crossIds.push(result[0]);
      }
      var consolidated = cdm.consolidateAllArrays(crossIds) || [];
      var crossSet = new Set(consolidated);
      return runIdSet.filter((id) => crossSet.has(id));
    }

    // Filter by primary metric name (e.g., "uperf::Gbps")
    if (req.query.primaryMetric) {
      runIds = await intersectRunIds(runIds, (inst) =>
        cdm.getRunIdsByPrimaryMetric(inst, getYdm(inst, 'iteration', req), req.query.primaryMetric)
      );
    }

    // Filter by tag pairs: tags=[{"name":"x","val":"y"}, ...]
    var tagFilters = [];
    if (req.query.tags) {
      try {
        tagFilters = JSON.parse(req.query.tags);
      } catch (e) {
        /* ignore parse errors */
      }
    }
    // Support legacy single tag filter
    if (tagFilters.length === 0 && (req.query.tagName || req.query.tagValue)) {
      tagFilters.push({ name: req.query.tagName || '', val: req.query.tagValue || '' });
    }
    for (const tag of tagFilters) {
      if (!tag.name && !tag.val) continue;
      // Support comma-separated tag values (OR within a tag, AND across tags)
      var tagVals = tag.val ? tag.val.split(',').filter(Boolean) : [null];
      if (tagVals.length === 1) {
        runIds = await intersectRunIds(runIds, (inst) =>
          cdm.getRunIdsByTag(inst, getYdm(inst, 'tag', req), tag.name || null, tagVals[0])
        );
      } else {
        var unionIds = new Set();
        for (const val of tagVals) {
          var ids = await intersectRunIds(runIds, (inst) =>
            cdm.getRunIdsByTag(inst, getYdm(inst, 'tag', req), tag.name || null, val)
          );
          ids.forEach((id) => unionIds.add(id));
        }
        runIds = runIds.filter((id) => unionIds.has(id));
      }
    }

    // Filter by param pairs: params=[{"arg":"x","val":"y,z"}, ...]
    // Comma-separated values are OR'd (union), then intersected with the run set.
    if (req.query.params) {
      try {
        var paramFilters = JSON.parse(req.query.params);
        for (const param of paramFilters) {
          if (!param.arg || !param.val) continue;
          var vals = param.val.split(',').filter(Boolean);
          if (vals.length === 1) {
            runIds = await intersectRunIds(runIds, (inst) =>
              cdm.getRunIdsByParam(inst, getYdm(inst, 'param', req), param.arg, vals[0])
            );
          } else {
            // Union run IDs across all values for this param
            var unionIds = new Set();
            for (const val of vals) {
              var ids = await intersectRunIds(runIds, (inst) =>
                cdm.getRunIdsByParam(inst, getYdm(inst, 'param', req), param.arg, val)
              );
              ids.forEach((id) => unionIds.add(id));
            }
            runIds = runIds.filter((id) => unionIds.has(id));
          }
        }
      } catch (e) {
        /* ignore parse errors */
      }
    }

    serverLog('[' + Date.now() + '] GET /api/v1/runs returned ' + runIds.length + ' run(s)');
    res.json({ runIds: runIds });
  } catch (error) {
    serverError('Error in GET /api/v1/runs:', error);
    res.status(500).json({
      code: 'INTERNAL_ERROR',
      error: 'Failed to search for runs: ' + error.message
    });
  }
});

// --------------------------------------------------------------------------------------------------------------
// GET /api/v1/run/:id/tags — get tags for a run
// --------------------------------------------------------------------------------------------------------------
app.get('/api/v1/run/:id/tags', resolveRun, async (req, res) => {
  try {
    const { instance, yearDotMonth, runId } = req.cdm;
    var tags = await cdm.getTags(instance, runId, yearDotMonth);
    if (typeof tags == 'undefined') {
      tags = [];
    }
    serverLog('[' + Date.now() + '] GET /api/v1/run/' + runId + '/tags returned ' + tags.length + ' tag(s)');
    res.json({ tags: tags });
  } catch (error) {
    serverError('Error in GET /api/v1/run/:id/tags:', error);
    res.status(500).json({
      code: 'INTERNAL_ERROR',
      error: 'Failed to get tags: ' + error.message
    });
  }
});

// --------------------------------------------------------------------------------------------------------------
// GET /api/v1/run/:id/benchmark — get benchmark name for a run
// --------------------------------------------------------------------------------------------------------------
app.get('/api/v1/run/:id/benchmark', resolveRun, async (req, res) => {
  try {
    const { instance, yearDotMonth, runId } = req.cdm;
    var benchmarkName = await cdm.getBenchmarkName(instance, runId, yearDotMonth);
    if (typeof benchmarkName == 'undefined' || benchmarkName == null) {
      return res.status(404).json({
        code: 'BENCHMARK_NOT_FOUND',
        error: 'No benchmark name found for run ' + runId
      });
    }
    serverLog('[' + Date.now() + '] GET /api/v1/run/' + runId + '/benchmark returned: ' + benchmarkName);
    res.json({ benchmark: benchmarkName });
  } catch (error) {
    serverError('Error in GET /api/v1/run/:id/benchmark:', error);
    res.status(500).json({
      code: 'INTERNAL_ERROR',
      error: 'Failed to get benchmark name: ' + error.message
    });
  }
});

// --------------------------------------------------------------------------------------------------------------
// GET /api/v1/run/:id/partial-status — get partial-run/dropped-engines status for a run
// --------------------------------------------------------------------------------------------------------------
app.get('/api/v1/run/:id/partial-status', resolveRun, async (req, res) => {
  try {
    const { instance, yearDotMonth, runId } = req.cdm;
    var runDataArr = await cdm.getRunData(instance, runId, yearDotMonth);
    var runData = (runDataArr && runDataArr[0]) || {};
    var partial = (runData.run && runData.run.partial) || false;
    var droppedEngines = (runData.run && runData.run['dropped-engines']) || [];
    serverLog('[' + Date.now() + '] GET /api/v1/run/' + runId + '/partial-status returned partial=' + partial);
    res.json({ partial: partial, 'dropped-engines': droppedEngines });
  } catch (error) {
    serverError('Error in GET /api/v1/run/:id/partial-status:', error);
    res.status(500).json({
      code: 'INTERNAL_ERROR',
      error: 'Failed to get partial-run status: ' + error.message
    });
  }
});

// --------------------------------------------------------------------------------------------------------------
// GET /api/v1/run/:id/iterations — get iteration UUIDs for a run
// --------------------------------------------------------------------------------------------------------------
app.get('/api/v1/run/:id/iterations', resolveRun, async (req, res) => {
  try {
    const { instance, yearDotMonth, runId } = req.cdm;
    var iterations = await cdm.getIterations(instance, runId, yearDotMonth);
    if (typeof iterations == 'undefined') {
      iterations = [];
    }
    console.log(
      '[' + Date.now() + '] GET /api/v1/run/' + runId + '/iterations returned ' + iterations.length + ' iteration(s)'
    );
    res.json({ iterations: iterations });
  } catch (error) {
    serverError('Error in GET /api/v1/run/:id/iterations:', error);
    res.status(500).json({
      code: 'INTERNAL_ERROR',
      error: 'Failed to get iterations: ' + error.message
    });
  }
});

// --------------------------------------------------------------------------------------------------------------
// POST /api/v1/run/:id/iterations/params — get params for iterations
// Body: { iterations: [...] }
// --------------------------------------------------------------------------------------------------------------
app.post('/api/v1/run/:id/iterations/params', resolveRun, async (req, res) => {
  try {
    const { instance, yearDotMonth, runId } = req.cdm;
    const { iterations } = req.body;

    if (!Array.isArray(iterations) || iterations.length === 0) {
      return res.status(400).json({
        code: 'MISSING_ITERATIONS',
        error: 'An array of iteration IDs is required in the request body'
      });
    }

    var params = await cdm.mgetParams(instance, iterations, yearDotMonth);
    if (typeof params == 'undefined') {
      params = [];
    }
    console.log(
      '[' +
        Date.now() +
        '] POST /api/v1/run/' +
        runId +
        '/iterations/params returned params for ' +
        iterations.length +
        ' iteration(s)'
    );
    res.json({ params: params });
  } catch (error) {
    serverError('Error in POST /api/v1/run/:id/iterations/params:', error);
    res.status(500).json({
      code: 'INTERNAL_ERROR',
      error: 'Failed to get iteration params: ' + error.message
    });
  }
});

// --------------------------------------------------------------------------------------------------------------
// POST /api/v1/run/:id/iterations/primary-period-name — get primary period names
// Body: { iterations: [...] }
// --------------------------------------------------------------------------------------------------------------
app.post('/api/v1/run/:id/iterations/primary-period-name', resolveRun, async (req, res) => {
  try {
    const { instance, yearDotMonth, runId } = req.cdm;
    const { iterations } = req.body;

    if (!Array.isArray(iterations) || iterations.length === 0) {
      return res.status(400).json({
        code: 'MISSING_ITERATIONS',
        error: 'An array of iteration IDs is required in the request body'
      });
    }

    var periodNames = await cdm.mgetPrimaryPeriodName(instance, iterations, yearDotMonth);
    if (typeof periodNames == 'undefined') {
      periodNames = [];
    }
    console.log(
      '[' +
        Date.now() +
        '] POST /api/v1/run/' +
        runId +
        '/iterations/primary-period-name returned ' +
        periodNames.length +
        ' name(s)'
    );
    res.json({ periodNames: periodNames });
  } catch (error) {
    serverError('Error in POST /api/v1/run/:id/iterations/primary-period-name:', error);
    res.status(500).json({
      code: 'INTERNAL_ERROR',
      error: 'Failed to get primary period names: ' + error.message
    });
  }
});

// --------------------------------------------------------------------------------------------------------------
// POST /api/v1/run/:id/iterations/samples — get sample IDs per iteration
// Body: { iterations: [...] }
// Returns: { samples: [[...], [...]] } (2D array indexed by iteration)
// --------------------------------------------------------------------------------------------------------------
app.post('/api/v1/run/:id/iterations/samples', resolveRun, async (req, res) => {
  try {
    const { instance, yearDotMonth, runId } = req.cdm;
    const { iterations } = req.body;

    if (!Array.isArray(iterations) || iterations.length === 0) {
      return res.status(400).json({
        code: 'MISSING_ITERATIONS',
        error: 'An array of iteration IDs is required in the request body'
      });
    }

    var samples = await cdm.mgetSamples(instance, iterations, yearDotMonth);
    if (typeof samples == 'undefined') {
      samples = [];
    }
    console.log(
      '[' +
        Date.now() +
        '] POST /api/v1/run/' +
        runId +
        '/iterations/samples returned samples for ' +
        iterations.length +
        ' iteration(s)'
    );
    res.json({ samples: samples });
  } catch (error) {
    serverError('Error in POST /api/v1/run/:id/iterations/samples:', error);
    res.status(500).json({
      code: 'INTERNAL_ERROR',
      error: 'Failed to get samples: ' + error.message
    });
  }
});

// --------------------------------------------------------------------------------------------------------------
// POST /api/v1/run/:id/samples/statuses — get pass/fail status per sample
// Body: { sampleIds: [[...], [...]] } (2D array indexed by iteration)
// Returns: { statuses: [[...], [...]] } (2D array indexed by iteration)
// --------------------------------------------------------------------------------------------------------------
app.post('/api/v1/run/:id/samples/statuses', resolveRun, async (req, res) => {
  try {
    const { instance, yearDotMonth, runId } = req.cdm;
    const { sampleIds } = req.body;

    if (!Array.isArray(sampleIds) || sampleIds.length === 0) {
      return res.status(400).json({
        code: 'MISSING_SAMPLE_IDS',
        error: 'A 2D array of sample IDs is required in the request body'
      });
    }

    var statuses = await cdm.mgetSampleStatuses(instance, sampleIds, yearDotMonth);
    if (typeof statuses == 'undefined') {
      statuses = [];
    }
    serverLog('[' + Date.now() + '] POST /api/v1/run/' + runId + '/samples/statuses completed');
    res.json({ statuses: statuses });
  } catch (error) {
    serverError('Error in POST /api/v1/run/:id/samples/statuses:', error);
    res.status(500).json({
      code: 'INTERNAL_ERROR',
      error: 'Failed to get sample statuses: ' + error.message
    });
  }
});

// --------------------------------------------------------------------------------------------------------------
// POST /api/v1/run/:id/samples/primary-period-id — get primary period IDs
// Body: { sampleIds: [[...], [...]], periodNames: [...] }
// Returns: { periodIds: [[...], [...]] } (2D array indexed by iteration)
// --------------------------------------------------------------------------------------------------------------
app.post('/api/v1/run/:id/samples/primary-period-id', resolveRun, async (req, res) => {
  try {
    const { instance, yearDotMonth, runId } = req.cdm;
    const { sampleIds, periodNames } = req.body;

    if (!Array.isArray(sampleIds) || sampleIds.length === 0) {
      return res.status(400).json({
        code: 'MISSING_SAMPLE_IDS',
        error: 'A 2D array of sample IDs is required in the request body'
      });
    }
    if (!Array.isArray(periodNames) || periodNames.length === 0) {
      return res.status(400).json({
        code: 'MISSING_PERIOD_NAMES',
        error: 'An array of period names is required in the request body'
      });
    }

    var periodIds = await cdm.mgetPrimaryPeriodId(instance, sampleIds, periodNames, yearDotMonth);
    if (typeof periodIds == 'undefined') {
      periodIds = [];
    }
    serverLog('[' + Date.now() + '] POST /api/v1/run/' + runId + '/samples/primary-period-id completed');
    res.json({ periodIds: periodIds });
  } catch (error) {
    serverError('Error in POST /api/v1/run/:id/samples/primary-period-id:', error);
    res.status(500).json({
      code: 'INTERNAL_ERROR',
      error: 'Failed to get primary period IDs: ' + error.message
    });
  }
});

// --------------------------------------------------------------------------------------------------------------
// POST /api/v1/run/:id/periods/range — get begin/end time for periods
// Body: { periodIds: [[...], [...]] } (2D array indexed by iteration)
// Returns: { ranges: [[{begin, end}, ...], ...] } (2D array indexed by iteration)
// --------------------------------------------------------------------------------------------------------------
app.post('/api/v1/run/:id/periods/range', resolveRun, async (req, res) => {
  try {
    const { instance, yearDotMonth, runId } = req.cdm;
    const { periodIds } = req.body;

    if (!Array.isArray(periodIds) || periodIds.length === 0) {
      return res.status(400).json({
        code: 'MISSING_PERIOD_IDS',
        error: 'A 2D array of period IDs is required in the request body'
      });
    }

    var ranges = await cdm.mgetPeriodRange(instance, periodIds, yearDotMonth);
    if (typeof ranges == 'undefined') {
      ranges = [];
    }
    serverLog('[' + Date.now() + '] POST /api/v1/run/' + runId + '/periods/range completed');
    res.json({ ranges: ranges });
  } catch (error) {
    serverError('Error in POST /api/v1/run/:id/periods/range:', error);
    res.status(500).json({
      code: 'INTERNAL_ERROR',
      error: 'Failed to get period ranges: ' + error.message
    });
  }
});

// --------------------------------------------------------------------------------------------------------------
// POST /api/v1/run/:id/iterations/primary-metric — get primary metric per iteration
// Body: { iterations: [...] }
// Returns: { primaryMetrics: [...] } (1D array, one per iteration)
// --------------------------------------------------------------------------------------------------------------
app.post('/api/v1/run/:id/iterations/primary-metric', resolveRun, async (req, res) => {
  try {
    const { instance, yearDotMonth, runId } = req.cdm;
    const { iterations } = req.body;

    if (!Array.isArray(iterations) || iterations.length === 0) {
      return res.status(400).json({
        code: 'MISSING_ITERATIONS',
        error: 'An array of iteration IDs is required in the request body'
      });
    }

    var primaryMetrics = await cdm.mgetPrimaryMetric(instance, iterations, yearDotMonth);
    if (typeof primaryMetrics == 'undefined') {
      primaryMetrics = [];
    }
    console.log(
      '[' +
        Date.now() +
        '] POST /api/v1/run/' +
        runId +
        '/iterations/primary-metric returned ' +
        primaryMetrics.length +
        ' metric(s)'
    );
    res.json({ primaryMetrics: primaryMetrics });
  } catch (error) {
    serverError('Error in POST /api/v1/run/:id/iterations/primary-metric:', error);
    res.status(500).json({
      code: 'INTERNAL_ERROR',
      error: 'Failed to get primary metrics: ' + error.message
    });
  }
});

// --------------------------------------------------------------------------------------------------------------
// GET /api/v1/run/:id/metric-sources — get all metric sources for a run
// --------------------------------------------------------------------------------------------------------------
app.get('/api/v1/run/:id/metric-sources', resolveRun, async (req, res) => {
  try {
    const { instance, yearDotMonth, runId } = req.cdm;
    var metricSourcesSets = await cdm.mgetMetricSources(instance, [runId], yearDotMonth);
    var sources = [];
    if (Array.isArray(metricSourcesSets) && metricSourcesSets.length > 0) {
      sources = metricSourcesSets[0];
    }
    console.log(
      '[' + Date.now() + '] GET /api/v1/run/' + runId + '/metric-sources returned ' + sources.length + ' source(s)'
    );
    res.json({ sources: sources });
  } catch (error) {
    serverError('Error in GET /api/v1/run/:id/metric-sources:', error);
    res.status(500).json({
      code: 'INTERNAL_ERROR',
      error: 'Failed to get metric sources: ' + error.message
    });
  }
});

// --------------------------------------------------------------------------------------------------------------
// POST /api/v1/run/:id/metric-types — get metric types per source
// Body: { sources: [...] }
// Returns: { types: [[...], [...]] } (2D array, one inner array per source)
// --------------------------------------------------------------------------------------------------------------
app.post('/api/v1/run/:id/metric-types', resolveRun, async (req, res) => {
  try {
    const { instance, yearDotMonth, runId } = req.cdm;
    const { sources } = req.body;

    if (!Array.isArray(sources) || sources.length === 0) {
      return res.status(400).json({
        code: 'MISSING_SOURCES',
        error: 'An array of metric sources is required in the request body'
      });
    }

    // mgetMetricTypes expects parallel arrays of runIds and sources
    var runIds = sources.map(() => runId);
    var types = await cdm.mgetMetricTypes(instance, runIds, sources, yearDotMonth);
    if (typeof types == 'undefined') {
      types = [];
    }
    console.log(
      '[' +
        Date.now() +
        '] POST /api/v1/run/' +
        runId +
        '/metric-types returned types for ' +
        sources.length +
        ' source(s)'
    );
    res.json({ types: types });
  } catch (error) {
    serverError('Error in POST /api/v1/run/:id/metric-types:', error);
    res.status(500).json({
      code: 'INTERNAL_ERROR',
      error: 'Failed to get metric types: ' + error.message
    });
  }
});

// --------------------------------------------------------------------------------------------------------------
// POST /api/v1/iterations/details — batch-hydrate iterations for multiple runs
// Body: { runIds: [...] }
// Returns fully assembled iteration objects in a single request, using batched
// mSearch calls to OpenSearch instead of per-run HTTP roundtrips.
// --------------------------------------------------------------------------------------------------------------
app.post('/api/v1/iterations/details', async (req, res) => {
  try {
    const { runIds, start, end } = req.body;

    if (!Array.isArray(runIds) || runIds.length === 0) {
      return res.status(400).json({
        code: 'MISSING_RUN_IDS',
        error: 'An array of run IDs is required in the request body'
      });
    }

    if (!instances || instances.length === 0) {
      return res.status(503).json({
        code: 'NO_INSTANCES',
        error: 'No OpenSearch instances configured'
      });
    }

    getInstancesInfo(instances);

    var allIterations = [];

    for (const inst of instances) {
      if (invalidInstance(inst)) continue;
      // Use the date range from the search to scope queries.
      // buildYearDotMonthRange returns docType-independent suffixes (e.g., "@2025.01,@2025.02")
      // so the same ydm works for all index types (run, iteration, param, etc.)
      var ydm = cdm.buildYearDotMonthRange(inst, 'run', start || null, end || null);
      var gRunIds = runIds;

      // Step 1: Batch-fetch run-level data for all runs in this group.
      // Run sequentially to avoid overwhelming OpenSearch's search thread pool
      // when querying across many monthly indices.
      var benchmarks = await cdm.mgetBenchmarkName(inst, gRunIds, ydm);
      var runDataByRun = await cdm.mgetRunData(inst, gRunIds, ydm);
      var iterationsByRun = await cdm.mgetIterations(inst, gRunIds, ydm);
      var tagsByRun = await cdm.mgetTags(inst, gRunIds, ydm);

      // Collect all iteration IDs across all runs, tracking which run each belongs to
      var allIterIds = [];
      var iterToRunMap = []; // parallel array: iterToRunMap[i] = { runIdx, runId, benchmark, tags, runBegin }
      for (var r = 0; r < gRunIds.length; r++) {
        var runIters = (iterationsByRun && iterationsByRun[r]) || [];
        var benchmark = (benchmarks && benchmarks[r] && benchmarks[r][0]) || null;
        var tags = (tagsByRun && tagsByRun[r]) || [];
        var runData = (runDataByRun && runDataByRun[r] && runDataByRun[r][0]) || {};
        var runBegin = (runData.run && runData.run.begin) || null;
        var runSource = (runData.run && runData.run.source) || null;
        var runName = (runData.run && runData.run.name) || null;
        var runEmail = (runData.run && runData.run.email) || null;
        for (var it = 0; it < runIters.length; it++) {
          allIterIds.push(runIters[it]);
          iterToRunMap.push({ runIdx: r, runId: gRunIds[r], benchmark, tags, runBegin, runSource, runName, runEmail });
        }
      }

      if (allIterIds.length === 0) continue;

      // Step 2: Batch-fetch iteration-level data for all iterations.
      // Run sequentially to avoid overwhelming OpenSearch's search thread pool.
      var params = await cdm.mgetParams(inst, allIterIds, ydm);
      var samples = await cdm.mgetSamples(inst, allIterIds, ydm);
      var primaryMetrics = await cdm.mgetPrimaryMetric(inst, allIterIds, ydm);
      var periodNames = await cdm.mgetPrimaryPeriodName(inst, allIterIds, ydm);

      // Step 3: Batch-fetch sample statuses
      var samplesByIter = samples || [];
      var statuses = [];
      if (samplesByIter.length > 0) {
        statuses = await cdm.mgetSampleStatuses(inst, samplesByIter, ydm);
        if (typeof statuses === 'undefined') statuses = [];
      }

      // Step 4: Compute common vs unique params
      // Group iterations by run to determine common params within each run
      var runIterGroups = {};
      for (var i = 0; i < allIterIds.length; i++) {
        var runId = iterToRunMap[i].runId;
        if (!runIterGroups[runId]) runIterGroups[runId] = [];
        runIterGroups[runId].push(i);
      }

      var commonParamsByRun = {};
      var uniqueParamsByIter = {};

      for (var runId in runIterGroups) {
        var idxs = runIterGroups[runId];
        var paramSets = idxs.map(function (idx) {
          var p = (params && params[idx]) || [];
          return Array.isArray(p) ? p : [];
        });

        var common = [];
        var unique = [];

        if (paramSets.length > 1) {
          var first = paramSets[0];
          for (var p = 0; p < first.length; p++) {
            var param = first[p];
            var isCommon = paramSets.every(function (ps) {
              return ps.some(function (pp) {
                return pp.arg === param.arg && pp.val === param.val;
              });
            });
            if (isCommon) common.push(param);
          }
          for (var s = 0; s < paramSets.length; s++) {
            unique.push(
              paramSets[s].filter(function (pp) {
                return !common.some(function (c) {
                  return c.arg === pp.arg && c.val === pp.val;
                });
              })
            );
          }
        } else {
          if (paramSets.length === 1) unique.push(paramSets[0]);
        }

        commonParamsByRun[runId] = common;
        for (var s = 0; s < idxs.length; s++) {
          uniqueParamsByIter[idxs[s]] = unique[s] || [];
        }
      }

      // Step 5: Assemble iteration objects
      for (var i = 0; i < allIterIds.length; i++) {
        var meta = iterToRunMap[i];
        var iterSamples = samplesByIter[i] || [];
        var iterStatuses = (statuses && statuses[i]) || [];
        var passCount = iterStatuses.filter(function (s) {
          return s === 'pass';
        }).length;
        var failCount = iterStatuses.filter(function (s) {
          return s === 'fail';
        }).length;

        allIterations.push({
          runId: meta.runId,
          iterationId: allIterIds[i],
          benchmark: meta.benchmark,
          tags: meta.tags,
          params: (params && params[i]) || [],
          commonParams: commonParamsByRun[meta.runId] || [],
          uniqueParams: uniqueParamsByIter[i] || [],
          sampleCount: iterSamples.length,
          passCount: passCount,
          failCount: failCount,
          primaryMetric: (primaryMetrics && primaryMetrics[i]) || null,
          runBegin: meta.runBegin,
          runSource: meta.runSource,
          runName: meta.runName,
          runEmail: meta.runEmail
        });
      }
    }

    serverLog(
      'POST /api/v1/iterations/details: ' + runIds.length + ' run(s) -> ' + allIterations.length + ' iteration(s)'
    );
    res.json({ iterations: allIterations });
  } catch (error) {
    serverError('Error in POST /api/v1/iterations/details: ' + error);
    res.status(500).json({
      code: 'INTERNAL_ERROR',
      error: 'Failed to get iteration details: ' + error.message
    });
  }
});

// --------------------------------------------------------------------------------------------------------------
// POST /api/v1/iterations/metric-values — fetch primary metric values for iterations
// Body: { runIds: [...], start, end }
// Returns: { values: { iterationId: { sampleValues, mean, stddevPct } } }
// --------------------------------------------------------------------------------------------------------------
app.post('/api/v1/iterations/metric-values', async (req, res) => {
  try {
    const { runIds, start, end } = req.body;

    if (!Array.isArray(runIds) || runIds.length === 0) {
      return res.status(400).json({
        code: 'MISSING_RUN_IDS',
        error: 'An array of run IDs is required in the request body'
      });
    }

    getInstancesInfo(instances);

    var result = {};

    for (const inst of instances) {
      if (invalidInstance(inst)) continue;
      var ydm = cdm.buildYearDotMonthRange(inst, 'run', start || null, end || null);

      // Fetch iterations, samples, statuses, period names, primary metrics
      var iterationsByRun = await cdm.mgetIterations(inst, runIds, ydm);
      var allIterIds = [];
      var iterRunIds = [];
      for (var r = 0; r < runIds.length; r++) {
        var runIters = (iterationsByRun && iterationsByRun[r]) || [];
        for (var it = 0; it < runIters.length; it++) {
          allIterIds.push(runIters[it]);
          iterRunIds.push(runIds[r]);
        }
      }

      if (allIterIds.length === 0) continue;

      var samples = await cdm.mgetSamples(inst, allIterIds, ydm);
      var statuses = await cdm.mgetSampleStatuses(inst, samples || [], ydm);
      if (typeof statuses === 'undefined') statuses = [];
      var periodNames = await cdm.mgetPrimaryPeriodName(inst, allIterIds, ydm);
      var primaryMetrics = await cdm.mgetPrimaryMetric(inst, allIterIds, ydm);

      // Build passing samples per iteration
      var passingSamplesByIter = [];
      var passingPeriodNamesByIter = [];
      for (var i = 0; i < allIterIds.length; i++) {
        var iterSamples = (samples && samples[i]) || [];
        var iterStatuses = (statuses && statuses[i]) || [];
        var iterPeriodName = (periodNames && periodNames[i]) || null;
        var passing = [];
        for (var s = 0; s < iterSamples.length; s++) {
          if (iterStatuses[s] === 'pass') passing.push(iterSamples[s]);
        }
        passingSamplesByIter.push(passing);
        passingPeriodNamesByIter.push(iterPeriodName);
      }

      // Get primary period IDs
      var primaryPeriodIds = [];
      var hasPassing = passingSamplesByIter.some(function (s) {
        return s.length > 0;
      });
      if (hasPassing) {
        primaryPeriodIds = await cdm.mgetPrimaryPeriodId(inst, passingSamplesByIter, passingPeriodNamesByIter, ydm);
        if (typeof primaryPeriodIds === 'undefined') primaryPeriodIds = [];
      }

      // Get period ranges
      var periodRanges = [];
      if (primaryPeriodIds.length > 0) {
        periodRanges = await cdm.mgetPeriodRange(inst, primaryPeriodIds, ydm);
        if (typeof periodRanges === 'undefined') periodRanges = [];
      }

      // Build metric data sets
      var metricSets = [];
      var metricSetMap = [];
      for (var i = 0; i < allIterIds.length; i++) {
        var pm = (primaryMetrics && primaryMetrics[i]) || null;
        if (!pm || typeof pm !== 'string') continue;
        var pmParts = pm.split('::');
        if (pmParts.length < 2) continue;
        var pmSource = pmParts[0];
        var pmType = pmParts[1];
        var iterPeriodIds = primaryPeriodIds[i] || [];
        var iterRanges = periodRanges[i] || [];
        for (var s = 0; s < iterPeriodIds.length; s++) {
          if (!iterPeriodIds[s]) continue;
          var range = iterRanges[s];
          if (!range || !range.begin || !range.end) continue;
          metricSets.push({
            run: iterRunIds[i],
            period: iterPeriodIds[s],
            source: pmSource,
            type: pmType,
            begin: range.begin,
            end: range.end,
            resolution: 1,
            breakout: []
          });
          metricSetMap.push(i);
        }
      }

      // Fetch metric values
      if (metricSets.length > 0) {
        var resp = await cdm.getMetricDataSets(inst, metricSets, ydm);
        if (resp['ret-code'] === 0 && resp['data-sets']) {
          var dataSets = resp['data-sets'];
          var valuesByIdx = {};
          for (var m = 0; m < dataSets.length; m++) {
            var iterIdx = metricSetMap[m];
            if (!valuesByIdx[iterIdx]) valuesByIdx[iterIdx] = [];
            if (dataSets[m] && dataSets[m].values) {
              var keys = Object.keys(dataSets[m].values);
              if (keys.length > 0 && dataSets[m].values[keys[0]].length > 0) {
                valuesByIdx[iterIdx].push(dataSets[m].values[keys[0]][0].value);
              }
            }
          }

          // Compute mean/stddev per iteration
          for (var idx in valuesByIdx) {
            var vals = valuesByIdx[idx];
            var iterId = allIterIds[idx];
            if (vals.length === 0) continue;
            var sum = 0;
            for (var v = 0; v < vals.length; v++) sum += vals[v];
            var mean = sum / vals.length;
            var variance = 0;
            for (var v = 0; v < vals.length; v++) variance += (vals[v] - mean) * (vals[v] - mean);
            var stddev = vals.length > 1 ? Math.sqrt(variance / (vals.length - 1)) : 0;
            var stddevPct = mean !== 0 ? (stddev / Math.abs(mean)) * 100 : 0;
            result[iterId] = { sampleValues: vals, mean: mean, stddevPct: stddevPct };
          }
        }
      }
    }

    serverLog('POST /api/v1/iterations/metric-values: ' + Object.keys(result).length + ' iteration(s) with values');
    res.json({ values: result });
  } catch (error) {
    serverError('Error in POST /api/v1/iterations/metric-values: ' + error);
    res.status(500).json({
      code: 'INTERNAL_ERROR',
      error: 'Failed to get metric values: ' + error.message
    });
  }
});

// --------------------------------------------------------------------------------------------------------------
// POST /api/v1/iterations/metric-sources — get available metric sources across runs
// Body: { runIds: [...], start, end }
// Returns: { sources: [...] }
// --------------------------------------------------------------------------------------------------------------
app.post('/api/v1/iterations/metric-sources', async (req, res) => {
  try {
    const { runIds, start, end } = req.body;
    if (!Array.isArray(runIds) || runIds.length === 0) {
      return res.status(400).json({ code: 'MISSING_RUN_IDS', error: 'An array of run IDs is required' });
    }
    getInstancesInfo(instances);
    var allSources = new Set();
    for (const inst of instances) {
      if (invalidInstance(inst)) continue;
      var ydm = cdm.buildYearDotMonthRange(inst, 'run', start || null, end || null);
      var sourcesPerRun = await cdm.mgetMetricSources(inst, runIds, ydm);
      if (sourcesPerRun) {
        sourcesPerRun.forEach(function (s) {
          if (Array.isArray(s))
            s.forEach(function (v) {
              allSources.add(v);
            });
        });
      }
    }
    var sorted = Array.from(allSources).sort();
    serverLog('POST /api/v1/iterations/metric-sources: ' + sorted.length + ' source(s)');
    res.json({ sources: sorted });
  } catch (error) {
    serverError('Error in POST /api/v1/iterations/metric-sources: ' + error);
    res.status(500).json({ code: 'INTERNAL_ERROR', error: 'Failed to get metric sources: ' + error.message });
  }
});

// --------------------------------------------------------------------------------------------------------------
// POST /api/v1/iterations/metric-types — get metric types for a source across runs
// Body: { runIds: [...], start, end, source }
// Returns: { types: [...] }
// --------------------------------------------------------------------------------------------------------------
app.post('/api/v1/iterations/metric-types', async (req, res) => {
  try {
    const { runIds, start, end, source } = req.body;
    if (!Array.isArray(runIds) || runIds.length === 0 || !source) {
      return res.status(400).json({ code: 'MISSING_PARAMS', error: 'runIds and source are required' });
    }
    getInstancesInfo(instances);
    var allTypes = new Set();
    for (const inst of instances) {
      if (invalidInstance(inst)) continue;
      var ydm = cdm.buildYearDotMonthRange(inst, 'run', start || null, end || null);
      // mgetMetricTypes needs parallel arrays of runIds and sources
      var sources = runIds.map(function () {
        return source;
      });
      var typesPerRun = await cdm.mgetMetricTypes(inst, runIds, sources, ydm);
      if (typesPerRun) {
        typesPerRun.forEach(function (t) {
          if (Array.isArray(t))
            t.forEach(function (v) {
              allTypes.add(v);
            });
        });
      }
    }
    var sorted = Array.from(allTypes).sort();
    serverLog('POST /api/v1/iterations/metric-types: ' + sorted.length + ' type(s) for source ' + source);
    res.json({ types: sorted });
  } catch (error) {
    serverError('Error in POST /api/v1/iterations/metric-types: ' + error);
    res.status(500).json({ code: 'INTERNAL_ERROR', error: 'Failed to get metric types: ' + error.message });
  }
});

// --------------------------------------------------------------------------------------------------------------
// POST /api/v1/iterations/breakout-values — get distinct values for each breakout dimension
// Body: { runIds: [...], start, end, source, type, breakouts: ["hostname", "num", ...] }
// Returns: { breakouts: { "hostname": ["host1", "host2"], "num": ["0", "1"] } }
// --------------------------------------------------------------------------------------------------------------
app.post('/api/v1/iterations/breakout-values', async (req, res) => {
  try {
    var knownFields = ['runIds', 'start', 'end', 'source', 'type', 'breakouts'];
    var fieldErr = validateBodyFields(req.body, knownFields);
    if (fieldErr) {
      return res.status(400).json({ code: 'UNKNOWN_FIELDS', error: fieldErr });
    }
    const { runIds, start, end, source, type, breakouts } = req.body;
    if (!Array.isArray(runIds) || runIds.length === 0 || !source || !type || !Array.isArray(breakouts)) {
      return res
        .status(400)
        .json({ code: 'MISSING_PARAMS', error: 'runIds, source, type, and breakouts are required' });
    }
    getInstancesInfo(instances);
    var merged = {};
    for (const inst of instances) {
      if (invalidInstance(inst)) continue;
      var ydm = cdm.buildYearDotMonthRange(inst, 'run', start || null, end || null);
      var result = await cdm.mgetBreakoutValues(inst, runIds, source, type, breakouts, ydm);
      // Merge values across instances
      Object.keys(result).forEach(function (dim) {
        if (!merged[dim]) merged[dim] = new Set();
        result[dim].forEach(function (v) {
          merged[dim].add(v);
        });
      });
    }
    // Convert Sets to sorted arrays
    var response = {};
    Object.keys(merged).forEach(function (dim) {
      response[dim] = Array.from(merged[dim]).sort();
    });
    serverLog(
      'POST /api/v1/iterations/breakout-values: ' +
        source +
        '::' +
        type +
        ' -> ' +
        Object.keys(response)
          .map(function (k) {
            return k + ':' + response[k].length;
          })
          .join(', ')
    );
    res.json({ breakouts: response });
  } catch (error) {
    serverError('Error in POST /api/v1/iterations/breakout-values: ' + error);
    res.status(500).json({ code: 'INTERNAL_ERROR', error: 'Failed to get breakout values: ' + error.message });
  }
});

// --------------------------------------------------------------------------------------------------------------
// POST /api/v1/iterations/supplemental-metric — get values for a specific metric source/type
// Body: { runIds: [...], start, end, source, type, breakout: [...] }
// Returns: { values: { iterationId: { labels: { label: { mean, stddevPct, sampleValues } }, remainingBreakouts: [...] } } }
// When breakout is empty, returns a single label "__all__" with the aggregated value.
// --------------------------------------------------------------------------------------------------------------
// POST /api/v1/iterations/period-info — get period IDs and time ranges per iteration
// Body: { iterations: [{iterationId, runId}], start, end, sampleIndex }
// Returns: { periods: { iterationId: { periodId, begin, end, runId } } }
// --------------------------------------------------------------------------------------------------------------
app.post('/api/v1/iterations/period-info', async (req, res) => {
  try {
    const { iterations: reqIterations, start, end, sampleIndex } = req.body;
    if (!Array.isArray(reqIterations) || reqIterations.length === 0) {
      return res.status(400).json({ code: 'MISSING_PARAMS', error: 'iterations array is required' });
    }
    var requestedSampleIdx = typeof sampleIndex === 'number' ? sampleIndex : null;
    var perIterSampleIdx =
      typeof sampleIndex === 'object' && sampleIndex !== null && !Array.isArray(sampleIndex) ? sampleIndex : null;

    getInstancesInfo(instances);
    var result = {};

    for (const inst of instances) {
      if (invalidInstance(inst)) continue;
      var ydm = cdm.buildYearDotMonthRange(inst, 'run', start || null, end || null);

      var allIterIds = reqIterations.map(function (it) {
        return it.iterationId;
      });
      var iterRunIds = reqIterations.map(function (it) {
        return it.runId;
      });

      var samples = await cdm.mgetSamples(inst, allIterIds, ydm);
      var statuses = await cdm.mgetSampleStatuses(inst, samples || [], ydm);
      if (typeof statuses === 'undefined') statuses = [];
      var periodNames = await cdm.mgetPrimaryPeriodName(inst, allIterIds, ydm);

      var passingSamplesByIter = [];
      var passingPeriodNamesByIter = [];
      for (var i = 0; i < allIterIds.length; i++) {
        var iterSamples = (samples && samples[i]) || [];
        var iterStatuses = (statuses && statuses[i]) || [];
        var iterPeriodName = (periodNames && periodNames[i]) || null;
        var passing = [];
        for (var s = 0; s < iterSamples.length; s++) {
          if (iterStatuses[s] === 'pass') passing.push(iterSamples[s]);
        }
        passingSamplesByIter.push(passing);
        passingPeriodNamesByIter.push(iterPeriodName);
      }

      var primaryPeriodIds = [];
      var hasPassing = passingSamplesByIter.some(function (s) {
        return s.length > 0;
      });
      if (hasPassing) {
        primaryPeriodIds = await cdm.mgetPrimaryPeriodId(inst, passingSamplesByIter, passingPeriodNamesByIter, ydm);
        if (typeof primaryPeriodIds === 'undefined') primaryPeriodIds = [];
      }

      var periodRanges = [];
      if (primaryPeriodIds.length > 0) {
        periodRanges = await cdm.mgetPeriodRange(inst, primaryPeriodIds, ydm);
        if (typeof periodRanges === 'undefined') periodRanges = [];
      }

      for (var i = 0; i < allIterIds.length; i++) {
        var iterPeriodIds = primaryPeriodIds[i] || [];
        var iterRanges = periodRanges[i] || [];
        if (iterPeriodIds.length === 0) continue;

        var selIdx = 0;
        if (perIterSampleIdx && perIterSampleIdx[allIterIds[i]] != null) {
          selIdx = perIterSampleIdx[allIterIds[i]];
        } else if (requestedSampleIdx !== null) {
          selIdx = requestedSampleIdx;
        }
        if (selIdx >= iterPeriodIds.length) selIdx = 0;

        if (!iterPeriodIds[selIdx]) continue;
        var range = iterRanges[selIdx];
        if (!range || !range.begin || !range.end) continue;

        result[allIterIds[i]] = {
          periodId: iterPeriodIds[selIdx],
          begin: range.begin,
          end: range.end,
          runId: iterRunIds[i]
        };
      }
    }

    serverLog('POST /api/v1/iterations/period-info: ' + Object.keys(result).length + ' period(s)');
    res.json({ periods: result });
  } catch (error) {
    serverError('Error in POST /api/v1/iterations/period-info: ' + error);
    res.status(500).json({ code: 'INTERNAL_ERROR', error: 'Failed to get period info: ' + error.message });
  }
});

// --------------------------------------------------------------------------------------------------------------
app.post('/api/v1/iterations/supplemental-metric', async (req, res) => {
  try {
    var knownFields = ['runIds', 'iterations', 'start', 'end', 'source', 'type', 'breakout', 'filter', 'sampleIndex'];
    var fieldErr = validateBodyFields(req.body, knownFields);
    if (fieldErr) {
      return res.status(400).json({ code: 'UNKNOWN_FIELDS', error: fieldErr });
    }
    const { runIds, iterations: reqIterations, start, end, source, type, breakout, filter, sampleIndex } = req.body;
    var breakoutArr = Array.isArray(breakout) ? breakout : [];
    var filterVal = filter || null;
    // sampleIndex can be a number (same for all iterations) or an object { iterationId: index }
    var requestedSampleIdx = typeof sampleIndex === 'number' ? sampleIndex : null;
    var perIterSampleIdx =
      typeof sampleIndex === 'object' && sampleIndex !== null && !Array.isArray(sampleIndex) ? sampleIndex : null;
    if (!source || !type) {
      return res.status(400).json({ code: 'MISSING_PARAMS', error: 'source and type are required' });
    }
    // Accept either iterations (array of {iterationId, runId}) or runIds (discover iterations)
    if (
      (!Array.isArray(reqIterations) || reqIterations.length === 0) &&
      (!Array.isArray(runIds) || runIds.length === 0)
    ) {
      return res.status(400).json({ code: 'MISSING_PARAMS', error: 'iterations or runIds are required' });
    }
    getInstancesInfo(instances);
    var result = {};
    var remainingBreakouts = [];
    var sampleInfo = {};

    for (const inst of instances) {
      if (invalidInstance(inst)) continue;
      var ydm = cdm.buildYearDotMonthRange(inst, 'run', start || null, end || null);

      var allIterIds = [];
      var iterRunIds = [];

      if (Array.isArray(reqIterations) && reqIterations.length > 0) {
        // Use provided iteration IDs directly
        for (var it = 0; it < reqIterations.length; it++) {
          allIterIds.push(reqIterations[it].iterationId);
          iterRunIds.push(reqIterations[it].runId);
        }
      } else {
        // Fall back to discovering iterations from run IDs
        var iterationsByRun = await cdm.mgetIterations(inst, runIds, ydm);
        for (var r = 0; r < runIds.length; r++) {
          var runIters = (iterationsByRun && iterationsByRun[r]) || [];
          for (var it2 = 0; it2 < runIters.length; it2++) {
            allIterIds.push(runIters[it2]);
            iterRunIds.push(runIds[r]);
          }
        }
      }
      if (allIterIds.length === 0) continue;

      var samples = await cdm.mgetSamples(inst, allIterIds, ydm);
      var statuses = await cdm.mgetSampleStatuses(inst, samples || [], ydm);
      if (typeof statuses === 'undefined') statuses = [];
      var periodNames = await cdm.mgetPrimaryPeriodName(inst, allIterIds, ydm);

      var passingSamplesByIter = [];
      var passingPeriodNamesByIter = [];
      for (var i = 0; i < allIterIds.length; i++) {
        var iterSamples = (samples && samples[i]) || [];
        var iterStatuses = (statuses && statuses[i]) || [];
        var iterPeriodName = (periodNames && periodNames[i]) || null;
        var passing = [];
        for (var s = 0; s < iterSamples.length; s++) {
          if (iterStatuses[s] === 'pass') passing.push(iterSamples[s]);
        }
        passingSamplesByIter.push(passing);
        passingPeriodNamesByIter.push(iterPeriodName);
      }

      var primaryPeriodIds = [];
      var hasPassing = passingSamplesByIter.some(function (s) {
        return s.length > 0;
      });
      if (hasPassing) {
        primaryPeriodIds = await cdm.mgetPrimaryPeriodId(inst, passingSamplesByIter, passingPeriodNamesByIter, ydm);
        if (typeof primaryPeriodIds === 'undefined') primaryPeriodIds = [];
      }

      var periodRanges = [];
      if (primaryPeriodIds.length > 0) {
        periodRanges = await cdm.mgetPeriodRange(inst, primaryPeriodIds, ydm);
        if (typeof periodRanges === 'undefined') periodRanges = [];
      }

      // Build metric sets — use the requested sample index (client determines best sample)
      // sampleIndex is an index into the passing samples array for each iteration
      var metricSets = [];
      var metricSetMap = [];
      for (var i = 0; i < allIterIds.length; i++) {
        var iterPeriodIds = primaryPeriodIds[i] || [];
        var iterRanges = periodRanges[i] || [];
        if (iterPeriodIds.length === 0) continue;

        // Use per-iteration sample index if available, otherwise global, otherwise 0
        var selIdx = 0;
        if (perIterSampleIdx && perIterSampleIdx[allIterIds[i]] != null) {
          selIdx = perIterSampleIdx[allIterIds[i]];
        } else if (requestedSampleIdx !== null) {
          selIdx = requestedSampleIdx;
        }
        if (selIdx >= iterPeriodIds.length) selIdx = 0;

        if (!iterPeriodIds[selIdx]) continue;
        var range = iterRanges[selIdx];
        if (!range || !range.begin || !range.end) continue;

        // Store sample count for this iteration
        sampleInfo[allIterIds[i]] = { sampleCount: iterPeriodIds.length, selectedIndex: selIdx };

        metricSets.push({
          run: iterRunIds[i],
          period: iterPeriodIds[selIdx],
          source: source,
          type: type,
          begin: range.begin,
          end: range.end,
          resolution: 1,
          breakout: breakoutArr.slice(),
          filter: filterVal
        });
        metricSetMap.push(i);
      }

      if (metricSets.length > 0) {
        var resp = await cdm.getMetricDataSets(inst, metricSets, ydm);
        if (resp['ret-code'] === 0 && resp['data-sets']) {
          var dataSets = resp['data-sets'];
          var allRemainingBreakouts = null;
          for (var m = 0; m < dataSets.length; m++) {
            var iterIdx = metricSetMap[m];
            var iterId = allIterIds[iterIdx];
            if (dataSets[m]) {
              if (allRemainingBreakouts === null && dataSets[m].remainingBreakouts) {
                allRemainingBreakouts = dataSets[m].remainingBreakouts;
              }
              if (dataSets[m].values) {
                var labels = {};
                Object.keys(dataSets[m].values).forEach(function (label) {
                  var entries = dataSets[m].values[label];
                  if (Array.isArray(entries) && entries.length > 0) {
                    labels[label] = { sampleValues: [entries[0].value], mean: entries[0].value, stddevPct: 0 };
                  }
                });
                if (Object.keys(labels).length > 0) {
                  result[iterId] = { labels: labels };
                }
              }
            }
          }
          remainingBreakouts = allRemainingBreakouts || [];
        }
      }
    }

    serverLog(
      'POST /api/v1/iterations/supplemental-metric: ' +
        source +
        '::' +
        type +
        ' breakout=' +
        JSON.stringify(breakoutArr) +
        ' sampleIndex=' +
        requestedSampleIdx +
        ' -> ' +
        Object.keys(result).length +
        ' iteration(s)'
    );
    res.json({ values: result, remainingBreakouts: remainingBreakouts, sampleInfo: sampleInfo });
  } catch (error) {
    serverError('Error in POST /api/v1/iterations/supplemental-metric: ' + error);
    res.status(500).json({ code: 'INTERNAL_ERROR', error: 'Failed to get supplemental metric: ' + error.message });
  }
});

// --------------------------------------------------------------------------------------------------------------
// FIELD VALUE ENDPOINTS — return distinct values for search dropdowns
// All accept optional ?start=YYYY.MM&end=YYYY.MM to limit index scope
// --------------------------------------------------------------------------------------------------------------

// Helper to build yearDotMonth for a given docType using request's start/end params
function getYdm(instance, docType, req) {
  var start = req.query.start || null;
  var end = req.query.end || null;
  return cdm.buildYearDotMonthRange(instance, docType, start, end);
}

async function getDistinctValues(instances, fetchFn) {
  getInstancesInfo(instances);
  var allValues = [];
  for (const instance of instances) {
    if (invalidInstance(instance)) continue;
    var values = await fetchFn(instance);
    if (values && values[0]) allValues.push(values[0]);
  }
  var consolidated = cdm.consolidateAllArrays(allValues);
  return (consolidated || []).sort();
}

// Return available YYYY.MM values from index names (for date range selectors)
app.get('/api/v1/fields/months', async (req, res) => {
  try {
    getInstancesInfo(instances);
    var allMonths = new Set();
    for (const instance of instances) {
      if (invalidInstance(instance)) continue;
      var months = cdm.getAvailableMonths(instance);
      months.forEach((m) => allMonths.add(m));
    }
    var sorted = Array.from(allMonths).sort();
    serverLog('GET /api/v1/fields/months returned ' + sorted.length + ' month(s)');
    res.json({ values: sorted });
  } catch (error) {
    serverError('Error in GET /api/v1/fields/months: ' + error);
    res.status(500).json({ code: 'INTERNAL_ERROR', error: 'Failed to get months: ' + error.message });
  }
});

app.get('/api/v1/fields/run-ids', async (req, res) => {
  try {
    var values = await getDistinctValues(instances, (inst) => cdm.getDistinctRunIds(inst, getYdm(inst, 'run', req)));
    serverLog('GET /api/v1/fields/run-ids returned ' + values.length + ' value(s)');
    res.json({ values: values });
  } catch (error) {
    serverError('Error in GET /api/v1/fields/run-ids: ' + error);
    res.status(500).json({ code: 'INTERNAL_ERROR', error: 'Failed to get run IDs: ' + error.message });
  }
});

app.get('/api/v1/fields/names', async (req, res) => {
  try {
    var values = await getDistinctValues(instances, (inst) => cdm.getDistinctNames(inst, getYdm(inst, 'run', req)));
    serverLog('GET /api/v1/fields/names returned ' + values.length + ' value(s)');
    res.json({ values: values });
  } catch (error) {
    serverError('Error in GET /api/v1/fields/names: ' + error);
    res.status(500).json({ code: 'INTERNAL_ERROR', error: 'Failed to get names: ' + error.message });
  }
});

app.get('/api/v1/fields/emails', async (req, res) => {
  try {
    var values = await getDistinctValues(instances, (inst) => cdm.getDistinctEmails(inst, getYdm(inst, 'run', req)));
    serverLog('GET /api/v1/fields/emails returned ' + values.length + ' value(s)');
    res.json({ values: values });
  } catch (error) {
    serverError('Error in GET /api/v1/fields/emails: ' + error);
    res.status(500).json({ code: 'INTERNAL_ERROR', error: 'Failed to get emails: ' + error.message });
  }
});

app.get('/api/v1/fields/benchmarks', async (req, res) => {
  try {
    var values = await getDistinctValues(instances, (inst) =>
      cdm.getDistinctBenchmarks(inst, getYdm(inst, 'run', req))
    );
    serverLog('GET /api/v1/fields/benchmarks returned ' + values.length + ' value(s)');
    res.json({ values: values });
  } catch (error) {
    serverError('Error in GET /api/v1/fields/benchmarks: ' + error);
    res.status(500).json({ code: 'INTERNAL_ERROR', error: 'Failed to get benchmarks: ' + error.message });
  }
});

app.get('/api/v1/fields/tag-names', async (req, res) => {
  try {
    var values = await getDistinctValues(instances, (inst) => cdm.getDistinctTagNames(inst, getYdm(inst, 'tag', req)));
    serverLog('GET /api/v1/fields/tag-names returned ' + values.length + ' value(s)');
    res.json({ values: values });
  } catch (error) {
    serverError('Error in GET /api/v1/fields/tag-names: ' + error);
    res.status(500).json({ code: 'INTERNAL_ERROR', error: 'Failed to get tag names: ' + error.message });
  }
});

app.get('/api/v1/fields/tag-values', async (req, res) => {
  try {
    var tagName = req.query.name || null;
    var values = await getDistinctValues(instances, (inst) =>
      cdm.getDistinctTagValues(inst, getYdm(inst, 'tag', req), tagName)
    );
    serverLog('GET /api/v1/fields/tag-values returned ' + values.length + ' value(s)');
    res.json({ values: values });
  } catch (error) {
    serverError('Error in GET /api/v1/fields/tag-values: ' + error);
    res.status(500).json({ code: 'INTERNAL_ERROR', error: 'Failed to get tag values: ' + error.message });
  }
});

app.get('/api/v1/fields/param-args', async (req, res) => {
  try {
    var values = await getDistinctValues(instances, (inst) =>
      cdm.getDistinctParamArgs(inst, getYdm(inst, 'param', req))
    );
    serverLog('GET /api/v1/fields/param-args returned ' + values.length + ' value(s)');
    res.json({ values: values });
  } catch (error) {
    serverError('Error in GET /api/v1/fields/param-args: ' + error);
    res.status(500).json({ code: 'INTERNAL_ERROR', error: 'Failed to get param args: ' + error.message });
  }
});

app.get('/api/v1/fields/param-values', async (req, res) => {
  try {
    var paramArg = req.query.arg || null;
    var values = await getDistinctValues(instances, (inst) =>
      cdm.getDistinctParamValues(inst, getYdm(inst, 'param', req), paramArg)
    );
    serverLog('GET /api/v1/fields/param-values returned ' + values.length + ' value(s)');
    res.json({ values: values });
  } catch (error) {
    serverError('Error in GET /api/v1/fields/param-values: ' + error);
    res.status(500).json({ code: 'INTERNAL_ERROR', error: 'Failed to get param values: ' + error.message });
  }
});

app.get('/api/v1/fields/primary-metrics', async (req, res) => {
  try {
    var values = await getDistinctValues(instances, (inst) =>
      cdm.getDistinctPrimaryMetrics(inst, getYdm(inst, 'iteration', req))
    );
    serverLog('GET /api/v1/fields/primary-metrics returned ' + values.length + ' value(s)');
    res.json({ values: values });
  } catch (error) {
    serverError('Error in GET /api/v1/fields/primary-metrics: ' + error);
    res.status(500).json({ code: 'INTERNAL_ERROR', error: 'Failed to get primary metrics: ' + error.message });
  }
});

// --------------------------------------------------------------------------------------------------------------
// POST /api/v1/metric-data — get metric data (existing endpoint, supports run or period)
// --------------------------------------------------------------------------------------------------------------
app.post('/api/v1/metric-data', async (req, res) => {
  try {
    var knownFields = [
      'run',
      'period',
      'begin',
      'end',
      'source',
      'type',
      'resolution',
      'breakout',
      'filter',
      'aggregation',
      'instances'
    ];
    var fieldErr = validateBodyFields(req.body, knownFields);
    if (fieldErr) {
      return res.status(400).json({ code: 'UNKNOWN_FIELDS', error: fieldErr });
    }
    var {
      run,
      period,
      begin,
      end,
      source,
      type,
      resolution,
      breakout,
      filter,
      aggregation,
      instances: reqInstances
    } = req.body;

    var validAggregations = ['sum', 'avg', 'max', 'min'];
    if (aggregation && !validAggregations.includes(aggregation)) {
      return res.status(400).json({
        code: 'INVALID_AGGREGATION',
        error: "Invalid aggregation '" + aggregation + "'. Must be one of: " + validAggregations.join(', ')
      });
    }

    var reqStart = Date.now();
    var breakoutStr = Array.isArray(breakout)
      ? breakout
          .map(function (b) {
            return typeof b === 'object' && b.name ? b.name : b;
          })
          .join(',')
      : breakout || 'none';
    serverLog(
      'POST /api/v1/metric-data: ' +
        source +
        '::' +
        type +
        ' resolution=' +
        resolution +
        ' breakout=[' +
        breakoutStr +
        ']' +
        (filter ? ' filter=' + filter : '') +
        ' run=' +
        (run || 'none').toString().substring(0, 8) +
        '... period=' +
        (period || 'none').toString().substring(0, 8) +
        '...',
      req.reqId
    );
    //serverLog('  curl: curl -s -X POST http://localhost:3000/api/v1/metric-data -H "Content-Type: application/json" -d \'' + JSON.stringify({ run: run, period: period, begin: begin, end: end, source: source, type: type, resolution: resolution, breakout: breakout, filter: filter }) + '\'', req.reqId);

    // Use instances from request if provided, otherwise use server's configured instances
    var instancesToUse = reqInstances && reqInstances.length > 0 ? reqInstances : instances;

    if (!instancesToUse || instancesToUse.length === 0) {
      return res.status(503).json({
        code: 'NO_INSTANCES',
        error:
          'No OpenSearch instances configured. Either start server with --host options or provide instances in request.'
      });
    }

    // Refresh instance index info before querying
    getInstancesInfo(instancesToUse);

    var yearDotMonth;
    var instance;
    if (run != null) {
      instance = await findInstanceFromRun(instancesToUse, run);
      if (instance == null) {
        return res.status(404).json({
          code: 'RUN_NOT_FOUND',
          error: 'Could not find run ID ' + run + ' in any OpenSearch instance'
        });
      }
    } else if (period != null) {
      instance = await findInstanceFromPeriod(instancesToUse, period);
      if (instance == null) {
        return res.status(404).json({
          code: 'PERIOD_NOT_FOUND',
          error: 'Could not find period ID ' + period + ' in any OpenSearch instance'
        });
      }
      // We don't yet know the yearDotMonth, so use wildcard to query all period indices
      run = await getRunFromPeriod(instance, period, '@*');
    } else {
      return res.status(400).json({
        code: 'MISSING_RUN_OR_PERIOD',
        error: 'Neither a period nor a run ID were provided'
      });
    }
    var yearDotMonth = await findYearDotMonthFromRun(instance, run);

    // getMetricDataSets expects breakout to be an array
    // Handle breakout as either an array (from new client) or string (from legacy clients)
    if (Array.isArray(breakout)) {
      // Already an array, use as-is
    } else if (typeof breakout === 'string') {
      // Legacy string format, do simple split
      breakout = breakout.split(',');
    } else {
      // Undefined or null
      breakout = [];
    }
    if (typeof resolution == 'undefined') {
      resolution = 1;
    }
    var set = {
      run: run,
      period: period,
      source: source,
      type: type,
      begin: begin,
      end: end,
      resolution: resolution,
      breakout: breakout,
      filter: filter,
      aggregation: aggregation
    };
    var resp = await cdm.getMetricDataSets(instance, [set], yearDotMonth);
    if (resp['ret-code'] != 0) {
      return res.status(500).json({
        code: 'METRIC_QUERY_FAILED',
        error: resp['ret-msg']
      });
    }
    var metric_data = resp['data-sets'][0];

    var labelCount = metric_data && metric_data.values ? Object.keys(metric_data.values).length : 0;
    var elapsed = Date.now() - reqStart;
    serverLog(
      'POST /api/v1/metric-data: ' + source + '::' + type + ' -> ' + labelCount + ' label(s) in ' + elapsed + 'ms',
      req.reqId
    );

    // Return the data
    res.json(metric_data);
  } catch (error) {
    serverError('Error in /api/v1/metric-data:', error);
    res.status(500).json({
      code: 'INTERNAL_ERROR',
      error: 'Internal server error while fetching metric data',
      details: error.message
    });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

// Error handling middleware
app.use((error, req, res, next) => {
  serverError('Unhandled error: ' + error);
  res.status(500).json({
    code: 'INTERNAL_ERROR',
    error: 'Internal server error',
    details: process.env.NODE_ENV === 'development' ? error.message : undefined
  });
});

// Serve the web UI static files (built React app)
var webUiDist = path.join(__dirname, 'web-ui', 'dist');
if (fs.existsSync(webUiDist)) {
  app.use(express.static(webUiDist));

  // SPA fallback: serve index.html for non-API routes
  app.get('*path', (req, res) => {
    res.sendFile(path.join(webUiDist, 'index.html'));
  });
  serverLog('Serving web UI from ' + webUiDist);
} else {
  // Handle 404 for unknown routes when no web UI is built
  app.use((req, res) => {
    res.status(404).json({
      code: 'ROUTE_NOT_FOUND',
      error: 'Route not found: ' + req.method + ' ' + req.originalUrl
    });
  });
}

// Start server
app.listen(PORT, () => {
  serverLog('CDM Query Server running on port ' + PORT);
  serverLog('API endpoints: http://localhost:' + PORT + '/api/v1/');
  serverLog('Health check: http://localhost:' + PORT + '/health');
});

module.exports = app;
