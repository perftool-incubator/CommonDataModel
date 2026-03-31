const express = require('express');
const cors = require('cors');
const app = express();
const cdm = require('./cdm');
const PORT = process.env.PORT || 3000;
const { Command } = require('commander');
const program = new Command();

var instances = [];

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
  .version('1.0.0')
  .option('--host <host[:port]>', 'The host and optional port of the OpenSearch instance', save_host)
  .option('--userpass <user:pass>', 'The user and password for the most recent --host', save_userpass)
  .option('--ver <v7dev|v8dev|v9dev>', 'The Common Data Model version to use for the most recent --host', save_ver)
  .parse(process.argv);

const options = program.opts();

// If the user does not specify any hosts, assume localhost:9200 is used
if (instances.length == 0) {
  save_host('localhost:9200');
}

getInstancesInfo(instances);

app.use(cors());
app.use(express.json());

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
    console.error('Error in resolveRun middleware:', error);
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
    if (req.query.run) {
      termKeys.push('run.run-uuid');
      values.push([req.query.run]);
    }
    if (req.query.harness) {
      termKeys.push('run.harness');
      values.push([req.query.harness]);
    }

    if (!instances || instances.length === 0) {
      return res.status(503).json({
        code: 'NO_INSTANCES',
        error: 'No OpenSearch instances configured'
      });
    }

    var allInstanceRunIds = [];
    for (const instance of instances) {
      if (invalidInstance(instance)) {
        continue;
      }
      var instanceRunIds = await cdm.mSearch(instance, 'run', '@*', termKeys, values, 'run.run-uuid', null, 1000);
      if (typeof instanceRunIds[0] != 'undefined') {
        allInstanceRunIds.push(instanceRunIds[0]);
      }
    }

    var runIds = cdm.consolidateAllArrays(allInstanceRunIds);
    if (typeof runIds == 'undefined') {
      runIds = [];
    }

    console.log('[' + Date.now() + '] GET /api/v1/runs returned ' + runIds.length + ' run(s)');
    res.json({ runIds: runIds });
  } catch (error) {
    console.error('Error in GET /api/v1/runs:', error);
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
    console.log('[' + Date.now() + '] GET /api/v1/run/' + runId + '/tags returned ' + tags.length + ' tag(s)');
    res.json({ tags: tags });
  } catch (error) {
    console.error('Error in GET /api/v1/run/:id/tags:', error);
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
    console.log('[' + Date.now() + '] GET /api/v1/run/' + runId + '/benchmark returned: ' + benchmarkName);
    res.json({ benchmark: benchmarkName });
  } catch (error) {
    console.error('Error in GET /api/v1/run/:id/benchmark:', error);
    res.status(500).json({
      code: 'INTERNAL_ERROR',
      error: 'Failed to get benchmark name: ' + error.message
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
    console.error('Error in GET /api/v1/run/:id/iterations:', error);
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
      '[' + Date.now() + '] POST /api/v1/run/' + runId + '/iterations/params returned params for ' +
      iterations.length + ' iteration(s)'
    );
    res.json({ params: params });
  } catch (error) {
    console.error('Error in POST /api/v1/run/:id/iterations/params:', error);
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
      '[' + Date.now() + '] POST /api/v1/run/' + runId + '/iterations/primary-period-name returned ' +
      periodNames.length + ' name(s)'
    );
    res.json({ periodNames: periodNames });
  } catch (error) {
    console.error('Error in POST /api/v1/run/:id/iterations/primary-period-name:', error);
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
      '[' + Date.now() + '] POST /api/v1/run/' + runId + '/iterations/samples returned samples for ' +
      iterations.length + ' iteration(s)'
    );
    res.json({ samples: samples });
  } catch (error) {
    console.error('Error in POST /api/v1/run/:id/iterations/samples:', error);
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
    console.log('[' + Date.now() + '] POST /api/v1/run/' + runId + '/samples/statuses completed');
    res.json({ statuses: statuses });
  } catch (error) {
    console.error('Error in POST /api/v1/run/:id/samples/statuses:', error);
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
    console.log('[' + Date.now() + '] POST /api/v1/run/' + runId + '/samples/primary-period-id completed');
    res.json({ periodIds: periodIds });
  } catch (error) {
    console.error('Error in POST /api/v1/run/:id/samples/primary-period-id:', error);
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
    console.log('[' + Date.now() + '] POST /api/v1/run/' + runId + '/periods/range completed');
    res.json({ ranges: ranges });
  } catch (error) {
    console.error('Error in POST /api/v1/run/:id/periods/range:', error);
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
      '[' + Date.now() + '] POST /api/v1/run/' + runId + '/iterations/primary-metric returned ' +
      primaryMetrics.length + ' metric(s)'
    );
    res.json({ primaryMetrics: primaryMetrics });
  } catch (error) {
    console.error('Error in POST /api/v1/run/:id/iterations/primary-metric:', error);
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
    console.error('Error in GET /api/v1/run/:id/metric-sources:', error);
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
      '[' + Date.now() + '] POST /api/v1/run/' + runId + '/metric-types returned types for ' +
      sources.length + ' source(s)'
    );
    res.json({ types: types });
  } catch (error) {
    console.error('Error in POST /api/v1/run/:id/metric-types:', error);
    res.status(500).json({
      code: 'INTERNAL_ERROR',
      error: 'Failed to get metric types: ' + error.message
    });
  }
});

// --------------------------------------------------------------------------------------------------------------
// POST /api/v1/metric-data — get metric data (existing endpoint, supports run or period)
// --------------------------------------------------------------------------------------------------------------
app.post('/api/v1/metric-data', async (req, res) => {
  try {
    var { run, period, begin, end, source, type, resolution, breakout, filter, instances: reqInstances } = req.body;

    console.log('[' + Date.now() + '] Fetching metric data with parameters:', {
      run,
      period,
      begin,
      end,
      source,
      type,
      resolution,
      breakout,
      filter,
      instances: reqInstances ? `${reqInstances.length} instance(s) provided` : 'using server instances'
    });

    // Use instances from request if provided, otherwise use server's configured instances
    var instancesToUse = reqInstances && reqInstances.length > 0 ? reqInstances : instances;

    if (!instancesToUse || instancesToUse.length === 0) {
      return res.status(503).json({
        code: 'NO_INSTANCES',
        error: 'No OpenSearch instances configured. Either start server with --host options or provide instances in request.'
      });
    }

    // If instances were provided in the request, we need to call getInstancesInfo on them
    if (reqInstances && reqInstances.length > 0) {
      getInstancesInfo(instancesToUse);
    }

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
      filter: filter
    };
    var resp = await cdm.getMetricDataSets(instance, [set], yearDotMonth);
    if (resp['ret-code'] != 0) {
      return res.status(500).json({
        code: 'METRIC_QUERY_FAILED',
        error: resp['ret-msg']
      });
    }
    metric_data = resp['data-sets'][0];

    console.log(
      '[' +
        Date.now() +
        '] Request completed from Opensearch instance: ' +
        instance['host'] +
        ' and cdm: ' +
        instance['ver'] +
        '\n'
    );

    // Return the data
    res.json(metric_data);
  } catch (error) {
    console.error('Error in /api/v1/metric-data:', error);
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
  console.error('Unhandled error:', error);
  res.status(500).json({
    code: 'INTERNAL_ERROR',
    error: 'Internal server error',
    details: process.env.NODE_ENV === 'development' ? error.message : undefined
  });
});

// Handle 404 for unknown routes
app.use((req, res) => {
  res.status(404).json({
    code: 'ROUTE_NOT_FOUND',
    error: 'Route not found: ' + req.method + ' ' + req.originalUrl
  });
});

// Start server
app.listen(PORT, () => {
  console.log(`CDM Query Server running on port ${PORT}`);
  console.log(`API endpoints: http://localhost:${PORT}/api/v1/`);
  console.log(`Health check: http://localhost:${PORT}/health`);
});

module.exports = app;
