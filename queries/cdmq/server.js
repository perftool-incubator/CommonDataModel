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

getInstancesInfo(instances);
console.log("instances:\n" + JSON.stringify(instances, null, 2));

// Middleware
app.use(cors());
app.use(express.json());


// API endpoint to get metric data
app.post('/api/metric-data', async (req, res) => {
  try {
    var { run, period, begin, end, source, type, resolution, breakout, filter } = req.body;

    console.log(`Fetching metric data with parameters:`, {
      run,
      period,
      begin,
      end,
      source,
      type,
      resolution,
      breakout,
      filter
    });

    getInstancesInfo(instances);
    var yearDotMonth;
    var instance;
    if (run != null) {
      instance = await findInstanceFromRun(instances, run);
      if (instance == null) {
        errMsg = 'Could not find run ID ' + period + ' in any of the Opensearch instances:\n' + JSON.stringify(instances, null, 2);
        console.error(errMsg);
        return res.status(500).json({ error: errMsg });
      }
    } else if (period != null) {
      instance = await findInstanceFromPeriod(instances, period);
      if (instance == null) {
        errMsg = 'Could not find period ID ' + period + ' in any of the Opensearch instances:\n' + JSON.stringify(instances, null, 2);
        console.error(errMsg);
        return res.status(500).json({ error: errMsg });
      }
      // We don't yet know the yearDotMonth, so use wildcard to query all period indices
      run = await getRunFromPeriod(instance, period, '@*');
    } else {
      errMsg = 'Neither a period nor a run ID were provided';
      console.error(errMsg);
      return res.status(500).json({ error: errMsg });
    }
    var yearDotMonth = await findYearDotMonthFromRun(instance, run);

    // getMetricDataSets expects breakout to be an array
    console.log("breakout: " + typeof breakout);
    if (typeof breakout != 'string') {
        breakout = [];
    } else {
        breakout = breakout.split(",");
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
      errMsg = resp['ret-msg'];
      console.error(errMsg);
      return res.status(500).json({ error: errMsg });
    }
    metric_data = resp['data-sets'][0];

    console.log('\nFrom Opensearch instance: ' + instance['host'] + ' and cdm: ' + instance['ver'] + '\n');
    console.log(JSON.stringify(resp, null, 2));

    // Return the data
    res.json(metric_data);
  } catch (error) {
    console.error('Error in /api/metric-data:', error);
    res.status(500).json({
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
    error: 'Internal server error',
    details: process.env.NODE_ENV === 'development' ? error.message : undefined
  });
});

// Handle 404 for unknown routes
  app.use((req, res) => {
  res.status(404).json({
    error: 'Route not found'
  });
});

// Start server
app.listen(PORT, () => {
  console.log(`🚀 Metric Data Server running on port ${PORT}`);
  console.log(`📊 API endpoint: http://localhost:${PORT}/api/metric-data`);
  console.log(`💚 Health check: http://localhost:${PORT}/health`);
});

module.exports = app;




