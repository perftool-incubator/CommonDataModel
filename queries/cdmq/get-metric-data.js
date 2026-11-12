// With a list of 1 or more labels in --breakout, output 1 or more
// metric groups, each group consisting of 1 or more metric IDs.
//
// To find valid labels, first run get-name-format with the same
// --period, --source, and --type options:
//
// #node ./get-name-format.js --url $eshost:9200 --period $period --source=fio --type=iops
// %host%-%job%-%action%
//
//# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript

var cdm = require('./cdm');
var program = require('commander');
const { transformMetricData } = require('./metric-results');
const { renderMetricData } = require('./metric-result-output');
const http = require('http');
const https = require('https');

function list(val) {
  // Commas separate breakout fields. Multiple values for a field use '+':
  //   field1,field2=value1+value2,field3
  return val.split(',');
}

/**
 * Make an HTTP POST request to the metric data API
 */
async function fetchMetricData(serverUrl, params) {
  return new Promise((resolve, reject) => {
    const url = new URL(serverUrl);
    const isHttps = url.protocol === 'https:';
    const client = isHttps ? https : http;

    const postData = JSON.stringify(params);

    const options = {
      hostname: url.hostname,
      port: url.port || (isHttps ? 443 : 80),
      path: url.pathname,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(postData)
      }
    };

    const req = client.request(options, (res) => {
      let data = '';

      res.on('data', (chunk) => {
        data += chunk;
      });

      res.on('end', () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          try {
            resolve(JSON.parse(data));
          } catch (e) {
            reject(new Error(`Failed to parse response: ${e.message}`));
          }
        } else {
          try {
            const errorData = JSON.parse(data);
            reject(new Error(errorData.error || `HTTP ${res.statusCode}: ${data}`));
          } catch (e) {
            reject(new Error(`HTTP ${res.statusCode}: ${data}`));
          }
        }
      });
    });

    req.on('error', (error) => {
      reject(new Error(`Failed to connect to server at ${serverUrl}: ${error.message}`));
    });

    req.write(postData);
    req.end();
  });
}

function list_hosts(val) {
  // Parse comma-separated host values
  return val.split(',');
}

function save_host(host) {
  if (!program.instances) {
    program.instances = [];
  }
  var host_info = { host: host, header: { 'Content-Type': 'application/json' } };
  program.instances.push(host_info);
}

function save_userpass(userpass) {
  if (!program.instances || program.instances.length == 0) {
    console.log('You must specify a --host before a --userpass');
    process.exit(1);
  }
  program.instances[program.instances.length - 1]['header'] = {
    'Content-Type': 'application/json',
    Authorization: 'Basic ' + btoa(userpass)
  };
}

function save_ver(ver) {
  if (!program.instances || program.instances.length == 0) {
    console.log('You must specify a --host before a --ver');
    process.exit(1);
  }
  if (cdm.isValidCdmVersion(ver)) {
    program.instances[program.instances.length - 1]['ver'] = ver;
  } else {
    console.log('The version must be one of: ' + cdm.supportedCdmVersions.join(', ') + ', not: ' + ver);
    process.exit(1);
  }
}

async function main() {
  program.instances = [];

  program
    .version('0.1.0')
    .option(
      '--server-url <url>',
      'The URL of the metric data API server (e.g., http://localhost:3000/api/v1/metric-data)',
      'http://localhost:3000/api/v1/metric-data'
    )
    .option(
      '--host <host[:port]>',
      'The host and optional port of the OpenSearch instance (passed to server)',
      save_host
    )
    .option(
      '--userpass <user:pass>',
      'The user and password for the most recent --host (passed to server)',
      save_userpass
    )
    .option(
      cdm.cdmVersionOptionDesc(),
      'The Common Data Model version to use for the most recent --host (passed to server)',
      save_ver
    )
    .option('--run <uuid>', 'The UUID from the run')
    .option('--period <uuid>', 'The UUID from the benchmark-iteration-sample-period')
    .option('--source <name>', 'The metric source, like a tool or benchmark name (sar, fio)')
    .option('--type <name>', 'The metric type, like Gbps or IOPS')
    .option(
      '--begin [uint]',
      "[optional] Timestamp in epochtime_ms, within the period's begin-end time range, where the calculation of the metric will begin"
    )
    .option(
      '--end [uint]',
      "[optional] Timestamp in epochtime_ms, within the period's begin-end time range, where the calculation of the metric will end.  If no --begin and no -end are provided, a begin and end timestamp will be derived based on when all metrics of this source and type have data present.  If --begin is before or --end is after these derived begin/end values, they will be adjusted (--begin is increased and/or --end is decreased) to fit within this range."
    )
    .option('--resolution [uint]', '[optional] The number of datapoints to produce in a data-series', 1)
    .option(
      '--breakout <label1,label2,label3...>',
      '[optional] List of labels to break-out the metric, like --breakout=host,id with --source=sar -type=ProcessorBusyUtil',
      list,
      []
    )
    .option(
      '--filter <gt|ge|lt|le:value>',
      '[optional] Filter out (do not output) metrics which do not pass the conditional.  gt=greater-than, ge=greater-than-or-equal, lt=less-than, le=less-than-or-equal'
    )
    .option('--aggregation <sum|avg|max|min>', '[optional] Override the default aggregation method for this query')
    .option(
      '--distribution-stats <stat1,stat2,...>',
      '[optional] Return duration-weighted native-timeline statistics (min,max,mean,median,stddev,pNN)',
      (value) =>
        value
          .split(',')
          .map((stat) => stat.trim())
          .filter(Boolean)
    )
    .option(
      '--allow-incompatible-aggregation',
      '[optional] Allow an aggregation explicitly disallowed by the metric definition'
    )
    .option('--sort <label|value>', '[optional] Sort by label or first-window value', 'label')
    .option('--sort-order <asc|desc>', '[optional] Sort direction', 'asc')
    .option('--top <uint>', '[optional] Keep at most N rows after sorting and filtering', Number)
    .option('--percent-of-total', '[optional] Add each value as a percentage of all metrics in its time window')
    .option(
      '--percent-decimal-places [uint]',
      '[optional] How many digits you want to the right of the decimal for percent-of-total columns',
      2
    )
    .option('--output-format <json|table|csv>', '[optional] Client-side output format', 'table')
    .option(
      '--date-format <default|epoch_ms>',
      '[optional] output date/time in DD-MM-YYYY HH:MM:SS (the default) or epoch time in milliseconds',
      'default'
    )
    .option(
      '--decimal-places [uint]',
      '[optional] How many digits you want to the right of the decimal for metric values',
      2
    )
    .option(
      '--output-content <all|values|headers>',
      '[optional] Output the entire table, just the headers, or just the values',
      'all'
    )
    .option(
      '--horizontal-break <yes|no>',
      '[optional] Add a horizontal break between the headers and the values',
      'yes'
    )
    .option(
      '--timestamp-rows <1-2>',
      'Use one or two rows to display timestamp.  When using two rows with UTC date & time, date will be on the first row and time on the second row',
      2
    )
    .parse(process.argv);

  if (program.run == null && program.period == null) {
    console.log('Exiting because neither a period nor a run ID were provided');
    process.exit(1);
  }

  // Request the full query scope. Presentation options, including the output-only
  // value filter, are applied locally so totals include rows hidden from output.
  // The API's existing filter field remains supported for direct callers.
  // Prepare the request parameters for the API
  const apiParams = {
    run: program.run,
    period: program.period,
    source: program.source,
    type: program.type,
    begin: program.begin,
    end: program.end,
    resolution: program.resolution,
    breakout: program.breakout, // Send as array to preserve complex breakout syntax
    aggregation: program.aggregation,
    'distribution-stats': program.distributionStats,
    'allow-incompatible-aggregation': program.allowIncompatibleAggregation,
    instances: program.instances.length > 0 ? program.instances : undefined
  };

  // Fetch metric data from the API
  let metric_data;
  try {
    metric_data = await fetchMetricData(program.serverUrl, apiParams);
  } catch (error) {
    console.log('Error fetching metric data: ' + error.message);
    process.exit(1);
  }

  if (Object.keys(metric_data.values).length == 0) {
    console.log('There were no metrics found, exiting');
    process.exit(1);
  }

  let output;
  try {
    const result = transformMetricData(metric_data, {
      sort: program.sort,
      sortOrder: program.sortOrder,
      top: program.top,
      percentOfTotal: Boolean(program.percentOfTotal),
      filter: program.filter
    });
    output = renderMetricData(result, {
      outputFormat: program.outputFormat,
      source: program.source,
      type: program.type,
      dateFormat: program.dateFormat,
      decimalPlaces: program.decimalPlaces,
      percentDecimalPlaces: program.percentDecimalPlaces,
      outputContent: program.outputContent,
      horizontalBreak: program.horizontalBreak,
      timestampRows: program.timestampRows
    });
  } catch (error) {
    console.log('Error transforming metric data: ' + error.message);
    process.exit(1);
  }

  console.log('\nMetric data retrieved from API server: ' + program.serverUrl + '\n');
  if (program.outputFormat !== 'json') {
    console.log('Available breakouts:  ' + metric_data.remainingBreakouts + '\n');
  }
  if (output) console.log(output);
}

main();
