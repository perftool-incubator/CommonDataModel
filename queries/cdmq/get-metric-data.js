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

var program = require('commander');
var sprintf = require('sprintf-js').sprintf;
const http = require('http');
const https = require('https');

function list(val) {
  // Parse breakout string to handle both:
  // - Simple breakouts: "hostname,cpu" -> ["hostname", "cpu"]
  // - Breakouts with values: "hostname=a,cpu=x" -> ["hostname=a", "cpu=x"]
  // - Breakouts with multiple values: "hostname=a,b,cpu=x,y" -> ["hostname=a,b", "cpu=x,y"]
  //
  // The key insight: a comma separates breakout fields UNLESS we're currently
  // parsing a value list (after '=' and before the next field with '=')

  var result = [];
  var current = '';
  var inValueList = false;
  var parts = val.split(',');

  for (var i = 0; i < parts.length; i++) {
    var part = parts[i];
    var hasEqual = part.includes('=');

    if (inValueList && !hasEqual) {
      // We're in a value list and this part doesn't have '=', so it's another value
      current += ',' + part;
    } else if (inValueList && hasEqual) {
      // We were in a value list, but now we hit a new key=value pair
      result.push(current);
      current = part;
      inValueList = true;
    } else if (!inValueList && hasEqual) {
      // Starting a new key=value pair
      if (current !== '') {
        result.push(current);
      }
      current = part;
      inValueList = true;
    } else {
      // !inValueList && !hasEqual - simple breakout field without value filter
      if (current !== '') {
        result.push(current);
      }
      current = part;
      inValueList = false;
    }
  }

  if (current !== '') {
    result.push(current);
  }

  return result;
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

async function main() {
  program
    .version('0.1.0')
    .option(
      '--server-url <url>',
      'The URL of the metric data API server (e.g., http://localhost:3000/api/metric-data)',
      'http://localhost:3000/api/metric-data'
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
      "[optional] Timestamp in epochtime_ms, within the period's begin-end time range, where the calculation of the metric will end.  If no --begin and no -end are provided, a begin and end timestamp will be derived based on when all metrics of this source and type have data present.  If --begin is before or --end is after these derived begin/end vaules, they will be adjusted (--begin is increased and/or --end is decreased) to fit within this range."
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
      '[optional] Filter out (do not output) metrics which do not pass the conditional.  gt=greather-than, ge=greater-than-or-equal, lt=less-than, le=less-than-or-equal'
    )
    .option('--output-format <json|table|csv>', 'table')
    .option(
      '--date-format <default|eopch_ms>',
      '[optional] otuput date/time in DD-MM-YYYY HH:MM:SS (the default) or epoch time in milliseconds',
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
    filter: program.filter
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

  console.log('\nMetric data retrieved from API server: ' + program.serverUrl + '\n');

  if (program.outputFormat == 'json') {
    console.log(JSON.stringify(metric_data, null, 2));
    process.exit(0);
  }

  // Rest of the code is for non-JSON output formats
  console.log('Available breakouts:  ' + metric_data.remainingBreakouts + '\n');
  var dataColumnLengths = [];
  var labelColumnLengths = [];
  var dataStartRow;
  if (program.dateFormat == 'epoch_ms') {
    dataStartRow = 1;
    program.timestampRows = 1;
  } else {
    dataStartRow = parseInt(program.timestampRows); // rows 0-[1|2] are used for labels (timestamps)
  }
  var labelStopRow = dataStartRow - 1;
  var row = dataStartRow;
  var vals = [];
  vals[0] = [];
  var labels = [];
  beginMarker = ' ';
  endMarker = '';

  Object.keys(metric_data.values)
    .sort((a, b) => {
      return a.localeCompare(b, undefined, {
        numeric: true,
        sensitivity: 'base'
      });
    })
    .forEach((key) => {
      labels[row] = [];
      labels[row][0] = program.source;
      labels[row][1] = program.type;
      var subKeys = key.replace(/^</, '').replace(/>$/, '').split('>-<'); // key is the string with breakouts, for example,  "client-2-10" for <cstype>-<csid>-<num> for source: mpstat type: Busy-CPU
      if (subKeys.length == 1 && subKeys[0] == '') {
        subKeys = [];
      }
      var col = 2; // colDataStart
      if (row == dataStartRow) {
        // populate the header rows now
        // first two columns are metric source and type
        labels[0] = [];
        if (program.timestampRows == 2) {
          labels[1] = [];
          labels[0][0] = '';
          labels[0][1] = '';
          labels[1][0] = 'source';
          labels[1][1] = 'type';
          metric_data.usedBreakouts.forEach((subMetric) => {
            labels[0][col] = '';
            labels[1][col] = subMetric;
            col++;
          });
        } else {
          labels[0][0] = 'source';
          labels[0][1] = 'type';
          metric_data.usedBreakouts.forEach((subMetric) => {
            labels[0][col] = subMetric;
            col++;
          });
        }
      }
      // populate the label array with subMetrics
      labels[row] = [];
      labels[row][0] = program.source;
      labels[row][1] = program.type;
      var col = 2;
      subKeys.forEach((subKey) => {
        var subMetric = subKey.replace(/<(\w+)>/, '$1');
        i;
        labels[row][col] = subMetric;
        col++;
      });
      var values_string = '';
      vals[row] = [];
      col = 0;
      metric_data.values[key].forEach((element) => {
        if (row == dataStartRow) {
          if (col == 0) {
            vals[0] = [];
          }
          var date = new Date(element.end);
          if (program.dateFormat == 'epoch_ms') {
            vals[0][col] = sprintf('%d', element.end);
          } else if (program.timestampRows == 2) {
            if (col == 0) {
              vals[1] = [];
            }
            vals[0][col] =
              sprintf('%02d', date.getUTCDate()) +
              '-' +
              sprintf('%02d', date.getUTCMonth() + 1) +
              '-' +
              sprintf('%04d', date.getUTCFullYear());
            vals[1][col] =
              sprintf('%02d', date.getUTCHours()) +
              ':' +
              sprintf('%02d', date.getUTCMinutes()) +
              ':' +
              sprintf('%02d', date.getUTCSeconds());
          } else {
            vals[0][col] =
              sprintf('%02d', date.getUTCDate()) +
              '-' +
              sprintf('%02d', date.getUTCMonth() + 1) +
              '-' +
              sprintf('%04d', date.getUTCFullYear()) +
              '/' +
              sprintf('%02d', date.getUTCHours()) +
              ':' +
              sprintf('%02d', date.getUTCMinutes()) +
              ':' +
              sprintf('%02d', date.getUTCSeconds());
          }
        }
        vals[row][col] = element.value.toFixed(program.decimalPlaces);
        col++;
      });
      row++;
    });

  // Adjust column widths according to longest string per column
  // (this should become a function)
  for (row = 0; row < vals.length; row++) {
    for (col = 0; col < vals[row].length; col++) {
      var length = vals[row][col].length;
      if (dataColumnLengths[col] == null || dataColumnLengths[col] < length) {
        dataColumnLengths[col] = length;
      }
    }
  }
  for (row = 0; row < labels.length; row++) {
    for (col = 0; col < labels[row].length; col++) {
      var length = labels[row][col].length;
      if (labelColumnLengths[col] == null || labelColumnLengths[col] < length) {
        labelColumnLengths[col] = length;
      }
    }
  }

  rowStart = 0;
  rowEnd = vals.length;
  if (program.outputContent == 'values') {
    // skip past the headers
    rowStart = dataStartRow;
  } else if (program.outputContent == 'headers') {
    // stop early to ensure we don't print the values
    rowEnd = dataStartRow;
  }

  for (row = rowStart; row < rowEnd; row++) {
    // add a horizontal break
    if (program.horizontalBreak == 'yes' && row == dataStartRow) {
      line = '';
      // construct the labels (left columns) for the row
      for (col = 0; col < labels[row].length; col++) {
        for (letter = 0; letter < labelColumnLengths[col] + 1; letter++) {
          line = line + sprintf('-');
        }
      }

      // construct the values for the row
      for (col = 0; col < vals[row].length; col++) {
        for (letter = 0; letter < dataColumnLengths[col] + 1; letter++) {
          line = line + sprintf('-');
        }
      }
      console.log(line);
    }

    line = '';
    // construct the labels (left columns) for the row
    for (col = 0; col < labels[row].length; col++) {
      line = line + sprintf(beginMarker + '%' + labelColumnLengths[col] + 's' + endMarker, labels[row][col]);
    }

    // construct the values for the row
    for (col = 0; col < vals[row].length; col++) {
      if (row >= dataStartRow) {
        line =
          line +
          sprintf(
            beginMarker + '%' + dataColumnLengths[col] + '.' + program.decimalPlaces + 'f' + endMarker,
            vals[row][col]
          );
      } else {
        line = line + sprintf(beginMarker + '%' + dataColumnLengths[col] + 's' + endMarker, vals[row][col]);
      }
    }
    console.log(line);
  }
}

main();
