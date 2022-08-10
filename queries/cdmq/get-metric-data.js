//# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript
// With a list of 1 or more labels in --breakout, output 1 or more
// metric groups, each group consisting of 1 or more metric IDs.
//
// To find valid labels, first run get-name-format with the same 
// --period, --source, and --type options:
//
// #node ./get-name-format.js --url $eshost:9200 --period $period --source=fio --type=iops
// %host%-%job%-%action%
//

var cdm = require('./cdm');
var program = require('commander');

function list(val, prev) {
  return val.split(',');
}


var currentTimePeriod = 0;
var timePeriods = [];
var currentSourceType = 0;
var sourceTypes = [];
var firstRunOrPeriodUsed = false;
var firstSourceUsed = false;

function saveTimePeriodArgs() {
  // Save all the args for the current time period
  var periodArgs = ['begin', 'end'];
  periodArgs.forEach(arg =>{
    if (program[arg] != null) {
      timePeriods[currentTimePeriod][arg] = program[arg];
      console.log("timePeriods[" + currentTimePeriod + "][" + arg + "] = " + program[arg]);
      program[arg] = null;
    }
  });
}

function saveSourceTypeArgs() {
  console.log("\nsaveSourceTypeArgs()");
  // Save all the args for the current sourceType
  var args = ['breakout', 'type', 'filter'];
  console.log("  sourceTypes before saving args:\n" + JSON.stringify(sourceTypes, null, 2));
  args.forEach(arg =>{
    if (program[arg] != null) {
      console.log("  assigning sourceTypes[" + currentSourceType + "][" + arg + "] = " + program[arg]);
      sourceTypes[currentSourceType][arg] = program[arg];
      program[arg] = null;
    }
  });
}

function saveSourceTypes() {
  // Save all the sourceTypes for the current time period
  timePeriods[currentTimePeriod]['sourceTypes'] = [];
  for (var i=0; i<sourceTypes.length; i++) {
    timePeriods[currentTimePeriod]['sourceTypes'].push(sourceTypes[i]);
  }
  sourceTypes = [];
  currentSourceType = 0;
}

function startNewTimePeriod(val, prev, runOrPeriod) {
  console.log("\nstartTimePeriod(" + val + ", " + prev + ", " + runOrPeriod + ")");
  if (!firstRunOrPeriodUsed) {
    console.log("  First invocation of --run or --period");
    console.log("  Not adding new time period");
    timePeriods[currentTimePeriod] = {};
    timePeriods[currentTimePeriod][runOrPeriod] = val;
    firstRunOrPeriodUsed = true;
    console.log("  timePeriods:\n" + JSON.stringify(timePeriods, null, 2));
    return val;
  }

  saveTimePeriodArgs();
  saveSourceTypeArgs();
  saveSourceTypes();

  // Advance the time period so new args end up going to it
  console.log("Adding new time period");
  currentTimePeriod++;
  timePeriods[currentTimePeriod] = {};
  timePeriods[currentTimePeriod][runOrPeriod] = val;
  firstSourceUsed = false;
  console.log("timePeriods:\n" + JSON.stringify(timePeriods, null, 2));
  return val;
}

function startNewSourceType(val, prev) {
  console.log("\nstartNewSourceType(" + val + ", " + prev + ")");
  if (!firstSourceUsed) {
    console.log("  This is the first invocation of --source");
    console.log("  Not adding new sourceType");
    sourceTypes[currentSourceType] = {};
    sourceTypes[currentSourceType].source = val;
    firstSourceUsed = true;
    console.log("sourceTypes:\n" + JSON.stringify(sourceTypes, null, 2));
    return val;
  }
  saveSourceTypeArgs();
  sourceTypes.push({source: val});
  currentSourceType++;

  console.log("sourceTypes:\n" + JSON.stringify(sourceTypes, null, 2));
  return val;
}

function startNewTimePeriodViaPeriod(val, prev) {
  console.log("\nstartTimePeriodViaPeriod(" + val + ", " + prev + ")");
  return startNewTimePeriod(val, prev, "period");
}

function startNewTimePeriodViaRun(val, prev) {
  console.log("\nstartTimePeriodViaRun(" + val + ", " + prev + ")");
  return startNewTimePeriod(val, prev, "run");
}

program
  .version('0.1.0')
  .option('--url <host:port>', 'The host and port of the Elasticsearch instance', 'localhost:9200')
  .option('--run <uuid>', 'The UUID from the run', startNewTimePeriodViaRun, )
  .option('--period <uuid>', 'The UUID from the benchmark-iteration-sample-period', startNewTimePeriodViaPeriod, )
  .option('--source <name>', 'The metric source, like a tool or benchmark name (sar, fio)', startNewSourceType, )
  .option('--type <name>', 'The metric type, like Gbps or IOPS')
  .option('--begin [uint]', '[optional] Timestamp in epochtime_ms, within the period\'s begin-end time range, where the calculation of the metric will begin')
  .option('--end [uint]', '[optional] Timestamp in epochtime_ms, within the period\'s begin-end time range, where the calculation of the metric will end.  If no --begin and no -end are provided, a begin and end timestamp will be derived based on when all metrics of this source and type have data present.  If --begin is before or --end is after these derived begin/end vaules, they will be adjusted (--begin is increased and/or --end is decreased) to fit within this range.')
  .option('--resolution [uint]', 'The number of datapoints to produce in a data-series', 1)
  .option('--breakout <label1,label2,label3...>', 'List of labels to break-out the metric, like --breakout=host,id with --source=sar -type=ProcessorBusyUtil', list, [])
  .option('--filter <gt|ge|lt|le:value>', 'Filter out (do not output) metrics which do not pass the conditional.  gt=greather-than, ge=greater-than-or-equal, lt=less-than, le=less-than-or-equal')
  .option('--output-format <json|table|amchart>', 'Output format.  json = json file, table = text table, amchart = html with amchart graphics.  Output format html requires the output-dir option.', list, [])
  .option('--output-dir <path>', 'Output directory.  If specified, output is written to a file metric-data.[json|txt|html+js].')
  .parse(process.argv);


saveTimePeriodArgs();
saveSourceTypeArgs();
saveSourceTypes();

console.log("timePeriods:\n" + JSON.stringify(timePeriods, null, 2));

// Convert the time periods to 'sets' for getMetricDataSets
var sets = [];
var count = 0;
for (var i=0; i<timePeriods.length; i++) {
  console.log("i: " + i);
  for (var j=0; j<timePeriods[i].sourceTypes.length; j++) {
    sets[count] = {};
    console.log("j: " + j);
    var timePeriodKeys = ['run', 'period', 'begin', 'end'];
    timePeriodKeys.forEach(thisKey =>{
      if (timePeriods[i][thisKey] != null) {
        sets[count][thisKey] = timePeriods[i][thisKey];
      }
    });
    var thisSet = {};
    var sourceTypeKeys = ['source', 'type', 'breakout'];
    console.log("timePeriods[" + i + "].sourceTypes[" + j + "]:\n" + JSON.stringify(timePeriods[i].sourceTypes[j], null, 2));
    sourceTypeKeys.forEach(thisKey =>{
      console.log("thisKey: " + thisKey);
      if (timePeriods[i].sourceTypes[j][thisKey] != null) {
        sets[count][thisKey] = timePeriods[i].sourceTypes[j][thisKey];
      } else if (thisKey == "breakout") {
        sets[count]['breakout'] = [];
      }
    });
    count++;
  }
}
        
console.log("sets:\n" + JSON.stringify(sets, null, 2));

metric_data = cdm.getMetricDataSets(program.url, sets);
//metric_data = cdm.getMetricData(program.url, program.run, program.period, program.source, program.type,
                                //program.begin, program.end, program.resolution, program.breakout, program.filter);

if (Object.keys(metric_data.values).length == 0) {
    console.log("There were no metrics found, exiting");
    process.exit(1);
}

const fs = require('fs');
if (program.outputFormat.includes("json")) {
  if (program.outputDir != null) {
    console.log("writing JSON file");
    try {
      fs.writeFileSync(program.outputDir + "/" + 'metric-data.json',
                       JSON.stringify(metric_data, null, 2));
    } catch (err) {
      console.error(err);
    }
  } else {
    console.log(JSON.stringify(metric_data, null, 2));
  }
}
if (program.outputFormat.includes("table")) {
  // convert json to table
  var table_txt = "";
}
if (program.outputFormat.includes("amchart")) {
  var title = program.source + "::" + program.type;
  var data = {};
  var js = "";
  var html_resources =
        '<!-- Resources -->\n' +
        '<script src="https://cdn.amcharts.com/lib/5/index.js"></script>\n' +
        '<script src="https://cdn.amcharts.com/lib/5/xy.js"></script>\n' +
        '<script src="https://cdn.amcharts.com/lib/5/themes/Animated.js"></script>\n' +
        '<script src="graph-data.js"></script>\n' +
        '<script src="graph.js"></script>\n';

  var html_styles =
        '<!-- Styles -->\n' + 
        '<style>\n';

  var html_div = '';
  html_div += '<div id="' + title + '"></div>\n';
  html_styles += '#' + title + ' {\n' +
                   '  width: 1000px;\n' +
                   '  height: 500px;\n' +
                   '}\n';
  html_styles += '</style>\n';


  // write a amchart compatible data struct
  Object.keys(metric_data.values).forEach(thisDataSeries =>{
    var newDataSeries = [];
    var prevEnd;
    metric_data.values[thisDataSeries].forEach(thisDataSample => {
      if (prevEnd != null) {
        newDataSeries.push({ "date": (prevEnd + 1), "value": thisDataSample['value'] });
      }
      newDataSeries.push({ "date": thisDataSample['end'], "value": thisDataSample['value'] });
      prevEnd = thisDataSample['end'];
    });
    data[thisDataSeries] = newDataSeries;
  });

  var graph_data = { "title": title, "data": data };
  var html = html_styles + html_resources + html_div;
  try {
    fs.writeFileSync(program.outputDir + "/" + 'graph-data.js', 'var graph_data = ' + JSON.stringify(graph_data, null, 2));
  } catch (err) {
    console.error(err);
  }
  try {
    fs.writeFileSync(program.outputDir + "/" + 'metric-data.html', html);
  } catch (err) {
    console.error(err);
  }
  try {
    fs.copyFileSync("graph.js", program.outputDir + "/" + 'graph.js');
  } catch (err) {
    console.log(err);
  }
}
