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
//var extsprintf = require('extsprintf');
//console.log(extsprintf.sprintf('hello %25s', 'world'));
var sprintf = require('sprintf-js').sprintf

function list(val) {
  return val.split(',');
}

program
  .version('0.1.0')
  .option('--url <host:port>', 'The host and port of the Elasticsearch instance', 'localhost:9200')
  .option('--run <uuid>', 'The UUID from the run')
  .option('--period <uuid>', 'The UUID from the benchmark-iteration-sample-period')
  .option('--source <name>', 'The metric source, like a tool or benchmark name (sar, fio)')
  .option('--type <name>', 'The metric type, like Gbps or IOPS')
  .option('--begin [uint]', '[optional] Timestamp in epochtime_ms, within the period\'s begin-end time range, where the calculation of the metric will begin')
  .option('--end [uint]', '[optional] Timestamp in epochtime_ms, within the period\'s begin-end time range, where the calculation of the metric will end.  If no --begin and no -end are provided, a begin and end timestamp will be derived based on when all metrics of this source and type have data present.  If --begin is before or --end is after these derived begin/end vaules, they will be adjusted (--begin is increased and/or --end is decreased) to fit within this range.')
  .option('--resolution [uint]', 'The number of datapoints to produce in a data-series', 1)
  .option('--breakout <label1,label2,label3...>', 'List of labels to break-out the metric, like --breakout=host,id with --source=sar -type=ProcessorBusyUtil', list, [])
  .option('--filter <gt|ge|lt|le:value>', 'Filter out (do not output) metrics which do not pass the conditional.  gt=greather-than, ge=greater-than-or-equal, lt=less-than, le=less-than-or-equal')
  .parse(process.argv);

metric_data = cdm.getMetricData(program.url, program.run, program.period, program.source, program.type,
                                program.begin, program.end, program.resolution, program.breakout, program.filter);

if (Object.keys(metric_data.values).length == 0) {
    console.log("There were no metrics found, exiting");
    process.exit(1);
}

var dateFormat = "default";
var decimalPlaces = 2;
console.log("Available breakouts:  " + metric_data.remainingBreakouts + "\n");
var dataColumnLengths = [];
var labelColumnLengths = [];
var dataStartRow = 3; // rows 0-2 are used for labels (timestamps)
var row = dataStartRow;
var vals = [];
vals[0] = [];
vals[1] = [];
vals[2] = [];
var labels = [];
labels[0] = [];
labels[1] = [];
labels[2] = [];
vals[0] = [];
labels[0][0] = "";
Object.keys(metric_data.values).sort((a, b) => {
  return a.localeCompare(b, undefined, {
    numeric: true,
    sensitivity: 'base'
  })
}).forEach(key =>{
    labels[row] = [];
    labels[row][0] = program.type;
    var subKeys = key.split("-");
    if (subKeys.length == 1 && subKeys[0]  == "") {
        subKeys = [];
    }
    var col = 1;
    if (row == dataStartRow) {
        labels[0][0] = "";
        labels[1][0] = "";
        labels[2][0] = "";
        metric_data.usedBreakouts.forEach(subMetric => {
            labels[0][col] = "";
            labels[1][col] = subMetric;
            labels[2][col] = "";
            col++;
        });
    }
    var col = 1;
    subKeys.forEach(subKey => {
        labels[row][col] = subKey.replace(/<(\w+)>/, "$1");
        col++;
    });
    var values_string = "";
    vals[row] = [];
    col = 0;
    metric_data.values[key].forEach(element =>{
        if (row == dataStartRow) {
            var date = new Date(element.end);
            if (dateFormat == "epoch_ms") {
                vals[0][col] = Math.trunc(element.end / 1000000000000) % 1000000;
                vals[1][col] = Math.trunc(element.end / 1000000) % 1000000;
                vals[2][col] = sprintf("%03d", element.end % 1000000);
            } else {
                vals[0][col] = sprintf("%02d", date.getUTCDate()) + "-" +
                               sprintf("%02d", date.getUTCMonth()) + "-" +
                               sprintf("%04d", date.getUTCFullYear());
                vals[1][col] = sprintf("%02d", date.getUTCHours()) + ":" +
                               sprintf("%02d", date.getUTCMinutes()) + ":" +
                               sprintf("%02d", date.getUTCSeconds());
                vals[3][col] = "";
            }
        }
        vals[row][col] = element.value.toFixed(decimalPlaces);
        col++;
    });
    row++;
})

// Adjust column widths according to longest string per column
for (row=0; row<vals.length; row++) {
    for (col=0; col<vals[row].length; col++) {
        var length = vals[row][col].length;
        if (dataColumnLengths[col] == null || dataColumnLengths[col] < length) {
            dataColumnLengths[col] = length;
        }
        
    }
}
for (row=0; row<labels.length; row++) {
    for (col=0; col<labels[row].length; col++) {
        var length = labels[row][col].length;
        if (labelColumnLengths[col] == null || labelColumnLengths[col] < length) {
            labelColumnLengths[col] = length;
        }
        
    }
}
//console.log(labels);

for (row=0; row<vals.length; row++) {
    //console.log("row is " + row);
    line = "";

    // construct the labels for the row
    for (col=0; col<labels[row].length; col++) {
        line = line + sprintf(" %" + labelColumnLengths[col] + "s ", labels[row][col]);
    }

    // construct the values for the row
    for (col=0; col<vals[row].length; col++) {
        if (row >= dataStartRow) {
            line = line + sprintf(" %" + dataColumnLengths[col] + "." + decimalPlaces + "f ", vals[row][col]);
        } else {
            line = line + sprintf(" %" + dataColumnLengths[col] + "s ", vals[row][col]);
        }
    }

    console.log(line);
}

//console.log(JSON.stringify(metric_data, null, 2));
