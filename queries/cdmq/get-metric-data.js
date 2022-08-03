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
  .option('--output <json|table|amchart>', 'Output format.  json = json file, table = text table, amchart = html with amchart graphics.  Output format html requires the output-dir option.')
  .option('--output-dir <path>', 'Output directory.  If specified, output is written to a file metric-data.[json|txt|html+js].')
  .parse(process.argv);

metric_data = cdm.getMetricData(program.url, program.run, program.period, program.source, program.type,
                                program.begin, program.end, program.resolution, program.breakout, program.filter);

if (Object.keys(metric_data.values).length == 0) {
    console.log("There were no metrics found, exiting");
    process.exit(1);
}

const fs = require('fs');
if (program.output == "json") {
  if (program.outputDir != null) {
    try {
      fs.writeFileSync(program.outputDir + "/" + 'metric-data.json',
                       console.log(JSON.stringify(metric_data, null, 2)));
    } catch (err) {
      console.error(err);
    }
  } else {
    console.log(JSON.stringify(metric_data, null, 2));
  }
}
if (program.output == "table") {
  // convert json to table
  var table_txt = "";
}
if (program.output == "amchart") {
  var data = {};
  var js = "";
  var html_resources =
        '<!-- Resources -->\n' +
        '<script src="https://cdn.amcharts.com/lib/5/index.js"></script>\n' +
        '<script src="https://cdn.amcharts.com/lib/5/xy.js"></script>\n' +
        '<script src="https://cdn.amcharts.com/lib/5/themes/Animated.js"></script>\n' +
        '<script src="data.js"></script>\n' +
        '<script src="chart.js"></script>\n';

  var html_styles =
        '<!-- Styles -->\n' + 
        '<style>\n';

  var html_div = '';
  html_div += '<div id="metric-data"></div>\n';
  html_styles += '#metric-data {\n' +
                   '  width: 1000px;\n' +
                   '  height: 1000px;\n' +
                   '}\n';
  html_styles += '</style>\n';

  var html = html_styles + html_resources + html_div;
  try {
    fs.writeFileSync(program.outputDir + "/" + 'data.js', 'var data = ' + JSON.stringify(data, null, 2));
  } catch (err) {
    console.error(err);
  }
  try {
    fs.writeFileSync(program.outputDir + "/" + 'result-summary.html', html);
  } catch (err) {
    console.error(err);
  }
  try {
    fs.copyFileSync("chart.js", program.outputDir + "/" + 'chart.js');
  } catch (err) {
    console.log(err);
  }
}
