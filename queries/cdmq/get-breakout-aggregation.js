var cdm = require('./cdm');
var program = require('commander');

program
  .version('0.1.0')
  .option('--source <metric source>')
  .option('--type  <metric type>')
  .option('--breakout <label1,label2,labelN>')
  .parse(process.argv);

var source = "fio";
var type = "iops";
var breakouts = [];
breakouts[0] = "host";
breakouts[1] = "job";
console.log(cdm.getBreakoutAggregation(source, type, breakouts));
