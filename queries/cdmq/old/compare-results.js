//# vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript

var cdm = require('./cdm');
var program = require('commander');

function list(val) {
  return val.split(',');
}

program
  .version('0.1.0')
  .option(
    '--filter-by-age <N:M>, newest-day:oldest-day',
    'Limit search for results that are between N and M days old (default is 0 to 30)',
    '0-30'
  )
  .option(
    '--filter-by-tags <tags>, tag1:value1[,tagN:valueN]',
    'Only include benchmark-iterations which match a tag <key>:<value> pair(s)',
    list,
    []
  )
  .option(
    '--filter-by-params <params>, param1:value1[,paramN:valueN>',
    'Only include benchmark-iterations which match a param <key>:<value> pair(s)',
    list,
    []
  )
  .option('--add-runs <ids>, id1[,id2]', 'Add all iterations from these run IDs (not subject to filters)', list, [])
  .option('--add-iterations <ids>, id1[,id2]', 'Add these iteraqtion IDs (not subject to filters)', list, [])
  .option(
    '--dont-breakout-tags <tags>, tag1[,tagN]',
    'Do not break out these tags (because of different values per iteration) into different clusters of iterations.  These tag values will show up in the label for the result instead',
    list,
    []
  )
  .option('--omit-tags <tags>, tag1[,tagN]', 'If these tags are found, just pretend they never existed', list, [])
  .option(
    '--dont-breakout-params <tags>, tag1[,tagN]',
    'Do not break out these params (because of different values per iteration) into different clusters of iterations.  These param values will show up in the label for the result instead',
    list,
    []
  )
  .option(
    '--omit-params <tags>, tag1[,tagN]',
    'If these params are found, just pretend they never existed.  Do not do this unless you really are sure these params are not relevant.',
    list,
    []
  )
  .option(
    '--breakout-order-params <arg>, arg1[,arg2]',
    'When performing the breakout of params, try to break them out in the order provided',
    list,
    []
  )
  .option(
    '--breakout-order-tags <name>, name1[,name]',
    'When performing the breakout of tags, try to break them out in the order provided',
    list,
    []
  )
  .parse(process.argv);

program.url = 'localhost:9200';

if (typeof program.dontBreakoutParams == 'undefined') {
  console.log('Setting program.dontBreakoutParams to empty array');
  program.dontBreakoutParams = [];
}

if (typeof program.dontBreakoutTags == 'undefined') {
  console.log('Setting program.dontBreakoutTags to empty array');
  program.dontBreakoutTags = [];
}

if (typeof program.breakoutOrderTags == 'undefined') {
  console.log('Setting program.breakoutOrderTags to empty array');
  program.breakoutOrderTags = [];
}

if (typeof program.breakoutOrderParams == 'undefined') {
  console.log('Setting program.breakoutOrderParams to empty array');
  program.breakoutOrderParams = [];
}

var iterTree = getIters(
  program.url,
  program.filterByAge,
  program.filterByTags,
  program.filterByParams,
  program.dontBreakoutTags,
  program.omitTags,
  program.dontBreakoutParams,
  program.omitParams,
  program.breakoutOrderTags,
  program.breakoutOrderParams,
  program.addRuns,
  program.addIterations
);
console.log('\n');
reportIters(iterTree);
