// # vim: autoindent tabstop=2 shiftwidth=2 expandtab softtabstop=2 filetype=javascript

const cdm = require('./cdm')
const program = require('commander')

function list(val) {
    return val.split(',')
}

program
    .version('0.1.0')
    .option('--user <"full user name">')
    .option('--email <email address>')
    .option('--run <run-ID>')
    .option('--harness <harness name>')
    .option('--url <host:port>')
    .option('--output-dir <path>, if not used, output is to console only')
    .option(
        '--output-format <fmt>, fmta[,fmtb]',
        'one or more output formats: txt html',
        list,
        []
    )
    .parse(process.argv)

// console.log("program.args:\n" + JSON.stringify(program, null, 2));
const termKeys = []
const values = []

if (program.user) {
    termKeys.push('run.name')
    values.push([program.user])
}
if (program.email) {
    termKeys.push('run.email')
    values.push([program.email])
}
if (program.run) {
    termKeys.push('run.id')
    values.push([program.run])
}
if (program.harness) {
    termKeys.push('run.harness')
    values.push([program.harness])
}
if (!program.url) {
    program.url = 'localhost:9200'
}

if (!program.outputDir) {
    program.outputDir = ''
}
if (!program.outputFormat) {
    program.outputFormat = ['']
}
const noHtml = subtractTwoArrays(program.outputFormat, ['html'])
let txt_summary = ''
let html_summary = '<pre>'

function logOutput(str, formats) {
    txt_summary += str + '\n'
    if (formats.includes('html')) {
        html_summary += str + '\n'
    }
}

const runIds = cdm.mSearch(
    program.url,
    'run',
    termKeys,
    values,
    'run.id',
    null,
    1000
)[0]
if (runIds == undefined || runIds.length == 0) {
    console.log('The run ID could not be found, exiting')
    process.exit(1)
}
runIds.forEach((runId) => {
    logOutput('\nrun-id: ' + runId, program.outputFormat)
    const tags = cdm.getTags(program.url, runId)
    tags.sort((a, b) => (a.name < b.name ? -1 : 1))
    let tagList = '  tags: '
    tags.forEach((tag) => {
        tagList += tag.name + '=' + tag.val + ' '
    })
    logOutput(tagList, program.outputFormat)
    const benchName = cdm.getBenchmarkName(program.url, runId)
    var benchmarks = list(benchName)
    logOutput('  benchmark: ' + benchName, program.outputFormat)
    const benchIterations = cdm.getIterations(program.url, runId)
    // console.log("benchIterations:\n" + JSON.stringify(benchIterations, null, 2));
    if (benchIterations.length == 0) {
        console.log('There were no iterations found, exiting')
        process.exit(1)
    }

    const iterParams = cdm.mgetParams(program.url, benchIterations)
    // returns 1D array [iter]
    const iterPrimaryPeriodNames = cdm.mgetPrimaryPeriodName(
        program.url,
        benchIterations
    )
    // input: 1D array
    // output: 2D array [iter][samp]
    const iterSampleIds = cdm.mgetSamples(program.url, benchIterations)
    // input: 2D array iterSampleIds: [iter][samp]
    // output: 2D array [iter][samp]
    const iterSampleStatus = cdm.mgetSampleStatus(program.url, iterSampleIds)
    // console.log("sampleStatus:\n" + JSON.stringify(iterSampleStatus, null, 2));
    // needs 2D array iterSampleIds: [iter][samp] and 1D array iterPrimaryPeriodNames [iter]
    // returns 2D array [iter][samp]
    const iterPrimaryPeriodIds = cdm.mgetPrimaryPeriodId(
        program.url,
        iterSampleIds,
        iterPrimaryPeriodNames
    )
    const iterPrimaryPeriodRanges = cdm.mgetPeriodRange(
        program.url,
        iterPrimaryPeriodIds
    )

    // Find the params which are the same in every iteration
    const iterPrimaryMetrics = cdm.mgetPrimaryMetric(
        program.url,
        benchIterations
    )
    var primaryMetrics = list(iterPrimaryMetrics[0])
    // For now only dump params when 1 primary metric is used
    if (primaryMetrics.length == 1) {
        const allParams = []
        const allParamsCounts = []
        iterParams.forEach((params) => {
            params.forEach((param) => {
                const newParam = param.arg + '=' + param.val
                idx = allParams.indexOf(newParam)
                if (idx == -1) {
                    allParams.push(newParam)
                    allParamsCounts.push(1)
                } else {
                    allParamsCounts[idx] += 1
                }
            })
        })
        var commonParams = []
        for (var idx = 0; idx < allParams.length; idx++) {
            if (allParamsCounts[idx] == benchIterations.length) {
                commonParams.push(allParams[idx])
            }
        }
        commonParams.sort()
        let commonParamsStr = '  common params: '
        commonParams.forEach((param) => {
            commonParamsStr += param + ' '
        })
        logOutput(commonParamsStr, program.outputFormat)
    }

    logOutput('  metrics:', program.outputFormat)
    const metricSources = cdm.getMetricSources(program.url, runId)
    const runIds = []
    for (var i = 0; i < metricSources.length; i++) {
        runIds[i] = runId
    }
    const metricTypes = cdm.mgetMetricTypes(program.url, runIds, metricSources)

    for (var i = 0; i < metricSources.length; i++) {
        logOutput('    source: ' + metricSources[i], program.outputFormat)
        let typeList = '      types: '
        for (var j = 0; j < metricTypes[i].length; j++) {
            typeList += metricTypes[i][j] + ' '
        }
        logOutput(typeList, program.outputFormat)
    }

    // build the sets for the mega-query
    var benchmarks = benchName.split(',')
    const sets = []
    for (var i = 0; i < benchIterations.length; i++) {
        for (var j = 0; j < iterSampleIds[i].length; j++) {
            var primaryMetrics = list(iterPrimaryMetrics[i])
            for (var k = 0; k < primaryMetrics.length; k++) {
                let source = ''
                let type = ''
                var sourceType = primaryMetrics[k].split('::')
                if (sourceType.length == 1) {
                    // Older runs have only 1 benchmark and only have "type" in primaryMetrics
                    source = benchmarks[0]
                    type = primaryMetrics[k]
                } else if (sourceType.length == 2) {
                    // Newer run data embeds source and type for primaryMetric
                    source = sourceType[0]
                    type = sourceType[1]
                } else {
                    console.log(
                        'sourceType array is an unexpected length, ' +
                            sourceType.length
                    )
                    process.exit(1)
                }
                const set = {
                    run: runId,
                    period: iterPrimaryPeriodIds[i][j],
                    source,
                    type,
                    begin: iterPrimaryPeriodRanges[i][j].begin,
                    end: iterPrimaryPeriodRanges[i][j].end,
                    resolution: 1,
                    breakout: [],
                }
                sets.push(set)
            }
        }
    }

    // do the mega-query
    const metricDataSets = cdm.getMetricDataSets(program.url, sets)

    // output the results
    const data = {}
    const numIter = {}
    var idx = 0
    for (var i = 0; i < benchIterations.length; i++) {
        var primaryMetrics = list(iterPrimaryMetrics[i])
        var series = {}
        logOutput('    iteration-id: ' + benchIterations[i], noHtml)

        if (primaryMetrics.length == 1) {
            var paramList = '      unique params: '
            series.label = ''
            iterParams[i]
                .sort((a, b) => (a.arg < b.arg ? -1 : 1))
                .forEach((param) => {
                    paramStr = param.arg + '=' + param.val
                    if (commonParams.indexOf(paramStr) == -1) {
                        paramList += param.arg + '=' + param.val + ' '
                        if (series.label == '') {
                            series.label = param.arg + '=' + param.val
                        } else {
                            series.label += ',' + param.arg + '=' + param.val
                        }
                    }
                })
            logOutput(paramList, noHtml)
        }

        logOutput(
            '      primary-period name: ' + iterPrimaryPeriodNames[i],
            noHtml
        )
        const primaryMetric = iterPrimaryMetrics[i]
        if (typeof data[primaryMetric] === 'undefined') {
            data[primaryMetric] = []
            numIter[primaryMetric] = 0
        }
        numIter[primaryMetric]++
        logOutput('      samples:', noHtml)
        const msampleCount = 0
        const msampleTotal = 0
        const msampleVals = []
        const msampleList = ''

        /*
    samples.forEach(sample => {
      if (cdm.getSampleStatus(program.url, sample) == "pass") {
        logOutput("        sample-id: " + sample, noHtml);
        var primaryPeriodId = cdm.getPrimaryPeriodId(program.url, sample, primaryPeriodName);
        if (primaryPeriodId == undefined || primaryPeriodId == null) {
          logOutput("          the primary perdiod-id for this sample is not valid, exiting\n", noHtml);
          process.exit(1);
        }
        logOutput("          primary period-id: " + primaryPeriodId, noHtml);
        var range = cdm.getPeriodRange(program.url, primaryPeriodId);
        if (range == undefined || range == null) {
          logOutput("          the range for the primary period is undefined, exiting", noHtml);
          process.exit(1);
        }
        logOutput("          period range: begin: " + range.begin + " end: " + range.end, noHtml);
        var breakout = []; // By default we do not break-out a benchmark metric, so this is empty
        // Needed for getMetricDataSets further below:
        var set = { "run": runId, "period": primaryPeriodId, "source": benchName, "type": primaryMetric, "begin": range.begin, "end": range.end, "resolution": 1, "breakout": [] };
        sets.push(set);
      }
    });
*/

        const allBenchMsampleVals = []
        const allBenchMsampleTotal = []
        const allBenchMsampleFixedList = []
        const allBenchMsampleCount = []
        for (var j = 0; j < iterSampleIds[i].length; j++) {
            if (
                iterSampleStatus[i][j] == 'pass' &&
                iterPrimaryPeriodRanges[i][j].begin !== undefined &&
                iterPrimaryPeriodRanges[i][j].end !== undefined
            ) {
                logOutput('        sample-id: ' + iterSampleIds[i][j], noHtml)
                logOutput(
                    '          primary period-id: ' +
                        iterPrimaryPeriodIds[i][j],
                    noHtml
                )
                logOutput(
                    '          period range: begin: ' +
                        iterPrimaryPeriodRanges[i][j].begin +
                        ' end: ' +
                        iterPrimaryPeriodRanges[i][j].end,
                    noHtml
                )
                // for (var k=0; k<benchmarks.length; k++) {
                var primaryMetrics = list(iterPrimaryMetrics[i])
                for (var k = 0; k < primaryMetrics.length; k++) {
                    var sourceType = primaryMetrics[k].split('::')
                    msampleVal = parseFloat(
                        metricDataSets[idx].values[''][0].value
                    )
                    if (allBenchMsampleVals[k] == null) {
                        allBenchMsampleVals[k] = []
                    }
                    allBenchMsampleVals[k].push(msampleVal)

                    if (allBenchMsampleTotal[k] == null) {
                        allBenchMsampleTotal[k] = 0
                    }
                    allBenchMsampleTotal[k] += msampleVal

                    msampleFixed = msampleVal.toFixed(6)

                    if (allBenchMsampleFixedList[k] == null) {
                        allBenchMsampleFixedList[k] = ''
                    }
                    allBenchMsampleFixedList[k] += ' ' + msampleFixed

                    if (allBenchMsampleCount[k] == null) {
                        allBenchMsampleCount[k] = 0
                    }
                    allBenchMsampleCount[k]++
                    idx++
                }
            }
        }
        for (var k = 0; k < primaryMetrics.length; k++) {
            var sourceType = primaryMetrics[k].split('::')
            if (allBenchMsampleCount[k] > 0) {
                var mean = allBenchMsampleTotal[k] / allBenchMsampleCount[k]
                var diff = 0
                allBenchMsampleVals[k].forEach((val) => {
                    diff += (mean - val) * (mean - val)
                })
                diff /= allBenchMsampleCount[k] - 1
                const mstddev = Math.sqrt(diff)
                const mstddevpct = (100 * mstddev) / mean
                logOutput(
                    '            result: (' +
                        sourceType[0] +
                        '::' +
                        sourceType[1] +
                        ') samples:' +
                        allBenchMsampleFixedList[k] +
                        ' mean: ' +
                        parseFloat(mean).toFixed(6) +
                        ' min: ' +
                        parseFloat(Math.min(...allBenchMsampleVals[k])).toFixed(
                            6
                        ) +
                        ' max: ' +
                        parseFloat(Math.max(...allBenchMsampleVals[k])).toFixed(
                            6
                        ) +
                        ' stddev: ' +
                        parseFloat(mstddev).toFixed(6) +
                        ' stddevpct: ' +
                        parseFloat(mstddevpct).toFixed(6),
                    noHtml
                )
                series.mean = mean
                series.min = Math.min(...allBenchMsampleVals[k])
                series.max = Math.max(...allBenchMsampleVals[k])
            }
        }
        data[primaryMetric].push(series)
    }

    html_summary += '</pre>\n'
    const html_resources =
        '<!-- Resources -->\n' +
        '<script src="https://cdn.amcharts.com/lib/5/index.js"></script>\n' +
        '<script src="https://cdn.amcharts.com/lib/5/xy.js"></script>\n' +
        '<script src="https://cdn.amcharts.com/lib/5/themes/Animated.js"></script>\n' +
        '<script src="data.js"></script>\n' +
        '<script src="chart.js"></script>\n'
    let html_styles = '<!-- Styles -->\n' + '<style>\n'
    let html_div = ''
    Object.keys(numIter).forEach((pri) => {
        html_div += '<div id="' + pri + '"></div>\n'
        html_styles +=
            '#' +
            pri +
            ' {\n' +
            '  width: 1000px;\n' +
            '  height: ' +
            (120 + 25 * numIter[pri]) +
            'px;\n' +
            '}\n'
    })
    html_styles += '</style>\n'
    const html = html_styles + html_resources + html_summary + html_div

    // Maintain default behavior of sending to stdout
    console.log(txt_summary)

    const fs = require('fs')
    if (program.outputFormat.includes('txt')) {
        try {
            fs.writeFileSync(
                program.outputDir + '/' + 'result-summary.txt',
                txt_summary
            )
        } catch (err) {
            console.error(err)
        }
    }
    if (program.outputFormat.includes('html')) {
        try {
            fs.writeFileSync(
                program.outputDir + '/' + 'data.js',
                'var data = ' + JSON.stringify(data, null, 2)
            )
        } catch (err) {
            console.error(err)
        }
        try {
            fs.writeFileSync(
                program.outputDir + '/' + 'result-summary.html',
                html
            )
        } catch (err) {
            console.error(err)
        }
        try {
            fs.copyFileSync('chart.js', program.outputDir + '/' + 'chart.js')
        } catch (err) {
            console.log(err)
        }
    }
})
