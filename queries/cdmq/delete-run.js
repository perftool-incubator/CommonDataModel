const cdm = require('./cdm')
const program = require('commander')

program
    .version('0.1.0')
    .option('--run <run ID>')
    .option(
        '--url <host:port>',
        'The host and port of the Elasticsearch instance',
        'localhost:9200'
    )
    .parse(process.argv)

async function waitFor(docTypes) {
    let numAttempts = 1
    const maxAttempts = 10
    while (numAttempts <= maxAttempts && docTypes.length > 0) {
        const promise = new Promise((resolve, reject) => {
            setTimeout(() => resolve('done!'), 2000)
        })
        const result = await promise

        console.log(
            '\nConfirming all documents are in deleted elasticsearch (attempt #' +
                numAttempts +
                ')'
        )
        for (let i = 0; i < docTypes.length; i++) {
            const thisNumDocs = cdm.getDocCount(
                program.url,
                program.run,
                docTypes[i]
            )
            console.log('  ' + docTypes[i] + ': doc count: ' + thisNumDocs)
            if (thisNumDocs == 0) {
                remainingDocTypes = remainingDocTypes.filter(
                    (val) => val !== docTypes[i]
                )
            }
        }
        docTypes = remainingDocTypes
        numAttempts++
    }
    if (docTypes.lenth > 0) {
        console.log(
            'ERROR: could not delete all documents for ' +
                docTypes +
                ' with ' +
                numAttempts
        )
    }
}

const nonMetricDocTypes = [
    'run',
    'iteration',
    'sample',
    'period',
    'param',
    'tag',
]
var remainingDocTypes = nonMetricDocTypes
let q = {}
if (program.run) {
    q = { query: { bool: { filter: [{ term: { 'run.id': program.run } }] } } }
}
cdm.deleteMetrics(program.url, program.run)
cdm.deleteDocs(program.url, nonMetricDocTypes, q)
waitFor(nonMetricDocTypes)
