const cdm = require('./cdm')
const program = require('commander')

program
    .version('0.1.0')
    .option('-r --run <run-ID>')
    .option('-u, --url <host:port>')
    .parse(process.argv)

console.log(cdm.getTags(program.url, program.run))
