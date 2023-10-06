# Contributing

When contributing, apply this project's JavaScript standard style, otherwise it will fail in CI.

## Requirements

- Node 18.18.0
- NPM
- ElasticSearch 7.17

## Standardizing JavaScript Style

1. Clone this repository.

```shell
$ git clone https://github.com/perftool-incubator/CommonDataModel.git
```

2. Change your current working directory to the CDMQ directory.

```shell
$ cd CommonDataModel
```

3. Install this project's Node JS dependencies.

```shell
$ npm install --prefix queries
```

4. Check current code style if changes are required with [prettier](https://prettier.io/docs/en/cli.html) using [npx](https://docs.npmjs.com/cli/v7/commands/npx).

```shell
$ npx prettier --check "queries/**/*.js"
```

5. Apply code style changes.

```shell
$ npx prettier --write "queries/**/*.js"
```

## Unit Tests

1. Verify Node 18

```shell
$ node -v
v18.18.0
```

2. Install this project's Node JS dependencies.

```shell
$ npm install --prefix queries
```

3. Execute all JavaScript unit test files with code coverage.

```shell
$ npm test --prefix queries

> cdmq@1.0.0 test
> jest

 PASS  test/cdm.test.js
  set(ish)-array behavior
    ✓ set(ish) difference (subtractTwoArrays) (6 ms)
    ✓ set(ish) intersection (intersectTwoArrays) (1 ms)
    ✓ intersect all arrays (1 ms)

----------|---------|----------|---------|---------|-------------------------------------------
File      | % Stmts | % Branch | % Funcs | % Lines | Uncovered Line #s
----------|---------|----------|---------|---------|-------------------------------------------
All files |   10.63 |      100 |    4.61 |   10.63 |
 cdm.js   |   10.63 |      100 |    4.61 |   10.63 | ...720-1729,1744-2022,2026-2041,2057-2180
----------|---------|----------|---------|---------|-------------------------------------------
Test Suites: 1 passed, 1 total
Tests:       3 passed, 3 total
Snapshots:   0 total
Time:        0.703 s, estimated 1 s
Ran all test suites.
```