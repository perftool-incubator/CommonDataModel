# Contributing

When contributing, apply this project's JavaScript standard style, otherwise it will fail in CI.

## Requirements

- Node 20.8.0
- NPM
- ElasticSearch 7.17

## Standardizing JavaScript Style

1. Clone this repository.

```shell
$ git clone https://github.com/perftool-incubator/CommonDataModel.git
```

2. Change your current working directory to the CDMQ directory.

```shell
$ cd CommonDataModel/queries/cdmq
```

3. Install this project's Node JS dependencies.

```shell
$ npm install
```

4. Check current code style if changes are required with [prettier](https://prettier.io/docs/en/cli.html) using [npx](https://docs.npmjs.com/cli/v7/commands/npx).

```shell
$ npx prettier *.js --check
```

5. Apply code style changes.

```shell
$ npx prettier *.js --write
```

## Unit Tests

1. Change your current working directory to the CDMQ directory.

```shell
$ cd CommonDataModel/queries/cdmq
```

2. Verify Node 20.8.0

```shell
$ node -v
v20.8.0
```

3. Install this project's Node JS dependencies.

```shell
$ npm install
```

3. Execute all JavaScript unit test files with code coverage.

```shell
$ node --experimental-test-coverage queries/cdmq/test/*.test.js

✔ set(ish) difference (subtractTwoArrays) (1.599154ms)
✔ set(ish) intersection (intersectTwoArrays) (0.161839ms)
✔ intersect all arrays (0.154675ms)
ℹ tests 3
ℹ suites 0
ℹ pass 3
ℹ fail 0
ℹ cancelled 0
ℹ skipped 0
ℹ todo 0
ℹ duration_ms 9.275733
ℹ start of coverage report
ℹ ---------------------------------------------------------------------------------------------
ℹ file     | line % | branch % | funcs % | uncovered lines
ℹ ---------------------------------------------------------------------------------------------
ℹ …/cdm.js |  13.30 |   100.00 |    8.82 | 5-7 22-28 31-39 64-76 86-178 188-195 199 204-211 2…
ℹ …test.js | 100.00 |   100.00 |  100.00 |
ℹ ---------------------------------------------------------------------------------------------
ℹ all fil… |  15.05 |   100.00 |   12.68 |
ℹ ---------------------------------------------------------------------------------------------
ℹ end of coverage report
```