# Contributing

When contributing, apply this project's JavaScript standard style, otherwise it will fail in CI.

## Requirements

- NodeJS 20.7.0
- NPM
- OpenSearch 2.x

## Standardizing JavaScript Style

1. Clone this repository.

```shell
git clone https://github.com/perftool-incubator/CommonDataModel.git
```

2. Change your current working directory to the CDMQ directory.

```shell
cd CommonDataModel/queries/cdmq
```

3. Install this project's Node JS dependencies.

```shell
npm install
```

4. Check current code style if changes are required with [prettier](https://prettier.io/docs/en/cli.html) using [npx](https://docs.npmjs.com/cli/v7/commands/npx).

```shell
npx prettier *.js --check
```

5. Apply code style changes.

```shell
npx prettier *.js --write
```
