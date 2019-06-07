# Inquirer Helpers

Collection of helpers for [Inquirer](https://www.npmjs.com/package/inquirer).

## Install

```shell
$ npm install --save inquirer-helpers
```

## API

### `Helpers.FULLSCREEN = true`

Showing as many lines as possible.

### `Helpers.PAGE_SIZE = 10`

Number of rows displayed.

### `Helpers.CLI_WIDTH = 80`

Default CLI width if it cannot be properly detected.

### `Helpers.confirm ( message: string, fallback: boolean = false )`

Prompt for a confirmation.

### `Helpers.noYes ( message: string )`

Prompt for a confirmation, giving `No` and `Yes` as possible options.

### `Helpers.yesNo ( message: string )`

Prompt for a confirmation, giving `Yes` and `No` as possible options.

### `Helpers.input ( message: string, fallback? )`

Prompt for a string input.

### `Helpers.list ( message: string, list, string[], fallback? )`

Prompt to pick a value from the list.

Auto-truncates values.

### `Helpers.checkbox ( message: string, list, string[], fallback? )`

Prompt to pick one or more values from the list.

Auto-truncates values.

### `Helpers.table ( message: string, table: string[][], values: any[], colors: string[] = [], fallback? )`

Prompt to pick a value from a list, formatted as a table.

Supports custom columns, colors and support for any terminal width.

## License

MIT © Fabio Spampinato
