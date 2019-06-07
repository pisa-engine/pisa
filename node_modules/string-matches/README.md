# stringMatches

Retrieves all the matches of a regex in a string.

## Install

```shell
$ npm install --save string-matches
```

## Usage

```js
import stringMatches from 'string-matches';

const str = 'A tidy tiger tied a tighter tie to tidy her tiny tail',
      regex = /tidy/g; // Don't forget the `g` flag, or it won't work

stringMatches ( str, regex ).length; // 2
```

## Related

- [string-replace-all](https://github.com/fabiospampinato/string-replace-all) - Replaces all the occurrences of a string into a string with another string.

## License

MIT © Fabio Spampinato
