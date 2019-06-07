/*!
 * remote-origin-url <https://github.com/jonschlinkert/remote-origin-url>
 *
 * Copyright (c) 2014-2017, Jon Schlinkert.
 * Released under the MIT License.
 */

'use strict';

var parse = require('parse-git-config');

function remoteOriginUrl(path, cb) {
  if (typeof path === 'function') {
    cb = path;
    path = null;
  }

  parse({path: path}, function(err, parsed) {
    if (err) {
      cb(err.code !== 'ENOENT' ? err : undefined);
      return;
    }
    var origin = parsed['remote "origin"'];
    cb(null, origin ? origin.url : null);
  });
}

remoteOriginUrl.sync = function(path) {
  try {
    var parsed = parse.sync({path: path});
    if (!parsed) {
      return null;
    }

    var origin = parsed['remote "origin"'];
    return origin ? origin.url : null;
  } catch (err) {
    throw err;
  }
};

/**
 * Expose `remoteOriginUrl`
 */

module.exports = remoteOriginUrl;
