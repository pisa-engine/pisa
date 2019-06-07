"use strict";

function isOption(str) {
  return (str || '').match(/^--?[^\d]+/);
}

function isMulti(str) {
  if (str.match(/^-([a-z]{2,})/i)) {
    return str.substr(1).split('');
  }
}

function compute(r, name, val) {
  const origName = name;
  name = name.replace(/^--?/, '');
  const mul = isMulti(origName);
  if (mul) {
    mul.forEach(o => r[o] = true);
    return r;
  }
  r[name] = name in r ? (Array.isArray(r[name]) ? r[name] : [r[name]]).concat(val) : val;
  return r;
}

function parseValue(name, val, opts) {
  if (typeof val === 'undefined') {
    val = true;
  }
  if (typeof val === 'string' && val === 'true') {
    val = true;
  } else if(typeof val === 'string' && val === 'false') {
    val = false;
  }
  if (Array.isArray(opts.string) && opts.string.indexOf(name) !== -1 && typeof val !== 'string') {
    val = undefined;
  }
  return val;
}

/**
 *
 * @param args
 * @param {Object} opts
 * @param {String|String[]} opts.string - A string or array of strings argument names to always treat as strings
 * @param {String|String[]} opts.boolean - A string or array of strings argument names to always treat as true values
 *
 * @returns {{_: Array}}
 */
function micromist(args, opts) {

  args = args.slice(2);
  opts = opts || {};

  if (typeof opts.string === 'string') {
    opts.string = [opts.string];
  }
  if (typeof opts.boolean === 'string') {
    opts.boolean = [opts.boolean];
  }

  const r = {_:[]};
  let pointer = 0;

  while(typeof args[pointer] !== 'undefined') {

    const val = args[pointer];

    if (!isOption(val)) {
      r._.push(val);
      pointer++;
      continue;
    } else if (val === '--') {
      r._ = r._.concat(args.slice(args.indexOf('--')));
      break;
    }

    const parts = val.split('=');
    let isNextOpt = true;

    if (parts.length > 1) {
      compute(r, parts[0], parts[1]);
    } else if (opts.boolean !== undefined && opts.boolean.includes(parts[0])) {
      compute(r, parts[0], true);
    } else {
      const next = args[pointer+1];
      isNextOpt = isOption(next);
      compute(r, parts[0], isNextOpt ? parseValue(parts[0], true, opts) : parseValue(parts[0], next, opts));
    }

    pointer += isNextOpt ? 1 : 2;
  }

  return r;
}

module.exports = micromist;
