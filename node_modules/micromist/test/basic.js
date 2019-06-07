#!/usr/bin/env node
const micromist = require('../');
const minimist = require('minimist');

const args2 = minimist(process.argv.slice(2), {boolean:'b'});

console.log("minimist");
console.log(args2);


const args = micromist(process.argv, {string:'-l', boolean:'-b'});

console.log("micromist");
console.log(args);


