/*!
 * bindex.js - index for bcoin
 * Copyright (c) 2014-2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const bindex = exports;

/**
 * Define a module for lazy loading.
 * @param {String} name
 * @param {String} path
 */

bindex.define = function define(name, path) {
  let cache = null;
  Object.defineProperty(bindex, name, {
    get() {
      if (!cache)
        cache = require(path);
      return cache;
    }
  });
};

bindex.define('Index', './index');
bindex.define('Plugin', './plugin');
bindex.define('ChainClient', './chainclient');
