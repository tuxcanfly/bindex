/*!
 * plugin.js - indexing plugin for bcoin
 * Copyright (c) 2014-2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const Index = require('./index');
const ChainClient = require('./chainclient');

/**
 * @exports plugin
 */

const plugin = exports;

/**
 * Plugin
 * @extends Index
 */

class Plugin extends Index {
  /**
   * Create a plugin.
   * @constructor
   * @param {Node} node
   */

  constructor(node) {
    this.config = node.config.filter('index');
    this.client = new ChainClient(node.chain);

    options = {
      client: this.client,
      network: node.network,
      logger: node.logger,
      client: this.client,
      prefix: this.config.prefix,
      indexTX: this.config.bool('tx'),
      indexAddress: this.config.bool('address'),
      indexFilters: this.config.bool('filters'),
      memory: this.config.bool('memory', node.memory),
      maxFiles: this.config.uint('max-files'),
      cacheSize: this.config.mb('cache-size')
    }

    super(options);
  }
}

/**
 * Plugin name.
 * @const {String}
 */

plugin.id = 'bindex';

/**
 * Plugin initialization.
 * @param {Node} node
 * @returns {Plugin}
 */

plugin.init = function init(node) {
  return new Plugin(node);
};
