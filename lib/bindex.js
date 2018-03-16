/*!
 * plugin.js - indexing plugin for bcoin
 * Copyright (c) 2014-2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const EventEmitter = require('events');
const IndexDB = require('./indexdb');
const NodeClient = require('./nodeclient');

/**
 * @exports plugin
 */

const plugin = exports;

/**
 * Plugin
 * @extends EventEmitter
 */

class Plugin extends EventEmitter {
  /**
   * Create a plugin.
   * @constructor
   * @param {Node} node
   */

  constructor(node) {
    super();

    this.network = node.network;
    this.logger = node.logger;
    this.config = node.config.filter('index');

    this.client = new NodeClient(node);

    this.idb = new IndexDB({
      network: this.network,
      logger: this.logger,
      client: this.client,
      prefix: this.config.prefix,
      indexTX: this.config.bool('tx'),
      indexAddress: this.config.bool('address'),
      memory: this.config.bool('memory', node.memory),
      maxFiles: this.config.uint('max-files'),
      cacheSize: this.config.mb('cache-size')
    });

    this.init();
  }

  init() {
    this.idb.on('error', err => this.emit('error', err));
  }

  async open() {
    await this.idb.open();
  }

  async close() {
    await this.idb.close();
  }

  /**
   * Get a transaction with metadata.
   * @param {Hash} hash
   * @returns {Promise} - Returns {@link TXMeta}.
   */

  getMeta(hash) {
    return this.idb.getMeta(hash);
  }

  /**
   * Retrieve a transaction.
   * @param {Hash} hash
   * @returns {Promise} - Returns {@link TX}.
   */

  getTX(hash) {
    return this.idb.getTX(hash);
  }

  /**
   * @param {Hash} hash
   * @returns {Promise} - Returns Boolean.
   */

  hasTX(hash) {
    return this.idb.hasTX(hash);
  }

  /**
   * Get all coins pertinent to an address.
   * @param {Address[]} addrs
   * @returns {Promise} - Returns {@link Coin}[].
   */

  getCoinsByAddress(addrs) {
    return this.idb.getCoinsByAddress(addrs);
  }

  /**
   * Get all transaction hashes to an address.
   * @param {Address[]} addrs
   * @returns {Promise} - Returns {@link Hash}[].
   */

  getHashesByAddress(addrs) {
    return this.idb.getHashesByAddress(addrs);
  }

  /**
   * Get all transactions pertinent to an address.
   * @param {Address[]} addrs
   * @returns {Promise} - Returns {@link TX}[].
   */

  getTXByAddress(addrs) {
    return this.idb.getTXByAddress(addrs);
  }

  /**
   * Get all transactions pertinent to an address.
   * @param {Address[]} addrs
   * @returns {Promise} - Returns {@link TXMeta}[].
   */

  getMetaByAddress(addrs) {
    return this.idb.getMetaByAddress(addrs);
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
