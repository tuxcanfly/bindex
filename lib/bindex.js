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

    this.plugins = Object.create(null);
    this.stack = [];

    this.client = new NodeClient(node);

    this.idb = new IndexDB({
      network: this.network,
      logger: this.logger,
      client: this.client,
      prefix: this.config.prefix,
      memory: this.config.bool('memory', node.memory),
      maxFiles: this.config.uint('max-files'),
      cacheSize: this.config.mb('cache-size')
    });

    this.init();
  }

  /**
   * Attach a plugin.
   * @param {Object} plugin
   * @returns {Object} Plugin instance.
   */

  use(plugin) {
    assert(plugin, 'Plugin must be an object.');
    assert(typeof plugin.init === 'function', '`init` must be a function.');

    const instance = plugin.init(this);

    assert(!instance.open || typeof instance.open === 'function',
      '`open` must be a function.');
    assert(!instance.close || typeof instance.close === 'function',
      '`close` must be a function.');

    if (plugin.id) {
      assert(typeof plugin.id === 'string', '`id` must be a string.');

      assert(!this.plugins[plugin.id], `${plugin.id} is already added.`);

      this.plugins[plugin.id] = instance;
    }

    this.stack.push(instance);

    if (typeof instance.on === 'function')
      instance.on('error', err => this.error(err));

    return instance;
  }

  /**
   * Test whether a plugin is available.
   * @param {String} name
   * @returns {Boolean}
   */

  has(name) {
    return this.plugins[name] != null;
  }

  /**
   * Get a plugin.
   * @param {String} name
   * @returns {Object|null}
   */

  get(name) {
    assert(typeof name === 'string', 'Plugin name must be a string.');

    return this.plugins[name] || null;
  }

  /**
   * Require a plugin.
   * @param {String} name
   * @returns {Object}
   * @throws {Error} on onloaded plugin
   */

  require(name) {
    const plugin = this.get(name);
    assert(plugin, `${name} is not loaded.`);
    return plugin;
  }

  /**
   * Load plugins.
   * @private
   */

  loadPlugins() {
    const plugins = this.config.array('plugins', []);
    const loader = this.config.func('loader');

    for (let plugin of plugins) {
      if (typeof plugin === 'string') {
        assert(loader, 'Must pass a loader function.');
        plugin = loader(plugin);
      }
      this.use(plugin);
    }
  }

  init() {
    this.idb.on('error', err => this.emit('error', err));
    this.loadPlugins();
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
