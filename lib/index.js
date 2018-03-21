/*!
 * index.js - indexer for bcoin
 * Copyright (c) 2014-2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const IndexDB = require('./indexdb');

/**
 * Index
 */

class Index {
  /**
   * Create a plugin.
   * @constructor
   * @param {Options} options
   */

  constructor(options) {
    this.db = new IndexDB(options);
  }

  /**
   * Open the index
   * @returns {Promise}
   */

  async open() {
    await this.db.open();
  }

  /**
   * Close the index
   * @returns {Promise}
   */

  async close() {
    await this.db.close();
  }

  /**
   * Get tip.
   * @param {Hash} hash
   * @returns {Promise}
   */

  getTip() {
    return this.db.getTip();
  }

  /**
   * Get a transaction with metadata.
   * @param {Hash} hash
   * @returns {Promise} - Returns {@link TXMeta}.
   */

  getMeta(hash) {
    return this.db.getMeta(hash);
  }

  /**
   * Retrieve a transaction.
   * @param {Hash} hash
   * @returns {Promise} - Returns {@link TX}.
   */

  getTX(hash) {
    return this.db.getTX(hash);
  }

  /**
   * @param {Hash} hash
   * @returns {Promise} - Returns Boolean.
   */

  hasTX(hash) {
    return this.db.hasTX(hash);
  }

  /**
   * Get all coins pertinent to an address.
   * @param {Address[]} addrs
   * @returns {Promise} - Returns {@link Coin}[].
   */

  getCoinsByAddress(addrs) {
    return this.db.getCoinsByAddress(addrs);
  }

  /**
   * Get all transaction hashes to an address.
   * @param {Address[]} addrs
   * @returns {Promise} - Returns {@link Hash}[].
   */

  getHashesByAddress(addrs) {
    return this.db.getHashesByAddress(addrs);
  }

  /**
   * Get all transactions pertinent to an address.
   * @param {Address[]} addrs
   * @returns {Promise} - Returns {@link TX}[].
   */

  getTXByAddress(addrs) {
    return this.db.getTXByAddress(addrs);
  }

  /**
   * Get all transactions pertinent to an address.
   * @param {Address[]} addrs
   * @returns {Promise} - Returns {@link TXMeta}[].
   */

  getMetaByAddress(addrs) {
    return this.db.getMetaByAddress(addrs);
  }
}

/*
 * Expose
 */

module.exports = Index;
