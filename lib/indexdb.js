/*!
 * indexdb.js - storage for indexdb
 * Copyright (c) 2014-2015, Fedor Indutny (MIT License)
 * Copyright (c) 2014-2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const assert = require('assert');
const path = require('path');
const EventEmitter = require('events');
const bdb = require('bdb');
const Logger = require('blgr');
const {Lock} = require('bmutex');
const {BloomFilter} = require('bfilter');
const Block = require('bcoin').primitives.Block;
const Address = require('bcoin').primitives.Address;
const TXMeta = require('bcoin').primitives.TXMeta;
const Network = require('bcoin').protocol.Network;
const CoinView = require('bcoin').coins.CoinView;
const ChainEntry = require('bcoin').blockchain.ChainEntry;
const NullClient = require('./nullclient');
const ChainClient = require('./chainclient');
const layout = require('./layout');
const {BlockMeta} = require('./records');
const GCSFilter = require('golomb/lib/golomb');

/**
 * Compact filter types.
 * @const {Number}
 * @default
 */

const FILTERS = {
  REGULAR: 0,
  EXTENDED: 1
};

/**
 * A hash of all zeroes.
 * @const {Buffer}
 * @default
 */

const ZERO_HASH = Buffer.alloc(32, 0x00);


/**
 * IndexDB
 * @alias module:index.IndexDB
 * @extends EventEmitter
 */

class IndexDB extends EventEmitter {
  /**
   * Create a index db.
   * @constructor
   * @param {Object} options
   */

  constructor(options) {
    super();

    this.options = IndexOptions.fromOptions(options);
    this.client = this.options.client || new NullClient(this);
    this.logger = this.options.logger.context('index');
    this.network = this.options.network;

    this.db = bdb.create(this.options);
    this.tip = new BlockMeta();
    this.lock = new Lock();

    this.init();
  }

  /**
   * Initialize indexdb.
   * @private
   */

  init() {
    this._bind();
  }

  /**
   * Index genesis block.
   * @returns {Promise}
   */

  async indexGenesis() {
    const genesis = this.network.genesisBlock;
    const block = Block.fromRaw(genesis, 'hex');
    const entry = ChainEntry.fromBlock(block);

    this.logger.info('Writing genesis block to IndexDB.');

    if (this.options.indexFilters) {
      const prevHash = Buffer.from(block.prevBlock, 'hex');

      // Genesis prev filter headers are defined to be zero hashes
      const b = this.db.batch();
      b.put(layout.G.build(prevHash), ZERO_HASH);
      b.put(layout.X.build(prevHash), ZERO_HASH);
      await b.write();
    }

    const view = new CoinView();
    await this.indexBlock(entry, block, view);
  }

  /**
   * Bind to node events.
   * @private
   */

  _bind() {
    this.client.on('error', (err) => {
      this.emit('error', err);
    });

    this.client.on('connect', async () => {
      try {
        await this.syncNode();
      } catch (e) {
        this.emit('error', e);
      }
    });

    this.client.bind('block connect', async (entry, block, view) => {
      try {
        await this.indexBlock(entry, block, view);
      } catch (e) {
        this.emit('error', e);
      }
    });

    this.client.bind('block disconnect', async (entry, block, view) => {
      try {
        await this.unindexBlock(entry, block, view);
      } catch (e) {
        this.emit('error', e);
      }
    });

    this.client.bind('chain reset', async (tip) => {
      try {
        await this.rollback(tip.height);
      } catch (e) {
        this.emit('error', e);
      }
    });
  }

  /**
   * Retrieve compact filter header by hash and type..
   * @param {Hash} hash
   * @param {Number} type
   * @returns {Promise} - Returns {@link Hash}.
   */

  async getCFHeader(hash, type) {
    assert(hash);
    assert(typeof type === 'number');

    if (!this.options.indexFilters)
      return false;

    let pair;
    switch (type) {
      case FILTERS.REGULAR:
        pair = layout.G;
        break;
      case FILTERS.EXTENDED:
        pair = layout.X;
        break;
      default:
        assert(false, 'Bad type.');
        break;
    }
    const cfheader = await this.db.get(pair.build(hash));
    assert(cfheader, `Missing cfheader ${hash.toString('hex')} ${type}.`);

    return cfheader;
  }

  /**
   * Index a transaction by txid.
   * @private
   * @param (ChainEntry) entry
   * @param (Block) block
   * @param (CoinView) view
   */

  async indexTX(entry, block, view) {
    if (!this.options.indexTX)
      return null;

    const b = this.db.batch();

    for (let i = 0; i < block.txs.length; i++) {
      const tx = block.txs[i];
      const hash = tx.hash();
      const meta = TXMeta.fromTX(tx, entry, i);
      b.put(layout.t.build(hash), meta.toRaw());
    }

    await b.write();
  }

  /**
   * Remove transaction from index.
   * @private
   * @param (ChainEntry) entry
   * @param (Block) block
   * @param (CoinView) view
   */

  async unindexTX(entry, block, view) {
    if (!this.options.indexTX)
      return null;

    const b = this.db.batch();

    for (let i = 0; i < block.txs.length; i++) {
      const tx = block.txs[i];
      const hash = tx.hash();
      b.del(layout.t.build(hash));
    }

    await b.write();
  }

  /**
   * Index a transaction by address.
   * @private
   * @param (ChainEntry) entry
   * @param (Block) block
   * @param (CoinView) view
   */

  async indexAddress(entry, block, view) {
    if (!this.options.indexAddress)
      return null;

    const b = this.db.batch();

    for (let i = 0; i < block.txs.length; i++) {
      const tx = block.txs[i];
      const hash = tx.hash();
      for (const addr of tx.getHashes(view))
        b.put(layout.T.build(addr, hash), null);

      if (!tx.isCoinbase()) {
        for (const {prevout} of tx.inputs) {
          const {hash, index} = prevout;
          const coin = view.getOutput(prevout);
          assert(coin);

          const addr = coin.getHash();

          if (!addr)
            continue;

          b.del(layout.C.build(addr, hash, index));
        }
      }

      for (let i = 0; i < tx.outputs.length; i++) {
        const output = tx.outputs[i];
        const addr = output.getHash();

        if (!addr)
          continue;

        b.put(layout.C.build(addr, hash, i), null);
      }
    }

    await b.write();
  }

  /**
   * Remove address from index.
   * @private
   * @param (ChainEntry) entry
   * @param (Block) block
   * @param (CoinView) view
   */

  async unindexAddress(entry, block, view) {
    if (!this.options.indexAddress)
      return null;

    const b = this.db.batch();
    for (let i = 0; i < block.txs.length; i++) {
      const tx = block.txs[i];
      for (const addr of tx.getHashes(view))
        b.del(layout.T.build(addr, hash));

      if (!tx.isCoinbase()) {
        for (const {prevout} of tx.inputs) {
          const {hash, index} = prevout;
          const coin = view.getOutput(prevout);
          assert(coin);

          const addr = coin.getHash();

          if (!addr)
            continue;

          b.put(layout.C.build(addr, hash, index), null);
        }
      }

      for (let i = 0; i < tx.outputs.length; i++) {
        const output = tx.outputs[i];
        const addr = output.getHash();

        if (!addr)
          continue;

        b.del(layout.C.build(addr, hash, i));
      }
    }

    await b.write();
  }

  /**
   * Save compact filter for block.
   * @private
   * @param (ChainEntry) entry
   * @param (Block) block
   * @param (CoinView) view
   */

  async indexFilters(entry, block, view) {
    if (!this.options.indexFilters)
      return;

    const hash = block.hash();

    const prevBasic = await this.getCFHeader(
      Buffer.from(block.prevBlock, 'hex'),
      FILTERS.REGULAR
    );
    const prevExt = await this.getCFHeader(
      Buffer.from(block.prevBlock, 'hex'),
      FILTERS.EXTENDED
    );

    let basicRaw;
    const b = this.db.batch();
    const basic = GCSFilter.fromBlock(block);
    if (basic.data.length > 0)
      basicRaw = basic.toRaw();
    b.put(layout.g.build(hash), basicRaw);
    b.put(layout.G.build(hash), basic.header(prevBasic));

    let extRaw;
    const ext = GCSFilter.fromExtended(block);
    if (ext.data.length > 0)
      extRaw = ext.toRaw();
    b.put(layout.x.build(hash), extRaw);
    b.put(layout.X.build(hash), ext.header(prevExt));

    await b.write();
  }

  /**
   * Remove compact filter for block.
   * @private
   * @param (ChainEntry) entry
   * @param (Block) block
   * @param (CoinView) view
   */

  async unindexFilters(entry, block, view) {
    if (!this.options.indexFilters)
      return;

    const b = this.db.batch();

    const hash = block.hash();
    b.del(layout.g(hash));
    b.del(layout.x(hash));
    b.del(layout.G(hash));
    b.del(layout.X(hash));

    await b.write();
  }

  /**
   * Index a block
   * @param (ChainEntry) entry
   * @param (Block) block
   * @param (CoinView) view
   * @returns {Promise}
   */

  async indexBlock(entry, block, view) {
    const unlock = await this.lock.lock();
    try {
      return await this._indexBlock(entry, block, view);
    } finally {
      unlock();
    }
  }

  /**
   * Index a block
   * @param (ChainEntry) entry
   * @param (Block) block
   * @param (CoinView) view
   * @returns {Promise}
   */

  async _indexBlock(entry, block, view) {
    const tip = BlockMeta.fromEntry(entry);

    if (tip.height < this.tip.height) {
      this.logger.warning(
        'IndexDB is connecting low blocks (%d).',
        tip.height);
      return
    }

    if (tip.height >= this.network.block.slowHeight)
      this.logger.debug('Adding block: %d.', tip.height);

    await this.indexTX(entry, block, view);
    await this.indexAddress(entry, block, view);
    await this.indexFilters(entry, block, block);

    // Sync the new tip.
    await this.setTip(tip);
  }

  /**
   * Unindex a block
   * @param (ChainEntry) entry
   * @param (Block) block
   * @param (CoinView) view
   * @returns {Promise}
   */

  async unindexBlock(entry, block, view) {
    const unlock = await this.lock.lock();
    try {
      return await this._unindexBlock(entry, block, view);
    } finally {
      unlock();
    }
  }

  /**
   * Unindex a block
   * @param (ChainEntry) entry
   * @param (Block) block
   * @param (CoinView) view
   * @returns {Promise}
   */

  async _unindexBlock(entry, block, view) {
    const tip = BlockMeta.fromEntry(entry);

    if (tip.height === 0)
      throw new Error('IDB: Bad disconnection (genesis block).');

    if (tip.height > this.tip.height) {
      this.logger.warning(
        'IndexDB is disconnecting high blocks (%d).',
        tip.height);
      return 0;
    }

    if (tip.height !== this.tip.height)
      throw new Error('IDB: Bad disconnection (height mismatch).');

    await this.unindexTX(entry, block, view);
    await this.unindexAddress(entry, block, view);
    await this.unindexFilters(entry, block, view);

    const prevEntry = await this.client.getEntry(tip.height - 1);
    assert(prevEntry);

    const prev = BlockMeta.fromEntry(prevEntry);
    assert(prev);

    // Sync the previous tip.
    await this.setTip(prev);
    return 0;
  }

  /**
   * Verify network.
   * @returns {Promise}
   */

  async verifyNetwork() {
    const raw = await this.db.get(layout.O.build());

    if (!raw) {
      const b = this.db.batch();
      b.put(layout.O.build(), fromU32(this.network.magic));
      return b.write();
    }

    const magic = raw.readUInt32LE(0, true);

    if (magic !== this.network.magic)
      throw new Error('Network mismatch for IndexDB.');

    return undefined;
  }

  /**
   * Open the indexdb, wait for the database to load.
   * @returns {Promise}
   */

  async open() {
    await this.db.open();

    this.tip = await this.getTip();
    if (this.tip.height != -1) {
      this.logger.info(
        'IndexDB loaded (height=%d, hash=%s).',
        this.tip.height,
        this.tip.hash);
    } else {
      await this.indexGenesis();
      this.logger.info('IndexDB initialized');
    }

    await this.db.verify(layout.V.build(), 'index', 0);
    await this.verifyNetwork();
    await this.connect();
  }

  /**
   * Close the indexdb, wait for the database to close.
   * @returns {Promise}
   */

  async close() {
    return this.db.close();
  }

  /**
   * Connect to the node server (client required).
   * @returns {Promise}
   */

  async connect() {
    return this.client.open();
  }

  /**
   * Disconnect from node server (client required).
   * @returns {Promise}
   */

  async disconnect() {
    return this.client.close();
  }


  /**
   * Sync the current chain tip.
   * @param {BlockMeta} tip
   * @returns {Promise}
   */

  async setTip(tip) {
    const b = this.db.batch();
    // Save tip.
    b.put(layout.h.build(), tip.toRaw());
    await b.write();

    this.tip = tip;
  }

  /**
   * Sync tip with server on every connect.
   * @returns {Promise}
   */

  async syncNode() {
    const unlock = await this.lock.lock();
    try {
      this.logger.info('Resyncing from server...');

      await this.syncChain();
    } finally {
      unlock();
    }
  }

  /**
   * Connect and sync with the chain server.
   * Rescan blockchain from a given height.
   * @private
   * @param {Number?} height
   * @returns {Promise}
   */

  async syncChain() {
    return this.scan();
  }

  /**
   * Rescan blockchain from a given height.
   * @private
   * @param {Number?} height
   * @returns {Promise}
   */

  async scan(height) {
    if (height == null)
      height = this.tip.height;

    this.logger.info('Syncing state from height %d.', height);

    const tip = await this.client.getTip();
    assert(tip);

    if (tip.height < height) {
      height = tip.height;
    }

    await this.rollback(height);

    this.logger.info(
      'IndexDB is scanning %d blocks.',
      tip.height - height);

    for (let height = this.tip.height + 1;
         height <= tip.height; height++) {
      this.logger.info('scanning block %d', height);

      const entry = await this.client.getEntry(height);
      const block = await this.client.getBlock(entry.hash);
      const view = await this.client.getBlockView(block);
      await this.indexBlock(entry, block, view);
    }
  }

  /**
   * Get tip.
   * @param {Hash} hash
   * @returns {Promise}
   */

  async getTip() {
    const raw = await this.db.get(layout.h.build());

    if (!raw) {
      return new BlockMeta();
    }

    const tip = BlockMeta.fromRaw(raw);
    if (!tip)
      throw new Error('IDB: Tip not found!');

    return tip;
  }

  /**
   * Sync with chain height.
   * @param {Number} height
   * @returns {Promise}
   */

  async rollback(height) {
    if (height > this.tip.height)
      throw new Error('IDB: Cannot rollback to the future.');

    if (height === this.tip.height) {
      this.logger.info('Rolled back to same height (%d).', height);
      return;
    }

    this.logger.info(
      'Rolling back %d IndexDB blocks to height %d.',
      this.tip.height - height, height);

    const entry = await this.client.getEntry(height);
    const tip = BlockMeta.fromEntry(entry);
    assert(tip);

    let prev = this.tip.hash;
    for (;;) {
      const block = await this.client.getBlock(prev);
      if (block == null || block.height === height)
        break; // TODO: handle block not found

      const view = await this.client.getBlockView(block);
      await this.unindexBlock(entry, block, view);

      prev = block.prevBlock;
    }

    const total = this.tip.height - height;
    this.logger.info('Rolled back %d IndexDB blocks.', total);
    await this.setTip(tip);
  }

  /**
   * Get a transaction with metadata.
   * @param {Hash} hash
   * @returns {Promise} - Returns {@link TXMeta}.
   */

  async getMeta(hash) {
    if (!this.options.indexTX)
      return null;

    const data = await this.db.get(layout.t.build(hash));

    if (!data)
      return null;

    return TXMeta.fromRaw(data);
  }

  /**
   * Retrieve a transaction.
   * @param {Hash} hash
   * @returns {Promise} - Returns {@link TX}.
   */

  async getTX(hash) {
    const meta = await this.getMeta(hash);

    if (!meta)
      return null;

    return meta.tx;
  }

  /**
   * @param {Hash} hash
   * @returns {Promise} - Returns Boolean.
   */

  async hasTX(hash) {
    if (!this.options.indexTX)
      return false;

    return this.db.has(layout.t.build(hash));
  }

  /**
   * Get all coins pertinent to an address.
   * @param {Address[]} addrs
   * @returns {Promise} - Returns {@link Coin}[].
   */

  async getCoinsByAddress(addrs) {
    if (!this.options.indexAddress)
      return [];

    if (!Array.isArray(addrs))
      addrs = [addrs];

    const coins = [];

    for (const addr of addrs) {
      const hash = Address.getHash(addr);

      const keys = await this.db.keys({
        gte: layout.C.min(hash),
        lte: layout.C.max(hash),
        parse: (key) => {
          const [, txid, index] = layout.C.parse(key);
          return [txid, index];
        }
      });

      for (const [hash, index] of keys) {
        const coin = await this.getCoin(hash, index);
        assert(coin);
        coins.push(coin);
      }
    }

    return coins;
  }

  /**
   * Get all transaction hashes to an address.
   * @param {Address[]} addrs
   * @returns {Promise} - Returns {@link Hash}[].
   */

  async getHashesByAddress(addrs) {
    if (!this.options.indexTX || !this.options.indexAddress)
      return [];

    const hashes = Object.create(null);

    for (const addr of addrs) {
      const hash = Address.getHash(addr);

      await this.db.keys({
        gte: layout.T.min(hash),
        lte: layout.T.max(hash),
        parse: (key) => {
          const [, txid] = layout.T.parse(key);
          hashes[txid] = true;
        }
      });
    }

    return Object.keys(hashes);
  }

  /**
   * Get all transactions pertinent to an address.
   * @param {Address[]} addrs
   * @returns {Promise} - Returns {@link TX}[].
   */

  async getTXByAddress(addrs) {
    const mtxs = await this.getMetaByAddress(addrs);
    const out = [];

    for (const mtx of mtxs)
      out.push(mtx.tx);

    return out;
  }

  /**
   * Get all transactions pertinent to an address.
   * @param {Address[]} addrs
   * @returns {Promise} - Returns {@link TXMeta}[].
   */

  async getMetaByAddress(addrs) {
    if (!this.options.indexTX || !this.options.indexAddress)
      return [];

    if (!Array.isArray(addrs))
      addrs = [addrs];

    const hashes = await this.getHashesByAddress(addrs);
    const mtxs = [];

    for (const hash of hashes) {
      const mtx = await this.getMeta(hash);
      assert(mtx);
      mtxs.push(mtx);
    }

    return mtxs;
  }
}

/**
 * Index Options
 * @alias module:index.IndexOptions
 */

class IndexOptions {
  /**
   * Create index options.
   * @constructor
   * @param {Object} options
   */

  constructor(options) {
    this.network = Network.primary;
    this.logger = Logger.global;
    this.chain = null;
    this.client = null;
    this.prefix = null;
    this.location = null;
    this.memory = true;
    this.maxFiles = 64;
    this.cacheSize = 16 << 20;
    this.compression = true;
    this.indexTX = false;
    this.indexAddress = false;
    this.indexFilters = false;

    if (options)
      this.fromOptions(options);
  }

  /**
   * Inject properties from object.
   * @private
   * @param {Object} options
   * @returns {IndexOptions}
   */

  fromOptions(options) {
    if (options.network != null) {
      assert(typeof options.network === 'string');
      this.network = Network.get(options.network);
    }

    if (options.logger != null) {
      assert(typeof options.logger === 'object');
      this.logger = options.logger;
    }

    if (options.chain != null) {
      assert(typeof options.chain === 'object');
      this.chain = options.chain;
      this.client = new ChainClient(this.chain);
    }

    if (options.client != null) {
      assert(typeof options.client === 'object');
      this.client = options.client;
    }

    assert(this.client);

    if (options.prefix != null) {
      assert(typeof options.prefix === 'string');
      this.prefix = options.prefix;
      this.location = path.join(this.prefix, 'index');
    }

    if (options.location != null) {
      assert(typeof options.location === 'string');
      this.location = options.location;
    }

    if (options.memory != null) {
      assert(typeof options.memory === 'boolean');
      this.memory = options.memory;
    }

    if (options.maxFiles != null) {
      assert((options.maxFiles >>> 0) === options.maxFiles);
      this.maxFiles = options.maxFiles;
    }

    if (options.cacheSize != null) {
      assert(Number.isSafeInteger(options.cacheSize) && options.cacheSize >= 0);
      this.cacheSize = options.cacheSize;
    }

    if (options.compression != null) {
      assert(typeof options.compression === 'boolean');
      this.compression = options.compression;
    }

    if (options.indexTX != null) {
      assert(typeof options.indexTX === 'boolean');
      this.indexTX = options.indexTX;
    }

    if (options.indexAddress != null) {
      assert(typeof options.indexAddress === 'boolean');
      this.indexAddress = options.indexAddress;
    }

    if (options.indexFilters != null) {
      assert(typeof options.indexFilters === 'boolean');
      this.indexFilters = options.indexFilters;
    }

    return this;
  }

  /**
   * Instantiate chain options from object.
   * @param {Object} options
   * @returns {IndexOptions}
   */

  static fromOptions(options) {
    return new this().fromOptions(options);
  }
}

/*
 * Helpers
 */

function fromU32(num) {
  const data = Buffer.allocUnsafe(4);
  data.writeUInt32LE(num, 0, true);
  return data;
}

/*
 * Expose
 */

module.exports = IndexDB;
