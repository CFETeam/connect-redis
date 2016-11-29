/*!
 * Connect - Redis
 * Copyright(c) 2012 TJ Holowaychuk <tj@vision-media.ca>
 * MIT Licensed
 */

var debug = require('debug')('connect:redis');
var redis = require('redis');
var util = require('util');
var HashRing = require('hashring');
var noop = function () { };

/**
 * One day in seconds.
 */

var oneDay = 86400;

function getTTL(store, sess) {
  var maxAge = sess.cookie.maxAge;
  return store.ttl || (typeof maxAge === 'number'
    ? Math.floor(maxAge / 1000)
    : oneDay);
}

/**
 * Return the `RedisStore` extending `express`'s session Store.
 *
 * @param {object} express session
 * @return {Function}
 * @api public
 */

module.exports = function (session) {

  /**
   * Express's session Store.
   */

  var Store = session.Store;

  /**
   * Initialize RedisStore with the given `options`.
   *
   * @param {Object} options
   * @api public
   */

  function RedisStore(options) {
    if (!(this instanceof RedisStore)) {
      throw new TypeError('Cannot call RedisStore constructor as a function');
    }

    var self = this;

    options = options || {};
    Store.call(this, options);
    this.prefix = options.prefix == null
      ? 'sess:'
      : options.prefix;

    delete options.prefix;

    this.serializer = options.serializer || JSON;

    if (options.url) {
      options.socket = options.url;
    }

    this.ttl = options.ttl;
    this.disableTTL = options.disableTTL;

    this.initClient(options);
  }

  /**
   * Inherit from `Store`.
   */

  util.inherits(RedisStore, Store);

  /**
   * Attempt to fetch session by the given `sid`.
   *
   * @param {String} sid
   * @param {Function} fn
   * @api public
   */

  RedisStore.prototype.get = function (sid, fn) {
    var store = this;
    var psid = store.prefix + sid;
    if (!fn) fn = noop;
    debug('GET "%s"', sid);

    store.getClient(psid).get(psid, function (er, data) {
      if (er) return fn(er);
      if (!data) return fn();

      var result;
      data = data.toString();
      debug('GOT %s', data);

      try {
        result = store.serializer.parse(data);
      }
      catch (er) {
        return fn(er);
      }
      return fn(null, result);
    });
  };

  /**
   * Commit the given `sess` object associated with the given `sid`.
   *
   * @param {String} sid
   * @param {Session} sess
   * @param {Function} fn
   * @api public
   */

  RedisStore.prototype.set = function (sid, sess, fn) {
    var store = this;
    var psid = store.prefix + sid;
    var args = [psid];
    if (!fn) fn = noop;

    try {
      var jsess = store.serializer.stringify(sess);
    }
    catch (er) {
      return fn(er);
    }

    args.push(jsess);

    if (!store.disableTTL) {
      var ttl = getTTL(store, sess);
      args.push('EX', ttl);
      debug('SET "%s" %s ttl:%s', sid, jsess, ttl);
    } else {
      debug('SET "%s" %s', sid, jsess);
    }

    store.getClient(psid).set(args, function (er) {
      if (er) return fn(er);
      debug('SET complete');
      fn.apply(null, arguments);
    });
  };

  /**
   * Destroy the session associated with the given `sid`.
   *
   * @param {String} sid
   * @api public
   */

  RedisStore.prototype.destroy = function (sid, fn) {
    sid = this.prefix + sid;
    debug('DEL "%s"', sid);
    this.getClient(sid).del(sid, fn);
  };

  /**
   * Refresh the time-to-live for the session with the given `sid`.
   *
   * @param {String} sid
   * @param {Session} sess
   * @param {Function} fn
   * @api public
   */

  RedisStore.prototype.touch = function (sid, sess, fn) {
    var store = this;
    var psid = store.prefix + sid;
    if (!fn) fn = noop;
    if (store.disableTTL) return fn();

    var ttl = getTTL(store, sess);

    debug('EXPIRE "%s" ttl:%s', sid, ttl);
    store.getClient(psid).expire(psid, ttl, function (er) {
      if (er) return fn(er);
      debug('EXPIRE complete');
      fn.apply(this, arguments);
    });
  };

  RedisStore.prototype.initClient = function (options) {
    this.clientIdMap = {};
    this.hashRingByClientId;

    this.serverNum = 1;
    this.client;

    if (options.servers && Array.isArray(options.servers)) {
      var servers = options.servers;
      delete options.servers;

      this.serverNum = servers.length;
      this.hashRingByClientId = new HashRing();

      for (var i = 0; i < this.serverNum; i++) {
        this.createRedisClient(merge(options, servers[i]));
      }

    } else if (options.client) {
      this.client = options.client;
    } else if (options.socket) {
      this.client = redis.createClient(options.socket, options);
    } else {
      this.client = this.createRedisClient(options);
    }

    debug('Connect redis, support hash ring: ', !!this.hashRingByClientId);
  };

  RedisStore.prototype.createRedisClient = function (options) {
    var self = this,
      maxAttempt,
      retryMaxDelay = 3000, // 3s
      maxTotalRetryTime = 3600000; // 1h

    options = options || {};
    if (options.max_attempts > 0) {
      maxAttempt = options.max_attempts;
      delete options.max_attempts;
    }
    if (options.connect_timeout > 0) {
      maxTotalRetryTime = options.connect_timeout;
      delete options.connect_timeout;
    }
    if (options.retry_max_delay > 0) {
      retryMaxDelay = options.retryMaxDelay;
      delete options.retryMaxDelay;
    }

    options.retry_strategy = function (data) {
      data = data || {};

      if (data.error && data.error.code === 'ECONNREFUSED') {
        var clientId = data.error.address + ':' + data.error.port;
        debug('Redis retry strategy error, clientId: %s, err: %o', clientId, data.error);
        self.removeClientFromRing(clientId);
      }

      if ((maxTotalRetryTime && data.totalRetryTime > maxTotalRetryTime) || (maxAttempt && data.attempt > maxAttempt)) {
        return undefined;
      }

      var retryDelayTime = data.attempt * 100;
      return retryDelayTime < 100 ? 100 : (retryMaxDelay && retryDelayTime > retryMaxDelay ? retryMaxDelay : 3000);
    }

    var client = redis.createClient(options);

    // logErrors
    if (options.logErrors) {
      // if options.logErrors is function, allow it to override. else provide default logger. useful for large scale deployment
      // which may need to write to a distributed log
      if (typeof options.logErrors != 'function') {
        options.logErrors = function (err) {
          console.error('Warning: connect-redis reported a client error: ' + err);
        };
      }
      client.on('error', options.logErrors);
    }

    if (options.pass) {
      client.auth(options.pass, function (err) {
        if (err) {
          throw err;
        }
      });
    }

    if (options.unref) client.unref();

    if ('db' in options) {
      if (typeof options.db !== 'number') {
        console.error('Warning: connect-redis expects a number for the "db" option');
      }

      client.select(options.db);
      client.on('connect', function () {
        client.select(options.db);
      });
    }

    client.on('error', function (er) {
      debug('Redis returned err', er);

      if (er && (er.code == 'NOAUTH' || (er.origin instanceof Object && er.origin.code === 'ECONNREFUSED')) && self.hashRingByClientId) {
        self.removeClientFromRing(client.address);
      }

      var readyClientList = Object.keys(self.clientIdMap);
      if (!self.hashRingByClientId || readyClientList.length <= 0) {
        self.emit('disconnect', er);
      }
    });

    client.on('ready', function () {
      if (self.hashRingByClientId) {
        self.addClientToRing(client);
      }

      self.clientIdMap[client.address] = client;
      self.emit('connect');
    });

    client.on('reconnecting', function () {
      if (self.hashRingByClientId) {
        self.removeClientFromRing(client.address);
      }
    });

    return client;
  };

  /**
   * 从 hashring 获取节点
   */
  RedisStore.prototype.getClient = function (sid) {
    if (!this.hashRingByClientId) return this.client;

    var clientId = this.hashRingByClientId.get(sid);

    debug('Redis client hash ring, get client address from ring: %s', clientId);

    if (!clientId || !this.clientIdMap[clientId]) {
      throw new Error('connect redis - no useful client');
    }

    var client = this.clientIdMap[clientId];
    debug('Redis client hash ring, select client address: %s, sessionId: %s', client.address, sid);

    return client;
  };

  /**
   * 添加节点到 hashring
   */
  RedisStore.prototype.addClientToRing = function (client) {
    if (!this.hashRingByClientId) return;

    this.clientIdMap[client.address] = client;
    this.hashRingByClientId.add(client.address);

    debug('Redis client hash ring, add client to ring, address: %s', client.address);
  };

  /**
   * 从 hashring 移除节点
   */
  RedisStore.prototype.removeClientFromRing = function (clientId) {
    if (!this.hashRingByClientId) return;

    if (this.hashRingByClientId.has(clientId) || this.clientIdMap[clientId]) {
      this.hashRingByClientId.remove(clientId);
      delete this.clientIdMap[clientId];

      debug('Redis client hash ring, remove client from ring, address: %s', clientId);
    }
  };

  function merge(a, b) {
    if (a && b) {
      for (var key in b) {
        a[key] = b[key];
      }
    }
    return a;
  };

  return RedisStore;
};
