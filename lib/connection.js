var util = require('util');
var events = require('events');
var _url = require('url');
var Logger = require('./logger');

/**
 * The commands to issue on write connections
 * @private
 */
var _writeCommands = [
  'sadd', 'srem', 'lpush', 'rpush', 'llen', 'lindex', 'del', 'exists', 'set',
  'rpop', 'lpop', 'lrange', 'ltrim', 'incr', 'decr', 'persist', 'expire',
  'hmset', 'hget', 'hset', 'hsetnx', 'hdel', 'hgetall', 'publish', 'eval', 'evalsha',
  'multi', 'exec', 'script'];

/**
 * The commands to issue on read connections
 * @private
 */
var _readCommands = ['brpop', 'subscribe', 'unsubscribe'];

/**
 * A connection to redis.
 * Connects to redis with a write connection and a read connection.
 * The write connection is used to emit commands to redis.
 * The read connection is used to listen for events.
 * This is required because once a connection is used to wait for events
 * it cannot be used to issue further requests.
 * @param index the index of the connection within the hosting bus
 * @param bus the bus object using the connection
 * @constructor
 */
function Connection(index, bus) {
  events.EventEmitter.call(this);
  this.setMaxListeners(0); // remove limit on listeners
  this.index = index;
  this.id = bus.id + ":connection:" + index;
  this._redis = bus._redis;
  this._logger = bus.logger.withTag(this.id);
  this.connected = false;
  this.ready = 0;
}

util.inherits(Connection, events.EventEmitter);

/**
 * Increment the number of ready redis connections
 * @returns {number} the new ready counter
 * @private
 */
Connection.prototype._incReady = function() {
  return ++this.ready;
};

/**
 * Decrement the number of ready redis connections
 * @returns {number} the new ready counter
 * @private
 */
Connection.prototype._decReady = function() {
  return Math.max(--this.ready, 0);
};

/**
 * Returns true if both redis connections are ready.
 * @returns {boolean} true if both redis connection are ready
 */
Connection.prototype.isReady = function() {
  return this.connected && this.ready === 2;
};

/**
 * Connects to redis at the specified url
 * @param url the url to connect to
 */
Connection.prototype.connect = function(url) {
  if (this.connected) {
    return;
  }

  this._logger.debug("connecting to: " + url);

  this.connected = true;
  this.url = url;
  var parsed = _url.parse(url);
  this.host = parsed.hostname;
  this.port = parsed.port || 6379;

  var _this = this;

  function emit(event, message) {
    // emit the ready event only once when all connections are ready.
    // all other events pass through
    if (event === 'ready') {
      if (_this.isReady()) {
        _this.emit(event, message);
      }
    } else {
      _this.emit(event, message);
    }
  }

  // open physical connections to redis.
  this.clients = {};
  // this connection is used for everything but listening for events
  this._connect('write', _writeCommands, function(event, message) {
    emit(event, message);
  });
  // this connection is used to listen for events.
  this._connect('read', _readCommands, function(event, message) {
    emit(event, message);
  });
};

/**
 * Open a physical connection to redis
 * @param type the type of the connection
 * @param commands set of commands to map from the {Connection} object to this physical connection
 * @param cb emit redis connection events to this cb
 * @returns a physical connection to redis
 * @private
 */
Connection.prototype._connect = function(type, commands, cb) {
  var _this = this;
  var connName = this.id + ":" + type;
  var client = this._redis.createClient(this.port, this.host, {});
  client.retry_backoff = 1;
  client.retry_delay = 1000;
  client.name = connName;

  this._setupListeners(client, type, cb);

  // setup sending the specified commands on the right connection
  commands.forEach(function(c) {
    // expose the commands on our object and delegate to the right redis connection
    _this[c] = function() {
      // type will be 'write' or 'read'.
      // in effect, we will get something like: this['write'][2]['lrpop'] to invoke
      // the 'lrpop' command on a 'write' connection located at index 2
      return _this.clients[type][c].apply(client, arguments);
    }
  });

  // push the connection into the physical connections array
  this.clients[type] = client;
};

/**
 * Hookup event listeners to a redis connection
 * @param client
 * @param type
 * @param cb
 * @private
 */
Connection.prototype._setupListeners = function(client, type, cb) {
  var _this = this;
  var clientLogger = this._logger.withTag(client.name);
  var active = true;
  client.on('shutdown', function() {
    // a requested shutdown by us
    active = false; // no more callbacks
    _this._stopPingPong(client);
    client.end();
    _this._decReady();
    cb && cb('end');
  });
  client.on("end", function (err) {
    clientLogger.debug("received end: " + err);
    _this._stopPingPong(client);
    _this._decReady();
    active && cb && cb('end', err);
  });
  client.on("close", function (err) {
    clientLogger.debug("received close: " + err);
    _this._stopPingPong(client);
    _this._decReady();
    active && cb && cb('close', err);
  });
  client.on("error", function (err) {
    clientLogger.debug("received error: " + err);
    active && cb && cb('error', err);
  });
  client.on("connect", function () {
    clientLogger.debug("received connected");
    active && cb && cb('connect');
  });
  client.on("ready", function () {
    clientLogger.debug("received ready");
    client.client('SETNAME', client.name);
    if (type === 'write') {
      // ping-pong only 'write' connections
      _this._startPingPong(client, clientLogger);
    }
    _this._incReady();
    active && cb && cb('ready');
  });
  client.on('drain', function () {
    if (type === 'write') {
      active && cb && cb('drain');
    }
  });
  client.on('unsubscribe', function (channel, count) {
    active && cb && cb('unsubscribe', channel, count);
  });
  client.on('subscribe', function (channel, count) {
    active && cb && cb('subscribe', channel, count);
  });
  client.on('message', function (channel, message) {
    active && cb && cb('message', channel, message);
  });

};

/**
 * Continuously ping-pongs the server to check that it is still alive
 * @private
 */
Connection.prototype._startPingPong = function(client, clientLogger) {
  if (client._pingPong) {
    return;
  }

  client._pingPong = setInterval(function() {
    client.ping(function(err, resp) {
      if (err) {
        clientLogger.error('error in ping-pong: ' + err);
      }
    });
  }, 1000);
};

/**
 * Stop the ping pong timer
 * @param client
 * @private
 */
Connection.prototype._stopPingPong = function(client) {
  if (!client._pingPong) {
    return;
  }

  clearInterval(client._pingPong);
  client._pingPong = null;
};

/**
 * Disconnect from redis. Ends all redis connections.
 */
Connection.prototype.disconnect = function() {
  this._logger.debug("disconnecting");

  this.clients.write.emit('shutdown');
  this.clients.read.emit('shutdown');

  this.connected = false;
};

exports = module.exports = Connection;