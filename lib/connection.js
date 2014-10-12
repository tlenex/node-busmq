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
  'hmset', 'hget', 'hset', 'hsetnx', 'hdel', 'publish', 'eval', 'multi', 'exec'];

/**
 * The commands to issue on read connections
 * @private
 */
var _readCommands = ['brpop', 'subscribe', 'unsubscribe'];

/**
 * A connection to redis.
 * Connects to redis with multiple write connections and read connections.
 * The write connections are used to emit commands to redis.
 * The read connections are used to listen for events.
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
  this.numPhysical = 2;
  this.nextPhysical = 0;
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
}

/**
 * Decrement the number of ready redis connections
 * @returns {number} the new ready counter
 * @private
 */
Connection.prototype._decReady = function() {
  return Math.max(--this.ready, 0);
}

/**
 * Returns true if both redis connections are ready.
 * @returns {boolean} true if both redis connection are ready
 */
Connection.prototype.isReady = function() {
  return this.ready === 2*this.numPhysical;
}

/**
 * Connects to redis at the specified url
 * @param url the url to connect to
 */
Connection.prototype.connect = function(url) {
  if (this.connected) {
    return this.emit('error', 'already connected');
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
  this.write = [];
  this.read = [];
  // we utilize multiple connections for sending commands and receiving events.
  // when issuing commands, we select the physical connection to use in a round robin fashion
  for (var i = 0; i < this.numPhysical; ++i) {
    // this connection is used for everything but listening for events
    this._connect('write', i, _writeCommands, function(event, message) {
      emit(event, message);
    });
    // this connection is used to listen for events.
    this._connect('read', i, _readCommands, function(event, message) {
      emit(event, message);
    });
  }
}

/**
 * Open a physical connection to redis
 * @param type the type of the connection
 * @param index the index to place the connection in
 * @param commands set of commands to map from the {Connection} object to this physical connection
 * @param cb emit redis connection events to this cb
 * @returns a physical connection to redis
 * @private
 */
Connection.prototype._connect = function(type, index, commands, cb) {
  var _this = this;
  var connName = this.id + ":" + type + ":" + index;
  var client = this._redis.createClient(this.port, this.host, {});
  client.retry_backoff = 1;
  client.retry_delay = 1000;
  client.name = connName;

  this._setupListeners(client, type, cb);

  // setup sending the specified commands on the right connection
  commands.forEach(function(c) {
    // expose the commands on our object and delegate to the right redis connection
    _this[c] = function() {
      // we select the actual connection to use in a round robin fashion
      _this.nextPhysical = ++_this.nextPhysical % _this.numPhysical;
      // type will be 'write' or 'read'.
      // in effect, we will get something like: this['write'][2]['lrpop'] to invoke
      // the 'lrpop' command on a 'write' connection located at index 2
      return _this[type][_this.nextPhysical][c].apply(client, arguments);
    }
  });

  // push the connection into the correct physical connections array
  this[type].push(client);
}

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
    active && cb && cb('drain');
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

  this.write.forEach(function(c) {
    c.emit('shutdown');
  });

  this.read.forEach(function(c) {
    c.emit('shutdown');
  });

  this.connected = false;
}

exports = module.exports = Connection;