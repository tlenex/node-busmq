var util = require('util');
var events = require('events');
var _url = require('url');
var redis = require("redis");
var Logger = require('./logger');

/**
 * The commands to issue on the main redis connection
 * @private
 */
var _mainCommands = [
  'sadd', 'srem', 'lpush', 'rpush', 'del', 'exists', 'set',
  'rpop', 'lpop', 'lrange', 'ltrim', 'incr', 'decr', 'persist', 'expire',
  'hmset', 'hget', 'hset', 'hsetnx', 'hdel', 'publish', 'eval', 'multi', 'exec'];

/**
 * The commands to issue on the read redis connection
 * @private
 */
var _readCommands = ['brpop', 'subscribe', 'unsubscribe'];

/**
 * A connection to redis.
 * Connects to redis with two connections.
 * The first connection is used to emit commands to redis.
 * The second connection is used to block on popping from queues.
 * This is required because once a connection is used to block on popping from a queue
 * it cannot be used to issue further requests.
 * @param id the id of the connection
 * @param bus the bus object using the connection.
 * @constructor
 */
function Connection(index, bus) {
  events.EventEmitter.call(this);
  this.setMaxListeners(0);
  this.index = index;
  this.id = bus.id + ":connection:" + index;
  this.logger = bus.logger.withTag(this.id);
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
  return --this.ready;
}

/**
 * Returns true if both redis connections are ready.
 * @returns {boolean} true if both redis connection are ready
 */
Connection.prototype.isReady = function() {
  return this.ready === 2;
}

/**
 * Connects to redis at the specified url
 * @param url the url to connect to
 */
Connection.prototype.connect = function(url) {

  this.logger.debug("connecting to: " + url);

  this.url = url;
  var parsed = _url.parse(url)
  this.host = parsed.hostname;
  this.port = parsed.port || 6379;

  // create two connection: main and read
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

  // open the main connection to redis.
  // this connection is used for everything but listening for events
  this.main = this._connect('main', _mainCommands, function(event, message) {
    emit(event, message);
  });

  // open the read connection to redis.
  // this connection is used to listen for events.
  this.read = this._connect('read', _readCommands, function(event, message) {
    emit(event, message);
  });

}

/**
 * Open a physical connection to redis
 * @param name the name of the connection
 * @param commands set of commands to map from the {Connection} object to this physical connection
 * @param cb emit redis connection events to this cb
 * @returns a physical connection to redis
 * @private
 */
Connection.prototype._connect = function(name, commands, cb) {
  var _this = this;
  var connName = this.id + ":" + name;
  var clientLogger = this.logger.withTag(connName);
  var client = redis.createClient(this.port, this.host, {});
  client.retry_backoff = 1;
  client.retry_delay = 1000;
  client.name = this.name;
  client.on("end", function (err) {
    clientLogger.debug("received end: " + err);
    _this._decReady();
    cb && cb('end', err);
  });
  client.on("error", function (err) {
    clientLogger.debug("received error: " + err);
    cb && cb('error', err);
  });
  client.on("connect", function () {
    clientLogger.debug("received connected");
    cb && cb('connect');
  });
  client.on("ready", function () {
    clientLogger.debug("received ready");
    client.client('SETNAME', connName);
    _this._incReady();
    cb && cb('ready');
  });
  client.on('unsubscribe', function (channel, count) {
    _this.emit('unsubscribe', channel, count);
  });
  client.on('subscribe', function (channel, count) {
    _this.emit('subscribe', channel, count);
  });
  client.on('message', function (channel, message) {
    _this.emit('message', channel, message);
  });

  // setup sending the specified commands to the connection
  commands.forEach(function(c) {
    // expose the commands on our object and delegate to the redis connection
    _this[c] = function() {
      return client[c].apply(client, arguments);
    }
  });

  return client;
}

/**
 * Disconnect from redis. Ends both redis connections.
 */
Connection.prototype.disconnect = function() {
  this.logger.debug("disconnecting");
  var _this = this;
  _this.main.end();
  _this.read.end();
  _this.emit('end');
}
//
///**
// * Returns whether this connection is currently waiting on events
// * @returns {boolean|*}
// */
//Connection.prototype.isWaiting = function() {
//  return this.waiting;
//}
//
///**
// * Handle a wait event
// * @param e the event
// * @returns {boolean} true to continue the wait, false to stop waiting
// * @private
// */
//Connection.prototype._handleWaitEvent = function(e) {
//  if (e === 'abort') {
//    this.logger.debug("abort received. stopping wait.");
//    this.emit('aborted');
//    return false;
//  }
//  return true;
//}
//
///**
// * Emit a wait event
// * @param event the event to emit
// * @private
// */
//Connection.prototype._emitWaitEvent = function(event) {
//  var _this = this;
//  this.main.lpush(this.waitKey, event, function(err, resp) {
//    if (err) {
////      _this.logger.debug("error emitting connection event " + event + ": " + err);
//      return _this.emit('error', "error emitting connection event " + event + ": " + err);
//    }
//  });
//  // keep the wait key for 10 seconds before removing it to prevent leaks
//  this.main.expire(this.waitKey, 10, function(err, resp) {
//    if (err) {
////      _this.logger.debug("error setting expiration on connection events key: " + err);
//      return _this.emit('error', "error setting expiration on connection events key: " + err);
//    }
//  });
//}
//
///**
// * Start listening for events on all the specified keys.
// * The callback receives the key and value that triggered the event
// * @private
// */
//Connection.prototype.wait = function(keys, cb) {
//
//  if (this.isWaiting()) {
//    return;
//  }
//
//  if (!this.isReady()) {
////    this.logger.debug("error waiting: connection not ready");
//    this.emit('error', "error waiting: connection not ready");
//  }
//
//  var _this = this;
//  this.waiting = true;
//  // prepend the the connection wait key to be able to abort the wait.
//  // concat(0) means to wait indefinitely
//  this.read.brpop([this.waitKey].concat(keys).concat(0), function(err, resp) {
//    _this.waiting = false;
//    if (err) {
////      _this.logger.debug("error waiting: " + err);
//      return _this.emit('error', "error waiting: " + err);
//    }
//
//    if (resp[0] === _this.waitKey) {
//      var continueToWait = _this._handleWaitEvent(resp[1]);
//      if (!continueToWait) {
//        return;
//      }
//    } else {
//      // invoke the callback
//      keys = cb(resp[0], resp[1]);
//    }
//
//    // continue to wait for more events
//    _this.wait(keys, cb);
//  });
//}
//
///**
// * Abort the wait. No further key events will be received.
// */
//Connection.prototype.abort = function() {
//  this.logger.debug("sending abort event");
//  this._emitWaitEvent('abort');
//}

exports = module.exports = Connection;