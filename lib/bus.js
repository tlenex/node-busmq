var fs = require('fs');
var events = require('events');
var util = require('util');
var crypto = require('crypto');
var redis = require("redis");
var Logger = require('./logger');
var Connection = require('./connection');
var Queue = require('./queue');
var Channel = require('./channel');
var FederationServer = require('./fedserver');
var Federate = require('./federate');

/**
 * A message bus.
 * Options:
 *  - redis - url string of a single redis to connect to or an array of string urls to connects to. a url must have the form redis://<host>[:<port>]
 *  - federate - object with the following options:
 *    - server - an http/https server object to listen for incoming federate connections. if undefined then federation server will not be open.
 *    - port - the port that the server should listen on. ignored if server is not provided.
 *    - secret - a secret key to authenticate with the federates. this key must be shared among all federates. default is 'notsosecret'.
 * @constructor
 */
function Bus(options) {
  events.EventEmitter.call(this);

  this.id = "bus:" + crypto.randomBytes(8).toString('hex');
  this.logger = new Logger(this.id);

  this.connections = [];

  this.online = false;
  this.eventsCallbacks = {};

  this._options(options);

  this._startFederationServer();
}

util.inherits(Bus, events.EventEmitter);

/**
 * Setup bus options
 * @param options options to set
 * @private
 */
Bus.prototype._options = function(options) {
  this.options = options || {};
  this.withRedis(redis);

  if (!this.options.redis) {
    this.options.redis = 'redis://127.0.0.1';
  }

  if (typeof this.options.redis === 'string') {
    this.options.redis = [this.options.redis];
  }

  if (this.options.logger) {
    this.withLog(this.options.logger);
  }
};

/**
 * Set the logger object
 * @param logger
 */
Bus.prototype.withLog = function(logger) {
  this.logger.withLog(logger);
  return this;
};

/**
 * Set the redis library to use for creating connections
 * @param redis
 */
Bus.prototype.withRedis = function(redis) {
  this._redis = redis;
  return this;
};

Bus.prototype._startFederationServer = function() {
  if (this.options.federate) {
    this.fedServer = new FederationServer(this, this.options.federate);
    this.fedServer.on('listening', function() {
    });
  }
}

/**
 * Connect the bus to the specified urls.
 * Events: error, online, offline
 */
Bus.prototype.connect = function() {
  if (this.connections.length > 0) {
    this.emit('error', 'already connected');
    return;
  }

  this.logger.debug("bus connecting to: " + this.options.redis);

  var _this = this;
  // open the connections to redis
  var readies = 0;
  this.options.redis.forEach(function(url, index) {
    if (url.indexOf('redis:') === 0) {
      var connection = new Connection(index, _this);
      _this.connections.push(connection);
      connection.on("end", function (err) {
        if (readies > 0 && --readies === 0) {
          _this._offline(err);
        }
      });
      connection.on("close", function (err) {
      });
      connection.on("error", function (err) {
        _this.emit('error', err);
      });
      connection.on("connect", function () {
      });
      connection.on("ready", function () {
        _this.logger.debug("connection to " + url + " is ready");
        // emit the ready event only when all connections are ready
        if (++readies === _this.connections.length) {
          _this._online();
        }
      });
      connection.on("subscribe", function (channel, count) {
        if (_this.eventsCallbacks[channel]) {
          _this.eventsCallbacks[channel]('subscribe', channel, count);
        }
      });
      connection.on("unsubscribe", function (channel, count) {
        if (_this.eventsCallbacks[channel]) {
          _this.eventsCallbacks[channel]('unsubscribe', channel, count);
        }
      });
      connection.on("message", function (channel, message) {
        if (_this.eventsCallbacks[channel]) {
          _this.eventsCallbacks[channel]('event', channel, message);
        } else {
          _this.logger.error('received limbo message on ' + channel + '. This should never happen!');
        }
      });
      connection.connect(url);
    } else {
      _this.emit('error', 'unsupported protocol for ' + url);
    }
  });

  if (this.fedServer) {
    this.fedServer.listen(this.options.federate.port);
  }
};


/**
 * Invoked when the bus is online, i.e. all connections are ready
 */
Bus.prototype._online = function() {
  this.online = true;
  this.logger.debug("bus is online");
  this.emit('online');
};

/**
 * Invoked when the bus is offline, i.e. all connections are down
 */
Bus.prototype._offline = function(err) {
  var shouldEmit = this.isOnline();
  this.online = false;
  this.logger.debug("bus is offline");
  if (shouldEmit) {
    this.emit('offline', err);
  }
};

/**
 * Returns whether the bus is online or not
 * @returns {boolean|*}
 */
Bus.prototype.isOnline = function() {
  return this.online;
};

/**
 * Get the next available connection
 * @returns {*}
 * @private
 */
Bus.prototype._connectionFor = function(metadataKey) {
  var sum = 0;
  for (var i = 0; i < metadataKey.length; ++i) {
    sum += metadataKey.charCodeAt(i);
  }
  var index = sum % this.connections.length;
  return this.connections[index];
};

/**
 * Get an existing connection to redis to serve the specified queue.
 * A random connection is chosen if the queue does not exist anywhere.
 * @param metadataKey
 * @param cb
 * @private
 */
Bus.prototype._connection = function(metadataKey, cb) {
  // most of the time the connections state will be stable so the fastest
  // way is to directly search a specific connection.
  // lets calculate the connection id: sum up all of the bytes and modulo the number of connections
  var _this = this;
  var defaultConnection = this._connectionFor(metadataKey);
  var connection = defaultConnection;
  if (connection.isReady()) {
    // check if the queue is in this connection
    connection.exists(metadataKey, function(err, resp) {
      if (err) {
        _this.emit('error', "error searching for existing key: " + err);
      }
      if (resp === 1) {
        // found that this connection is hosting the queue
        cb(connection);
        return;
      }
      // need a full search through all connections
      _search(_this.connections);
    });
  }

  function _search(connections) {
    connection = null;
    var responses = connections.length;
    function _callback() {
      if (--responses === 0) {
        // got a response from all the connections
        if (!connection) {
          // the queue was not found, let's go with the connection we searched in the beginning
          connection = defaultConnection;
        }
        cb && cb(connection);
      }
    }

    // search for the queue in the given connections
    connections.forEach(function(c) {
      if (!c.isReady()) {
        _callback();
      }
      c.exists(metadataKey, function(err, resp) {
        if (err) {
          _this.emit('error', "error searching for existing key " + metadataKey + ": " + err);
        }
        // found it
        if (resp === 1) {
          connection = c;
        }
        _callback();
      });
    });
  }
};

/**
 * Disconnect all redis connections
 */
Bus.prototype.disconnect = function() {
  this.logger.debug("disconnecting");
  this.connections.forEach(function(c) {
    c.disconnect();
  });
  this.connections = [];
  if (this.fedServer) {
    this.fedServer.close();
  }
};

/**
 * Subscribe to events on the specified channels on the provided connection
 * @param connection
 * @param channels
 * @param cb
 * @private
 */
Bus.prototype._subscribe = function(connection, channels, cb) {
  if (!Array.isArray(channels)) {
    channels = [channels];
  }
  var _this = this;
  channels.forEach(function(channel) {
    _this.eventsCallbacks[channel] = cb;
  });
  connection.subscribe(channels, function(err) {
    if (err) {
      _this.emit('error', "error subscribing to channels " + channels + ": " + err);
    }
  });
};

/**
 * Unsubscribe from events on the specified channels on the provided connection
 * @param connection
 * @param channels
 * @private
 */
Bus.prototype._unsubscribe = function(connection, channels) {
  if (!Array.isArray(channels)) {
    channels = [channels];
  }
  var _this = this;
  channels.forEach(function(channel) {
    delete _this.eventsCallbacks[channel];
  });
  connection.unsubscribe(channels, function(err) {
    if (err) {
      _this.emit('error', "error unsubscribing from channels " + channels + ": " + err);
    }
  });
};

/**
 * Create a message queue.
 * @param name
 * @returns {Queue} a Queue object
 */
Bus.prototype.queue = function(name) {
  return new Queue(this, name);
};

/***
 * Create a bi-directional channel.
 * @param name the name of the channel
 * @param local the name of the local endpoint. default is 'client' if calling #connect and 'server' if calling #listen.
 * @param remote the name of the remote endpoint. default is 'server' if calling #connect and 'client' if calling #listen.
 * @returns {Channel} a Channel object
 */
Bus.prototype.channel = function(name, local, remote) {
  return new Channel(this, name, local, remote);
};

/**
 * Federate the specified object to the specified remote bus
 * @param object either queue or channel
 * @param target url of the remote bus
 * @returns {Federate} the federation object
 */
Bus.prototype.federate = function(object, target) {
  var fed = new Federate(object, this.options.federate);
  return fed.to(target);
};

function create(options) {
  return new Bus(options);
}

exports = module.exports.create = create;