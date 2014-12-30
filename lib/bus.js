var fs = require('fs');
var events = require('events');
var util = require('util');
var crypto = require('crypto');
var redis = require("redis");
var Logger = require('./logger');
var Connection = require('./connection');
var WSPool = require('./wspool');
var Queue = require('./queue');
var Channel = require('./channel');
var Persistify = require('./persistify');
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

  this._loadScripts();
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

  this.wspool = new WSPool(this, this.options.federate);
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
};

/**
 * Load the lua scripts used for managing the queue
 * @private
 */
Bus.prototype._loadScripts = function() {
  this.scripts = {};
  this.scriptsToLoad = 0;
  this.scriptsLoaded = false;
  this._readScript('push');
  this._readScript('pop');
  this._readScript('ack');
  this._readScript('index');
};

/**
 * Read a single script to memory
 * @param name the script name to load
 * @private
 */
Bus.prototype._readScript = function(name) {
  ++this.scriptsToLoad;
  var _this = this;
  var file = 'lib/lua/'+name+'.lua';
  fs.readFile(file, function(err, content) {
    if (err) {
      _this.logger.error('error reading lua script ' + file + ': ' + JSON.stringify(err));
      _this.emit('error', err);
      return;
    }
    _this.scripts[name] = {content: content.toString().trim(), name: name};
    if (--_this.scriptsToLoad === 0) {
      _this.scriptsLoaded = true;
      _this._online();
    }
  });
};

/**
 * Get the hash of a script by name
 * @param name
 * @returns {*}
 * @private
 */
Bus.prototype._script = function(name) {
  return this.scripts[name].hash;
};

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
          _this.connectionsReady = false;
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
          _this.connectionsReady = true;
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
  if (this.connectionsReady && this.scriptsLoaded) {
    var _this = this;
    // load the scripts to redis
    this.connections.forEach(function(connection) {
      Object.keys(_this.scripts).forEach(function(key) {
        var script = _this.scripts[key];
        // calculate the hash of the script
        script.hash = crypto.createHash("sha1").update(script.content).digest("hex");
        // send the script to redis
        connection.script('load', script.content, function(err, resp) {
          if (err) {
            _this.emit('error', 'failed to load script ' + script.name + ' to redis: ' + err);
          }
        });
      });
    });
    this.online = true;
    this.logger.debug("bus is online");
    this.emit('online');
  }
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
Bus.prototype._connectionFor = function(key) {
  var sum = 0;
  for (var i = 0; i < key.length; ++i) {
    sum += key.charCodeAt(i);
  }
  var index = sum % this.connections.length;
  return this.connections[index];
};

/**
 * Get an existing connection to redis to serve the specified key.
 * A random connection is chosen if the key does not exist anywhere.
 * @param key
 * @param cb
 * @private
 */
Bus.prototype._connection = function(key, cb) {
  // most of the time the connections state will be stable so the fastest
  // way is to directly search a specific connection.
  // lets calculate the connection id: sum up all of the bytes and modulo the number of connections
  var _this = this;
  var defaultConnection = this._connectionFor(key);
  var connection = defaultConnection;
  if (connection.isReady()) {
    // check if the queue is in this connection
    connection.exists(key, function(err, resp) {
      if (err) {
        _this.emit('error', "error searching for existing key: " + err);
      }
      if (resp === 1) {
        // found that this connection is hosting the key
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
          // the key was not found, let's go with the connection we searched in the beginning
          connection = defaultConnection;
        }
        cb && cb(connection);
      }
    }

    // search for the key in the given connections
    connections.forEach(function(c) {
      if (!c.isReady()) {
        _callback();
      }
      c.exists(key, function(err, resp) {
        if (err) {
          _this.emit('error', "error searching for existing key " + key + ": " + err);
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
 * Disconnect all redis connections, close the fedserver and close all the wspool websocket connections
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
  this.wspool.close();
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
 * Persistify the provided object, enabling the object to be persisted to redis
 * @param name name of the object
 * @param object the object to persist
 * @param attributes array of property names to persist in the object. The properties will be automatically defined on the object.
 * @returns {*}
 */
Bus.prototype.persistify = function(name, object, attributes) {
  return Persistify(this, name, object, attributes);
};

/**
 * Federate an object to the specified remote bus
 * @param object queue, channel or persisted object
 * @param target string url of the remote bus to federate to
 * @returns {Federate} the federation object. It's possible to use the federation object only after the 'ready' event is emitted.
 */
Bus.prototype.federate = function(object, target) {
  var fed = new Federate(object, this.options.federate);
  this.wspool.get(target, function(err, ws) {
    if (err) {
      if (err === 'unauthorized') {
        fed.emit('unauthorized');
      } else {
        fed.emit('error', err);
      }
      return;
    }
    fed.to(ws);
  });
  return fed;
};

function create(options) {
  return new Bus(options);
}

exports = module.exports.create = create;