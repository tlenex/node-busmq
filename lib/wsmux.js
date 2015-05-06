var EventEmitter = require('events').EventEmitter;
var util = require('util');
var crypto = require('crypto');

/**
 * A channel on a mulitplexed websocket
 * @param id the id of the channel
 * @param mux the parent websocket multiplexer
 * @constructor
 */
function WSMuxChannel(id, mux) {
  EventEmitter.call(this);
  this.id = id;
  this.mux = mux;
  this.url = this.mux._url();

  // websocket-stream relies on this method
  this.__defineGetter__('readyState', function() {
    return this.mux._readyState();
  });
}

util.inherits(WSMuxChannel, EventEmitter);

/**
 * shim for websocket-stream
 */
WSMuxChannel.prototype.addEventListener = function(event, cb) {
  this.on(event, cb);
};

/**
 * Send a message on the channel
 * @param message
 */
WSMuxChannel.prototype.send = function(message) {
  this.mux._sendMessage(this.id, message);
};

/**
 * Close this channel
 */
WSMuxChannel.prototype.close = function() {
  this.mux._closeChannel(this.id, true);
};

/**
 * Websocket multiplexer
 * @param ws the webscoket to multiplex
 * @constructor
 */
function WSMux(ws) {
  EventEmitter.call(this);
  this.ws = ws;
  this.channels = {};
  this._setup();
}

util.inherits(WSMux, EventEmitter);

/**
 * Initial setup
 * @private
 */
WSMux.prototype._setup = function() {
  var _this = this;

  function onMessage(message) {
    _this._onMessage(message);
  }

  function onClose() {
    _this.close();
  }

  function onError(err) {
    _this.emit('error', err);
    _this.close();
  }

  function onShutdown() {
    _this.ws.removeListener('message', onMessage);
    _this.ws.removeListener('close', onClose);
    _this.ws.removeListener('error', onError);
    _this.ws.removeListener('shutdown', onShutdown);
    process.nextTick(function() {
      _this.ws.close();
      _this.ws = null;
      _this.emit('close');
    });
  }

  this.ws.on('message', onMessage);
  this.ws.on('close', onClose);
  this.ws.on('error', onError);
  this.ws.on('shutdown', onShutdown);
};

/**
 * Get the websocket ready state
 * @private
 */
WSMux.prototype._readyState = function() {
  return this.ws.readyState;
};

/**
 * Get the websocket ready state
 * @private
 */
WSMux.prototype._url = function() {
  return this.ws.url;
};

/**
 * Create a new channel over the multiplexed websocket
 * @param id
 * @returns {WSMuxChannel}
 */
WSMux.prototype.channel = function(id) {
  return this._createChannel(id, true);
};

/**
 * Create a new channel
 * @param id
 * @param send
 * @returns {WSMuxChannel}
 * @private
 */
WSMux.prototype._createChannel = function(id, send) {
  id = id || crypto.randomBytes(8).toString('hex');
  var channel = new WSMuxChannel(id, this);
  this.channels[id] = channel;
  if (send) {
    // tell the remote end to create a new channel
    this.ws.send(JSON.stringify({id: id, create: true}));
  }
  return channel;
};


/**
 * Send a message on the channel
 * @param id id of the channel
 * @param message the message to send
 * @private
 */
WSMux.prototype._sendMessage = function(id, message) {
  this.ws.send(JSON.stringify({id: id, msg: message, binary: (typeof message !== 'string')}));
};

/**
 * Close a channel
 * @param id the id of the channel to close
 * @param send whether to send the other side that we want to close this channel
 * @private
 */
WSMux.prototype._closeChannel = function(id, send) {
  // test if websocket is open
  if (send && this.ws.readyState === 1) {
    this.ws.send(JSON.stringify({id: id, close: true}));
  }

  var _this = this;
  process.nextTick(function() {
    if (_this.channels[id]) {
      var channel = _this.channels[id];
      delete _this.channels[id];
      channel.emit('close');
    }
  });
};

/**
 * Handle a message received on the weboscket and pass it on to the correct channel
 * @param message
 * @private
 */
WSMux.prototype._onMessage = function(message) {
  var packet = JSON.parse(message);

  var channel = this.channels[packet.id];

  // create a channel
  if (packet.create) {
    if (!channel) {
      channel = this._createChannel(packet.id, false);
      this.channels[packet.id] = channel;
      this.emit('channel', channel);
    }
    return;
  }

  // close the channel
  if (packet.close) {
    if (channel) {
      // because we got the close from the other end, no need to send it over
      this._closeChannel(packet.id, false);
    }
    return;
  }

  // if we don't have a channel, just ignore
  if (channel) {
    var msg = packet.msg;
    // if the message was binary, wrap it in a buffer
    if (packet.binary) {
      msg = {data: new Buffer(msg)};
    }
    channel.emit('message', msg);
  }

};

/**
 * Close this websocket multiplexer
 */
WSMux.prototype.close = function () {
  var _this = this;
  Object.keys(this.channels).forEach(function(id) {
    _this._closeChannel(id, true);
  });
  process.nextTick(function() {
    _this.ws.emit('shutdown');
  });
};

exports = module.exports = WSMux;