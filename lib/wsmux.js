var EventEmitter = require('events').EventEmitter;
var util = require('util');
var crypto = require('crypto');
var WebSocket = require('ws');

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
  this.url = this.mux.url;

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
 * @param urlOrWs the url to open the webscoket to multiplex
 * @param options
 * @constructor
 */
function WSMux(urlOrWs, options) {
  EventEmitter.call(this);
  this.options = util._extend({}, options);
  this.closed = false;
  this.channels = {};
  this.pending = [];
  this._openWebsocket(urlOrWs);
}

util.inherits(WSMux, EventEmitter);

/**
 * attach a websocket to this multiplexer
 * @private
 */
WSMux.prototype._openWebsocket = function(urlOrWs) {

  var url, ws, reopen;
  if (typeof urlOrWs === 'string') {
    reopen = true;
    url = urlOrWs;
    ws = new WebSocket(url + '?secret=' + this.options.secret);
  } else {
    reopen = false;
    ws = urlOrWs;
    url = ws.url;
  }

  this.url = url;
  var _this = this;

  var heartbeatTimer;
  function clearHeartbeat() {
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
  }

  function onOpen() {
    heartbeatTimer = setInterval(function() {
      ws.ping('hb', {}, true);
    }, 10*1000);

    // send any pending messages
    _this.pending.forEach(function(msg) {
      _this._wsSend(msg);
    });
    _this.pending = [];

    _this.emit('open');
  }

  function onClose(code) {
    replace();
    if (_this.closed || !reopen) {
      _this.emit('close', code);
    }
  }

  function onError(error) {
    _this.emit('error', error);
    replace(_this.options.replaceDelay);
  }

  function onUnexpectedResponse(req, res) {
    shutdown();
    // 401 means wrong secret key
    if (res.statusCode === 401) {
      _this.emit('fatal', 'unauthorized');
      // do not replace the websocket
    } else {
      _this.emit('error', 'websocket received unexpected response: ' + res.statusCode);
      // try to open the websocket again
      replace(_this.options.replaceDelay);
    }
  }

  function onMessage(message) {
    _this._onMessage(message);
  }

  function shutdown() {
    clearHeartbeat();
    if (ws) {
      ws.removeListener('open', onOpen);
      ws.removeListener('close', onClose);
      ws.removeListener('error', onError);
      ws.removeListener('unexpected-response', onUnexpectedResponse);
      ws.removeListener('message', onMessage);
      ws.removeListener('replace', replace);
      ws.removeListener('shutdown', shutdown);
      ws.close();
      ws = null;
      _this.ws = null;
    }
  }

  function replace(delay) {
    shutdown();

    if (_this.closed || !reopen) {
      return;
    }

    _this.emit('reopen');

    // open a new websocket.
    // we either do it immediately or delay it by the amount specified
    setTimeout(function() {
      _this._openWebsocket(url);
    }, delay || 0);
  }

  ws.on('open', onOpen);
  ws.on('close', onClose);
  ws.on('error', onError);
  ws.on('unexpected-response', onUnexpectedResponse);
  ws.on('message', onMessage);
  ws.on('replace', replace);
  ws.on('shutdown', shutdown);

  this.ws = ws;

};

/**
 * Get the websocket ready state
 * @private
 */
WSMux.prototype._readyState = function() {
  return this.ws.readyState;
};

/**
 * Create a new channel over the multiplexed websocket
 * @param id
 * @returns {WSMuxChannel}
 */
WSMux.prototype.channel = function(id) {
  return this._createChannel(id);
};

/**
 * Send a message now or later when the websocket is open
 * @param msg
 * @private
 */
WSMux.prototype._wsSend = function(msg) {
  if (this.ws && this.ws.readyState === 1) {
    this.ws.send(JSON.stringify(msg));
  } else {
    this.pending.push(msg);
  }
};

/**
 * Create a new channel
 * @param id
 * @returns {WSMuxChannel}
 * @private
 */
WSMux.prototype._createChannel = function(id) {
  id = id || crypto.randomBytes(8).toString('hex');
  var channel = new WSMuxChannel(id, this);
  this.channels[id] = channel;
  return channel;
};


/**
 * Send a message on the channel
 * @param id id of the channel
 * @param message the message to send
 * @private
 */
WSMux.prototype._sendMessage = function(id, message) {
  var binary = typeof message !== 'string';
  if (binary) {
    // convert to a string to make the payload smaller
    message = message.toString();
  }
  this._wsSend({id: id, msg: message, binary: binary});
};

/**
 * Close a channel
 * @param id the id of the channel to close
 * @param send whether to send the other side that we want to close this channel
 * @private
 */
WSMux.prototype._closeChannel = function(id, send) {
  if (send) {
    this._wsSend({id: id, close: true});
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

  // close the channel
  if (packet.close) {
    if (channel) {
      // because we got the close from the other end, no need to send it over
      this._closeChannel(packet.id, false);
    }
    return;
  }

  // if we don't have a channel, create one now
  if (!channel) {
    channel = this._createChannel(packet.id);
    this.channels[packet.id] = channel;
    this.emit('channel', channel);
  }

  var msg = packet.msg;
  // if the message was binary, wrap it in a buffer
  if (packet.binary) {
    msg = {data: new Buffer(msg)};
  }
  channel.emit('message', msg);
};

/**
 * Close this websocket multiplexer
 */
WSMux.prototype.close = function () {
  this.closed = true;
  var _this = this;
  Object.keys(this.channels).forEach(function(id) {
    _this._closeChannel(id, true);
  });
  process.nextTick(function() {
    _this.emit('shutdown');
  });
};

exports = module.exports = WSMux;