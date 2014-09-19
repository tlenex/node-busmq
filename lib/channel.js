var events = require('events');
var util = require('util');
var Logger = require('./logger');
var Queue = require('./queue');

/**
 Creates a new bi-directional message channel between two peers utilizing message queues.
 */
function Channel(bus, name) {
  events.EventEmitter.call(this);
  this.bus = bus;
  this.name = name;

  this.handlers = {
    '1': this._onConnect,
    '2': this._onMessage,
    '3': this._onDisconnect,
    '4': this._onEnd
  }
}

util.inherits(Channel, events.EventEmitter);

/**
 * Wait for the channel peer to connect.
 * Events:
 *   connect - emitted when the peer connects to the channel
 *   message - emitted when a message is received from the peer
 *   end - emitted when the peer disconnects from the channel
 */
Channel.prototype.listen = function() {
  if (this.isBound()) {
    this.emit('error', 'cannot listen: channel is already bound');
    return;
  }

  this.qConsumer = new Queue(this.bus, 'channel:server:' + this.name);
  this.qProducer = new Queue(this.bus, 'channel:client:' + this.name);
  this._attachQueues();
};

/**
 * Connect to the channel endpoint.
 *   connect - emitted when connected to the channel and the server is ready to accept messages
 *   message - emitted when a message is received from the peer
 *   end - emitted when the server disconnects from the channel
 */
Channel.prototype.connect = function() {
  if (this.isBound()) {
    this.emit('error', 'cannot connect: channel is already bound');
    return;
  }

  this.qConsumer = new Queue(this.bus, 'channel:client:' + this.name);
  this.qProducer = new Queue(this.bus, 'channel:server:' + this.name);
  this._attachQueues();
};

/**
 * Send a message to the peer
 * @param message the message to send
 */
Channel.prototype.send = function(message) {
  if (!this.isBound()) {
    return;
  }

  this.qProducer.push(':2:'+message);
}

/**
 * Send a message to the server without connecting to the channel
 * @param msg the message to send
 */
Channel.prototype.sendToServer = function(msg) {
  this._sendTo('server', msg);
}

/**
 * Send a message to the client without connecting to the channel
 * @param msg the message to send
 */
Channel.prototype.sendToClient = function(msg) {
  this._sendTo('client', msg);
}

/**
 * Send a message to the specified endpoint
 * @param endpoint
 * @param msg
 * @private
 */
Channel.prototype._sendTo = function(endpoint, msg) {
  var _this = this;
  var q = new Queue(this.bus, 'channel:' + endpoint + ':' + this.name);
  q.on('error', function(err) {
    _this._logger.error('error on replay in queue ' + q.name + ': ' + err);
    q.detach();
  });
  q.on('attached', function() {
    try {
      q.push(':2:' + msg);
    } finally {
      q.detach();
    }
  });
  q.attach();
}

/**
 * Disconnect this endpoint from the channel without sending the 'end' event to the peer.
 * The channel will remain alive and allow for a different peer to attach as the server or client.
 */
Channel.prototype.disconnect = function() {
  if (!this.isBound()) {
    return;
  }

  this.qProducer.push(':3:');
  this._detachQueues();
}


/**
 * Ends the channel. no more messages can be sent or will be received.
 */
Channel.prototype.end = function() {
  if (!this.isBound()) {
    return;
  }

  this.qProducer.push(':4:');
  this._detachQueues();
}

/**
 * Returns whether this channel is bound to the message queues
 */
Channel.prototype.isBound = function() {
  return this.qProducer && this.qConsumer;
}

/**
 * Attach to the bus queues
 * @private
 */
Channel.prototype._attachQueues = function() {
  var _this = this;
  var attachedCount = 0;

  function _attached() {
    if (attachedCount === 2) {
      _this.emit('ready');
    }
  }

  function _detached() {
    if (attachedCount === 0) {
      _this.emit('disconnect');
    }
  }

  this.qConsumer.on('error', function(err) {
    if (_this.isBound()) {
      _this.emit('error', err);
    }
  });
  this.qConsumer.on('attached', function() {
    ++attachedCount;
    // make sure the channel wasn't closed in the mean time
    if (_this.isBound()) {
      _this.qConsumer.consume();
      _attached();
    }
  });
  this.qConsumer.on('message', function(msg) {
    if (_this.isBound()) {
      _this._handleMessage(msg);
    }
  });
  this.qConsumer.on('detached', function() {
    --attachedCount;
    if (_this.isBound()) {
      _detached();
    }
  });
  this.qConsumer.attach();

  this.qProducer.on('error', function(err) {
    if (_this.isBound()) {
      _this.emit('error', err);
    }
  });
  this.qProducer.on('attached', function() {
    ++attachedCount;
    if (_this.isBound()) {
      _this.qProducer.push(':1:');
      _attached();
    }
  });
  this.qProducer.on('detached', function() {
    --attachedCount;
    if (_this.isBound()) {
      _detached();
    }
  });
  this.qProducer.attach();
}

/**
 * Handle a message
 * @param msg the received message
 * @private
 */
Channel.prototype._handleMessage = function(msg) {
  if (msg.length >= 3 && msg.charAt(0) === ':' && msg.charAt(2) === ':') {
    var type = msg.charAt(1);
    var message = msg.substring(3);
    this.handlers[type].call(this, message);
  } else {
    this.emit('error', 'received unrecognized message type');
  }
}

/**
 * Handle a received connect event
 * @private
 */
Channel.prototype._onConnect = function() {
  this.emit('connect');
}

/**
 * Handle a received message
 * @private
 */
Channel.prototype._onMessage = function(msg) {
  this.emit('message', msg);
}

/**
 * Handle a received disconnect event
 * @private
 */
Channel.prototype._onDisconnect = function() {
  this.emit('disconnect');
}

/**
 * Handle a received end event
 * @private
 */
Channel.prototype._onEnd = function() {
  this._detachQueues();
  this.emit('end');
}

/**
 * Detach from the bus queues
 * @private
 */
Channel.prototype._detachQueues = function() {
  if (this.qConsumer) {
    this.qConsumer.detach();
    this.qConsumer = null;
  }
  if (this.qProducer) {
    this.qProducer.detach();
    this.qProducer = null;
  }
}

exports = module.exports = Channel;