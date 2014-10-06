var events = require('events');
var util = require('util');
var Queue = require('./queue');

/**
 * Creates a new bi-directional message channel between two endpoints utilizing message queues.
 * @param bus
 * @param name
 * @param local
 * @param remote
 * @constructor
 */
function Channel(bus, name, local, remote) {
  events.EventEmitter.call(this);
  this.bus = bus;
  this.name = name;
  this.type = 'channel';
  this.id = 'bus:channel:' + name;
  this.logger = bus.logger.withTag(name);
  this.local = local || 'local';
  this.remote = remote || 'remote';

  this.handlers = {
    '1': this._onRemoteConnect,
    '2': this._onMessage,
    '3': this._onRemoteDisconnect,
    '4': this._onEnd
  }
}

util.inherits(Channel, events.EventEmitter);

/**
 * Connect to the channel, using the 'local' role to consume messages and 'remote' role to send messages.
 *   connect - emitted when this endpoint is connected to the channel
 *   remote:connect - emitted when the remote endpoint connects to the channel
 *   disconnect - emitted when this endpoint is disconnected from the channel
 *   remote:disconnect - emitted when the remote endpoint is disconnected from the channel
 *   message - emitted when a message is received from the peer
 *   end - emitted when the peer disconnects from the channel
 */
Channel.prototype.connect = function() {
  if (this.isAttached()) {
    this.emit('error', 'cannot connect: channel is already bound');
    return;
  }

  this.qConsumer = new Queue(this.bus, 'channel:' + this.local + ':' + this.name);
  this.qProducer = new Queue(this.bus, 'channel:' + this.remote + ':' + this.name);
  this._attachQueues();
};

Channel.prototype.attach = Channel.prototype.connect;

/**
 * Connect to the channel as a "listener", using the 'local' role to send messages and 'remote' role to consume messages.
 * This is just a syntactic-sugar for a connect with a reverse semantic of the local/remote roles.
 * Events:
 *   connect - emitted when this endpoint is connected to the channel
 *   remote:connect - emitted when the remote endpoint connects to the channel
 *   disconnect - emitted when this endpoint is disconnected from the channel
 *   remote:disconnect - emitted when the remote endpoint is disconnected from the channel
 *   message - emitted when a message is received from the peer
 *   end - emitted when the peer disconnects from the channel
 */
Channel.prototype.listen = function() {
  var remote = this.remote;
  this.remote = this.local;
  this.local = remote;
  this.connect();
};


/**
 * Send a message to the other endpoint
 * @param message the message to send
 */
Channel.prototype.send = function(message) {
  if (!this.isAttached()) {
    return;
  }

  this.qProducer.push(':2:'+message);
};

/**
 * Send a message to the specified endpoint without connecting to the channel
 * @param endpoint
 * @param msg
 * @private
 */
Channel.prototype.sendTo = function(endpoint, msg) {
  var _this = this;
  var q = new Queue(this.bus, 'channel:' + endpoint + ':' + this.name);
  q.on('error', function(err) {
    _this.logger.error('error on channel sendTo ' + q.name + ': ' + err);
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
};

/**
 * Disconnect this endpoint from the channel without sending the 'end' event to the remote endpoint.
 * The channel will remain open.
 */
Channel.prototype.disconnect = function() {
  if (!this.isAttached()) {
    return;
  }

  this.qProducer.push(':3:');
  this._detachQueues();
};

Channel.prototype.detach = Channel.prototype.disconnect;

/**
 * Ends the channel and causes both endpoints to disconnect.
 * No more messages can be sent or will be received.
 */
Channel.prototype.end = function() {
  if (!this.isAttached()) {
    return;
  }

  this.qProducer.push(':4:');
  this._detachQueues();
};

/**
 * Returns whether this channel is bound to the message queues
 */
Channel.prototype.isAttached = function() {
  return this.qProducer && this.qConsumer;
};

/**
 * Attach to the bus queues
 * @private
 */
Channel.prototype._attachQueues = function() {
  var _this = this;
  var attachedCount = 0;

  function _attached() {
    if (attachedCount === 2) {
      _this.emit('connect');
    }
  }

  function _detached() {
    if (attachedCount === 0) {
      _this.emit('disconnect');
    }
  }

  this.qConsumer.on('error', function(err) {
    if (_this.isAttached()) {
      _this.emit('error', err);
    }
  });
  this.qConsumer.on('attached', function() {
    ++attachedCount;
    // make sure the channel wasn't closed in the mean time
    if (_this.isAttached()) {
      _attached();
      _this.qConsumer.consume();
    }
  });
  this.qConsumer.on('message', function(msg) {
    if (_this.isAttached()) {
      _this._handleMessage(msg);
    }
  });
  this.qConsumer.on('detached', function() {
    --attachedCount;
    if (_this.isAttached()) {
      _detached();
    }
  });
  this.qConsumer.attach();

  this.qProducer.on('error', function(err) {
    if (_this.isAttached()) {
      _this.emit('error', err);
    }
  });
  this.qProducer.on('attached', function() {
    ++attachedCount;
    if (_this.isAttached()) {
      _attached();
      _this.qProducer.push(':1:');
    }
  });
  this.qProducer.on('detached', function() {
    --attachedCount;
    if (_this.isAttached()) {
      _detached();
    }
  });
  this.qProducer.attach();
};

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
};

/**
 * Handle a received remote:connect event
 * @private
 */
Channel.prototype._onRemoteConnect = function() {
  if (!this._remoteConnected) {
    this._remoteConnected = true;
    this.emit('remote:connect');
  }
};

/**
 * Handle a received message
 * @private
 */
Channel.prototype._onMessage = function(msg) {
  // make sure we emit the 'remote:connected' event
  this._onRemoteConnect();
  this.emit('message', msg);
};

/**
 * Handle a received remote:disconnect event
 * @private
 */
Channel.prototype._onRemoteDisconnect = function() {
  // puh a new 'connect' message so that a new connecting remote
  // will know we are connected regardless of any other messages we sent
  this.qProducer.push(':1:');
  if (this._remoteConnected) {
    this._remoteConnected = false;
    this.emit('remote:disconnect');
  }
};

/**
 * Handle a received end event
 * @private
 */
Channel.prototype._onEnd = function() {
  this._detachQueues();
  this.emit('end');
};

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
};

exports = module.exports = Channel;