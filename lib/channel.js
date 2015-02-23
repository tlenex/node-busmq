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
 * @param options message consumption options (same as Queue#consume)
 */
Channel.prototype.connect = function(options) {
  if (this.isAttached()) {
    this.emit('error', 'cannot connect: channel is already bound');
    return;
  }

  var options = options || {};
  this.options = util._extend({}, options);

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
 * @param options message consumption options (same as Queue#consume)
 */
Channel.prototype.listen = function(options) {
  var remote = this.remote;
  this.remote = this.local;
  this.local = remote;
  this.connect(options);
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
 * Ack the specified message id. applicable only if consuming in reliable mode.
 * @param id the message id to ack
 */
Channel.prototype.ack = function(id) {
  if (!this.isAttached()) {
    return;
  }

  this.qConsumer.ack(id);
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

  var onConsumerError = function(err) {
    if (_this.qConsumer) {
      _this.emit('error', err);
    }
  };

  var onCosumerAttached = function() {
    ++attachedCount;
    if (_this.qConsumer) {
      _attached();
      _this.qConsumer.consume(_this.options);
    }
  };

  var onCosumerMessage = function(msg, id) {
    if (_this.qConsumer) {
      _this._handleMessage(msg, id);
    }
  };

  var onConsumerDetached = function() {
    // register an empty error listener so errors don't throw exceptions
    _this.qConsumer.on('error', function(){});
    _this.qConsumer.removeListener('error', onConsumerError);
    _this.qConsumer.removeListener('attached', onCosumerAttached);
    _this.qConsumer.removeListener('message', onCosumerMessage);
    _this.qConsumer.removeListener('detached', onConsumerDetached);
    _this.qConsumer = null;
    --attachedCount;
    _detached();
  };

  this.qConsumer.on('error', onConsumerError);
  this.qConsumer.on('message', onCosumerMessage);
  this.qConsumer.on('attached', onCosumerAttached);
  this.qConsumer.on('detached', onConsumerDetached);
  this.qConsumer.attach();

  var onProducerError = function(err) {
    if (_this.qProducer) {
      _this.emit('error', err);
    }
  };

  var onProducerAttached = function() {
    ++attachedCount;
    if (_this.qProducer) {
      _attached();
      _this.qProducer.push(':1:');
    }
  };

  var onProducerDetached = function() {
    // register an empty error listener so errors don't throw exceptions
    _this.qProducer.on('error', function(){});
    _this.qProducer.removeListener('error', onProducerError);
    _this.qProducer.removeListener('attached', onProducerAttached);
    _this.qProducer.removeListener('detached', onProducerDetached);
    _this.qProducer = null;
    --attachedCount;
    _detached();
  };

  this.qProducer.on('error', onProducerError);
  this.qProducer.on('attached', onProducerAttached);
  this.qProducer.on('detached', onProducerDetached);
  this.qProducer.attach();
};

/**
 * Handle a message
 * @param msg the received message
 * @private
 */
Channel.prototype._handleMessage = function(msg, id) {
  if (msg.length >= 3 && msg.charAt(0) === ':' && msg.charAt(2) === ':') {
    var type = msg.charAt(1);
    var message = msg.substring(3);
    this.handlers[type].call(this, message, id);
  } else {
    this.emit('error', 'received unrecognized message type');
  }
};

/**
 * Handle a remote:connect event
 * @private
 */
Channel.prototype._onRemoteConnect = function(message, id) {
  id && this.ack(id);
  if (!this._remoteConnected) {
    this._remoteConnected = true;
    this.emit('remote:connect');
  }
};

/**
 * Handle a received message
 * @private
 */
Channel.prototype._onMessage = function(msg, id) {
  // make sure we emit the 'remote:connected' event
  this._onRemoteConnect();
  this.emit('message', msg, id);
};

/**
 * Handle a remote:disconnect event
 * @private
 */
Channel.prototype._onRemoteDisconnect = function(message, id) {
  // puh a new 'connect' message so that a new connecting remote
  // will know we are connected regardless of any other messages we sent
  this.qProducer.push(':1:');
  this.ack(id);
  if (this._remoteConnected) {
    this._remoteConnected = false;
    this.emit('remote:disconnect');
  }
};

/**
 * Handle an end event
 * @private
 */
Channel.prototype._onEnd = function(message, id) {
  this.ack(id);
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
  }
  if (this.qProducer) {
    this.qProducer.detach();
  }
};

exports = module.exports = Channel;