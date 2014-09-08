var events = require('events');
var util = require('util');
var crypto = require('crypto');
var Logger = require('./logger');

/**
 * Queue. Do not instantiate directly, instead use {Bus#queue} to create a new Queue.
 * @param bus
 * @param name
 * @constructor
 */
function Queue(bus, name) {
  events.EventEmitter.call(this);
  this.bus = bus;
  this.id = "bus:queue:" + name;
  this.logger = bus.logger.withTag(this.id);
  this.name = name;
  this.attached = false;
  this.messagesKey = this.id + ":messages";
  this.metadataKey = this.id + ":metadata";
  this.messageAvailableChannel = this.id + ":available";
  this.qKeys = [this.metadataKey, this.messagesKey];
  this.toucher = null;
  this._pushed = 0;
  this._consumed = 0;
}

util.inherits(Queue, events.EventEmitter);

/**
 * Setup association to the connection
 * @private
 */
Queue.prototype._connect = function(cb) {
  if (this.connection) {
    return cb && cb();
  }

  var _this = this;
  var consuming = false;

  // get a connection to use for this queue.
  // it is expected that if the queue already exists
  // in one of the redis servers then a connection to
  // that server will be provided
  this.bus._connection(this.metadataKey, function(connection) {
    if (!connection) {
      return _this.emit('error', 'no connection available');
    }

    function _endOrClose() {
      consuming = _this.isConsuming();
      if (_this.isAttached()) {
        _this.detach();
      }
    }

    _this.connection = connection;
    _this.connection.on('end', _endOrClose);
    _this.connection.on('close', _endOrClose);
    _this.connection.on('drain', function() {
      _this.emit('drain');
    });
    _this.connection.on('ready', function() {
      //_this.logger.debug("connection to queue resumed");
      _this.attach(function() {
        if (consuming) {
          _this.consume();
        }
      });
    });
    cb && cb();
  });
}

/**
 * Emit the attached event. This sets an interval timer to continuously refresh the expiration of the queue keys.
 * @private
 */
Queue.prototype._emitAttached = function() {
  if (this.isAttached()) {
    return;
  }

  var _this = this;
  this.ttl(function(ttl) {
    if (!ttl) {
      return _this.emit('error', 'queue not found');
    }
    _this.attached = true;
    // start the touch timer.
    // we touch the various queue keys to keep them alive for as long as we're attached.
    // the interval time is a third of the ttl time.
    var touchInterval = ttl / 3;
    _this._touch(ttl);
    _this.toucher = setInterval(function() {
      _this._touch(ttl);
    }, (touchInterval * 1000));
    _this.emit('attached');
  });
}

/**
 * set the queue keys expiration to the ttl time (to keep them alive)
 * @param ttl
 * @private
 */
Queue.prototype._touch = function(ttl) {
  var _this = this;
  this.qKeys.forEach(function(key) {
    _this.connection.expire(key, ttl, function(err, resp) {
      if (err) {
        return _this.emit('error', "error setting key " + key + " expiration to " + ttl + ": " + err);
      }
    });
  })
}

/**
 * Emit the detached event. Clears the interval timer that refreshes the queue keys expiration
 * @private
 */
Queue.prototype._emitDetached = function() {
  if (!this.isAttached()) {
    return;
  }
  clearInterval(this.toucher);
  this.toucher = null;
  this.attached = false;
  this.emit('detached');
}

/**
 * Return true if we are attached to the queue
 */
Queue.prototype.isAttached = function() {
  return this.attached;
}

/**
 * Attach to a queue, optionally setting some options.
 * Events:
 *  - attaching - starting to attach to the queue
 *  - attached - messages can be pushed to the queue or consumed from it
 *  - error - some error occurred
 */
Queue.prototype.attach = function(options) {
  if (this.isAttached()) {
    return this.emit('error', 'cannot attach to queue: already attached');
  }

  var _this = this;
  function _attach() {
    _this.emit('attaching');
    options = options || {};
    options.ttl = options.ttl || 30; // default ttl of 30 seconds

    _this.exists(function(exists) {
      if (!exists) {
        // set options for the queue, creating the metadata key
        return _this.option('ttl', options.ttl, function() {
          _this._emitAttached();
        });
      }
      // queue already exists, do not set options
      _this._emitAttached();
    });
  };

  this._connect(_attach);
}


/**
 * Detach from the queue.
 * No further messages can be pushed or consumed until attached again.
 * This does not remove the queue. Other clients may still push and consume messages from the queue.
 * If this is the last client that detaches, then the queue will automatically be destroyed if no
 * client attaches to it within the defined ttl of the queue.
 * Events:
 *  - detached - detached from the queue
 *  - error - some error occurred
 */
Queue.prototype.detach = function() {
  if (!this.isAttached()) {
    return this.emit('error', 'cannot detach from queue: already detached');
  }

  this.emit('detaching');
  this.stop();
  this._emitDetached();
};

/**
 * Get the ttl of the queue. The ttl is the time in seconds for the queue to live without any clients attached to it.
 * @param cb receives the ttl
 */
Queue.prototype.ttl = function(cb) {
  this.option('ttl', function(ttl) {
    if (ttl !== null) {
      ttl = parseInt(ttl);
    }
    cb && cb(ttl);
  });
};

/**
 * Get or set an option by name
 * @param name the option name. If a value is not provided, the option will be retrieved.
 * @param value if a value is provided, the option will be set to the value
 * @param cb if getting, will be provided with the value, if setting will be called upon success.
 */
Queue.prototype.option = function(name, value, cb) {
  var _this = this;
  if (typeof value === 'function') {
    cb = value;
    value = null;
  }

  if (value) {
    this.connection.hsetnx(this.metadataKey, name, value, function(err) {
      if (err) {
        return _this.emit('error', "error creating queue metadata: " + err);
      }
      cb && cb();
    });
  } else {
    this.connection.hget(this.metadataKey, name, function(err, resp) {
      if (err) {
        return _this.emit('error', "error reading metadata option " + name + ": " + err);
      }
      cb && cb(resp);
    });
  }
};

/**
 * Closes the queue and destroys all pending messages. No more messages can be pushed or consumed.
 * Clients attempting to attach to the queue will receive the closed event.
 * Clients currently attached to the queue will receive the closed event.
 * Events:
 *  - error - some error occurred
 *  - closed - the queue was closed
 */
Queue.prototype.close = function() {
  if (!this.isAttached()) {
    var err = 'not attached';
    return this.emit('error', "error closing queue: " + err);
  }

  var _this = this;
  this.detach();

  var closes = 0;

  // delete the metadata key
  this.qKeys.forEach(function(key) {
    ++closes;
    _this._deleteKey(key, function() {
      if (--closes === 0) {
        _this.emit('closed');
      }
    });
  });
};

/**
 * Delete a key from redis
 * @private
 */
Queue.prototype._deleteKey = function(key, cb) {
  var _this = this;
  this.connection.del(key, function(err, resp) {
    if (err) {
      _this.emit('error', "error deleting key: " + err);
    }
    cb && cb(resp);
  });
}

/**
 * Check if the queue exists.
 * @param cb receives true if the queue exists and false if not
 */
Queue.prototype.exists = function(cb) {
  var _this = this;
  function _exists() {
    // check if the metadata exists, as the queue itself might not contain any messages
    // meaning that it doesn't actually exist
    _this.connection.exists(_this.metadataKey, function(err, exists) {
      if (err) {
        return _this.emit('error', "error checking if queue exists: " + err);
      }
      cb && cb(exists === 1);
    });
  }
  this._connect(_exists);

};

/**
 * Push a message to the queue.
 * The message will remain in the queue until a consumer reads it
 * or until the queue is closed or until it expires.
 * @param message string or object
 * @param cb invoked when the push was actually performed
 * @return returns true if the commands are successfully flushed to the kernel for immediate sending,
 *         and false if the buffer is full and the commands are queued to be sent when the buffer is ready again
 */
Queue.prototype.push = function(message, cb) {
  if (!this.isAttached()) {
    var err = 'not attached';
    return this.emit('error', "error pushing to queue: " + err);
  }

  if (typeof message === 'object') {
    message = JSON.stringify(message);
  }

  // push the message to the queue
  var _this = this;
  var pushed = ++_this._pushed;
  var multi = this.connection.multi();
  multi.rpush(this.messagesKey, message, function(err, resp){
    if (err) {
      return _this.emit('error', "error pushing to queue (rpush): " + err);
    }
  });
  multi.publish(this.messageAvailableChannel, pushed, function(err, resp){
    if (err) {
      return _this.emit('error', "error pushing to queue (publish): " + err);
    }
//    console.log('[%s] pushed %s', _this.name, pushed);
    cb && cb();
  });
  return multi.exec(function(err) {
    if (err) {
      return _this.emit('error', "error pushing to queue (exec): " + err);
    }
  });

};

/**
 * Returns the number of messages pushed to this queue
 * @returns {number}
 */
Queue.prototype.pushed = function() {
  return this._pushed;
}

/**
 * Returns the number of messages consumed by this queue
 * @returns {number}
 */
Queue.prototype.consumed = function() {
  return this._consumed;
}

/**
 * Set the consuming state and emit it
 * @param state
 * @private
 */
Queue.prototype._consuming = function(state) {
  this.consuming = state;
  this.emit('consuming', state);
}

/**
 * Stop consuming messages. This will prevent further reading of messages from the queue.
 * Events:
 *  - consuming - the new consuming state, which will be false when no longer consuming
 *  - error - on some error
 */
Queue.prototype.stop = function() {
  if (!this.isConsuming()) {
    return;
  }

  this.bus._unsubscribe(this.connection, this.messageAvailableChannel);
  this._consuming(false);
}

/**
 * Returns true of this queue is consuming messages
 */
Queue.prototype.isConsuming = function() {
  return this.consuming;
}

/**
 * Read a single message from the queue.
 * Will continue to read messages until there are no more messages to read.
 * @private
 */
Queue.prototype._consumeMessages = function() {
  if (!this.isConsuming()) {
    return;
  }

  this._popping = true;
  var _this = this;
  function _pop() {
    _this.connection.lpop(_this.messagesKey, function(err, message) {
      if (err) {
        return _this.emit('error', "error consuming message: " + err);
      }

      if (message) {
        ++_this._consumed;
  //      console.log('[%s] consumed %s', _this.name, _this._consumed);
        // emit the message to the consumer
        _this.emit('message', message);
        // go fetch another one
        _pop();
      } else if (_this._messageAvailable) {
        // we received a push event to the queue while we were popping.
        // to make sure the event wasn't received between the time that
        // redis return a null message
        _this._messageAvailable = false;
        _pop();
      } else {
        _this._popping = false;
      }
    });
  }
  _pop();
}

/**
 * handle the event that a message was inserted into the queue
 * @param channel
 * @param event
 * @private
 */
Queue.prototype._handleQueueEvent = function(channel, message) {
  if (channel === this.messageAvailableChannel) {
    if (this._popping) {
      this._messageAvailable = true;
    } else {
      this._messageAvailable = false;
      this._consumeMessages();
    }
  }
}

/**
 * Consume messages form the queue. Must be attached to the queue or an error will be emitted.
 * To stop consuming messages call Queue#stop.
 * Events:
 *  - consuming - the new consuming state, which will be true, after which message events will start being fired
 *  - message - received a message from the queue
 *  - error - not attached, the queue does not exist, or some error occurred
 */
Queue.prototype.consume = function() {
  if (this.isConsuming()) {
    return;
  }

  if (!this.isAttached()) {
    var err = 'not attached';
    return this.emit('error', "error consuming from queue: " + err);
  }

  var _this = this;
  function _event(type, channel, message) {
    switch (type) {
      case 'subscribe':
        _this._consuming(true);
        // also immediately try to consume messages from the queue
        _this._consumeMessages();
        break;
      case 'unsubscribe':
        break;
      case 'event':
        _this._handleQueueEvent(channel, message);
        break;
    }
  }

  this.bus._subscribe(this.connection, this.messageAvailableChannel, _event);

};

exports = module.exports = Queue;
