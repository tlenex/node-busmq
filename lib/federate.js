var events = require('events');
var util = require('util');
var url = require('url');
var crypto = require('crypto');
var through2 = require('through2');
var WebSocket = require('ws');
var WebSocketStream = require('websocket-stream');
var dnode = require('dnode');

/**
 * Federate
 * @param object
 * @param target
 * @param wspool
 * @param options
 * @constructor
 */
function Federate(object, target, wspool, options) {
  events.EventEmitter.call(this);
  this.object = object;
  this.target = target;
  this.wspool = wspool;
  this.ended = false;
  this.closed = false;
  this.queue = [];
  this._options(options);
  this._attachWs(false);
}

util.inherits(Federate, events.EventEmitter);

/**
 * Setup Federate options
 * @param options options to set
 * @private
 */
Federate.prototype._options = function(options) {
  this.options = util._extend({}, options);
  this._setupMethods();
};

/**
 * Set the methods that need to be federated.
 * if not specifically specified then federate all the public methods.
 * @private
 */
Federate.prototype._setupMethods = function() {
  var _this = this;
  if (this.object.federatedMethods) {
    this.options.methods = this.object.federatedMethods;
  } else {
    var methods = ['on', 'once'].concat(Object.getOwnPropertyNames(_this.object.constructor.prototype));
    this.options.methods = methods.filter(function(prop) {
      return typeof _this.object[prop] === 'function' && prop.indexOf('_') !== 0 && prop !== 'constructor';
    });
  }
};

/**
 * Attach a websocket to this federate object by retrieving one from the websocket pool
 * @param reconnect
 * @private
 */
Federate.prototype._attachWs = function(reconnect) {
  var _this = this;
  this.wspool.get(this.target, function(err, ws) {
    if (err) {
      if (err === 'unauthorized') {
        _this.emit('unauthorized');
      } else {
        _this.emit('error', err);
      }
      return;
    }
    _this._to(ws, reconnect);
  });
};

/**
 * Cleanup the federation transport
 * @private
 */
Federate.prototype._endFederation = function(cb) {
  var _this = this;

  function _done() {
    if (_this.wsStream) {
      _this.throughStream.unpipe();
      _this.throughStream = null;
      _this.wsStream.unpipe();
      _this.wsStream = null;
    }
    if (_this.dnode) {
      _this.dnode.emit('shutdown');
      _this.dnode = null;
    }

    cb && cb();
  }

  if (!this.ended) {
    this.ended = true;
    this.object.logger.debug('sending end federation to fedserver');
    this.object._endFederation(function() {
      _done();
    });
  } else {
    _done();
  }
};

/**
 * emit the close event if not already closed
 * @private
 */
Federate.prototype._emitClose = function() {
  if (!this.closed) {
    this.closed = true;
    this.emit('close');
  }
};

/**
 * Start federating the object through the specified websocket
 * @param ws an already open websocket
 * @param reconnect whether this is a reconnection for the same object
 * @returns {Federate}
 */
Federate.prototype._to = function(ws, reconnect) {
  if (this.ws) {
    this.emit('error', 'already federating to ' + this.object.id);
  }

  var _this = this;

  var _onWsMessage = function(msg) {
    if (msg === 'ready') {
      _this._federate(reconnect);
    }
  };

  var _onWsFederationEnd = function() {
    _this._endFederation(function() {
      _this.object.logger.debug('federation ended. putting websocket back into pool');
      _this.wspool.put(_this.ws, function(added) {
        // if the websocket was successfully re-added to the pool
        if (added) {
          cleanup();
          _this._emitClose();
        } else {
          _this.ws.emit('shutdown', 'pool rejected the websocket');
        }
      });
    });
  };

  var _onWsUnexpectedResponse = function(req, res) {
    // unauthorized means wrong secret key
    var reason = 'unexpected response';
    var error;
    if (res.statusCode === 401) {
      reason = 'unauthorized';
      _this.emit(reason);
    } else {
      error = 'federation received unexpected response ' + res.statusCode;
    }
    _onWsShutdown(reason, error);
  };

  var _onWsError = function(error) {
    _this.object.logger.debug('federation transport error: ' + error);
    _onWsShutdown('error', error);
  };

  var _onWsClose = function(code, message) {
    _this.object.logger.debug('federation transport closed. code: ' + code + ', message: ' + message);
    _onWsShutdown('unexpected closed', 'closed due to code ' + code + ' (' + message + ')');
  };

  var _onWsShutdown = function(reason, error) {
    _this.object.logger.debug('federation transport shutting down: ' + reason + (error ? '('+error+')' : ''));
    cleanup();
    _this._endFederation(function() {
      if (_this.ws) {
        _this.ws.close();
        _this.ws = null;
      }

      if (error) {
        _this._reconnect(reason);
      } else {
        _this._emitClose();
      }
    });
  };

  function cleanup() {
    ws.removeListener('message', _onWsMessage);
    ws.removeListener('unexpected-response', _onWsUnexpectedResponse);
    ws.removeListener('error', _onWsError);
    ws.removeListener('close', _onWsClose);
    ws.removeListener('shutdown', _onWsShutdown);
    ws.removeListener('federation:end', _onWsFederationEnd);
  }

  ws.once('message', _onWsMessage);
  ws.on('unexpected-response', _onWsUnexpectedResponse);
  ws.on('error', _onWsError);
  ws.on('close', _onWsClose);
  ws.on('shutdown', _onWsShutdown);
  ws.on('federation:end', _onWsFederationEnd);

  this.ws = ws;

  var parsedTarget = url.parse(ws.url, true);
  delete parsedTarget.search;
  delete parsedTarget.query.secret;
  this.target = url.format(parsedTarget);
  this.object.logger.debug('starting federation to ' + this.target);

  this._sendCreationMessage();

  return this;
};

/**
 * Send the server the object creation message. federation will start once the server sends back the 'ready' message.
 * @private
 */
Federate.prototype._sendCreationMessage = function() {
  var msg = JSON.stringify(this._objectCreationMessage(this.object));
  this.object.logger.debug('sending federation creation message ' + msg);
  this.ws.send(msg);
};

/**
 * Reconnect this federation object over a new transport
 */
Federate.prototype._reconnect = function(reason) {
  if (this.ended) {
    return;
  }
  if (this.ws) {
    this.emit('error', 'cannot reconnect - already connected');
    return;
  }
  this.emit('reconnecting', reason);
  this._attachWs(true);
};

/**
 * Stop federating the object and disconnect the underlying websocket
 */
Federate.prototype.close = function() {
  if (this.ended) {
    return;
  }
  if (this.ws) {
    this.ws.emit('federation:end');
  }
};

/**
 * Create the message to the send to the federation server for creating the correct object
 * @param object
 * @returns {{type: (*|cmd.type|string|exports.tok.type|type|number), methods: (*|Array)}}
 * @private
 */
Federate.prototype._objectCreationMessage = function(object) {
  var type = object.type;
  if (!type) {
    if (object._p) {
      type = 'persistify';
    }
  }
  var message = {type: type, methods: this.options.methods};
  switch(type) {
    case 'queue':
      message.args = [object.name];
      break;
    case 'channel':
      message.args = [object.name, object.local, object.remote];
      break;
    case 'persistify':
      message.args = [object._p.name, {}, object._p.attributes];
      break;
  }
  return message;
};

/**
 * Federate the object methods
 * @private
 */
Federate.prototype._federate = function(reconnect) {
  var _this = this;

  this.object.logger.debug('hooking up remote federation methods');

  function callRemote(method, args) {
    if (!_this.closed) {
      _this.remote[method].call(_this.remote, args);
    }
  }

  // federate the methods to the remote endpoint.
  var methods = ['_endFederation'].concat(_this.options.methods);
  methods.forEach(function(method) {
    _this.object[method] = function() {
      // store the method calls until we are online
      if (!_this.remote) {
        _this.queue.push({method: method, args: arguments});
        return;
      }
      callRemote(method, arguments);
    }
  });

  this.dnode = dnode();

  var _onDnodeRemote = function(remote) {
    _this.remote = remote;

    // call any methods that are pending because we were offline
    _this.queue.forEach(function(e) {
      callRemote(e.method, e.args);
    });
    _this.queue = [];

    if (reconnect) {
      _this.emit('reconnected', _this.object);
    } else {
      _this.emit('ready', _this.object);
    }
  };

  var _onDnodeError = function(err) {
    _this.emit('error', err);
  };

  var _onDnodeFail = function(err) {
    _this.emit('fail', err);
  };

  var _onDnodeShutdown = function() {
    _this.remote = null;
    _this.dnode.removeListener('remote', _onDnodeRemote);
    _this.dnode.removeListener('shutdown', _onDnodeShutdown);
    _this.dnode.removeListener('error', _onDnodeError);
    _this.dnode.removeListener('fail', _onDnodeFail);
    _this.dnode.end();
  };

  this.dnode.on('remote', _onDnodeRemote);
  this.dnode.on('shutdown', _onDnodeShutdown);
  this.dnode.on('error', _onDnodeError);
  this.dnode.on('fail', _onDnodeFail);

  // wrap the websocket with a stream
  this.wsStream = WebSocketStream(this.ws);
  // create a through stream so we can unpipe dnode from the websocket before calling end on dnode.
  // we do this because dnode does not implement unpiping, and calling end on dnode
  // later on will close the websocket if we don't unpipe it first.
  this.throughStream = through2(function(chunk, enc, callback) {
    this.push(chunk);
    callback();
  });
  // pipe the stream through dnode
  this.wsStream.pipe(this.dnode).pipe(this.throughStream).pipe(this.wsStream);
};

module.exports = exports = Federate;
