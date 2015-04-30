var events = require('events');
var util = require('util');
var url = require('url');
var crypto = require('crypto');
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
  this._options(options);
  this.queue = [];
  this._attachWs(false);
}

util.inherits(Federate, events.EventEmitter);

/**
 * Setup Federate options
 * @param options options to set
 * @private
 */
Federate.prototype._options = function(options) {
  this.options = options || {};
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
    ws.removeListener('message', _onWsMessage);
    ws.removeListener('unexpected-response', _onWsUnexpectedResponse);
    ws.removeListener('error', _onWsError);
    ws.removeListener('close', _onWsClose);
    ws.removeListener('shutdown', _onWsShutdown);

    if (_this.dnode) {
      _this.dnode.end();
      _this.dnode = null;
    }

    if (_this.wsStream) {
      _this.wsStream.unpipe();
      _this.wsStream = null;
    }

    if (_this.ws) {
      _this.ws.close();
      _this.ws = null;
    }

    if (error) {
      _this._reconnect(reason);
    } else {
      _this.emit('close');
    }
  };

  ws.once('message', _onWsMessage);
  ws.on('unexpected-response', _onWsUnexpectedResponse);
  ws.on('error', _onWsError);
  ws.on('close', _onWsClose);
  ws.on('shutdown', _onWsShutdown);

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
  if (this.ws) {
    this.ws.emit('shutdown', 'requested shutdown');
  }
};

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

  function callRemote(method, args) {
    _this.remote[method].call(_this.remote, args);
  }

  // federate the methods to the remote endpoint.
  var methods = _this.options.methods;
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

  var _onDnodeEnd = function() {
    _this.dnode.removeListener('remote', _onDnodeRemote);
    _this.dnode.removeListener('end', _onDnodeEnd);
    _this.remote = null;
  };

  this.dnode.on('remote', _onDnodeRemote);
  this.dnode.on('end', _onDnodeEnd);

  // wrap the websocket with a stream
  this.wsStream = WebSocketStream(this.ws);
  // pipe the stream through dnode
  this.wsStream.pipe(this.dnode).pipe(this.wsStream);
};

module.exports = exports = Federate;
