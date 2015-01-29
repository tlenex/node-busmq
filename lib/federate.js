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
 * @param options
 * @constructor
 */
function Federate(object, options) {
  events.EventEmitter.call(this);
  this.object = object;
  this._options(options);
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

/**
 * Start federating the object through the specified websocket
 * @param ws an already open websocket
 * @returns {Federate}
 */
Federate.prototype.to = function(ws) {
  if (this.ws) {
    this.emit('error', 'already federating to ' + this.object.id);
  }

  this.ws = ws;

  var _this = this;

  var _onWsMessage = function(msg) {
    if (msg === 'ready') {
      _this._federate();
    }
  };

  var _onWsUnexpectedResponse = function(req, res) {
    // unauthorized means wrong secret key
    if (res.statusCode === 401) {
      _this.emit('unauthorized');
    } else {
      _this.object.logger.debug('federation received unexpected response: ' + res.statusCode);
      _this.emit('error', 'federation error - unexpected response ' + res.statusCode);
    }
  };

  var _onWsError = function(error) {
    _this.object.logger.debug('federation transport error: ' + error);
    _this.emit('error', error);
  };

  var _onWsClose = function(code, message) {
    _this.object.logger.debug('federation transport closed. code: ' + code + ', message: ' + message);
    _this.ws.removeListener('message', _onWsMessage);
    _this.ws.removeListener('unexpected-response', _onWsUnexpectedResponse);
    _this.ws.removeListener('error', _onWsError);
    _this.ws.removeListener('close', _onWsClose);
    _this.ws = null;
    _this.close(true);
  };

  this.ws.once('message', _onWsMessage);
  this.ws.on('unexpected-response', _onWsUnexpectedResponse);
  this.ws.on('error', _onWsError);
  this.ws.on('close', _onWsClose);

  var parsedTarget = url.parse(this.ws.url, true);
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
 * Stop federating the object and disconnect the underlying websocket
 * @param noDisconnect do not disconnect the underlying websocket
 */
Federate.prototype.close = function(noDisconnect) {

  if (this.dnode) {
    this.dnode.end();
    this.dnode = null;
  }

  if (this.wsStream) {
    this.wsStream.unpipe(this.dnode);
    this.wsStream = null;
  }
  if (this.ws) {
    this.ws.close();
  }
  this.emit('close');
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
Federate.prototype._federate = function() {
  var _this = this;

  this.dnode = dnode();

  var _onDnodeRemote = function(remote) {
    // federate the methods to the remote endpoint.
    // the endFederation method is used to signal the server to disconnect the federation object
    var methods = _this.options.methods;
    methods.forEach(function(method) {
      _this.object[method] = function() {
        remote[method].call(remote, arguments);
      }
    });

    _this.emit('ready', _this.object);
  };

  var _onDnodeEnd = function() {
    _this.dnode.removeListener('remote', _onDnodeRemote);
    _this.dnode.removeListener('end', _onDnodeEnd);
  };

  this.dnode.on('remote', _onDnodeRemote);
  this.dnode.on('end', _onDnodeEnd);

  // wrap the websocket with a stream
  this.wsStream = WebSocketStream(this.ws);
  // pipe the stream through dnode
  this.wsStream.pipe(this.dnode).pipe(this.wsStream);
};

module.exports = exports = Federate;
