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
  this.options.secret = this.options.secret || 'notsosecret';
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
 * Start federating the object to the specified target
 * @param target
 * @returns {Federate}
 */
Federate.prototype.to = function(target) {
  if (this.ws) {
    this.emit('error', 'already federating to ' + this.object.name);
  }

  this.target = target;
  this.object.logger.debug('starting federation to ' + target);

  var _this = this;
  var parsed = url.parse(target, true);
  parsed.query.secret = _this.options.secret;
  var targetUrl = url.format(parsed);
  this.ws = new WebSocket(targetUrl);
  this.ws.once('message', this._onMessage.bind(this));
  this.ws.on('open', this._onOpen.bind(this));
  this.ws.on('unexpected-response', this._onUnexpectedResponse.bind(this));
  this.ws.on('error', this._onError.bind(this));
  this.ws.on('close', this._onClose.bind(this));
  return this;
};

Federate.prototype._onOpen = function() {
  this.object.logger.debug('federation to ' + this.target + ' is open');
  var msg = JSON.stringify(this._objectCreationMessage(this.object));
  this.object.logger.debug('sending federation creation message ' + msg);
  this.ws.send(msg);
};

Federate.prototype._onMessage = function(msg) {
  if (msg === 'ready') {
    this._federate();
  }
};

Federate.prototype._onUnexpectedResponse = function(req, res) {
  // unauthorized means wrong secret key
  if (res.statusCode === 401) {
    this.emit('unauthorized');
  } else {
    this.object.logger.debug('federation received unexpected response: ' + res.statusCode);
    this.emit('error', 'federation error - unexpected response ' + res.statusCode);
  }
};

Federate.prototype._onError = function(error) {
  this.object.logger.debug('federation transport error: ' + error);
  this.emit('error', error);
};

Federate.prototype._onClose = function(code, message) {
  this.object.logger.debug('federation transport closed. code: ' + code + ', message: ' + message);
  this.close(true);
};

/**
 * Stop federating the object
 * @param disconnect disconnect the underlying websocket
 */
Federate.prototype.close = function(disconnect) {
  if (this.wsStream) {
    this.wsStream.unpipe(this.dnode);
    this.wsStream = null;
  }
  if (this.dnode) {
    this.dnode.end();
    this.dnode = null;
  }
  if (this.ws) {
    this.ws.removeListener('open', this._onOpen.bind(this));
    this.ws.removeListener('unexpected-response', this._onUnexpectedResponse.bind(this));
    this.ws.removeListener('error', this._onError.bind(this));
    this.ws.removeListener('close', this._onClose.bind(this));
    if (disconnect) {
      this.ws.close();
    }
    this.ws = null;
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
  this.dnode.on('remote', function (remote) {
    // federate the methods to the remote endpoint
    _this.options.methods.forEach(function(method) {
      _this.object[method] = function() {
        remote[method].call(remote, arguments);
      }
    });

    _this.emit('ready', _this.object);
  });

  // wrap the websocket with a stream
  this.wsStream = WebSocketStream(this.ws);
  // pipe the stream through dnode
  this.wsStream.pipe(this.dnode).pipe(this.wsStream);
};

module.exports = exports = Federate;
