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

  this.object.logger.debug('starting federation to ' + target);

  var _this = this;
  var parsed = url.parse(target, true);
  parsed.query.secret = _this.options.secret;
  var targetUrl = url.format(parsed);
  this.ws = new WebSocket(targetUrl);
  this.ws.on('open', function() {
    _this.object.logger.debug('federation to ' + target + ' is open');
    var msg = JSON.stringify(_this._objectCreationMessage(_this.object));
    _this.object.logger.debug('sending federation creation message ' + msg);
    _this.ws.send(msg);
  });

  this.ws.once('message', function(msg) {
    if (msg === 'ready') {
      _this._federate();
    }
  });
  this.ws.on('unexpected-response', function(req, res) {
    // unauthorized means wrong secret key
    if (res.statusCode === 401) {
      _this.emit('unauthorized');
    } else {
      _this.object.logger.debug('federation received unexpected response: ' + res.statusCode);
      _this.emit('error', 'federation error - unexpected response ' + res.statusCode);
    }
  });
  this.ws.on('error', function(error) {
    _this.object.logger.debug('federation error: ' + error);
    _this.emit('error', error);
  });
  this.ws.on('close', function(code, message) {
    _this.object.logger.debug('federation closed. code: ' + code + ', message: ' + message);
    _this.dnode.end();
    _this.dnode = null;
    _this.ws = null;
  });
  return this;
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
