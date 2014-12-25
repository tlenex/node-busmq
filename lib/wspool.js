var events = require('events');
var util = require('util');
var _url = require('url');
var WebSocket = require('ws');
var logger = require('./logger');

/**
 * A pool of websockets that keeps a minimum of open websockets to a list of bus federation servers
 * @param bus the bus owning this pool
 * @param options additional options
 * @constructor
 */
function WSPool(bus, options) {
  events.EventEmitter.call(this);
  this.bus = bus;
  this.logger = bus.logger.withTag(bus.id+':wspool');

  options = options || {};
  this.options = options;
  this.options.secret = options.secret || 'notsosecret';
  if (!options.poolSize || options.poolSize <= 0) {
    this.logger.error('websocket pool poolSize is ' + options.poolSize + '. re-setting pool size to default 10');
    options.poolSize = 10;
  }
  this.options.poolSize = options.poolSize;
  this.options.replaceDelay = this.options.replaceDelay || 5000;

  this.pool = {};

  var _this = this;
  this.options.urls = this.options.urls || [];
  this.logger.info('setting up websocket pools with minimum size ' + this.options.poolSize + ' for urls: ' + JSON.stringify(this.options.urls));
  this.options.urls.forEach(function(url) {
    _this.pool[url] = [];
    for (var i = 0; i < _this.options.poolSize; ++i) {
      _this._add(url);
    }
  });
}

util.inherits(WSPool, events.EventEmitter);

/**
 * Add a new websocket to the pool
 * @param url the url to open the websocket to
 * @private
 */
WSPool.prototype._add = function(url) {
  if (!this.pool[url]) {
    this.logger.info('cannot add websocket to ' + url + ': url is not recognized');
    return;
  }

  var _this = this;
  _this.logger.debug('opening websocket to ' + url);
  var ws = new WebSocket(url + '?secret=' + this.options.secret);

  function onOpen() {
    _this.pool[url].push(ws);
    _this.logger.debug('websocket to ' + url + ' added to pool. pool size is now ' + _this.pool[url].length);
    _this.emit('pool:'+url+':add');
  }

  function onClose() {
    _this.logger.info('websocket to ' + url + ' closed');
    replace();
  }

  function onError(error) {
    _this.logger.error('websocket to ' + url + ' error: ' + JSON.stringify(error));
    replace(_this.options.replaceDelay);
  }

  function onUnexpectedResponse(req, res) {
    ws.close();
    // 401 means wrong secret key
    if (res.statusCode === 401) {
      _this.logger.error('websocket to ' + url + ': unauthorized. marking url as in error state');
      // mark the url in error state
      _this.pool[url] = 'unauthorized';
      _this.emit('pool:'+url+':error');
      // do not replace the weboscket
    } else {
      _this.logger.error('websocket received unexpected response: ' + res.statusCode);
      // try to open the webscoket again
      replace(_this.options.replaceDelay);
    }
  }

  function cleanup() {
    ws.removeListener('open', onOpen);
    ws.removeListener('close', onClose);
    ws.removeListener('error', onError);
    ws.removeListener('unexpected-response', onUnexpectedResponse);
    ws.removeListener('pool:replace', replace);
    ws.removeListener('pool:cleanup', cleanup);
  }

  // replace an open websocket with a new websocket in the pool.
  // this is done to keep the pool size with the needed minimum.
  function replace(delay) {
    if (!ws) {
      return;
    }

    cleanup();

    var index = _this.pool[url].indexOf(ws);
    if (index !== -1) {
      ws.close();
      _this.pool[url].splice(index, 1);
      _this.logger.debug('websocket to ' + url + ' removed from pool. pool size is now ' + _this.pool[url].length);
    }


    // add a new websocket to replace the one that was just removed.
    // we either do it immediately or delay it by the amount specified
    setTimeout(function() {
      _this._add(url);
    }, delay || 0);
  }

  ws.on('open', onOpen);
  ws.on('close', onClose);
  ws.on('error', onError);
  ws.on('unexpected-response', onUnexpectedResponse);
  ws.on('pool:replace', replace);
  ws.on('pool:cleanup', cleanup);
};

/**
 * Get a websocket for the specified url
 * @param url the utl to get the websocket for. if none is available right now it will be retrieved once one is available.
 * @param cb receives the websocket
 */
WSPool.prototype.get = function(url, cb) {
  // the url is not supported
  if (!this.pool[url]) {
    process.nextTick(function() {
      cb && cb('url ' + url + ' is not recognized');
    });
    return;
  }

  var _this = this;

  // the url is in error state
  if (typeof this.pool[url] === 'string') {
    process.nextTick(function() {
      cb && cb(_this.pool[url]);
    });
    return;
  }

  // give a websocket from the pool to the callback
  function give() {
    var ws = _this.pool[url].shift();
    _this.logger.debug('websocket to ' + url + ' retrieved from pool. pool size is now ' + _this.pool[url].length);
    ws.emit('pool:replace');
    cb && cb(null, ws);
  }

  // if the pool is currently empty, wait until there are websockets available again
  if (this.pool[url].length === 0) {
    // wiat for the event that a websocket was added tot he pool
    this.once('pool:'+url+':add', function() {
      give();
    });
    // also check if the url is in error state
    this.once('pool:'+url+':error', function() {
      cb && cb(_this.pool[url]);
    });
  } else {
    // a websocket is available, give it immediately
    give();
  }
};

/**
 * Close the pool and disconnect all open websockets
 */
WSPool.prototype.close = function() {
  var _this = this;
  Object.keys(this.pool).forEach(function(url) {
    if (typeof _this.pool[url] !== 'string') {
      _this.pool[url].forEach(function(ws) {
        ws.emit('pool:cleanup');
        ws.close();
      });
    }
  });
  this.pool = {};
};


exports = module.exports = WSPool;