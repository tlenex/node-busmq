"use strict";

var util = require('util');
var events = require('events');
var redis = require('redis');

var childProcess = require('child_process')
  , keyRE = /(port:\s+\d+)|(pid:\s+\d+)|(already\s+in\s+use)|(not\s+listen)|error|denied|(server\s+is\s+now\s+ready)|(sentinel\s+runid)/ig
  , strRE = / /ig;

function RedisHelper() {
  this.pid = null;
  this.process = null;
  this.isClosing = false;
  this.isRunning = false;
  this.isOpening = false;
}

util.inherits(RedisHelper, events.EventEmitter);

RedisHelper.prototype.open = function(args, done) {
  if (this.isOpening || this.process !== null) {
    this.emit('error', 'Already running');
    return this;
  }

  var _this = this;

  this.process = childProcess.spawn('redis-server', args);
  this.isOpening = true;

  function parse(value) {
    var t = value.split(':')
      , k = t[0].toLowerCase().replace(strRE, '');

    switch (k) {
      case 'alreadyinuse':
        _this.isOpening = false;
        return _this.emit('error', 'Address already in use');

      case 'denied':
        _this.isOpening = false;
        return _this.emit('error', 'Permission denied');

      case 'error':
      case 'notlisten':
        _this.isOpening = false;
        console.log('<==== Redis pid: '+_this.process.pid+'\n'+value.toString()+'\n');
        return _this.emit('error', 'Invalid port number');

      case 'serverisnowready':
      case 'sentinelrunid':
        _this.isOpening = false;
        // _this.client = redis.createClient(_this.port, '127.0.0.1', {auth_pass: _this.auth});
        return _this.emit('ready');

      case 'pid':
      case 'port':
        _this[k] = Number(t[1]);

        if (!(_this.port === null || _this.pid === null)) {
          _this.isRunning = true;
          _this.isOpening = false;
          break;
        }
    }
  }

  function onData(data) {
    //console.log('<==== Redis pid: '+_this.process.pid+'\n'+data.toString()+'\n');
    var matches = data.toString().match(keyRE);
    if (matches !== null) {
      matches.forEach(parse);
    }
  }

  function onError(err) {
    done(new Error(err));
  }

  function onReady() {
    done(null, _this);
  }

  this.on('error', onError);
  this.on('ready', onReady);
  this.process.stdout.on('data', onData);
  this.process.once('exit', function () {
    _this.process.stdout.removeListener('data', onData);
    _this.process = null;
    _this.isRunning = false;
    _this.isClosing = false;
    _this.removeListener('error', onError);
    _this.removeListener('ready', onReady);
  });


  // process.on('exit', function () {
  //   self.close();
  // });

  return this;
};

RedisHelper.prototype.slaveOf = function(port, callback) {
  var client = redis.createClient(this.port, '127.0.0.1');
  function cb() {
    client.end();
    callback();
  };
  if (port) {
    client.slaveof('127.0.0.1', port, cb);
  } else {
    client.slaveof('no', 'one', cb);
  }
};

RedisHelper.prototype.close = function(callback) {
  if (this.isClosing || this.process === null) {
    if (callback) {
      callback(null);
    }

    return false;
  }

  this.isClosing = true;

  if (callback) {
    this.process.once('exit', function () {
      callback(null);
    });
  }

  this.process.kill();

  return true;
};

function open(args, done) {
  return new RedisHelper().open(args, done);
}
module.exports = {open: open};
