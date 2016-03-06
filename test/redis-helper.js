"use strict";

var util = require('util');
var events = require('events');
var redis = require('redis');

process.setMaxListeners(100);

var childProcess = require('child_process')
  , keyRE = /(port:\s+\d+)|(pid:\s+\d+)|(already\s+in\s+use)|(not\s+listen)|error|denied|(server\s+is\s+now\s+ready)/ig
  , strRE = / /ig;

function RedisHelper(port, auth) {
  this.pid = null;
  this.port = port;
  this.auth = auth;
  this.process = null;
  this.isClosing = false;
  this.isRunning = false;
  this.isOpening = false;
}

util.inherits(RedisHelper, events.EventEmitter);

RedisHelper.prototype.open = function() {
  if (this.isOpening || this.process !== null) {
    if (callback) {
      callback(null);
    }

    return false;
  }

  var self = this;

  var args = ['--port', this.port];
  if (this.auth) {
    args.push('--requirepass');
    args.push(this.auth);
  }
  this.process = childProcess.spawn('redis-server', args);
  this.isOpening = true;

  function parse(value) {
    var t = value.split(':')
      , k = t[0].toLowerCase().replace(strRE, '')

    switch (k) {
      case 'alreadyinuse':
        self.isOpening = false;
        return self.emit('error', 'Address already in use');

      case 'denied':
        self.isOpening = false;
        return self.emit('error', 'Permission denied');

      case 'error':
      case 'notlisten':
        self.isOpening = false;
        return self.emit('error', 'Invalid port number');

      case 'serverisnowready':
        self.isOpening = false;
        self.client = redis.createClient(self.port, '127.0.0.1', {auth_pass: self.auth});
        self.client.on('error', function(){});
        return self.emit('ready');

      case 'pid':
      case 'port':
        self[k] = Number(t[1]);

        if (!(self.port === null || self.pid === null)) {
          self.isRunning = true;
          self.isOpening = false;
          break;
        }
    }
  }

  this.process.stdout.on('data', function (data) {
    var matches = data.toString().match(keyRE);

    if (matches !== null) {
      matches.forEach(parse);
    }
  });

  this.process.on('close', function () {
    self.process = null;
    self.isRunning = false;
    self.isClosing = false;
  });

  process.on('exit', function () {
    self.close();
  });

  return true;
};

RedisHelper.prototype.slaveOf = function(port, callback) {
  if (port) {
    this.client.slaveof('127.0.0.1', port, callback);
  } else {
    this.client.slaveof('no', 'one', callback);
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
    this.process.on('close', function () {
      callback(null);
    });
  }

  this.process.kill();
  this.client = null;

  return true;
};

function open(port, auth, callback) {
  var helper = new RedisHelper(port, auth);
  RedisHelper.prototype.open.apply(helper, callback);
  return helper;
}
module.exports = {open: open};