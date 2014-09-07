"use strict";

var util = require('util');
var events = require('events');

var childProcess = require('child_process')
  , keyRE = /(port:\s+\d+)|(pid:\s+\d+)|(already\s+in\s+use)|(not\s+listen)|error|denied|(server\s+is\s+now\s+ready)/ig
  , strRE = / /ig;

function RedisHelper(configOrPort) {
  this.config = typeof configOrPort === 'number' ? { port: configOrPort } : (configOrPort || {});
  this.pid = null;
  this.port = null;
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

  this.process = childProcess.exec('redis-server --port ' + this.config.port);
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

  return true;
};

function open(port, callback) {
  var helper = new RedisHelper(port);
  RedisHelper.prototype.open.apply(helper, callback);
  return helper;
}
module.exports = {open: open};