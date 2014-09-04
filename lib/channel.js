var events = require('events');
var util = require('util');
var Logger = require('./logger');

/**
 Channel.
 Events: 'message', 'error', 'close'
 */
function Channel(bus, name) {
  events.EventEmitter.call(this);
}

util.inherits(Channel, events.EventEmitter);

/*
 Send a message to the peer of the channel
 */
Channel.prototype.send = function(message) {

};

exports = module.exports = Channel;