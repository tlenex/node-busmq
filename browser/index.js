var events = require('events');
var util = require('util');
var bus = require('../lib/bus');

function BusClient(url, secret) {
  events.EventEmitter.call(this);
  this.url = url;
  this.bus = bus.create({
    federate: {
      poolSize: 1,
      urls: [url],
      secret: secret
    }
  });
}

util.inherits(BusClient, events.EventEmitter);

BusClient.prototype.queue = function(name, cb) {
  var fed = this.bus.federate(this.bus.queue(name), this.url);
  fed.on('ready', function(q) {
    cb(null, q);
  });
  fed.on('error', cb);
};

BusClient.prototype.channel = function(name, local, remote, cb) {
  var fed = this.bus.federate(this.bus.channel(name, local, remote), this.url);
  fed.on('ready', function(c) {
    cb(null, c);
  });
  fed.on('error', cb);
};

BusClient.prototype.persistify = function(name, object, attributes, cb) {
  var fed = this.bus.federate(this.bus.persistify(name, object, attributes), this.url);
  fed.on('ready', function(p) {
    cb(null, p);
  });
  fed.on('error', cb);
};

exports = module.exports = function(url, secret) {
  return new BusClient(url, secret);
};
