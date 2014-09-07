require('should');
var redisHelper = require('./redis-helper');
var Bus = require('../lib/bus');

var redisPorts = [9888];
var redises = [];

function redisStart(port, done) {
  var redis = redisHelper.open(port);
  redises.push(redis);
  redis.on('error', function(err) {
    done(new Error(err));
  });
  redis.on('ready', function() {
    done();
  });
}

function redisStop(redis, done) {
  redis.close(function() {
    redises = redises.splice(redises.indexOf(redis), 1);
    done();
  });
}

describe('Bus usage', function() {

  if (this.timeout() === 0) {
    this.enableTimeouts(false);
  }

  // start the redis servers
  before(function(done) {
    var dones = 0;
    for (var i = 0; i < redisPorts.length; ++i) {
      redisStart(redisPorts[i], function() {
        if (++dones === redisPorts.length) {
          done();
        }
      });
    }
  });

  // stop all redis servers
  after(function(done) {
    var dones = 0;
    for (var i = 0; i < redisPorts.length; ++i) {
      redisStop(redises[i], function() {
        if (++dones === redisPorts.length) {
          done();
        }
      });
    }
  });

  describe('bus connection', function() {

    it('should emit online event when connected and offline event after disconnecting', function(done) {
      var bus = Bus.create();
      bus.on('error', function(err) {
        done(err);
      });
      bus.on('online', function() {
        bus.disconnect();
      });
      bus.on('offline', function() {
        done();
      });
      bus.connect('redis://127.0.0.1:9888');
    });

    it('should emit error if calling connect twice', function(done) {
      var bus = Bus.create();
      var dones = 0;
      var onlines = 0;
      bus.on('error', function(err) {
        err.should.be.exactly('already connected');
        bus.disconnect();
      });
      bus.on('online', function() {
        if (++onlines === 1) {
          bus.connect('redis://127.0.0.1:9888');
        } else {
          done('online should not have been called twice');
        }
      });
      bus.on('offline', function() {
        ++dones;
        if (dones === 1) {
          done();
        } else if (dones > 1) {
          done('offline should not have been called twice');
        }
      });
      bus.connect('redis://127.0.0.1:9888');
    });

    it('should emit offline when redis goes down, and online when it\'s back again', function(done) {
      var bus = Bus.create();
      var onlines = 0;
      var offlines = 0;
      bus.on('error', function(){});
      bus.on('online', function() {
        ++onlines;
        if (onlines === 1) {
          redisStop(redises[0], function() {});
        } else if (onlines === 2) {
          bus.disconnect();
        } else {
          done('too many online events');
        }
      });
      bus.on('offline', function() {
        ++offlines;
        if (offlines === 1) {
          redisStart(redisPorts[0], function(){});
        } else if (offlines === 2) {
          done();
        } else {
          done('too many offline events');
        }
      });
      bus.connect('redis://127.0.0.1:9888');
    })
  })
});