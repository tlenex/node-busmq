var redisHelper = require('./redis-helper');

function RedisGroup(ports, auths) {

  this.ports = ports;
  this.auths = auths;
  this.urls = ports.map( function(port, i) {
    return 'redis://' + (auths[i]?(auths[i]+'@'):'') + '127.0.0.1:'+port
  });
  this.redises= {};
}

function startRedis(port, auth, done) {
  console.log('--starting redis on port ' + port + ' with auth ' + auth + '');
  var redis = redisHelper.open(port, auth);
  this.redises[port]=redis;
  redis.on('error', function(err) {
    console.error('--failed starting redis on port ' + port + ', error:  '+err.message+' ');
    done(new Error(err));
  });
  redis.on('ready', function() {
    console.log('--redis on port ' + port + ' started');
    done();
  });
}

function stopRedis(port, done) {
  var redis = this.redises[port];
  var _this = this;
  if (typeof redis === 'undefined') {
    console.log('--redis on port '+redis.port+' is already closed');
    done();
  }
  console.log('--closing redis on port '+redis.port+'');
  redis.close(function() {
    console.log('--redis on port '+redis.port+' closed');
    delete _this.redises[port];
    done();
  });
}


RedisGroup.prototype.start = function(done) {
  var started = 0;
  var _this = this;
  this.ports.forEach(function(port, i) {
    startRedis.call(_this, port, _this.auths[i], function(err) {
      if (err) {
        done && done(err);
        return
      }
      if (++started === _this.ports.length) {
        done && done()
      }
    })
  })
};

RedisGroup.prototype.stop = function(done) {
  var stopped = 0;
  var _this = this;
  this.ports.forEach(function(port, i) {
    stopRedis.call(_this, port, function() {
      /* ignore errors */
      if (++stopped === _this.ports.length) {
        done()
      }
    })
  })
};

RedisGroup.prototype.makeSlave = function(slavePort, masterPort, done) {
  var slave = this.redises[slavePort];
  if (masterPort) {
    console.log('--changing redis ' + slavePort + ' to slave of ' + masterPort + '');
    slave.slaveOf(masterPort, function(err) {
      if (!err) console.log('--redis on '+slavePort+' is now slave of '+masterPort);
      done(err);
    });
  } else {
    console.log('--changing redis ' + slavePort + ' to master>');
    slave.slaveOf(null, function(err) {
      if (!err) console.log('--redis on '+slavePort+' is now a master');
      done(err);
    });
  }
};

exports = module.exports = RedisGroup;
