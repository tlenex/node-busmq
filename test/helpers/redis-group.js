var redisHelper = require('./redis-helper');

function RedisGroup(ports, auths) {

  this.ports = ports;
  this.auths = auths;
  this.urls = ports.map( function(port, i) {
    return 'redis://' + (auths[i]?(auths[i]+'@'):'') + '127.0.0.1:'+port
  });
  this.redises= {};
}



function stopRedis(args, done) {
  var redis = this.redises[args];
  var _this = this;
  if (typeof redis === 'undefined') {
    done();
  }
  redis.close(function() {
    delete _this.redises[args];
    done();
  });
}


RedisGroup.prototype.start = function(done) {
  var started = 0;
  var _this = this;
  this.ports.forEach(function(port,i) {

    var args = ['--port', port];
    if (_this.auths[i]) {
      args.push('--requirepass');
      args.push(_this.auths[i]);
    }
    
    redisHelper.open(args, function(err, redis){
      if (err) {
        done && done(err);
        return
      }
      _this.redises[port] = redis;
      if (++started === _this.ports.length) {
        done && done()
      }
    });
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
    // console.log('--changing redis ' + slavePort + ' to master');
    slave.slaveOf(null, function(err) {
      if (!err) console.log('--redis on '+slavePort+' is now a master');
      done(err);
    });
  }
};

exports = module.exports = RedisGroup;
