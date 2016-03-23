var redisHelper = require('./redis-helper');
var fs = require('fs');

function RedisSentinels() {
  this.redises= {};
}

function deleteFolderRecursive(path) {
  if( fs.existsSync(path) ) {
    fs.readdirSync(path).forEach(function(file){
      var curPath = path + "/" + file;
      if(fs.lstatSync(curPath).isDirectory()) { // recurse
        deleteFolderRecursive(curPath);
      } else { // delete file
        fs.unlinkSync(curPath);
      }
    });
    fs.rmdirSync(path);
  }
};

function master(done) {
  var dir = __dirname+'/../tmp/sentinels/master';
  fs.mkdirSync(dir);
  fs.writeFileSync(dir+'/redis.conf',
    'port 6279\n'+
    'maxclients 200\n'+
    'dir '+dir
  );
  redisHelper.open([dir+'/redis.conf'],done);
}

function slave(num, done) {
  var dir = __dirname+'/../tmp/sentinels/slave'+num;
  fs.mkdirSync(dir);
  fs.writeFileSync(dir+'/redis.conf',
    'port '+(6279+num)+'\n'+
    'dir '+dir+'\n'+
    'slaveof 127.0.0.1 6279\n'+
    'maxclients 200'
  );
  redisHelper.open([dir+'/redis.conf'],done);
}

function sentinel(num, done){
  var dir = __dirname+'/../tmp/sentinels/sentinel'+num;
  fs.mkdirSync(dir);
  fs.writeFileSync(dir+'/redis.conf',
    'port '+(26279+num-1)+'\n'+
    'maxclients 200\n'+
    'sentinel monitor mymaster 127.0.0.1 6279 2'+'\n'+
    'sentinel down-after-milliseconds mymaster 5000'+'\n'+
    'sentinel failover-timeout mymaster 60000'+'\n'+
    'sentinel config-epoch mymaster 3'+'\n'+
    'dir '+dir
  );
  redisHelper.open([dir+'/redis.conf', '--sentinel'],done);
}


RedisSentinels.prototype.start = function(done) {
  if( !fs.existsSync(__dirname+'/../tmp') ) fs.mkdirSync(__dirname+'/../tmp');
  //cleanup
  deleteFolderRecursive(__dirname+'/../tmp/sentinels');
  fs.mkdirSync(__dirname+'/../tmp/sentinels');

  var _this = this;

  master(function(err, redis){
    if (err) {
      _this.stop();
      done( err );
      return;
    }
    _this.redises['master'] = redis;
    slave(1, function(err, redis){
      if (err) {
        _this.stop();
        done( err );
        return;
      }
      _this.redises['slave1'] = redis;
      slave(2, function(err, redis){
        if (err) {
          _this.stop();
          done( err );
          return;
        }
        _this.redises['slave2'] = redis;
        sentinel(1, function(err, redis){
          if (err) {
            _this.stop();
            done( err );
            return;
          }
          _this.redises['sentinel1'] = redis;
          sentinel(2, function(err, redis){
            if (err) {
              _this.stop();
              done( err );
              return;
            }
            _this.redises['sentinel2'] = redis;
            sentinel(3, function(err, redis){
              if (err) {
                _this.stop();
                done( err );
                return;
              }
              _this.redises['sentinel3'] = redis;
              done && done();
            });
          });
        });
      });
    });
  });
};

RedisSentinels.prototype.stop = function(done) {
  var stopped = 0;
  var _this = this;
  ['master', 'slave1','slave2','sentinel1','sentinel2','sentinel3'].forEach(function(r) {
    if (_this.redises[r]) {
      _this.redises[r].close( function () {
        delete _this.redises[r];
        /* ignore errors */
        if ( ++stopped === 6 ) {
          _this.redises = {};
          done && done()
        }
      } );
    }
    else {
      //partial start
      _this.redises = {};
      done && done();
    }
  });
};

exports = module.exports = RedisSentinels;
