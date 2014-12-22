var http = require('http');
var Should = require('should');
var redisHelper = require('./redis-helper');
var Bus = require('../lib/bus');

var redisPorts = [9888];
var redisUrls = [];
redisPorts.forEach(function(port) {
  redisUrls.push('redis://127.0.0.1:'+port);
})

var redises = [];

function redisStart(port, done) {
  console.log('<starting redis>');
  var redis = redisHelper.open(port);
  redises.push(redis);
  redis.on('error', function(err) {
    done(new Error(err));
  });
  redis.on('ready', function() {
    console.log('<redis started>');
    done();
  });
}

function redisStop(redis, done) {
  console.log('<closing redis>');
  redis.close(function() {
    console.log('<redis closed>');
    redises.splice(redises.indexOf(redis), 1);
    done();
  });
}

describe('Bus', function() {

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
      var bus = Bus.create({redis: redisUrls, logger: console});
      bus.on('error', function(err) {
        done(err);
      });
      bus.on('online', function() {
        bus.disconnect();
      });
      bus.on('offline', function() {
        done();
      });
      bus.connect();
    });

    it('should emit error if calling connect twice', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console});
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
      bus.connect();
    });

    it('should emit offline when redis goes down, and online when it\'s back again', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console});
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
      bus.connect();
    })
  });

  describe('queues', function() {

    it('should receive attach/detach events', function(done) {
      var count = 0;
      function _count() {
        ++count;
      }
      var bus = Bus.create({redis: redisUrls, logger: console});
      bus.on('error', done);
      bus.on('online', function() {
        var qName = 'test'+Math.random();
        // create producer
        var p = bus.queue(qName);
        p.on('error', done);
        p.on('detaching', _count);
        p.on('detached', function() {
          _count();
          bus.disconnect();
        });
        p.on('attaching', _count);
        p.on('attached', function() {
          _count();
          var c = bus.queue(qName);
          c.on('error', done);
          c.on('detaching', _count);
          c.on('detached', function() {
            _count();
            p.detach();
          });
          c.on('attaching', _count);
          c.on('attached', function() {
            _count();
            c.detach();
          });
          c.attach();
        });
        p.attach();
      });
      bus.on('offline', function() {
        count.should.be.exactly(8);
        done();
      });
      bus.connect();
    });

    describe('pushing and consuming messages', function() {

      it('producer attach -> producer push -> consumer attach -> consumer receive', function(done) {
        var testMessage = 'test message';
        var consumed = 0;
        var bus = Bus.create({redis: redisUrls, logger: console});
        bus.on('error', done);
        bus.on('online', function() {
          var qName = 'test'+Math.random();
          // create producer
          var p = bus.queue(qName);
          p.on('error', done);
          p.on('detached', function() {
            var c = bus.queue(qName);
            c.on('error', done);
            c.on('message', function(message) {
              message.should.be.exactly(testMessage);
              ++consumed;
              c.detach();
            });
            c.on('detached', function() {
              bus.disconnect();
            });
            c.on('attached', function() {
              // wait for messages
              c.consume();
            });
            c.attach();
          });
          p.on('attached', function() {
            // push a message
            p.push(testMessage);
            p.detach();
          });
          p.attach();
        });
        bus.on('offline', function() {
          consumed.should.be.exactly(1);
          done();
        });
        bus.connect();
      });

      it('producer attach -> consumer attach -> producer push -> consumer receive', function(done) {
        var testMessage = 'test message';
        var consumed = 0;
        var bus = Bus.create({redis: redisUrls, logger: console});
        bus.on('error', done);
        bus.on('online', function() {
          var qName = 'test'+Math.random();
          // create producer
          var p = bus.queue(qName);
          p.on('error', done);
          p.on('attached', function() {
            var c = bus.queue(qName);
            c.on('error', done);
            c.on('message', function(message) {
              message.should.be.exactly(testMessage);
              ++consumed;
              p.detach();
              c.detach();
            });
            c.on('detached', function() {
              bus.disconnect();
            });
            c.on('attached', function() {
              // wait for messages
              c.consume();
              // push a messages
              p.push(testMessage)
            });
            c.attach();
          });
          p.attach();
        });
        bus.on('offline', function() {
          consumed.should.be.exactly(1);
          done();
        });
        bus.connect();
      });

      it('consumer attach -> producer attach -> producer push -> consumer receive', function(done) {
        var testMessage = 'test message';
        var consumed = 0;
        var bus = Bus.create({redis: redisUrls, logger: console});
        bus.on('error', done);
        bus.on('online', function() {
          var qName = 'test'+Math.random();
          // create consumer
          var p;
          var c = bus.queue(qName);
          c.on('error', done);
          c.on('message', function(message) {
            message.should.be.exactly(testMessage);
            ++consumed;
            c.detach();
            p.detach();
          });
          c.on('attached', function() {
            // wait for messages
            c.consume();
            // create producer
            p = bus.queue(qName);
            p.on('error', done);
            p.on('detached', function() {
              bus.disconnect();
            });
            p.on('attached', function() {
              // push a messages
              p.push(testMessage)
            });
            p.attach();
          });
          c.attach();

        });
        bus.on('offline', function() {
          consumed.should.be.exactly(1);
          done();
        });
        bus.connect();
      });

      it('producer attach -> producer push(5) -> consumer attach -> consumer receive(5)', function(done) {
        var testMessage = 'test message';
        var consumed = 0;
        var bus = Bus.create({redis: redisUrls, logger: console});
        bus.on('error', done);
        bus.on('online', function() {
          var qName = 'test'+Math.random();
          // create producer
          var p = bus.queue(qName);
          p.on('error', done);
          p.on('detached', function() {
            var c = bus.queue(qName);
            c.on('error', done);
            c.on('message', function(message) {
              message.should.be.exactly(testMessage);
              if (++consumed === 5) {
                c.detach();
              }
            });
            c.on('detached', function() {
              bus.disconnect();
            });
            c.on('attached', function() {
              // wait for messages
              c.consume();
            });
            c.attach();
          });
          p.on('attached', function() {
            // push 5 message
            for (var i = 0; i < 5; ++i) {
              p.push(testMessage);
            }
            p.detach();
          });
          p.attach();
        });
        bus.on('offline', function() {
          consumed.should.be.exactly(5);
          done();
        });
        bus.connect();
      });

      it('producer push(5) -> producer attach -> consumer attach -> consumer receive(5)', function(done) {
        var testMessage = 'test message';
        var consumed = 0;
        var bus = Bus.create({redis: redisUrls, logger: console});
        bus.on('error', done);
        bus.on('online', function() {
          var qName = 'test'+Math.random();
          // create producer
          var p = bus.queue(qName);
          p.on('error', done);
          p.on('detached', function() {
            var c = bus.queue(qName);
            c.on('error', done);
            c.on('message', function(message) {
              message.should.be.exactly(testMessage);
              if (++consumed === 5) {
                c.detach();
              }
            });
            c.on('detached', function() {
              bus.disconnect();
            });
            c.consume();
            c.attach();
          });
          p.on('attached', function() {
            p.detach();
          });
          // push 5 message
          for (var i = 0; i < 5; ++i) {
            p.push(testMessage);
          }
          // attach
          p.attach();
        });
        bus.on('offline', function() {
          consumed.should.be.exactly(5);
          done();
        });
        bus.connect();
      });

      it('queue should not expire if detaching and re-attaching before queue ttl passes', function(done) {
        var testMessage = 'test message';
        var bus = Bus.create({redis: redisUrls, logger: console});
        bus.on('error', done);
        bus.on('online', function() {
          var qName = 'test'+Math.random();
          // create producer
          var p = bus.queue(qName);
          p.on('error', done);
          p.on('detached', function() {
            setTimeout(function() {
              // ttl is 2 seconds, we re-attach after 1 second
              p.exists(function(exists) {
                exists.should.be.exactly(true);
                bus.disconnect();
              });
            }, 1000);
          });
          p.on('attached', function() {
            p.push(testMessage);
            var c = bus.queue(qName);
            c.on('error', done);
            c.on('message', function(message) {
              done('message should not have been received')
            });
            c.on('attached', function() {
              c.detach();
              p.detach();
            });
            c.attach();
          });
          p.attach({ttl: 5});
        });
        bus.on('offline', function() {
          done();
        });
        bus.connect();
      });

      it('queue should expire: producer attach -> consumer attach -> producer push -> detach all', function(done) {
        var testMessage = 'test message';
        var bus = Bus.create({redis: redisUrls, logger: console});
        bus.on('error', done);
        bus.on('online', function() {
          var qName = 'test'+Math.random();
          // create producer
          var p = bus.queue(qName);
          p.on('error', done);
          p.on('detached', function() {

          });
          p.on('attached', function() {
            p.push(testMessage);
            var c = bus.queue(qName);
            c.on('error', done);
            c.on('message', function(message) {
              done('message should not have been received')
            });
            c.on('detached', function() {
              // ttl is 1 second, so the queue must be expired after 1.5 seconds
              setTimeout(function() {
                c.exists(function(exists) {
                  exists.should.be.exactly(false);
                  bus.disconnect();
                });
              }, 1500);
            });
            c.on('attached', function() {
              c.detach();
              p.detach();
            });
            c.attach();
          });
          p.attach({ttl: 1});
        });
        bus.on('offline', function() {
          done();
        });
        bus.connect();
      });
    });

    it('consume max', function(done) {
      var testMessage = 'test message';
      var consumed = 0;
      var bus = Bus.create({redis: redisUrls, logger: console});
      bus.on('error', done);
      bus.on('online', function() {
        var qName = 'test'+Math.random();
        // create producer
        var p = bus.queue(qName);
        p.on('error', done);
        p.on('detached', function() {
          bus.disconnect();
        });
        p.on('attached', function() {
          p.push(testMessage);
          p.push(testMessage);
          p.push(testMessage);
          p.push(testMessage);
          var c = bus.queue(qName);
          c.on('error', done);
          c.on('message', function(message) {
            message.should.be.exactly(testMessage);
            ++consumed;
          });
          c.on('detached', function() {
            p.detach();
          });
          c.on('consuming', function(status) {
            if (consumed === 0 || consumed === 3) {
              status.should.be.exactly(true);
              c.isConsuming().should.be.exactly(true);
            }
            if (consumed === 2) {
              status.should.be.exactly(false);
              c.isConsuming().should.be.exactly(false);
              consumed = 3;
              c.consume({max: 1});// consume only 1
            }
            if (consumed === 4) {
              status.should.be.exactly(false);
              c.isConsuming().should.be.exactly(false);
              c.detach();
            }
          });
          c.on('attached', function() {
            c.consume({max: 2}); // consume only 2
          });
          c.attach();
        });
        p.attach({ttl: 100});
      });
      bus.on('offline', function() {
        done();
      });
      bus.connect();
    });

    it('consume without removing', function(done) {
      var testMessage = 'test message';
      var consumed = 0;
      var bus = Bus.create({redis: redisUrls, logger: console});
      bus.on('error', done);
      bus.on('online', function() {
        var qName = 'test'+Math.random();
        // create producer
        var p = bus.queue(qName);
        p.on('error', done);
        p.on('detached', function() {
          bus.disconnect();
        });
        p.on('attached', function() {
          p.push(testMessage+0);
          p.push(testMessage+1);
          p.push(testMessage+2);
          p.push(testMessage+3);
          var c = bus.queue(qName);
          c.on('error', done);
          c.on('message', function(message) {
            message.should.be.exactly(testMessage+consumed);
            ++consumed;
            if (consumed === 4) {
              var c2 = bus.queue(qName);
              c2.on('error', done);
              c2.on('message', function(message) {
                message.should.be.exactly(testMessage+consumed);
                ++consumed;
                if (consumed === 4) {
                  c2.detach();
                }
              });
              c2.on('detached', function() {
                c.detach();
              });
              c2.on('attached', function() {
                consumed = 0;
                c2.consume();
              });
              c2.attach();
            }
          });
          c.on('detached', function() {
            p.detach();
          });
          c.on('attached', function() {
            c.consume({remove: false}); // do not remove consumed messages
          });
          c.attach();
        });
        p.attach({ttl: 100});
      });
      bus.on('offline', function() {
        done();
      });
      bus.connect();
    });

    it('flush messages', function(done) {
      var testMessage = 'test message';
      var bus = Bus.create({redis: redisUrls, logger: console});
      bus.on('error', done);
      bus.on('online', function() {
        var qName = 'test'+Math.random();
        // create producer
        var p = bus.queue(qName);
        p.on('error', done);
        p.on('detached', function() {
          var q = bus.queue(qName);
          q.on('error', done);
          q.on('attached', function(exists) {
            exists.should.be.exactly(true);
            q.count(function(count) {
              count.should.be.exactly(0);
              q.detach();
            });
          });
          q.on('detached', function() {
            bus.disconnect();
          });
          q.attach();
        });
        p.on('attached', function() {
          p.push(testMessage);
          p.push(testMessage);
          p.push(testMessage);
          setImmediate(function() {p.detach();p.flush();});
        });
        p.attach({ttl: 1});
      });
      bus.on('offline', function() {
        done();
      });
      bus.connect();
    });

    it('should set and get arbitrary metadata', function(done) {
      var key1 = 'key1';
      var value1 = 'value1';
      var key2 = 'key2';
      var value2 = 'value2';
      var bus = Bus.create({redis: redisUrls, logger: console});
      bus.on('error', done);
      bus.on('online', function() {
        var qName = 'test'+Math.random();
        var q = bus.queue(qName);
        q.on('error', done);
        q.on('detached', function() {
          bus.disconnect();
        });
        q.on('attached', function() {
          q.metadata(key1, value1, function() {
            q.metadata(key2, value2, function() {
              q.metadata(key1, function(val1) {
                val1.should.be.exactly(value1);
                q.metadata(key2, function(val2) {
                  val2.should.be.exactly(value2);
                  q.detach();
                });
              });
            });
          });
        });
        q.attach({ttl: 1});
      });
      bus.on('offline', function() {
        done();
      });
      bus.connect();
    });

  });

  describe('channels', function() {

    it('server listens -> client connects', function(done) {
      var testMessage = 'test message';
      var sEvents = {'message': 0};
      var cEvents = {'message': 0};
      var bus = Bus.create({redis: redisUrls, logger: console});
      bus.on('error', done);
      bus.on('online', function() {
        var cName = 'test'+Math.random();
        var cServer = bus.channel(cName);
        var cClient = bus.channel(cName);
        cServer.on('error', function(error) {
          sEvents['error'] = error;
          bus.disconnect();
        });
        cServer.on('connect', function() {
          sEvents['connect'] = true;
          cClient.on('error', function(error) {
            cEvents['error'] = error;
            bus.disconnect();
          });
          cClient.on('connect', function() {
            cEvents['connect'] = true;
          });
          cClient.on('remote:connect', function() {
            cEvents['remote:connect'] = true;
            cClient.send(testMessage);
          });
          cClient.on('message', function(message) {
            message.should.be.exactly(testMessage);
            if (++cEvents['message'] < 5) {
              cClient.send(testMessage);
            } else {
              cClient.end();
            }
          });
          cClient.on('end', function() {
            cEvents['end'] = true;
          });
          cClient.connect();
        });
        cServer.on('remote:connect', function() {
          sEvents['remote:connect'] = true;
        });
        cServer.on('message', function(message) {
          message.should.be.exactly(testMessage);
          ++sEvents['message'];
          cServer.send(testMessage);
        });
        cServer.on('end', function() {
          sEvents['end'] = true;
          bus.disconnect();
        });
        cServer.listen();
      });
      bus.on('offline', function() {
        Should(sEvents['connect']).equal(true);
        Should(sEvents['remote:connect']).equal(true);
        Should(sEvents['message']).equal(5);
        Should(sEvents['end']).equal(true);
        Should(sEvents['error']).equal(undefined);

        Should(cEvents['connect']).equal(true);
        Should(cEvents['remote:connect']).equal(true);
        Should(cEvents['message']).equal(5);
        Should(cEvents['end']).equal(undefined);
        Should(cEvents['error']).equal(undefined);

        done();
      });
      bus.connect();
    });

    it('client connects -> server listens', function(done) {
      var testMessage = 'test message';
      var sEvents = {'message': 0};
      var cEvents = {'message': 0};
      var bus = Bus.create({redis: redisUrls, logger: console});
      bus.on('error', done);
      bus.on('online', function() {
        var cName = 'test'+Math.random();
        var cServer = bus.channel(cName);
        var cClient = bus.channel(cName);
        cClient.on('error', function(error) {
          cEvents['error'] = error;
          bus.disconnect();
        });
        cClient.on('connect', function() {
          cEvents['connect'] = true;
          cServer.on('error', function(error) {
            sEvents['error'] = error;
            bus.disconnect();
          });
          cServer.on('connect', function() {
            sEvents['connect'] = true;
          });
          cServer.on('remote:connect', function() {
            sEvents['remote:connect'] = true;
            cServer.send(testMessage);
          });
          cServer.on('message', function(message) {
            message.should.be.exactly(testMessage);
            if (++sEvents['message'] < 5) {
              cServer.send(testMessage);
            } else {
              cServer.end();
            }
          });
          cServer.on('end', function() {
            sEvents['end'] = true;
          });
          cServer.listen();
        });
        cClient.on('remote:connect', function() {
          cEvents['remote:connect'] = true;
        });
        cClient.on('message', function(message) {
          message.should.be.exactly(testMessage);
          ++cEvents['message'];
          cClient.send(testMessage);
        });
        cClient.on('end', function() {
          cEvents['end'] = true;
          bus.disconnect();
        });
        cClient.connect();
      });
      bus.on('offline', function() {
        Should(sEvents['connect']).equal(true);
        Should(sEvents['remote:connect']).equal(true);
        Should(sEvents['message']).equal(5);
        Should(sEvents['end']).equal(undefined);
        Should(sEvents['error']).equal(undefined);

        Should(cEvents['connect']).equal(true);
        Should(cEvents['remote:connect']).equal(true);
        Should(cEvents['message']).equal(5);
        Should(cEvents['end']).equal(true);
        Should(cEvents['error']).equal(undefined);

        done();
      });
      bus.connect();
    });
  });

  describe('persistency', function() {

    it('saves and loads an object', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console});
      bus.on('error', done);
      bus.on('online', function() {
        var object = bus.persistify('data1', {}, ['field1', 'field2', 'field3']);
        object.field1 = 'val1';
        object.field2 = 2;
        object.field3 = true;
        object.save(function(err) {
          Should(err).equal(undefined);
          object.field1 = 'val2';
          object.save(function(err) {
            Should(err).equal(undefined);
            var object2 = bus.persistify('data1', {}, ['field1', 'field2', 'field3']);
            object2.load(function(err, exists) {
              Should(err).equal(null);
              exists.should.be.exactly(true);
              Should(object2.field1).equal('val2');
              Should(object2.field2).equal(2);
              Should(object2.field3).equal(true);
              bus.disconnect();
            })
          });
        });
      });
      bus.on('offline', function() {
        done();
      });
      bus.connect();
    });
  });

  describe('federation', function() {

    it('federates queue events', function(done) {
      var fedserver = http.createServer();
      var bus = Bus.create({redis: redisUrls, federate: {server: fedserver, port: 9777}, logger: console});
      bus.on('error', function(err) {
        done(err);
      });
      bus.on('online', function() {
        // create a second bus to federate requests
        var busFed = Bus.create({redis: redisUrls, logger: console});
        busFed.on('error', function(err) {
          done(err);
        });
        busFed.on('online', function() {
          var msgs = [];
          var name = 'q'+Date.now();
          var f = busFed.federate(busFed.queue(name), 'http://127.0.0.1:9777');
          f.on('error', done);
          f.on('unauthorized', function() {
            done('unauthorized')
          });
          f.on('ready', function(q) {
            q.on('error', done);
            q.on('attached', function() {
              q.consume();
            });
            q.on('consuming', function(state) {
              if (state) {
                q.push('hello');
                q.push('world');
              } else {
                q.detach();
              }
            });
            q.on('message', function(msg) {
              msgs.push(msg);
              if (msgs.length === 2) {
                q.stop();
              }
            });
            q.on('detached', function() {
              msgs.length.should.be.exactly(2);
              msgs[0].should.be.exactly('hello');
              msgs[1].should.be.exactly('world');
              busFed.disconnect();
            });
            q.attach();
          });
        });
        busFed.on('offline', function() {
          bus.disconnect();
        });
        busFed.connect();

      });
      bus.on('offline', function() {
        done();
      });
      bus.connect();
    });

    it('federates channel events', function(done) {
      var fedserver = http.createServer();
      var bus = Bus.create({redis: redisUrls, federate: {server: fedserver, port: 9777}, logger: console});
      bus.on('error', function(err) {
        done(err);
      });
      bus.on('online', function() {
        // create a second bus to federate requests
        var busFed = Bus.create({redis: redisUrls, logger: console});
        busFed.on('error', function(err) {
          done(err);
        });
        busFed.on('online', function() {
          var name = 'q'+Date.now();
          var f = busFed.federate(busFed.channel(name, 'local', 'remote'), 'http://127.0.0.1:9777');
          f.on('error', done);
          f.on('unauthorized', function() {
            done('unauthorized')
          });
          f.on('ready', function(c) {
            c.on('error', done);
            c.on('remote:connect', function() {
              c.send('hello');
            });
            c.on('message', function(message) {
              message.should.be.exactly('world');
              c.disconnect();
              busFed.disconnect();
            });
            c.connect();
          });

          var c2 = busFed.channel(name, 'remote', 'local');
          c2.on('error', done);
          c2.on('remote:connect', function() {
          });
          c2.on('message', function(message) {
            message.should.be.exactly('hello');
            c2.send('world');
          });
          c2.on('remote:disconnect', function() {
            c2.disconnect();
          });
          c2.connect();
        });
        busFed.on('offline', function() {
          bus.disconnect();
        });
        busFed.connect();

      });
      bus.on('offline', function() {
        done();
      });
      bus.connect();
    });

    it('federates persisted objects', function(done) {
      var fedserver = http.createServer();
      var bus = Bus.create({redis: redisUrls, federate: {server: fedserver, port: 9777}, logger: console});
      bus.on('error', function(err) {
        done(err);
      });
      bus.on('online', function() {
        // create a second bus to federate requests
        var busFed = Bus.create({redis: redisUrls, logger: console});
        busFed.on('error', function(err) {
          done(err);
        });
        busFed.on('online', function() {
          var f = busFed.federate(busFed.persistify('p1', {}, ['f1', 'f2', 'f3']), 'http://127.0.0.1:9777');
          f.on('error', done);
          f.on('unauthorized', function() {
            done('unauthorized')
          });
          f.on('ready', function(p) {
            p.f1 = 'v1';
            p.f2 = 2;
            p.f3 = true;
            p.save(function(err) {
              Should(err).equal(undefined);
              p.f1 = 'v2';
              p.save(function(err) {
                Should(err).equal(undefined);
                var f2 = busFed.federate(busFed.persistify('p1', {}, ['f1', 'f2', 'f3']), 'http://127.0.0.1:9777');
                f2.on('error', done);
                f2.on('unauthorized', function() {
                  done('unauthorized')
                });
                f2.on('ready', function(p) {
                  p.load(function(err, exist) {
                    Should(err).equal(null);
                    Should(exist).equal(true);
                    Should(p.f1).equal('v2');
                    Should(p.f2).equal(2);
                    Should(p.f3).equal(true);
                  });
                });
              });
              busFed.disconnect();
            });
          });
        });
        busFed.on('offline', function() {
          bus.disconnect();
        });
        busFed.connect();

      });
      bus.on('offline', function() {
        done();
      });
      bus.connect();
    });

    it('does not allow federation with wrong secret', function(done) {
      var fedserver = http.createServer();
      var bus = Bus.create({redis: redisUrls, federate: {server: fedserver, port: 9777, secret: 'thisisit'}, logger: console});
      bus.on('error', function(err) {
        done(err);
      });
      bus.on('online', function() {
        // create a second bus to federate requests
        var busFed = Bus.create({redis: redisUrls, federate: {secret: 'thisisNOTit'}, logger: console});
        busFed.on('error', function(err) {
          done(err);
        });
        busFed.on('online', function() {
          var name = 'q'+Date.now();
          var f = busFed.federate(busFed.queue(name), 'http://127.0.0.1:9777');
          f.on('error', done);
          f.on('unauthorized', function() {
            busFed.disconnect();
          });
          f.on('ready', function() {
            done('ready should not have been called')
          });
        });
        busFed.on('offline', function() {
          bus.disconnect();
        });
        busFed.connect();

      });
      bus.on('offline', function() {
        done();
      });
      bus.connect();
    });
  });
});