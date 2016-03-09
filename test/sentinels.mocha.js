/**
 * This file runs the tests for direct connections to redis(es)
 * Using the node-redis driver and using the ioredis driver
 */


var http = require('http');
var crypto = require('crypto');
var Bus = require('../lib/bus');
var tf = require('./test.functions');
var RedisSentinels = require('./helpers/redis-sentinels');


var redises = [];

describe('BusMQ sentinels', function() {

  var redisSentinels = new RedisSentinels();

  this.timeout(0);
  if (this.timeout() === 0) {
    this.enableTimeouts(false);
  }

  // start the redis servers
  before(function(done) {
    redisSentinels.start(done);
  });

  // stop all redis servers
  after(function(done) {
    redisSentinels.stop(done);
  });

  describe('bus connection', function() {

    it('should emit online event when connected and offline event after disconnecting', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console, logLevel: 'debug'});
      tf.onlineOffline(bus,done);
    });

    it('should emit error if calling connect twice', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console});
      tf.connectTwice(bus,done,redisPorts[0]);
    });

    it('should emit offline when the redises go down, and online when they are back again', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console});
      tf.downAndBack(bus, redisGroup, done);
    });

    it('should resume silently when redis turns into slave and turns back to master', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console});
      tf.resumeSilently(bus,redisGroup, redisPorts[0], redisPorts[2], done);
    });
  });

  describe('queues', function() {

    it('should receive attach/detach events', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console});
      tf.attachDetachEvents(bus,done);
    });

    describe('pushing and consuming messages', function() {

      it('producer attach -> producer push -> consumer attach -> consumer receive', function(done) {
        var bus = Bus.create({redis: redisUrls, logger: console});
        tf.pAttachPPushCAttachCReceive(bus,done);
      });

      it('producer attach -> consumer attach -> producer push -> consumer receive', function(done) {
        var bus = Bus.create({redis: redisUrls, logger: console});
        tf.pAttachCAttachPPushCReceive(bus,done);
      });

      it('consumer attach -> producer attach -> producer push -> consumer receive', function(done) {
        var bus = Bus.create({redis: redisUrls, logger: console});
        tf.cAttachPAttachPPUshCReceive(bus,done);
      });

      it('producer attach -> producer push(5) -> consumer attach -> consumer receive(5)', function(done) {
        var bus = Bus.create({redis: redisUrls, logger: console});
        tf.pAttachPPush5CAttachCReceive5(bus,done);
      });

      it('producer push(5) -> producer attach -> consumer attach -> consumer receive(5)', function(done) {
        var bus = Bus.create({redis: redisUrls, logger: console});
        tf.pPUsh5PAttachCAttachCReceive5(bus,done);
      });

      it('queue should not expire if detaching and re-attaching before queue ttl passes', function(done) {
        var bus = Bus.create({redis: redisUrls, logger: console});
        tf.doNotExpireBeforeTTL(bus,done);
      });

      it('queue should expire: producer attach -> consumer attach -> producer push -> detach all', function(done) {
        var bus = Bus.create({redis: redisUrls, logger: console});
        tf.queueShouldExpire(bus,done);
      });

      it('produces and consumes 10 messages in 10 queues', function(done) {
        var bus = Bus.create({redis: redisUrls, logger: console});
        tf.testManyMessages(bus, 10, 10, done);
      });

      it('produces and consumes 100 messages in 10 queues', function(done) {
        var bus = Bus.create({redis: redisUrls, logger: console});
        tf.testManyMessages(bus, 100, 10, done);
      });

      it('produces and consumes 100 messages in 100 queues', function(done) {
        var bus = Bus.create({redis: redisUrls, logger: console});
        tf.testManyMessages(bus, 100, 100, done);
      });

    });

    it('consume max', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console});
      tf.queueConsumesMax(bus,done);
    });

    it('consume without removing', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console});
      tf.queueConsumeWithoutRemoving(bus,done);
    });

    it('count and flush messages', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console});
      tf.queueCountAndFlush(bus,done);
    });

    it('should not receive additional messages if stop was called', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console});
      tf.queueNotReceiveWhenStopped(bus,done);
    });

    it('consume reliable', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console});
      tf.queueConsumeReliable(bus,done);
    });

    it('should set and get arbitrary metadata', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console});
      tf.queueGetSetMatadata(bus,done);
    });

  });

  describe('channels', function() {

    it('server listens -> client connects', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console});
      tf.channelServerListensClientConnects(bus,done);
    });

    it('client connects -> server listens', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console});
      tf.channelClientConnectsServerListens(bus,done);
    });

    it('reliable channel', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console});
      tf.reliableChannel(bus,done);
    });
  });

  describe('persistency', function() {

    it('saves and loads an object', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console});
      tf.persistencySavesAndLoads(bus,done);
    });

    it('persists 10 objects', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console});
      tf.testManySavesAndLoades(bus, 10, done);
    });

    it('persists 100 objects', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console});
      tf.testManySavesAndLoades(bus, 100, done);
    });

    it('persists 1000 objects', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console});
      tf.testManySavesAndLoades(bus, 1000, done);
    });
  });

  describe('pubsub', function() {

    it('should receive subscribe/unsubscribe and message events', function(done) {
      var bus = Bus.create({redis: redisUrls, logger: console});
      tf.pubSubSubscribeUnsubscribe(bus,done);
    });
  });

  describe('federation', function() {

    var fedserver;

    beforeEach(function(done) {
      fedserver = http.createServer();
      fedserver.listen(9777, function() {
        done();
      });
    });

    afterEach(function(done) {
      fedserver.on('error', function() {}); // ignore socket errors at this point
      setTimeout(function() {
        fedserver && fedserver.close();
        done();
      }, 100);
    });

    function fedBusCreator(federate) {
      var options = {
        redis: redisUrls,
        driver: 'node-redis',
        logger: console,
        layout: 'direct',
        federate: federate
      };
      return Bus.create(options);
    }

    it('federates queue events', function(done) {
      var bus = Bus.create({redis: redisUrls, federate: {server: fedserver, path: '/federate'}, logger: console});
      tf.fedFederateQueueEvents(bus, done, fedBusCreator);
    });

    it('federates channel events', function(done) {
      var bus = Bus.create({redis: redisUrls, federate: {server: fedserver}, logger: console});
      tf.fedFederateChannelEvents(bus, done, fedBusCreator);
    });

    it('federates persisted objects', function(done) {
      var bus = Bus.create({redis: redisUrls, federate: {server: fedserver}, logger: console});
      tf.fedFederatePersistedObjects(bus, done, fedBusCreator);
    });

    it('federate pubsub events', function(done) {
      var bus = Bus.create({redis: redisUrls, federate: {server: fedserver}, logger: console});
      tf.fedPubSubEvents(bus, done, fedBusCreator);
    });

    it('federation websocket of queue closes and reopens', function(done) {
      var fedserver = http.createServer();
      fedserver.listen(9788, function() {
        var bus = Bus.create({redis: redisUrls, federate: {server: fedserver, path: '/federate'}, logger: console});
        tf.fedWebsocketQueueClosesReopens(bus, fedBusCreator, fedserver, 9788, function() {
          fedserver.on('error', function() {}); // ignore socket errors at this point
          setTimeout(function() {
            fedserver && fedserver.close();
            done();
          }, 100);
        });
      });
    });

    it('federation websocket of channel closes and reopens', function(done) {
      var fedserver = http.createServer();
      fedserver.listen(9789, function() {
        var bus = Bus.create({redis: redisUrls, federate: {server: fedserver, path: '/federate'}, logger: console});
        tf.fedWebsocketChannelClosesReopens(bus, fedBusCreator, fedserver, 9789, function() {
          fedserver.on('error', function() {}); // ignore socket errors at this point
          setTimeout(function() {
            fedserver && fedserver.close();
            done();
          }, 100);
        });
      });
    });

    it('does not allow federation with wrong secret', function(done) {
      var bus = Bus.create({redis: redisUrls, federate: {server: fedserver, secret: 'thisisit'}, logger: console});
      tf.fedNotWithWrongSecret(bus, done, fedBusCreator);
    });

    it('produces and consumes 10 messages in 10 queues over federation', function(done) {
      var bus = Bus.create({redis: redisUrls, federate: {server: fedserver, path: '/federate'}, logger: console});
      tf.testManyMessagesOverFederation(bus, 10, 10, done, fedBusCreator);
    });

    it('produces and consumes 100 messages in 10 queues over federation', function(done) {
      var bus = Bus.create({redis: redisUrls, federate: {server: fedserver, path: '/federate'}, logger: console});
      tf.testManyMessagesOverFederation(bus, 100, 10, done, fedBusCreator);
    });

    it('produces and consumes 100 messages in 100 queues over federation', function(done) {
      var bus = Bus.create({redis: redisUrls, federate: {server: fedserver, path: '/federate'}, logger: console});
      tf.testManyMessagesOverFederation(bus, 100, 100, done, fedBusCreator);
    });

    it('persists 10 objects over federation', function(done) {
      var bus = Bus.create({redis: redisUrls, federate: {server: fedserver, path: '/federate'}, logger: console});
      tf.testManySavesAndLoadesOverFederation(bus, 10, done, fedBusCreator);
    });

    it('persists 100 objects over federation', function(done) {
      var bus = Bus.create({redis: redisUrls, federate: {server: fedserver, path: '/federate'}, logger: console});
      tf.testManySavesAndLoadesOverFederation(bus, 100, done, fedBusCreator);
    });

    it('persists 500 objects over federation', function(done) {
      var bus = Bus.create({redis: redisUrls, federate: {server: fedserver, path: '/federate'}, logger: console});
      tf.testManySavesAndLoadesOverFederation(bus, 500, done, fedBusCreator);
    });

  });
});


