var crypto = require('crypto');
var bunyan = require('bunyan');
var logger = bunyan.createLogger({name: 'bus', level: 'debug'});

var binary = crypto.randomBytes(256).toString('utf8');
console.log('BINARY LENGTH: ' + binary.length);

var bus = require('./lib/bus').withLog(logger);

bus.on('error', function(err) {
  logger.error('ERROR: ' + err);
});
bus.on('offline', function(err) {
  logger.error('OFFLINE: ' + err);
});
bus.on('online', function() {
  logger.info('ONLINE');
  produce();
});
bus.connect(['redis://127.0.0.1', 'redis://127.0.0.1']);

var queueName = crypto.randomBytes(8).toString('hex');

function produce() {
  var queue = bus.queue(queueName);
  queue.on('error', function(err) {
    logger.error('producer ERROR: ' + err);
  });
  queue.on('attaching', function() {
    logger.info('producer ATTACHING');
  });
  queue.on('detached', function() {
    logger.info('producer DETACHED');
  });
  queue.on('closed', function() {
    logger.info('producer CLOSED');
  });
  queue.on('attached', function() {
    logger.info('producer ATTACHED');
    consume();
    queue.push({hello: 'world'});
    queue.push({binary: binary});
    queue.detach();
  });

  queue.create();
}

function consume() {
  var queue2 = bus.queue(queueName);

  queue2.on('message', function(message) {
    logger.info('consumer MESSAGE: ' + message);
    var parsed = JSON.parse(message);
    if (parsed.binary) {
      logger.info('BINARY MESSAGE RECEIVED: ' + (parsed.binary === binary));
    }

    setTimeout(function() {
      queue2.push({hello: 'world'});
    }, 1000);

  });
  queue2.on('error', function(err) {
    logger.info('consumer ERROR: ' + err);
  });
  queue2.on('attaching', function() {
    logger.info('consumer ATTACHING');
  });
  queue2.on('detached', function() {
    logger.info('consumer DETACHED');
  });
  queue2.on('closed', function() {
    logger.info('consumer CLOSED');
    bus.disconnect();
  });
  queue2.on('attached', function() {
    logger.info('consumer ATTACHED');
    queue2.consume();
  });
  setTimeout(function() {
    queue2.stop();
    setTimeout(function() {
      queue2.consume();
      setTimeout(function() {
        queue2.detach();
      }, 5000);
    }, 5000);
  }, 5000);
  queue2.attach();
}
