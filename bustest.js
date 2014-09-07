var crypto = require('crypto');

var binary = crypto.randomBytes(256).toString('utf8');
console.log('BINARY LENGTH: ' + binary.length);

var bus = require('./lib/bus').create();

bus.on('error', function(err) {
  console.error('ERROR: ' + err);
});
bus.on('offline', function(err) {
  console.error('OFFLINE: ' + err);
});
bus.on('online', function() {
  console.info('ONLINE');
  produce();
});
bus.connect(['redis://127.0.0.1:7776', 'redis://127.0.0.1:7777']);

var queueName = crypto.randomBytes(8).toString('hex');

function produce() {
  var queue = bus.queue(queueName);
  queue.on('error', function(err) {
    console.error('producer ERROR: ' + err);
  });
  queue.on('attaching', function() {
    console.info('producer ATTACHING');
  });
  queue.on('detached', function() {
    console.info('producer DETACHED');
  });
  queue.on('closed', function() {
    console.info('producer CLOSED');
  });
  queue.on('attached', function() {
    console.info('producer ATTACHED');
    consume();
    queue.push({hello: 'world'});
    queue.push({binary: binary});
    queue.detach();
  });

  queue.attach();
}

function consume() {
  var queue2 = bus.queue(queueName);

  queue2.on('message', function(message) {
    console.info('consumer MESSAGE: ' + message);
    var parsed = JSON.parse(message);
    if (parsed.binary) {
      console.info('BINARY MESSAGE RECEIVED: ' + (parsed.binary === binary));
    }

    setTimeout(function() {
      bus.isOnline() && queue2.push({hello: 'world'});
    }, 1000);

  });
  queue2.on('error', function(err) {
    console.info('consumer ERROR: ' + err);
  });
  queue2.on('attaching', function() {
    console.info('consumer ATTACHING');
  });
  queue2.on('detached', function() {
    console.info('consumer DETACHED');
  });
  queue2.on('closed', function() {
    console.info('consumer CLOSED');
    bus.disconnect();
  });
  queue2.on('attached', function() {
    console.info('consumer ATTACHED');
    queue2.consume();
  });
  setTimeout(function() {
    queue2.stop();
    setTimeout(function() {
      queue2.consume();
      setTimeout(function() {
//        queue2.detach();
        queue2.close();
      }, 2000);
    }, 2000);
  }, 2000);
  queue2.attach();
}
