var crypto = require('crypto');
var cluster = require('cluster');
var config = require('./config').rabbitmq;
var logger = console;
logger.debug = logger.info;

process.on('uncaughtException', function (err) {
  console.log('Caught exception: ' + ((err instanceof Error) ? err.stack : err));
});

var message = new Buffer(config.message);

// -- state
var producers = [];
var consumers = [];


// -- create the bus
var amqp = require('amqplib/callback_api');

var connection;
amqp.connect(config.urls, function(err, con) {
  if (err) {
    return console.log('RABBITMQ ERROR: ' + err);
  }
  connection = con;
  connection.on('close', function() {
    logger.debug('CONNECTION CLOSE');
  });
  connection.on('error', function(err) {
    logger.debug('CONNECTION ERROR: ' + err);
  });
  setupBenchmark();
});


// -- setup the benchmark
function setupBenchmark() {
  for (var i = 0; i < config.numQueues; ++i) {
    setupQueue(i);
  }
}


// -- setup queues
var queuesReady = 0;
var totalConsumed = 0;
var totalPushed = 0;
function setupQueue(i) {
  var pqName = 'w'+((workerId+1) % config.numWorkers)+'-q'+i;
  var cqName = 'w'+(workerId % config.numWorkers)+'-q'+i;
  connection.createChannel(function(err, c) {
    if (err) {
      return console.log('RABBITMQ ERROR: ' + err);
    }
    c.assertQueue(cqName);
    c.qName = cqName;
    c.on('close', function() {
      logger.debug('consumer CLOSE');
    });
    c.on('error', function(err) {
      logger.debug('consumer ERROR' + err);
    });
    connection.createChannel(function(err, p) {
      if (err) {
        return console.log('RABBITMQ ERROR: ' + err);
      }
      producers.push(p);
      p.assertQueue(pqName);
      p.qName = pqName;
      p.on('close', function() {
        logger.debug('producer CLOSE');
      });
      p.on('error', function(err) {
        logger.debug('producer ERROR' + err);
      });
      p.on('drain', function() {
        setTimeout(function() {
          pump(p);
        }, 0)
      });

      c.consume(c.qName, function(m) {
        ++totalConsumed;
      });

      if (++queuesReady === config.numQueues) {
        // all producers and consumers are ready
        readyBenchmark();
      }
    });
  });
}

// -- start the benchmark
function readyBenchmark() {
  if (cluster.isWorker) {
    process.on('message', function(message) {
      switch (message) {
        case 'start':
          startBenchmark();
          break;
      }
    })
    process.send('ready');
  } else {
    startBenchmark()
  }
}

function startBenchmark() {
  producers.forEach(function(p) {
    pump(p);
  });
  setInterval(reportBenchmark, 2000);
}

function reportBenchmark() {
  if (cluster.isWorker) {
    var pushed = totalPushed;
    var consumed = totalConsumed;
    totalPushed = 0;
    totalConsumed = 0;
    process.send(JSON.stringify({p: pushed, c: consumed}));
  }
}

// -- pump messages on a producer
function pump(p) {
  function push() {
    if (p.sendToQueue(p.qName, message)) {
      setTimeout(push, 0);
    }
  }
  push();
}

var workerId = _workerId();
function _workerId() {
  if (cluster.isWorker) {
    return parseInt(cluster.worker.id) - 1;
  }
  return 0;
}

// -- try to exist the benchmark
function tryDisconnect() {
  if (producers.length === 0 && consumers.length === 0) {
    bus.disconnect();
  }
}


