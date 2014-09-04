var crypto = require('crypto');
var bunyan = require('bunyan');
var cluster = require('cluster');
var config = require('./config').rabbitmq;
var logger = bunyan.createLogger({name: 'rabbitmq', level: config.logLevel});

process.on('uncaughtException', function (err) {
  console.log('Caught exception: ' + ((err instanceof Error) ? err.stack : err));
});

// -- state variables
var message = new Buffer(config.message);

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
  };
}

// -- setup queues
var queuesReady = 0;
var queuesDone = 0;
var producers = [];
function setupQueue(i) {
  var pqName = 'w'+((workerId+1) % config.numWorkers)+'-q'+i;
  var cqName = 'w'+(workerId % config.numWorkers)+'-q'+i;
  connection.createChannel(function(err, c) {
    if (err) {
      return console.log('RABBITMQ ERROR: ' + err);
    }
    c.assertQueue(cqName);
    c.qName = cqName;
    c.consumed = 0;
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
      p.pumped = 0;
      p.on('close', function() {
        logger.debug('producer CLOSE');
      });
      p.on('error', function(err) {
        logger.debug('producer ERROR' + err);
      });
      p.on('drain', function() {
        pump(p);
      })

      c.consume(c.qName, function(m) {
        if (++c.consumed === config.numMessages) {
          c.consumed = 0;
          if (++queuesDone === config.numQueues) {
            queuesDone = 0;
            reportBenchmark();
          }
        }
      });

      if (++queuesReady === config.numQueues) {
        readyBenchmark();
      }
    });
  });
}

// -- start the benchmark
function readyBenchmark() {
  process.on('message', function(message) {
    switch (message) {
      case 'start':
        producers.forEach(function(p) {
          pump(p);
        });
        break;
    }
  })
  process.send('ready');
}

function reportBenchmark() {
  process.send('report');

//  producers.forEach(function(p) {
//    p.detach();
//  });
//  consumers.forEach(function(c) {
//    c.detach();
//  });
}

// -- pump messages on a producer
function pump(p) {
  function push() {
    if (p.sendToQueue(p.qName, message) && ++p.pumped < config.numMessages) {
      return setImmediate(push);
    }
    p.pumped = 0;
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


