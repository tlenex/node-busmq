var crypto = require('crypto');
var bunyan = require('bunyan');
var cluster = require('cluster');
var config = require('./config').redis;
var logger = bunyan.createLogger({name: 'bus', level: config.logLevel});

process.on('uncaughtException', function (err) {
  console.log('Caught exception: ' + ((err instanceof Error) ? err.stack : err));
});

// -- state
var producers = [];
var consumers = [];

// -- create the bus
var bus = require('./../lib/bus').withLog(logger);
bus.on('error', function(err) {
  console.log('BUS ERROR: ' + err);
});
bus.on('offline', function(err) {
  //logger.debug('Bus is offline');
  cluster.worker.disconnect();
});
bus.on('online', function() {
  //logger.debug('Bus is online');
  setupBenchmark();
});
bus.connect(config.urls);

// -- setup the benchmark
function setupBenchmark() {
  for (var i = 0; i < config.numQueues; ++i) {
    setupQueue(i);
  };
}

// -- setup queues
var queuesReady = 0;
var queuesDone = 0;
function setupQueue(i) {
  var pqName = 'w'+((workerId+1) % config.numWorkers)+'-q'+i;
  var cqName = 'w'+(workerId % config.numWorkers)+'-q'+i;
//  console.log('worker %s, produce to %s, consume from %s', workerId, pqName, cqName);
  var p = bus.queue(pqName);
  p.on('error', function(err) {
    logger.error('producer ERROR: ' + err);
  });
  p.on('attaching', function() {
    //logger.debug('producer ATTACHING');
  });
  p.on('detached', function() {
    //logger.debug('producer DETACHED');
    producers.pop();
    tryDisconnect();
  });
  p.on('closed', function() {
    //logger.debug('producer CLOSED');
  });
  p.on('attached', function() {
    var c = bus.queue(cqName);
    c.on('attached', function() {
      c.consume();
    });
    c.on('consuming', function(consuming) {
      if (consuming) {
        if (++queuesReady === config.numQueues) {
          // all producers and consumers are ready
          readyBenchmark();
        }
      }
    });
    c.on('message', function(m) {
//      console.log('[%s] consumed=%s', c.name, c.consumed);
        // this queue is done
      if (c.consumed() === config.numMessages) {
        c._consumed = 0;
        // are all queues done?
        if (++queuesDone === config.numQueues) {
          queuesDone = 0;
          reportBenchmark();
        }
      }
    });
    c.on('error', function(err) {
      logger.error('consumer ERROR: ' + err);
    });
    c.on('attaching', function() {
      //logger.debug('consumer ATTACHING');
    });
    c.on('detached', function() {
      //logger.debug('consumer DETACHED');
      consumers.pop();
      tryDisconnect();
    });
    c.on('closed', function() {
      //logger.debug('consumer CLOSED');
    });
    consumers.push(c);
    c.attach();
  });
  producers.push(p);
  p.attach();
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
}

function reportBenchmark() {
  if (cluster.isWorker) {
    process.send('report');
  }
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
    p.push(config.message, function() {
      if (p.pushed() < config.numMessages) {
        return push();
      }
      p._pushed = 0;
    });
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

// -- try to exit the benchmark
function tryDisconnect() {
  if (producers.length === 0 && consumers.length === 0) {
    bus.disconnect();
  }
}


