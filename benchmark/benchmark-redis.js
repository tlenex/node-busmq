var crypto = require('crypto');
var cluster = require('cluster');
var config = require('./config').redis;

process.on('uncaughtException', function (err) {
  console.log('Caught exception: ' + ((err instanceof Error) ? err.stack : err));
});

// -- state
var producers = [];
var consumers = [];

// -- create the bus
var bus = require('./../lib/bus').create({redis: config.urls});
bus.on('error', function(err) {
  console.log('BUS ERROR: ' + err);
});
bus.on('offline', function(err) {
  cluster.worker.disconnect();
});
bus.on('online', function() {
  setupBenchmark();
});
bus.connect();

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
  var p = bus.queue(pqName);
  p.on('error', function(err) {
    console.error('producer ERROR: ' + err);
  });
  p.on('attaching', function() {
  });
  p.on('detached', function() {
    producers.pop();
    tryDisconnect();
  });
  p.on('closed', function() {
  });
  p.on('drain', function() {
    setTimeout(function() {
      pump(p);
    }, 0)
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
      ++totalConsumed;
    });
    c.on('error', function(err) {
      console.error('consumer ERROR: ' + err);
    });
    c.on('attaching', function() {
    });
    c.on('detached', function() {
      consumers.pop();
      tryDisconnect();
    });
    c.on('closed', function() {
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
//  producers.forEach(function(p) {
//    p.detach();
//  });
//  consumers.forEach(function(c) {
//    c.detach();
//  });
}

// -- pump messages on a producer
function pump(p) {
  if (p.pumping) return;
  p.pumping = true;
  function push() {
    ++totalPushed;
    if (p.push(config.message) !== false) {
      setTimeout(push,0);
    } else {
      p.pumping = false;
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

// -- try to exit the benchmark
function tryDisconnect() {
  if (producers.length === 0 && consumers.length === 0) {
    bus.disconnect();
  }
}


