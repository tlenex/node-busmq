var crypto = require('crypto');
var cluster = require('cluster');

//var config = require('./config').rabbitmq;
var config = require('./config').redis;

var logger = console;
logger.debug = logger.info;

process.on('uncaughtException', function (err) {
  console.error('Caught exception: ' + ((err instanceof Error) ? err.stack : err));
  process.exit(1);
});

if (cluster.isMaster) {
  // Print summary
  console.log('');
  console.log('Benchmark ' + config.system);
  console.log('------------------------');
  console.log(config.numWorkers + ' workers');
  console.log(config.numQueues + ' queues per worker');
  console.log(config.messageLength + ' bytes per message');
  console.log('------------------------');
  console.log('');

  // Fork workers
  process.stdout.write('Setting up benchmark... ');
  for (var i = 0; i < config.numWorkers; i++) {
    cluster.fork();
  }

  function sendStart() {
    Object.keys(cluster.workers).forEach(function(id) {
      cluster.workers[id].send('start');
    });
    startTime = process.hrtime();
  }

  var startTime;
  var cycle = 0;
  var ready = 0
  var reported = 0;
  var totalPushed = 0;
  var totalConsumed = 0;
  cluster.on('online', function(worker) {
    worker.on('message', function(message) {
      logger.debug('received message ' + message + ' from worker ' + worker.id);
      switch(message) {
        case 'ready':
          if (++ready === config.numWorkers) {
            console.log('done');
            console.log('Running benchmark... ');
            sendStart();
          }
          break;
        default:
          var report = JSON.parse(message);
          totalPushed += report.p;
          totalConsumed += report.c;
          if (++reported === config.numWorkers) {
            var diff = process.hrtime(startTime);
            var seconds = (diff[0] * 1e9 + diff[1])/(1000*1000*1000);
            console.log('[%d] push: %d msgs/sec, consume: %d msgs/sec', ++cycle, Math.ceil(totalPushed / seconds), Math.ceil(totalConsumed / seconds));
            reported = 0;
            totalPushed = 0;
            totalConsumed = 0;
            startTime = process.hrtime();
          }
          break;
      }
    });
  });
//  cluster.on('exit', function(worker, code, signal) {
//    console.log('worker ' + worker.process.pid + ' died');
//  });
} else {
  require('./benchmark-'+config.system);
}
