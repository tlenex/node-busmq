var crypto = require('crypto');

var messageLength = 1024;
var message = crypto.randomBytes(messageLength/2).toString('hex');
var logLevel = 'info';

exports.redis = {
  system: 'redis',
  urls: ['redis://127.0.0.1:7776', 'redis://127.0.0.1:7777', 'redis://127.0.0.1:7778', 'redis://127.0.0.1:7779'],
  numQueues: 1000,
  numMessages: 50,
  numWorkers: 4,
  messageLength: messageLength,
  message: message,
  logLevel: logLevel
}

exports.rabbitmq = {
  system: 'rabbitmq',
  urls: 'amqp://127.0.0.1:5672',
  numQueues: 1000,
  numMessages: 50,
  numWorkers: 2,
  messageLength: messageLength,
  message: message,
  logLevel: logLevel
}