/**
 * Logger.
 * @param tag aa tag to use before every message
 * @param logger the underlying logger instance
 * @constructor
 */
function Logger(tag, logger) {
  this.tag = tag;
  this.logger = logger;

  // support the various logging functions
  var _this = this;
  ['log', 'trace', 'debug', 'info', 'warn', 'warning', 'error', 'fatal', 'exception'].forEach(function(f) {
    _this[f] = function() {
      var message;
      // add the tag to the message
      for (var i = 0; i < arguments.length; ++i) {
        if (typeof arguments[i] === 'string') {
          arguments[i] = "["+_this.tag+"] " + arguments[i];
          message = arguments[i];
          break;
        }
      }

      if (!_this.logger) {
        console.log(f.toUpperCase() + " " + message);
      } else {
        _this.logger[f].apply(_this.logger, arguments);
      }
    }
  });
}

/**
 * Set the underlying logger
 * @param logger
 */
Logger.prototype.withLog = function(logger) {
  this.logger = logger;
}

/**
 * Create a new logger backed by the same logger object but with a different tag
 * @param tag the tag to log with
 * @returns {Logger}
 */
Logger.prototype.withTag = function(tag) {
  return new Logger(tag, this.logger);
}

exports = module.exports = Logger;