/**
 * Logger.
 * @param tag a tag to use on every message
 * @param logger the underlying logger instance
 * @constructor
 */
function Logger(tag, logger) {
  this._tag = tag;
  this._logger = logger;

  // support the various logging functions
  var _this = this;
  ['log', 'trace', 'debug', 'info', 'warn', 'warning', 'error', 'fatal', 'exception'].forEach(function(f) {
    _this[f] = function() {
      if (_this._logger) {
        var message;
        // add the tag to the message
        for (var i = 0; i < arguments.length; ++i) {
          if (typeof arguments[i] === 'string') {
            arguments[i] = "["+_this._tag+"] " + arguments[i];
            message = arguments[i];
            break;
          }
        }
        var method = _this._logger[f] || _this._logger['log'];
        method.apply(_this._logger, arguments);
      }
    }
  });
}

/**
 * Set the underlying logger
 * @param logger
 */
Logger.prototype.withLog = function(logger) {
  this._logger = logger;
};

/**
 * Create a new logger backed by the same logger object but with a different tag
 * @param tag the tag to log with
 * @returns {Logger}
 */
Logger.prototype.withTag = function(tag) {
  return new Logger(tag, this._logger);
};

exports = module.exports = Logger;