var util = require('util');

exports = module.exports = function(bus, name, object, attributes) {

  object._persistinit = function(bus, name, attrs) {
    if (this._p) {
      return;
    }
    this._p = {};
    this._p.bus = bus;
    this._p.name = name;
    this._p.id = 'bus:persisted:' + name;
    if (!this.id) {
      this.id = this._p.id;
    }
    this._p.logger = bus.logger.withTag(this.id);
    if (!this.logger) {
      this.logger = this._p.logger;
    }
    this._p.attributes = attrs;
    this._p.persistKey = this._p.id;
    this._p.persistDirty = {};
    this._p.persistAttrs = {};
    this._p.persistTtl = 60;
    var _this = this;
    attrs.forEach(function(attr) {
      Object.defineProperty(_this, attr, {
        enumerable: true,
        set: function(value) {
          _this._p.persistDirty[attr] = value;
        },
        get: function() {
          return _this._p.persistDirty.hasOwnProperty(attr) ? _this._p.persistDirty[attr] : _this._p.persistAttrs[attr];
        }
      })
    });
  };

  object._pconnection = function() {
    var _this = this;
    if (!this._p.connection) {
      this._p.connection = this._p.bus._connectionFor(this._p.persistKey);
    }

    return this._p.connection;
  };

  object.persist = function(ttl) {
    if (this._p.persistTimer) {
      return;
    }

    this._p.persistTtl = ttl;
    var _this = this;
    // start the ptouch timer to keep the object alive
    this._p.persistTimer = setInterval(function() {
      _this.ptouch();
    }, (ttl / 3) * 1000);
  };

  object.unpersist = function() {
    if (this._p.persistTimer) {
      clearInterval(this._p.persistTimer);
      this._p.persistTimer = null;
    }

    if (this._p.connection) {
      this._p.connection = null;
    }

    this._p.persistDirty = {};
    this._p.persistAttrs = {};
  };

  object.ptouch = function() {
    var _this = this;
    this._pconnection().expire(this._p.persistKey, this._p.persistTtl, function(err, resp) {
      if (err) {
        _this._p.logger.error("error touching object " + _this._p.persistKey + ": " + err);
      }
    })
  };

  object.load = function(cb) {
    var _this = this;
    this.pread(this._p.persistKey, function(err, resp) {
      if (err) {
        _this._p.logger.error("error loading object " + _this._p.persistKey + ": " + err);
        cb && cb(err);
        return;
      }
      if (resp) {
        Object.keys(resp).forEach(function(key) {
          if (resp.hasOwnProperty(key)) {
            _this._p.persistAttrs[key] = JSON.parse(resp[key]);
          }
        });
      }
      cb && cb(null, resp !== null)
    });
  };

  object.pread = function(key, cb) {
    this._pconnection().hgetall(key, function(err, resp) {
      cb && cb(err, resp);
    });
  };

  object.save = function(cb) {
    var dirtyKeys = Object.keys(this._p.persistDirty);
    if (dirtyKeys.length === 0) {
      cb && cb();
      return;
    }

    var _this = this;
    var dirtyValues = util._extend({}, this._p.persistDirty);
    this.pwrite(this._p.persistKey, dirtyValues, function(err, resp) {
      if (err || resp !== 'OK') {
        _this._p.logger.error("error saving object info " + _this._p.persistKey + " with info " + _this.stringify() + '. error: ' + err);
        cb && cb(err || ('hmset returned ' + resp));
        return;
      }
      // move the saved keys to the un-dirty state
      dirtyKeys.forEach(function(key) {
        _this._p.persistAttrs[key] = dirtyValues[key];
        delete _this._p.persistDirty[key];
      });
      cb && cb();
    });
  };

  object.pwrite = function(key, dirty, cb) {
    var _this = this;
    var fieldsvalues = [key];
    var dirtyKeys = Object.keys(dirty);
    // save only the dirty attributes
    dirtyKeys.forEach(function(key) {
      fieldsvalues.push(key);
      fieldsvalues.push(JSON.stringify(dirty[key]));
    });
    this._pconnection().hmset(fieldsvalues, function(err, resp) {
      _this.ptouch();
      cb && cb(err, resp);
    });
  };


  object.stringify = function() {
    return JSON.stringify(this._p.persistAttrs);
  };

  object.federatedMethods = ['pread', 'pwrite', 'ptouch'];

  object._persistinit(bus, name, attributes);
  return object;
};