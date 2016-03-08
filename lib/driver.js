var _url = require('url');

function driver( name ) {
  switch ( name ) {
    case 'node-redis':
      return getNodeRedisDriver();
    case 'ioredis':
      return getIORedisDriver();
    default:
      throw 'unsupported driver: ' + name;
  }
}

function getNodeRedisDriver() {

  var driver = require('redis');

  function normalizeOptions(options) {
    return options || {};
  }

  function normalizeClient(client) {
    client.retry_delay = 1000;
    client.retry_backoff = 1;
    //normalize to getServerInfo()
    client.getServerInfo = function(){return client.server_info};
    return client;
  }

  return {
    name: 'node-redis',
    direct: function(url, options) {
      var opts = normalizeOptions(options);
      var parsed = _url.parse( url );
      if (parsed.auth) {
        opts.auth_pass =  parsed.auth;
      }
      return normalizeClient(driver.createClient(parsed.port || 6379, parsed.hostname, opts));
    },
    sentinels: function(urls, options) {
      throw new Error('the node-redis driver does not support sentinels');
    },
    cluster: function(urls, options) {
      throw new Error('the node-redis driver does not support redis clusters');
    }
  }
}

function getIORedisDriver() {

  var driver = require('ioredis');

  function normalizeDirectOptions(url, options) {
    var opts = options || {};

    var parsed = _url.parse( url );
    opts.port = parsed.port || 6379;
    opts.host = parsed.hostname;
    if (parsed.auth) {
      opts.password =  parsed.auth;
    }
    opts.retryStrategy = function(times) {
      return 1000;
    };
    return opts;
  }

  function normalizeSentinelsOptions(urls, options) {
    var us = Array.isArray(urls) ? urls : [urls];
    var sentinels = us.map(function(u) {
      var parsed = _url.parse( u );
      return {host: parsed.hostname, port: parsed.port || 26379, password: parsed.auth};
    });

    var opts = options || {};
    opts.sentinels = sentinels;
    opts.name = options.name || 'mymaster';
    opts.retryStrategy = function(times) {
      return 1000;
    };
    opts.sentinelRetryStrategy = function(times) {
      return 1000;
    };
    return opts;
  }


  function normalizeClusterOptions(options) {
    var opts = options || {};
    opts.retryStrategy = function(times) {
      return 1000;
    };
    opts.clusterRetryStrategy = function(times) {
      return 1000;
    };
    opts.retryDelayOnClusterDown = 1000;
    return opts;
  }

  function normalizeClient(client) {
    //normalize to getServerInfo()
    client.getServerInfo = function(){return client.serverInfo};
    return client;
  }

  return {
    name: 'ioredis',
    direct: function(url, options) {
      return normalizeClient(new driver(normalizeDirectOptions(url, options)));
    },
    sentinels: function(urls, options) {
      return normalizeClient(new driver(normalizeSentinelsOptions(urls,options)));
    },
    cluster: function(urls, options) {
      var us = Array.isArray(urls) ? urls : [urls];
      var nodes = us.map(function(u) {
        var parsed = _url.parse( u );
        return {host: parsed.hostname, port: parsed.port || 26379, password: parsed.auth_pass};
      });
      return normalizeClient(new driver.Cluster(nodes,normalizeClusterOptions(options)));
    }
  }
}

exports = module.exports = driver;
