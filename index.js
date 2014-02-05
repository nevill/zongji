var util = require('util'),
    url = require('url'),
    EventEmitter = require('events').EventEmitter;

var binding = require('./build/Release/zongji');
var binlog = require('./lib');

function parseDSN(dsn) {
  var params = url.parse(dsn);

  if (params.hostname) {
    if (params.protocol !== 'mysql:') {
      throw new Error('only be enable to connect MySQL server');
    }
    var hostname = params.hostname,
        port     = params.port || 3306,
        auth     = params.auth ? params.auth.split(':') : [ ],
        user     = auth.shift() || '',
        password = auth.shift() || '';

    return [ user, password, hostname, port ];

  } else {
    throw new Error('bad DSN string, cannot connect to MySQL server');
  }
}

function ZongJi(connection, options) {
  EventEmitter.call(this);
  this.connection = connection;
  this.options = options;
  this.ready = false;
}

util.inherits(ZongJi, EventEmitter);

ZongJi.prototype.setOption = function(options) {
  this.params = {};
  this.params.logLevel = options.logLevel || 'info';
  this.params.retryLimit = options.retryLimit || 10;
  this.params.timeout = options.timeout || 3; // in seconds
};

ZongJi.prototype.start = function() {
  var self = this;

  if (!this.ready) {
    this.ready = this.connection.beginBinlogDump();
  }

  var nextEventCb = function(err, eventBuffer) {
    var theEvent = binlog.create(eventBuffer);
    //console.log('\nEvent header (%d):', eventBuffer.length, eventBuffer.slice(0,20));
    //TODO record next binlog to resume
    if (theEvent instanceof binlog.Rotate) {
      // var pos = theEvent.position;
      // var binlogFile = theEvent.binlogName;
    }
    self.emit(theEvent.getEventName(), theEvent);
    self.connection.waitForNextEvent(nextEventCb);
  };

  this.connection.waitForNextEvent(nextEventCb);
};

exports.connect = function(dsn) {
  var connection = binding.init();
  var params = parseDSN(dsn);
  var options = {
    user: params[0],
    password: params[1],
    host: params[2],
    port: params[3]
  };
  connection.connect.apply(connection, params);

  return new ZongJi(connection, options);
};

exports.parseDSN = parseDSN;
