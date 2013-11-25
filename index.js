var util = require('util'),
    url = require('url'),
    EventEmitter = require('events').EventEmitter;

var binding = require('./build/Release/zongji');
var binlogevent = require('./binlog_event');

function parseDSN(dsn) {
  var result;

  var params = url.parse(dsn);

  if (params.hostname) {
    if (params.protocol !== 'mysql:') {
      throw new Error("only be enable to connect MySQL server");
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

function ZongJi(options) {
  EventEmitter.call(this);
  this.options = options;
  this.ready = false;
}

util.inherits(ZongJi, EventEmitter);

ZongJi.prototype.setOption = function(options) {
  this.params = {};
  params.logLevel = options.logLevel || 'info';
  params.retryLimit = options.retryLimit || 10;
  params.timeout = options.timeout || 3; // in seconds
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

  ZongJi.prototype.start = function() {
    var self = this;

    if (!this.ready) {
      this.ready = connection.beginBinlogDump();
    }

    var nextEventCb = function(err, eventBuffer) {
      var theEvent = binlogevent.create(eventBuffer);
      // console.log("Event buffer(%d):", eventBuffer.length, eventBuffer.slice(0,20));
      //TODO record next binlog to resume
      if (theEvent instanceof binlogevent.Rotate) {
        // var pos = theEvent.position;
        // var binlogFile = theEvent.binlogName;
      }
      self.emit(theEvent.getEventName(), theEvent);
    };

    while(true) {
      connection.waitForNextEvent(nextEventCb);
    }
  };

  return new ZongJi(options);
};

exports.parseDSN = parseDSN;
