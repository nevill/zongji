var util = require('util');
var EventEmitter = require('events').EventEmitter;
var binding = require('./build/Release/zongji');

function ZongJi(connection) {
  EventEmitter.call(this);
  this.connection = connection;
}

util.inherits(ZongJi, EventEmitter);

var connect = function() {
  var connection = binding.connect();
  return new ZongJi(connection);
};

ZongJi.prototype.setOption = function(options) {
  this.params = {};
  params.logLevel = options.logLevel || 'info';
  params.retryLimit = options.retryLimit || 10;
  params.timeout = options.timeout || 3; // in seconds
};

ZongJi.prototype.start = function() {
  var self = this;
  this.connection.waitForNextEvent(function(err, event) {
    self.emit(event.type. event);
  });
};

exports.connect = connect;
