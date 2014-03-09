var mysql = require('mysql');
var util = require('util');
var EventEmitter = require('events').EventEmitter;

var capture = require('./lib/capture');
var patch = require('./lib/patch');

function ZongJi(connection) {
  EventEmitter.call(this);
  this.connection = connection;
  this.ready = false;
}

util.inherits(ZongJi, EventEmitter);

ZongJi.prototype.start = function() {
  var self = this;
  var connection = this.connection;

  if (!this.ready) {
    connection.connect();
    this.ready = true;
  }
  connection.dumpBinlog(function(err, packet) {
    self.emit('binlog', packet);
  });
};

exports.connect = function(dsn) {
  var connection = mysql.createConnection(dsn);
  patch(capture(connection));
  return new ZongJi(connection);
};
