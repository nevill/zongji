var Util = require('util');
var Packet = require('../packet');

var BinlogClass;

module.exports = function(claz) {
  var Sequence = claz.Sequence;

  function Binlog(callback) {
    Sequence.call(this, callback);
  }

  Util.inherits(Binlog, Sequence);

  Binlog.prototype.start = function() {
    this.emit('packet', new Packet.ComBinlog());
  };

  Binlog.prototype.determinePacket = function(firstByte) {
    switch (firstByte) {
    case 0xfe:
      return Packet.Eof;
    case 0xff:
      return Packet.Error;
    default:
      return Packet.BinlogHeader;
    }
  };

  Binlog.prototype['OkPacket'] = function(packet) {
    console.log('Received one OkPacket ...');
  };

  Binlog.prototype['BinlogHeader'] = function(packet) {
    if (this._callback) {
      this._callback.call(this, null, packet.getEvent());
    }
  };

  BinlogClass = Binlog;

  return BinlogClass;
};
