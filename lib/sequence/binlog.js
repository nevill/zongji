var Util = require('util');
var Packet = require('../packet');
var capture = require('../capture');

module.exports = function(connection, options) {
  var Sequence = capture(connection).Sequence;
  var BinlogHeader = Packet.initBinlogHeader(options);

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
      return BinlogHeader;
    }
  };

  Binlog.prototype['OkPacket'] = function(packet) {
    console.log('Received one OkPacket ...');
  };

  Binlog.prototype['BinlogHeader'] = function(packet) {
    if (this._callback) {
      var event, error;
      try{
        event = packet.getEvent();
      }catch(err){
        error = err;
      }
      this._callback.call(this, error, event);
    }
  };

  return Binlog;
};
