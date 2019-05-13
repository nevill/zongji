const Util = require('util');
const { EofPacket, ErrorPacket, ComBinlog, initBinlogHeaderClass } = require('../packet');
const Sequence = require('mysql/lib/protocol/sequences').Sequence;

module.exports = function(zongji, options) {
  // options include: tableMap / useChecksum
  var BinlogHeaderPacket = initBinlogHeaderClass(zongji);
  var binlogOptions = options;

  function Binlog(callback) {
    Sequence.call(this, callback);
    this.binlogOptions = binlogOptions;
  }

  Util.inherits(Binlog, Sequence);

  Binlog.prototype.start = function() {
    // options include: position / nonBlock / serverId / filename
    this.emit('packet', new ComBinlog(this.binlogOptions));
  };

  Binlog.prototype.determinePacket = function(firstByte) {
    switch (firstByte) {
    case 0xfe:
      return EofPacket;
    case 0xff:
      return ErrorPacket;
    default:
      return BinlogHeaderPacket;
    }
  };

  Binlog.prototype['OkPacket'] = function() {
    console.log('Received one OkPacket ...');
  };

  Binlog.prototype['BinlogHeader'] = function(packet) {
    if (this._callback) {
      var event, error;
      try {
        event = packet.getEvent();
      } catch (err) {
        error = err;
      }
      this._callback.call(this, error, event);
    }
  };

  return Binlog;
};
