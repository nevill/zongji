const Util = require('util');
const { EofPacket, ErrorPacket, ComBinlog, initBinlogPacketClass } = require('../packet');
const Sequence = require('mysql/lib/protocol/sequences').Sequence;

module.exports = function(zongji) {
  const BinlogPacket = initBinlogPacketClass(zongji);

  function Binlog(callback) {
    Sequence.call(this, callback);
  }

  Util.inherits(Binlog, Sequence);

  Binlog.prototype.start = function() {
    // options include: position / nonBlock / serverId / filename
    let options = zongji.get([
      'serverId', 'position', 'filename', 'nonBlock',
    ]);
    this.emit('packet', new ComBinlog(options));
  };

  Binlog.prototype.determinePacket = function(firstByte) {
    switch (firstByte) {
    case 0xfe:
      return EofPacket;
    case 0xff:
      return ErrorPacket;
    default:
      return BinlogPacket;
    }
  };

  Binlog.prototype['OkPacket'] = function() {
    console.log('Received one OkPacket ...');
  };

  Binlog.prototype['BinlogPacket'] = function(packet) {
    if (this._callback) {

      // Check event filtering
      if (zongji._skipEvent(packet.eventName.toLowerCase())) {
        return this._callback.call(this);
      }

      let event, error;
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
