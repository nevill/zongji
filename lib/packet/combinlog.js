function ComBinlog(options) {
  options = options || {};
  this.command = 0x12;
  this.position = options.position || 4;

  // will send eof package if there is no more binlog event
  // https://dev.mysql.com/doc/internals/en/com-binlog-dump.html#binlog-dump-non-block
  this.flags = options.nonBlock ? 1 : 0;

  this.serverId = options.serverId || 1;
  this.filename = options.filename || '';
}

ComBinlog.prototype.write = function(writer) {
  writer.writeUnsignedNumber(1, this.command);
  writer.writeUnsignedNumber(4, this.position);
  writer.writeUnsignedNumber(2, this.flags);
  writer.writeUnsignedNumber(4, this.serverId);
  writer.writeNullTerminatedString(this.filename);
};

ComBinlog.prototype.parse = function() {
  throw new Error('should never be callede here');
};

module.exports = ComBinlog;
