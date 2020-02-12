const invariant = require('invariant');

function ComBinlogGTID({serverId, nonBlock, filename, position, gtidsData}) {
  this.command = 0x1e;
  this.position = position || 4;

  this.flags = 0;
  this.flags |= (nonBlock ? 1 : 0);
  this.flags |= 0x04; /*BINLOG_THROUGH_GTID*/

  this.serverId = serverId || 1;
  this.filename = filename || '';

  this.gtidsData = gtidsData || {};
}

/**
 * https://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html
 */
ComBinlogGTID.prototype.write = function(writer) {
  writer.writeUnsignedNumber(1, this.command);
  writer.writeUnsignedNumber(2, this.flags);
  writer.writeUnsignedNumber(4, this.serverId);

  writer.writeUnsignedNumber(4, Buffer.byteLength(this.filename, 'utf-8'));
  writer.writeString(this.filename);

  writer.writeUnsignedNumber(4, this.position);
  writer.writeUnsignedNumber(4, 0); // high-part of this.position

  if (!(this.flags & 0x04)) return;

  const gtidsDataEntries = Object.entries(this.gtidsData);
  // TODO: Support for multiple intervals per SID
  writer.writeUnsignedNumber(4, 8 + gtidsDataEntries.length * 40); //data_length

  writer.writeUnsignedNumber(4, gtidsDataEntries.length); //n_sids
  writer.writeUnsignedNumber(4, 0); //n_sids

  for (let i = 0; i < gtidsDataEntries.length; i++) {
    const [sid, intervals] = gtidsDataEntries[i];

    const sidBuffer = Buffer.from(sid, 'hex');
    invariant(sidBuffer.length === 16, 'SID should be 16-bytes hex-string');
    writer.writeBuffer(sidBuffer);  // sid
    // Buffer.byteLength(value, 'utf-8')

    writer.writeUnsignedNumber(4, intervals.length); // n_intervals
    writer.writeUnsignedNumber(4, 0); // high-part of n_intervals

    for (let j = 0; j < intervals.length; j++) {
      const [start, end] = intervals[j];
      writer.writeUnsignedNumber(4, start);
      writer.writeUnsignedNumber(4, 0); // high-part of start
      writer.writeUnsignedNumber(4, end);
      writer.writeUnsignedNumber(4, 0); // high-part of end
    }
  }
  console.log("!!!ComBinlogGTID::write", this, writer._buffer, writer._offset);
  console.log(writer._buffer.toString('hex'));
};

ComBinlogGTID.prototype.parse = function() {
  throw new Error('should never be called here');
};

module.exports = ComBinlogGTID;
