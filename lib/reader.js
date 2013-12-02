function BufferReader(buffer) {
  this.buffer = buffer;
  this.position = 0;
}

BufferReader.prototype.readUInt8 = function() {
  var pos = this.position;
  this.position += 1;

  return this.buffer.readUInt8(pos);
};

BufferReader.prototype.readUInt16 = function() {
  var pos = this.position;
  this.position += 2;

  return this.buffer.readUInt16LE(pos);
};

BufferReader.prototype.readUInt32 = function() {
  var pos = this.position;
  this.position += 4;

  return this.buffer.readUInt32LE(pos);
};

BufferReader.prototype.readUInt64 = function() {
  var pos = this.position;
  this.position += 8;

  // from http://stackoverflow.com/questions/17687307/convert-a-64bit-little-endian-integer-to-number
  return this.buffer.readInt32LE(pos) +
    0x100000000 * this.buffer.readUInt32LE(pos + 4);
};

BufferReader.prototype.readString = function() {
  var strBuf = this.buffer.slice(this.position);
  this.position = this.buffer.length;

  return strBuf.toString('ascii');
};

BufferReader.prototype.readStringInBytes = function(length) {
  var strBuf = this.buffer.slice(this.position, this.position + length);
  this.position += length;

  return strBuf.toString('ascii');
};

exports.BufferReader = BufferReader;
