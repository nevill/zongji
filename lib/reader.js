// Constants for variable length encoded binary
var NULL_COLUMN = 251;
var UNSIGNED_CHAR_COLUMN = 251;
var UNSIGNED_SHORT_COLUMN = 252;
var UNSIGNED_INT24_COLUMN = 253;
var UNSIGNED_INT64_COLUMN = 254;

var UNSIGNED_CHAR_LENGTH = 1;
var UNSIGNED_SHORT_LENGTH = 2;
var UNSIGNED_INT24_LENGTH = 3;
var UNSIGNED_INT64_LENGTH = 8;

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

BufferReader.prototype.readUInt24 = function() {
  var low = this.readUInt16();
  var high = this.readUInt8();
  return (high << 16) + low;
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

BufferReader.prototype.readHexInBytes = function(length) {
  var buf = this.buffer.slice(this.position, this.position + length);
  this.position += length;

  return buf.toString('hex');
};

BufferReader.prototype.readBytesArray = function(length) {
  var result = [];
  var hexString = this.readHexInBytes(length);
  for (var i = 0; i < hexString.length; i = i + 2) {
    result.push(parseInt(hexString.substr(i, 2), 16));
  }
  return result;
};

// Read a variable-length "Length Coded Binary" integer. This is derived
// from the MySQL protocol, and re-used in the binary log format. This
// format uses the first byte to alternately store the actual value for
// integer values <= 250, or to encode the number of following bytes
// used to store the actual value, which can be 2, 3, or 8. It also
// includes support for SQL NULL as a special case.
BufferReader.prototype.readVariant = function() {
  var result = null;
  var firstByte = this.readUInt8();

  if (firstByte < UNSIGNED_CHAR_COLUMN) {
    result = firstByte;
  } else if (firstByte === NULL_COLUMN) {
    result = null;
  } else if (firstByte === UNSIGNED_SHORT_COLUMN) {
    result = this.readUInt16();
  } else if (firstByte === UNSIGNED_INT24_COLUMN) {
    result = this.readUInt24();
  } else if (firstByte === UNSIGNED_INT64_COLUMN) {
    result = this.readUInt64();
  } else {
    throw new Error('Invalid variable-length integer');
  }

  return result;
};

var padWith = function(val, length) {
  var bits = val.split('');
  if (bits.length < length) {
    var left = length - bits.length;
    for (var j = left - 1; j >= 0; j--) {
      bits.unshift('0');
    }
    val = bits.join('');
  }

  return val;
};

// Read an arbitrary-length bitmap, provided its length.
// Returns an array of true/false values.
BufferReader.prototype.readBitArray = function(length) {
  var size = Math.floor((length + 7) / 8);

  var bytes = [];
  for (var i = size - 1; i >= 0; i--) {
    bytes.unshift(this.readUInt8());
  }

  var bitmap = [];
  var bitmapStr = bytes.map(function(aByte) {
    return padWith(aByte.toString(2), 8);
  }).join('');

  for (var k = bitmapStr.length - 1; k >= 0; k--) {
    bitmap.push(bitmapStr[k] === '1');
  }

  return bitmap.slice(0, length);
};

exports.BufferReader = BufferReader;
