var MysqlTypes = {
  'DECIMAL': 0,
  'TINY': 1,
  'SHORT': 2,
  'LONG': 3,
  'FLOAT': 4,
  'DOUBLE': 5,
  'NULL': 6,
  'TIMESTAMP': 7,
  'LONGLONG': 8,
  'INT24': 9,
  'DATE': 10,
  'TIME': 11,
  'DATETIME': 12,
  'YEAR': 13,
  'NEWDATE': 14,
  'VARCHAR': 15,
  'BIT': 16,
  'NEWDECIMAL': 246,
  'ENUM': 247,
  'SET': 248,
  'TINY_BLOB': 249,
  'MEDIUM_BLOB': 250,
  'LONG_BLOB': 251,
  'BLOB': 252,
  'VAR_STRING': 253,
  'STRING': 254,
  'GEOMETRY': 255,
};

// Debug helper function
var zeroPad = function(num, size) {
  // Max 32-bits
  var s = "00000000000000000000000000000000" + num;
  return s.substr(s.length-size);
};

exports.remain = function(parser) {
  return parser._packetEnd - parser._offset;
};

exports.parseUInt64 = function(parser) {
  var low = parser.parseUnsignedNumber(4);
  var high = parser.parseUnsignedNumber(4);
  // jshint bitwise: false
  return (high << 32) + low;
};

exports.parseUInt48 = function(parser) {
  var low = parser.parseUnsignedNumber(4);
  var high = parser.parseUnsignedNumber(2);
  // jshint bitwise: false
  return (high << 32) + low;
};

exports.parseBytesArray = function(parser, length) {
  var result = new Array(length);
  for (var i = 0; i < length; i++) {
    result[i] = parser.parseUnsignedNumber(1);
  }
  return result;
};

var parseSetColumnType = function(type){
  var options = type.match(/^set\(([^)]+)\)$/i);
  if(!options) return options;
  options = options[1].split(','); // SET type options should not include commas
  return options.map(function(opt){
    return opt.replace(/^['"]/, '').replace(/['"]$/, '');
  });
};

var sliceBits = function(input, start, end){
  // Check single bit
  if(start === end || end === undefined)
    return (input & (1 << start)) !== 0 ? 1 : 0;
  // ex: start: 10, end: 15 = "111110000000000"
  var match = (((1 << end) - 1) ^ ((1 << start) - 1));
  return (input & match) >> start;
};

// See information about IEEE-754 Floating point numbers:
// http://www.h-schmidt.net/FloatConverter/IEEE754.html
// http://babbage.cs.qc.cuny.edu/IEEE-754.old/64bit.html
// Pass only high for 32-bit float, pass high and low for 64-bit double
var parseIEEE754Float = function(high, low){
  var lastSignificantBit, sigFigs, expLeading;
  if(low !== undefined){
    // 64-bit: 1 sign, 11 exponent, 52 significand
    lastSignificantBit = 20;
    sigFigs = 52;
    expLeading = 1023; // 2^(11-1) - 1
  }else{
    // 32-bit: 1 sign, 8 exponent, 23 significand
    lastSignificantBit = 23;
    sigFigs = 23;
    expLeading = 127; // 2^(8-1) - 1
  }

  var sign            = sliceBits(high, 31) === 1 ? -1 : 1;
  var exponent        = sliceBits(high, lastSignificantBit, 31) - expLeading;
  var significandBits = sliceBits(high, 0, lastSignificantBit);
  var significand     = 1; // Becomes value between 1, 2

  for(var i = 0; i < lastSignificantBit; i++){
    if(significandBits & (1 << i)){
      significand += 1 / (1 << (sigFigs - i));
    }
  }

  if(low !== undefined){
    for(var i = 0; i < 32; i++){
      if(low & (1 << i)){
        // Bitwise operators only work on up to 32 bits
        significand += 1 / Math.pow(2, sigFigs - i);
      }
    }
  }

  return sign * Math.pow(2, exponent) * significand;
};

var readInt24BE = function(buf, offset, noAssert) {
  return (buf.readInt8(offset, noAssert) << 16) +
          buf.readUInt16BE(offset + 1, noAssert);
};

var readIntBE = function(buf, offset, length, noAssert){
  switch(length){
    case 1: return buf.readInt8(offset, noAssert);
    case 2: return buf.readInt16BE(offset, noAssert);
    case 3: return readInt24BE(buf, offset, noAssert);
    case 4: return buf.readInt32BE(offset, noAssert);
  }
};

// Adapted from jeremycole's Ruby implementation:
// https://github.com/jeremycole/mysql_binlog/blob/master/lib/mysql_binlog/binlog_field_parser.rb
// Some more information about DECIMAL types:
// http://dev.mysql.com/doc/refman/5.5/en/precision-math-decimal-characteristics.html
var parseNewDecimal = function(parser, column) {
  // Constants of format
  var digitsPerInteger = 9;
  var bytesPerInteger = 4;
  var compressedBytes = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];

  var scale = column.metadata.decimals;
  var integral = column.metadata.precision - scale;
  var uncompIntegral = Math.floor(integral / digitsPerInteger);
  var uncompFractional = Math.floor(scale / digitsPerInteger);
  var compIntegral = integral - (uncompIntegral * digitsPerInteger);
  var compFractional = scale - (uncompFractional * digitsPerInteger);

  // Grab buffer portion
  var size = (uncompIntegral * 4) + compressedBytes[compIntegral] +
             (uncompFractional * 4) + compressedBytes[compFractional];
  var buffer = parser._buffer.slice(parser._offset, parser._offset + size);
  parser._offset += size; // Move binlog parser position forward

  var str, mask, pos = 0;
  var isPositive = (buffer.readInt8(0) & (1 << 7)) === 128;
  buffer.writeInt8(buffer.readInt8(0) ^ (1 << 7), 0, true);
  if(isPositive){
    // Positive number
    str = '';
    mask = 0;
  }else{
    // Negative number
    str = '-';
    mask = -1;
  }

  // Build integer digits
  var compIntegralSize = compressedBytes[compIntegral];
  if(compIntegralSize > 0){
    str += (readIntBE(buffer, 0, compIntegralSize) ^ mask).toString(10);
    pos += compIntegralSize;
  }

  for(var i = 0; i < uncompIntegral; i++){
    str += (buffer.readInt32BE(pos) ^ mask).toString(10);
    pos += 4;
  }

  str += '.'; // Proceeding bytes are fractional digits

  for(var i = 0; i < uncompFractional; i++){
    str += (buffer.readInt32BE(pos) ^ mask).toString(10);
    pos += 4;
  }

  var compFractionalSize = compressedBytes[compFractional];
  if(compFractionalSize > 0){
    str += (readIntBE(buffer, pos, compFractionalSize) ^ mask).toString(10);
  }

  return parseFloat(str);
};

var convertToMysqlType = exports.convertToMysqlType = function(code) {
  var result;
  Object.keys(MysqlTypes).forEach(function(name) {
    if (MysqlTypes[name] === code) {
      result = name;
      return;
    }
  });
  return result;
};

exports.readMysqlValue = function(parser, column, columnSchema) {
  var result;
  // jshint indent: false
  switch (column.type) {
    case MysqlTypes.TINY:
      result = parser.parseUnsignedNumber(1);
      break;
    case MysqlTypes.SHORT:
    case MysqlTypes.YEAR:
      result = parser.parseUnsignedNumber(2);
      break;
    case MysqlTypes.LONG:
    case MysqlTypes.INT24:
      result = parser.parseUnsignedNumber(4);
      break;
    case MysqlTypes.LONGLONG:
      result = exports.parseUInt64(parser);
      break;
    case MysqlTypes.FLOAT:
      // 32-bit IEEE-754
      var raw = parser.parseUnsignedNumber(4);
      result = parseIEEE754Float(raw);
      break;
    case MysqlTypes.DOUBLE:
      // 64-bit IEEE-754
      var low = parser.parseUnsignedNumber(4);
      var high = parser.parseUnsignedNumber(4);
      result = parseIEEE754Float(high, low);
      break;
    case MysqlTypes.NEWDECIMAL:
      result = parseNewDecimal(parser, column);
      break;
    case MysqlTypes.SET:
      var raw = parser.parseUnsignedNumber(column.metadata.size);
      var choices = parseSetColumnType(columnSchema.COLUMN_TYPE);
      result = [];
      for(var i = 0; raw > Math.pow(2, i); i++){
        if(raw & Math.pow(2, i)) result.push(choices[i]);
      }
      break;
    case MysqlTypes.VAR_STRING:
      result = parser.parseLengthCodedString();
      break;
    case MysqlTypes.VARCHAR:
    case MysqlTypes.STRING:
      var prefixSize = column.metadata['max_length'] > 255 ? 2 : 1;
      result = parser.parseString(
        parser.parseUnsignedNumber(prefixSize));
      break;
    case MysqlTypes.TINY_BLOB:
    case MysqlTypes.MEDIUM_BLOB:
    case MysqlTypes.LONG_BLOB:
    case MysqlTypes.BLOB:
      var lengthSize = column.metadata['length_size'];
      result = parser.parseString(
        parser.parseUnsignedNumber(lengthSize));
      break;
    // TODO: Types still to implement!
    case MysqlTypes.NULL:
    case MysqlTypes.TIMESTAMP:
    case MysqlTypes.DATE:
    case MysqlTypes.TIME:
    case MysqlTypes.DATETIME:
    case MysqlTypes.NEWDATE:
    case MysqlTypes.BIT:
    case MysqlTypes.ENUM:
    case MysqlTypes.GEOMETRY:
    case MysqlTypes.DECIMAL: // Deprecated in MySQL > 5.0.3
    default:
      throw new Error('Unsupported type: ' + convertToMysqlType(column.type));
  }
  return result;
};
