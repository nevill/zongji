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
  // Fractional temporal types in MySQL >=5.6.4
  'TIMESTAMP2': 17,
  'DATETIME2': 18,
  'TIME2': 19,
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

exports.parseUInt64 = function(parser) {
  var low = parser.parseUnsignedNumber(4);
  var high = parser.parseUnsignedNumber(4);

  if(this){
    // Pass extra output to context
    this.low = low;
    this.high = high;
  }

  // jshint bitwise: false
  return (high * Math.pow(2,32)) + low;
};

exports.parseUInt48 = function(parser) {
  var low = parser.parseUnsignedNumber(4);
  var high = parser.parseUnsignedNumber(2);
  // jshint bitwise: false
  return (high * Math.pow(2, 32)) + low;
};

exports.parseUInt24 = function(parser) {
  var low = parser.parseUnsignedNumber(2);
  var high = parser.parseUnsignedNumber(1);
  // jshint bitwise: false
  return (high << 16) + low;
};

exports.parseBytesArray = function(parser, length) {
  var result = new Array(length);
  for (var i = 0; i < length; i++) {
    result[i] = parser.parseUnsignedNumber(1);
  }
  return result;
};

var parseSetEnumTypeDef = function(type){
  var prefixLen;
  if(type.substr(0,4).toLowerCase() === 'set('){
    prefixLen = 4;
  }else if(type.substr(0,5).toLowerCase() === 'enum('){
    prefixLen = 5;
  }else{
    throw 'not set or enum type';
  }
  // listed distinct elements should not include commas
  return type.substr(prefixLen, type.length - prefixLen - 1)
    .split(',').map(function(opt){
      return (opt[0] === '"' || opt[0] === "'") ?
        opt.substr(1, opt.length - 2) : opt;
    });
};

var zeroPad = function(num, size) {
  // Max 32 digits
  var s = "00000000000000000000000000000000" + num;
  return s.substr(s.length-size);
};

var sliceBits = function(input, start, end){
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

  var sign            = (high & (1 << 31)) !== 0 ? -1 : 1;
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

var getUInt32Value = function(input){
  // Last bit is not sign, it is part of value!
  if(input & (1 << 31)) return Math.pow(2, 31) + (input & ((1 << 31) -1));
  else return input;
};

var parseAnyInt = function(parser, column, columnSchema) {
  var result, int64, size;
  switch (column.type) {
    case MysqlTypes.TINY:
      size = 1;
      result = parser.parseUnsignedNumber(size);
      break;
    case MysqlTypes.SHORT:
      size = 2;
      result = parser.parseUnsignedNumber(size);
      break;
    case MysqlTypes.INT24:
      size = 3;
      result = exports.parseUInt24(parser);
      break;
    case MysqlTypes.LONG:
      size = 4;
      result = parser.parseUnsignedNumber(size);
      break;
    case MysqlTypes.LONGLONG:
      size = 8;
      int64 = {};
      result = exports.parseUInt64.call(int64, parser);
      break;
  }
  if(columnSchema.COLUMN_TYPE.indexOf('unsigned') === -1){
    var length = size * 8;
    // Flip bits on negative signed integer
    if(!int64 && (result & (1 << (length - 1)))){
      result = ((result ^ (Math.pow(2, length) - 1)) * -1) - 1;
    }else if(int64 && (int64.high & (1 << 31))){
      // Javascript integers only support 2^53 not 2^64, must trim bits!
      // 64-53 = 11, 32-11 = 21, so grab first 21 bits of high word only
      var mask = Math.pow(2, 32) - 1;
      var high = sliceBits(int64.high ^ mask, 0, 21);
      var low = int64.low ^ mask;
      result = ((high * Math.pow(2, 32)) * - 1) - getUInt32Value(low) - 1;
    }
  }
  return result;
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

// Did not work in place. Function cribbed from lines 311-363 of
// https://github.com/felixge/node-mysql/blob/cfd0ce3572d75c3c82103418d1d03cbe67eaf8a1/lib/protocol/Parser.js
var parseGeometryValue = function(buffer){
  var offset = 4;

  if (buffer === null || !buffer.length) {
    return null;
  }

  function parseGeometry() {
    var result = null;
    var byteOrder = buffer.readUInt8(offset); offset += 1;
    var wkbType = byteOrder? buffer.readUInt32LE(offset) : buffer.readUInt32BE(offset); offset += 4;
    switch(wkbType) {
      case 1: // WKBPoint
        var x = byteOrder? buffer.readDoubleLE(offset) : buffer.readDoubleBE(offset); offset += 8;
        var y = byteOrder? buffer.readDoubleLE(offset) : buffer.readDoubleBE(offset); offset += 8;
        result = {x: x, y: y};
        break;
      case 2: // WKBLineString
        var numPoints = byteOrder? buffer.readUInt32LE(offset) : buffer.readUInt32BE(offset); offset += 4;
        result = [];
        for(var i=numPoints;i>0;i--) {
          var x = byteOrder? buffer.readDoubleLE(offset) : buffer.readDoubleBE(offset); offset += 8;
          var y = byteOrder? buffer.readDoubleLE(offset) : buffer.readDoubleBE(offset); offset += 8;
          result.push({x: x, y: y});
        }
        break;
      case 3: // WKBPolygon
        var numRings = byteOrder? buffer.readUInt32LE(offset) : buffer.readUInt32BE(offset); offset += 4;
        result = [];
        for(var i=numRings;i>0;i--) {
          var numPoints = byteOrder? buffer.readUInt32LE(offset) : buffer.readUInt32BE(offset); offset += 4;
          var line = [];
          for(var j=numPoints;j>0;j--) {
            var x = byteOrder? buffer.readDoubleLE(offset) : buffer.readDoubleBE(offset); offset += 8;
            var y = byteOrder? buffer.readDoubleLE(offset) : buffer.readDoubleBE(offset); offset += 8;
            line.push({x: x, y: y});
          }
          result.push(line);
        }
        break;
      case 4: // WKBMultiPoint
      case 5: // WKBMultiLineString
      case 6: // WKBMultiPolygon
      case 7: // WKBGeometryCollection
        var num = byteOrder? buffer.readUInt32LE(offset) : buffer.readUInt32BE(offset); offset += 4;
        var result = [];
        for(var i=num;i>0;i--) {
          result.push(parseGeometry());
        }
        break;
    }
    return result;
  }
  return parseGeometry();
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

var readTemporalFraction = function(parser, fractionPrecision) {
  if(!fractionPrecision) return false;
  var fractionSize = Math.ceil(fractionPrecision / 2);
  var fraction = readIntBE(parser._buffer, parser._offset, fractionSize);
  parser._offset += fractionSize;
  if(fractionPrecision % 2 !== 0) fraction /= 10; // Not using full space
  if(fraction < 0) fraction *= -1; // Negative time, fraction not negative

  var milliseconds;
  if(fractionPrecision > 3){
    milliseconds = Math.floor(fraction / Math.pow(10, fractionPrecision - 3));
  }else if(fractionPrecision < 3){
    milliseconds = fraction * Math.pow(10, 3 - fractionPrecision);
  }else{
    milliseconds = fraction;
  }

  return {
    value: fraction,
    precision: fractionPrecision,
    milliseconds: milliseconds
  };
};

exports.readMysqlValue = function(parser, column, columnSchema) {
  var result;
  // jshint indent: false
  switch (column.type) {
    case MysqlTypes.TINY:
    case MysqlTypes.SHORT:
    case MysqlTypes.INT24:
    case MysqlTypes.LONG:
    case MysqlTypes.LONGLONG:
      result = parseAnyInt.apply(this, arguments);
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
      var high, low;
      if(column.metadata.size === 8){
        low = parser.parseUnsignedNumber(4);
        high = parser.parseUnsignedNumber(4);
      }else{
        low = parser.parseUnsignedNumber(column.metadata.size);
      }

      var choices = parseSetEnumTypeDef(columnSchema.COLUMN_TYPE);
      result = '';
      for(var i = 0; low >= Math.pow(2, i); i++){
        if(low & Math.pow(2, i)) result += choices[i] + ',';
      }
      if(high){
        for(var i = 0; high >= Math.pow(2, i); i++){
          if(high & Math.pow(2, i)) result += choices[i + 32] + ',';
        }
      }
      if(result.length > 0) result = result.substr(0, result.length - 1);
      break;
    case MysqlTypes.ENUM:
      var raw = parser.parseUnsignedNumber(column.metadata.size);
      var choices = parseSetEnumTypeDef(columnSchema.COLUMN_TYPE);
      result = choices[raw - 1];
      break;
    case MysqlTypes.VAR_STRING:
      // Never used?
      result = parser.parseLengthCodedString();
      break;
    case MysqlTypes.VARCHAR:
    case MysqlTypes.STRING:
      var prefixSize = column.metadata['max_length'] > 255 ? 2 : 1;
      var size = parser.parseUnsignedNumber(prefixSize);
      var def = columnSchema.COLUMN_TYPE;
      var defPrefix = def.substr(0, 6);
      if(defPrefix === 'binary'){
        result = new Buffer(parseInt(def.substr(7, def.length - 2), 10));
        result.fill(0);
        parser.parseBuffer(size).copy(result);
      }else if(defPrefix === 'varbin'){
        result = parser.parseBuffer(size);
      }else{
        result = parser.parseString(size);
      }
      break;
    case MysqlTypes.TINY_BLOB:
    case MysqlTypes.MEDIUM_BLOB:
    case MysqlTypes.LONG_BLOB:
    case MysqlTypes.BLOB:
      var lengthSize = column.metadata['length_size'];
      result = parser.parseString(
        parser.parseUnsignedNumber(lengthSize));
      break;
    case MysqlTypes.GEOMETRY:
      var lengthSize = column.metadata['length_size'];
      var size = parser.parseUnsignedNumber(lengthSize);
      var buffer = parser.parseBuffer(size);
      result = parseGeometryValue(buffer);
      break;
    case MysqlTypes.DATE:
      var raw = exports.parseUInt24(parser);
      result = new Date(
        sliceBits(raw, 9, 24),     // year
        sliceBits(raw, 5, 9) - 1,  // month
        sliceBits(raw, 0, 5)       // day
      );
      break;
    case MysqlTypes.TIME:
      var raw = exports.parseUInt24(parser);

      var isNegative = (raw & (1 << 23)) !== 0;
      if(isNegative) raw = raw ^ ((1 << 24) - 1); // flip all bits

      var hour = Math.floor(raw / 10000);
      var minute = Math.floor((raw % 10000) / 100);
      var second = raw % 100;
      if(isNegative) second += 1;

      result = (isNegative ? '-' : '') +
               zeroPad(hour, hour > 99 ? 3 : 2) + ':' +
               zeroPad(minute, 2) + ':' +
               zeroPad(second, 2);
      break;
    case MysqlTypes.TIME2:
      var raw = readIntBE(parser._buffer, parser._offset, 3);
      parser._offset += 3;
      var fraction = readTemporalFraction(parser, column.metadata.decimals);

      var isNegative = (raw & (1 << 23)) === 0;
      if(isNegative) raw = raw ^ ((1 << 24) - 1); // flip all bits

      var hour = sliceBits(raw, 12, 22);
      var minute = sliceBits(raw, 6, 12);
      var second = sliceBits(raw, 0, 6);

      if(isNegative && (fraction === false || fraction.value === 0)){
        second++;
      }

      result = (isNegative ? '-' : '') +
               zeroPad(hour, hour > 99 ? 3 : 2) + ':' +
               zeroPad(minute, 2) + ':' +
               zeroPad(second, 2);

      if(fraction !== false){
        result += '.' + zeroPad(fraction.value, fraction.precision);
      }
      break;
    case MysqlTypes.DATETIME:
      var raw = exports.parseUInt64(parser);
      var date = Math.floor(raw / 1000000);
      var time = raw % 1000000;
      result = new Date(
        Math.floor(date / 10000),             // year
        Math.floor((date % 10000) / 100) - 1, // month
        date % 100,                           // day
        Math.floor(time / 10000),             // hour
        Math.floor((time % 10000) / 100),     // minutes
        time % 100                            // seconds
      );
      break;
    case MysqlTypes.DATETIME2:
      // Overlapping high-low to get all data in 32-bit numbers
      var rawHigh = readIntBE(parser._buffer, parser._offset, 4);
      var rawLow = readIntBE(parser._buffer, parser._offset + 1, 4);
      parser._offset += 5;
      var fraction = readTemporalFraction(parser, column.metadata.decimals);

      var yearMonth = sliceBits(rawHigh, 14, 31);
      result = new Date(
        Math.floor(yearMonth / 13), // year
        (yearMonth % 13) - 1,       // month
        sliceBits(rawLow, 17, 22),  // day
        sliceBits(rawLow, 12, 17),  // hour
        sliceBits(rawLow, 6, 12),   // minutes
        sliceBits(rawLow, 0, 6),    // seconds
        fraction !== false ? fraction.milliseconds : 0
      );
      break;
    case MysqlTypes.TIMESTAMP:
      var raw = parser.parseUnsignedNumber(4);
      result = new Date(raw * 1000);
      break;
    case MysqlTypes.TIMESTAMP2:
      var raw = readIntBE(parser._buffer, parser._offset, 4);
      parser._offset += 4;
      var fraction = readTemporalFraction(parser, column.metadata.decimals);
      var milliseconds = fraction !== false ? fraction.milliseconds : 0;
      result = new Date((raw * 1000) + milliseconds);
      break;
    case MysqlTypes.YEAR:
      var raw = parser.parseUnsignedNumber(1);
      result = raw + 1900;
      break;
    case MysqlTypes.BIT:
      var size = Math.floor((column.metadata.bits + 7) / 8);
      result = parser._buffer.slice(parser._offset, parser._offset + size);
      parser._offset += size; // Move binlog parser position forward
      break;
    case MysqlTypes.NULL: // Uses nullBitmap in lib/rows_event :: readRow
    case MysqlTypes.DECIMAL: // Deprecated in MySQL > 5.0.3
    case MysqlTypes.NEWDATE: // Not used
    default:
      throw new Error('Unsupported type: ' + convertToMysqlType(column.type));
  }
  return result;
};
