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
    // TODO: Types still to implement!
    case MysqlTypes.DECIMAL:
    case MysqlTypes.NULL:
    case MysqlTypes.TIMESTAMP:
    case MysqlTypes.DATE:
    case MysqlTypes.TIME:
    case MysqlTypes.DATETIME:
    case MysqlTypes.NEWDATE:
    case MysqlTypes.BIT:
    case MysqlTypes.NEWDECIMAL:
    case MysqlTypes.ENUM:
    case MysqlTypes.TINY_BLOB:
    case MysqlTypes.MEDIUM_BLOB:
    case MysqlTypes.LONG_BLOB:
    case MysqlTypes.BLOB:
    case MysqlTypes.GEOMETRY:
    default:
      throw new Error('Unsupported type: ' + convertToMysqlType(column.type));
  }
  return result;
};
