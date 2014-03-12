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

exports.parseUInt64 = function(parser) {
  //TODO to add bignumber support
  var low = parser.parseUnsignedNumber(4);
  var high = parser.parseUnsignedNumber(4);
  // jshint bitwise: false
  return (high << 32) + low;
};

exports.parseUInt48 = function(parser) {
  //TODO to add bignumber support
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

exports.convertToMysqlType = function(code) {
  var result;
  Object.keys(MysqlTypes).forEach(function(name) {
    if (MysqlTypes[name] === code) {
      result = name;
      return;
    }
  });
  return result;
};
