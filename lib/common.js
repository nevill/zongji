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
  var result, raw, choices, i;
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
    case MysqlTypes.SET:
      raw = parser.parseUnsignedNumber(column.metadata.size);
      choices = parseSetColumnType(columnSchema.COLUMN_TYPE);
      i = 0;
      result = [];
      while(raw > Math.pow(2, i)){
        if(raw & Math.pow(2, i)) result.push(choices[i]);
        i++;
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
    case MysqlTypes.FLOAT:
    case MysqlTypes.DOUBLE:
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
