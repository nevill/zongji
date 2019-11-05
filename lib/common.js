const iconv = require('iconv-lite');
const decodeJson = require('./json_decode');
const dtDecode = require('./datetime_decode');
const bigInt = require('big-integer');

const MysqlTypes = exports.MysqlTypes = {
  DECIMAL: 0,
  TINY: 1,
  SHORT: 2,
  LONG: 3,
  FLOAT: 4,
  DOUBLE: 5,
  NULL: 6,
  TIMESTAMP: 7,
  LONGLONG: 8,
  INT24: 9,
  DATE: 10,
  TIME: 11,
  DATETIME: 12,
  YEAR: 13,
  NEWDATE: 14,
  VARCHAR: 15,
  BIT: 16,
  // Fractional temporal types in MySQL >=5.6.4
  TIMESTAMP2: 17,
  DATETIME2: 18,
  TIME2: 19,
  // JSON data type added in MySQL 5.7.7
  JSON: 245,
  NEWDECIMAL: 246,
  ENUM: 247,
  SET: 248,
  TINY_BLOB: 249,
  MEDIUM_BLOB: 250,
  LONG_BLOB: 251,
  BLOB: 252,
  VAR_STRING: 253,
  STRING: 254,
  GEOMETRY: 255,
};

const TWO_TO_POWER_THIRTY_TWO = Math.pow(2, 32);
const TWO_TO_POWER_SIXTY_THREE = '9223372036854775808'; // Math.pow(2, 63) or 1 << 63
// This function will return a Number
// if the reuslt < Math.MAX_SAFE_INTEGER or reuslt > Math.MIN_SAFE_INTEGER,
// otherwise, will return a string.
const parseUInt64 = exports.parseUInt64 = function(parser) {
  const low = parser.parseUnsignedNumber(4);
  const high = parser.parseUnsignedNumber(4);

  if (high >>> 21) { // using bigint here
    return bigInt(TWO_TO_POWER_THIRTY_TWO).multiply(high).add(low).toString();
  }

  return (high * Math.pow(2,32)) + low;
};

exports.parseUInt48 = function(parser) {
  const low = parser.parseUnsignedNumber(4);
  const high = parser.parseUnsignedNumber(2);
  return (high * Math.pow(2, 32)) + low;
};

const parseUInt24 = exports.parseUInt24 = function(parser) {
  const low = parser.parseUnsignedNumber(2);
  const high = parser.parseUnsignedNumber(1);
  return (high << 16) + low;
};

exports.parseBytesArray = function(parser, length) {
  const result = new Array(length);
  for (let i = 0; i < length; i++) {
    result[i] = parser.parseUnsignedNumber(1);
  }
  return result;
};

// Parse column definition list string for SET and ENUM data types
// @param type      String  Definition of column 'set(...)' or 'enum(...)'
// @param prefixLen Integer Number of characters before list starts
//                          (e.g. 'set(': 4, 'enum(': 5)
const parseSetEnumTypeDef = function(type, prefixLen) {
  // listed distinct elements should not include commas
  return type.substr(prefixLen, type.length - prefixLen - 1)
    .split(',').map(function(opt) {
      return (opt[0] === '"' || opt[0] === "'") ?
        opt.substr(1, opt.length - 2) : opt;
    });
};

const zeroPad = exports.zeroPad = function(num, size) {
  // Max 32 digits
  const s = '00000000000000000000000000000000' + num;
  return s.substr(s.length-size);
};

const sliceBits = exports.sliceBits = function(input, start, end) {
  // ex: start: 10, end: 15 = "111110000000000"
  const match = (((1 << end) - 1) ^ ((1 << start) - 1));
  return (input & match) >> start;
};

// See information about IEEE-754 Floating point numbers:
// http://www.h-schmidt.net/FloatConverter/IEEE754.html
// http://babbage.cs.qc.cuny.edu/IEEE-754.old/64bit.html
// Pass only high for 32-bit float, pass high and low for 64-bit double
const parseIEEE754Float = exports.parseIEEE754Float = function(high, low) {
  let lastSignificantBit, sigFigs, expLeading;
  if (low !== undefined) {
    // 64-bit: 1 sign, 11 exponent, 52 significand
    lastSignificantBit = 20;
    sigFigs = 52;
    expLeading = 1023; // 2^(11-1) - 1
  } else {
    // 32-bit: 1 sign, 8 exponent, 23 significand
    lastSignificantBit = 23;
    sigFigs = 23;
    expLeading = 127; // 2^(8-1) - 1
  }

  const sign            = (high & (1 << 31)) !== 0 ? -1 : 1;
  const exponent        = sliceBits(high, lastSignificantBit, 31) - expLeading;
  const significandBits = sliceBits(high, 0, lastSignificantBit);
  let significand     = 1; // Becomes value between 1, 2

  for (let i = 0; i < lastSignificantBit; i++) {
    if (significandBits & (1 << i)) {
      significand += 1 / (1 << (sigFigs - i));
    }
  }

  if (low !== undefined) {
    for (let j = 0; j < 32; j++) {
      if (low & (1 << j)) {
        // Bitwise operators only work on up to 32 bits
        significand += 1 / Math.pow(2, sigFigs - j);
      }
    }
  }

  return sign * Math.pow(2, exponent) * significand;
};

const getUInt32Value = exports.getUInt32Value = function(input) {
  // Last bit is not sign, it is part of value!
  if (input & (1 << 31)) return Math.pow(2, 31) + (input & ((1 << 31) -1));
  else return input;
};

const parseAnyInt = function(parser, column, columnSchema) {
  let result, size;
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
      result = parseUInt24(parser);
      break;
    case MysqlTypes.LONG:
      size = 4;
      result = parser.parseUnsignedNumber(size);
      break;
    case MysqlTypes.LONGLONG:
      size = 8;
      result = parseUInt64(parser);
      break;
  }
  if (columnSchema.COLUMN_TYPE.indexOf('unsigned') === -1) {
    const length = size * 8;
    const int64 = (length == 64);
    // Flip bits on negative signed integer
    if (!int64 && (result & (1 << (length - 1)))) {
      result = ((result ^ (Math.pow(2, length) - 1)) * -1) - 1;
    } else if (int64 && bigInt(result).greaterOrEquals(bigInt(TWO_TO_POWER_SIXTY_THREE))) {
      const Max64BitNumber = bigInt('18446744073709551615'); // 2^64 - 1
      result = bigInt(result).xor(Max64BitNumber).add(1).multiply(-1);
      // Javascript integers only support 2^53, if not within the range, return a String
      if (result.greater(Number.MAX_SAFE_INTEGER) || result.lesser(Number.MIN_SAFE_INTEGER)) {
        result = result.toString();
      }
      // Otherwise return a Number
      else {
        result = result.toJSNumber();
      }
    }
  }
  return result;
};

const readInt24BE = function(buf, offset, noAssert) {
  return (buf.readInt8(offset, noAssert) << 16) +
          buf.readUInt16BE(offset + 1, noAssert);
};

const readIntBE = function(buf, offset, length, noAssert) {
  switch (length) {
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
const parseNewDecimal = function(parser, column) {
  // Constants of format
  const digitsPerInteger = 9;
  const compressedBytes = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];

  const scale = column.metadata.decimals;
  const integral = column.metadata.precision - scale;
  const uncompIntegral = Math.floor(integral / digitsPerInteger);
  const uncompFractional = Math.floor(scale / digitsPerInteger);
  const compIntegral = integral - (uncompIntegral * digitsPerInteger);
  const compFractional = scale - (uncompFractional * digitsPerInteger);

  // Grab buffer portion
  const size = (uncompIntegral * 4) + compressedBytes[compIntegral] +
             (uncompFractional * 4) + compressedBytes[compFractional];
  const buffer = parser._buffer.slice(parser._offset, parser._offset + size);
  parser._offset += size; // Move binlog parser position forward

  let str, mask;
  let pos = 0;
  const isPositive = (buffer.readUInt8(0) & (1 << 7)) === 128;
  buffer.writeUInt8(buffer.readUInt8(0) ^ (1 << 7), 0);
  if (isPositive) {
    // Positive number
    str = '';
    mask = 0;
  } else {
    // Negative number
    str = '-';
    mask = -1;
  }

  // Build integer digits
  const compIntegralSize = compressedBytes[compIntegral];
  if (compIntegralSize > 0) {
    str += (readIntBE(buffer, 0, compIntegralSize) ^ mask).toString(10);
    pos += compIntegralSize;
  }

  for (let i = 0; i < uncompIntegral; i++) {
    str += zeroPad((buffer.readInt32BE(pos) ^ mask).toString(10), 9);
    pos += 4;
  }

  // Build fractional digits
  let fractionDigits = '';

  for (let k = 0; k < uncompFractional; k++) {
    fractionDigits += zeroPad((buffer.readInt32BE(pos) ^ mask).toString(10), 9);
    pos += 4;
  }

  const compFractionalSize = compressedBytes[compFractional];
  if (compFractionalSize > 0) {
    fractionDigits += zeroPad((readIntBE(buffer, pos, compFractionalSize) ^ mask).toString(10), compFractional);
  }

  // Fractional digits may have leading zeros
  str += '.' + fractionDigits;

  return parseFloat(str);
};

// Did not work in place. Function cribbed from lines 311-363 of
// https://github.com/felixge/node-mysql/blob/cfd0ce3572d75c3c82103418d1d03cbe67eaf8a1/lib/protocol/Parser.js
const parseGeometryValue = function(buffer) {
  let offset = 4;

  if (buffer === null || !buffer.length) {
    return null;
  }

  function parseGeometry() {
    let result = null;
    const byteOrder = buffer.readUInt8(offset); offset += 1;
    const wkbType = byteOrder? buffer.readUInt32LE(offset) : buffer.readUInt32BE(offset); offset += 4;
    let x, y, numPoints, i;

    switch (wkbType) {
      case 1: // WKBPoint
        x = byteOrder? buffer.readDoubleLE(offset) : buffer.readDoubleBE(offset); offset += 8;
        y = byteOrder? buffer.readDoubleLE(offset) : buffer.readDoubleBE(offset); offset += 8;
        result = {x: x, y: y};
        break;
      case 2: // WKBLineString
        numPoints = byteOrder? buffer.readUInt32LE(offset) : buffer.readUInt32BE(offset); offset += 4;
        result = [];
        for (i = numPoints; i > 0; i--) {
          x = byteOrder? buffer.readDoubleLE(offset) : buffer.readDoubleBE(offset); offset += 8;
          y = byteOrder? buffer.readDoubleLE(offset) : buffer.readDoubleBE(offset); offset += 8;
          result.push({x: x, y: y});
        }
        break;
      case 3: {// WKBPolygon
        const numRings = byteOrder? buffer.readUInt32LE(offset) : buffer.readUInt32BE(offset); offset += 4;
        result = [];
        for (i = numRings; i > 0; i--) {
          numPoints = byteOrder? buffer.readUInt32LE(offset) : buffer.readUInt32BE(offset); offset += 4;
          const line = [];
          for (let j = numPoints; j > 0; j--) {
            x = byteOrder? buffer.readDoubleLE(offset) : buffer.readDoubleBE(offset); offset += 8;
            y = byteOrder? buffer.readDoubleLE(offset) : buffer.readDoubleBE(offset); offset += 8;
            line.push({x: x, y: y});
          }
          result.push(line);
        }
        break;
      }
      case 4: // WKBMultiPoint
      case 5: // WKBMultiLineString
      case 6: // WKBMultiPolygon
      case 7: {// WKBGeometryCollection
        const num = byteOrder? buffer.readUInt32LE(offset) : buffer.readUInt32BE(offset); offset += 4;
        result = [];
        for (i = num; i > 0; i--) {
          result.push(parseGeometry());
        }
        break;
      }
    }
    return result;
  }
  return parseGeometry();
};

// Returns false, or an object describing the fraction of a second part of a
// TIME, DATETIME, or TIMESTAMP.
const readTemporalFraction = function(parser, fractionPrecision) {
  if (!fractionPrecision) return false;
  let fractionSize = Math.ceil(fractionPrecision / 2);
  let fraction = readIntBE(parser._buffer, parser._offset, fractionSize);
  parser._offset += fractionSize;
  if (fractionPrecision % 2 !== 0) fraction /= 10; // Not using full space
  if (fraction < 0) fraction *= -1; // Negative time, fraction not negative

  let milliseconds;
  if (fractionPrecision > 3) {
    milliseconds = Math.floor(fraction / Math.pow(10, fractionPrecision - 3));
  } else if (fractionPrecision < 3) {
    milliseconds = fraction * Math.pow(10, 3 - fractionPrecision);
  } else {
    milliseconds = fraction;
  }

  return {
    value: fraction,              // the integer after the decimal place
    precision: fractionPrecision, // the number of digits after the decimal
    milliseconds: milliseconds    // the unrounded 3 digits after the decimal
  };
};

// This function is used to read and interpret non-null values from parser.
// The parser object contains the raw value, and functions useful for value
// interpretation, but not the underlying column type or how the value ought to
// be interpreted based on that column type.
exports.readMysqlValue = function(
    parser,       // node-mysql parser instance, from mysql/lib/protocol/Parser
    column,       // tableMap.columns[columnNumber]
    columnSchema, // tableMap.columnSchemas[columnNumber]
    tableMap,     // used directly only to get extra info for error messages
    zongji        // the ZongJi instance, used to read options and emit errors
  )
{
  let result;
  let high, low;
  let raw;
  let choices;
  let size, lengthSize;
  let buffer;
  let isNegative;
  let fraction;
  let hour, minute, second;
  let date, time, yearMonth;

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
      raw = parser.parseUnsignedNumber(4);
      result = parseIEEE754Float(raw);
      break;
    case MysqlTypes.DOUBLE:
      // 64-bit IEEE-754
      low = parser.parseUnsignedNumber(4);
      high = parser.parseUnsignedNumber(4);
      result = parseIEEE754Float(high, low);
      break;
    case MysqlTypes.NEWDECIMAL:
      result = parseNewDecimal(parser, column);
      break;
    case MysqlTypes.SET:
      if (column.metadata.size === 8) {
        low = parser.parseUnsignedNumber(4);
        high = parser.parseUnsignedNumber(4);
      } else {
        low = parser.parseUnsignedNumber(column.metadata.size);
      }

      // Second argument: prefixLen = 4 'set('
      choices = parseSetEnumTypeDef(columnSchema.COLUMN_TYPE, 4);
      result = '';
      for (let i = 0; low >= Math.pow(2, i); i++) {
        if (low & Math.pow(2, i)) result += choices[i] + ',';
      }
      if (high) {
        for (let i = 0; high >= Math.pow(2, i); i++) {
          if (high & Math.pow(2, i)) result += choices[i + 32] + ',';
        }
      }
      if (result.length > 0) result = result.substr(0, result.length - 1);
      break;
    case MysqlTypes.ENUM:
      raw = parser.parseUnsignedNumber(column.metadata.size);
      // Second argument: prefixLen = 5 'enum('
      choices = parseSetEnumTypeDef(columnSchema.COLUMN_TYPE, 5);
      result = choices[raw - 1];
      break;
    case MysqlTypes.VAR_STRING:
      // Never used?
      result = parser.parseLengthCodedString();
      break;
    case MysqlTypes.VARCHAR:
    case MysqlTypes.STRING: {
      const prefixSize = column.metadata['max_length'] > 255 ? 2 : 1;
      size = parser.parseUnsignedNumber(prefixSize);
      const def = columnSchema.COLUMN_TYPE;
      const defPrefix = def.substr(0, 6);
      if (defPrefix === 'binary') {
        const bufsize = parseInt(def.substr(7, def.length - 2), 10);
        result = Buffer.alloc(bufsize, 0);
        parser.parseBuffer(size).copy(result);
      } else if (defPrefix === 'varbin') {
        result = parser.parseBuffer(size);
      } else {
        result = parser.parseString(size);
      }
      break;
    }
    case MysqlTypes.TINY_BLOB:
    case MysqlTypes.MEDIUM_BLOB:
    case MysqlTypes.LONG_BLOB:
    case MysqlTypes.BLOB:
      lengthSize = column.metadata['length_size'];
      result = parser.parseBuffer(parser.parseUnsignedNumber(lengthSize));

      // Blobs can be sometimes return as String instead of Buffer
      // e.g. TINYTEXT, MEDIUMTEXT, LONGTEXT, TEXT data types
      if (column.charset !== null) {
        // Javascript UTF8 always allows up to 4 bytes per character
        column.charset = column.charset === 'utf8mb4' ? 'utf8' : column.charset;
        result = iconv.decode(result, column.charset);
      }
      break;
    case MysqlTypes.JSON:
      lengthSize = column.metadata['length_size'];
      size = parser.parseUnsignedNumber(lengthSize);
      buffer = parser.parseBuffer(size);
      result = decodeJson(buffer);
      break;
    case MysqlTypes.GEOMETRY:
      lengthSize = column.metadata['length_size'];
      size = parser.parseUnsignedNumber(lengthSize);
      buffer = parser.parseBuffer(size);
      result = parseGeometryValue(buffer);
      break;
    case MysqlTypes.DATE:
      raw = parseUInt24(parser);
      result = dtDecode.getDate(
        zongji.connection.config.dateStrings, // node-mysql dateStrings option
        sliceBits(raw, 9, 24),     // year
        sliceBits(raw, 5, 9),      // month
        sliceBits(raw, 0, 5)       // day
      );
      break;
    case MysqlTypes.TIME:
      raw = parseUInt24(parser);

      isNegative = (raw & (1 << 23)) !== 0;
      if (isNegative) raw = raw ^ ((1 << 24) - 1); // flip all bits

      hour = Math.floor(raw / 10000);
      minute = Math.floor((raw % 10000) / 100);
      second = raw % 100;
      if (isNegative) second += 1;

      result = (isNegative ? '-' : '') +
               zeroPad(hour, hour > 99 ? 3 : 2) + ':' +
               zeroPad(minute, 2) + ':' +
               zeroPad(second, 2);
      break;
    case MysqlTypes.TIME2:
      raw = readIntBE(parser._buffer, parser._offset, 3);
      parser._offset += 3;
      fraction = readTemporalFraction(parser, column.metadata.decimals);

      isNegative = (raw & (1 << 23)) === 0;
      if (isNegative) raw = raw ^ ((1 << 24) - 1); // flip all bits

      hour = sliceBits(raw, 12, 22);
      minute = sliceBits(raw, 6, 12);
      second = sliceBits(raw, 0, 6);

      if (isNegative && (fraction === false || fraction.value === 0)) {
        second++;
      }

      result = (isNegative ? '-' : '') +
               zeroPad(hour, hour > 99 ? 3 : 2) + ':' +
               zeroPad(minute, 2) + ':' +
               zeroPad(second, 2);

      if (fraction !== false) {
        result += dtDecode.getFractionString(fraction);
      }
      break;
    case MysqlTypes.DATETIME:
      raw = parseUInt64(parser);
      date = Math.floor(raw / 1000000);
      time = raw % 1000000;
      result = dtDecode.getDateTime(
        zongji.connection.config.dateStrings,  // node-mysql dateStrings option
        Math.floor(date / 10000),             // year
        Math.floor((date % 10000) / 100),     // month
        date % 100,                           // day
        Math.floor(time / 10000),             // hour
        Math.floor((time % 10000) / 100),     // minutes
        time % 100                            // seconds
      );
      break;
    case MysqlTypes.DATETIME2: {
      // Overlapping high-low to get all data in 32-bit numbers
      const rawHigh = readIntBE(parser._buffer, parser._offset, 4);
      const rawLow = readIntBE(parser._buffer, parser._offset + 1, 4);
      parser._offset += 5;
      fraction = readTemporalFraction(parser, column.metadata.decimals);

      yearMonth = sliceBits(rawHigh, 14, 31);
      result = dtDecode.getDateTime(
        zongji.connection.config.dateStrings, // node-mysql dateStrings option
        Math.floor(yearMonth / 13), // year
        (yearMonth % 13),           // month
        sliceBits(rawLow, 17, 22),  // day
        sliceBits(rawLow, 12, 17),  // hour
        sliceBits(rawLow, 6, 12),   // minutes
        sliceBits(rawLow, 0, 6),    // seconds
        fraction                    // fraction of a second object
      );
      break;
    }
    case MysqlTypes.TIMESTAMP:
      raw = parser.parseUnsignedNumber(4);
      result = dtDecode.getTimeStamp(zongji.connection.config.dateStrings, raw);
      break;
    case MysqlTypes.TIMESTAMP2:
      raw = readIntBE(parser._buffer, parser._offset, 4);
      parser._offset += 4;
      fraction = readTemporalFraction(parser, column.metadata.decimals);
      result = dtDecode.getTimeStamp(zongji.connection.config.dateStrings,
                                     raw,       // seconds from epoch
                                     fraction); // fraction of a second object
      break;
    case MysqlTypes.YEAR:
      raw = parser.parseUnsignedNumber(1);
      result = raw + 1900;
      break;
    case MysqlTypes.BIT:
      size = Math.floor((column.metadata.bits + 7) / 8);
      result = parser._buffer.slice(parser._offset, parser._offset + size);
      parser._offset += size; // Move binlog parser position forward
      break;
    case MysqlTypes.NULL: // Uses nullBitmap in lib/rows_event :: readRow
    case MysqlTypes.DECIMAL: // Deprecated in MySQL > 5.0.3
    case MysqlTypes.NEWDATE: // Not used
    default:
      result = undefined;
      zongji.emit('error',
        new Error('Unsupported type "' + column.type +
          '" on column "' + column.name +
          '" of the table "' + tableMap.tableName +
          '" in the database "' + tableMap.parentSchema + '"'));
  }
  return result;
};
