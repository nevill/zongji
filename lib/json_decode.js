var common = require('./common');
var Parser = require('mysql/lib/protocol/Parser');

var JSONB_TYPE_SMALL_OBJECT = 0;
var JSONB_TYPE_LARGE_OBJECT = 1;
var JSONB_TYPE_SMALL_ARRAY  = 2;
var JSONB_TYPE_LARGE_ARRAY  = 3;
var JSONB_TYPE_LITERAL      = 4;
var JSONB_TYPE_INT16        = 5;
var JSONB_TYPE_UINT16       = 6;
var JSONB_TYPE_INT32        = 7;
var JSONB_TYPE_UINT32       = 8;
var JSONB_TYPE_INT64        = 9;
var JSONB_TYPE_UINT64       = 10;
var JSONB_TYPE_DOUBLE       = 11;
var JSONB_TYPE_STRING       = 12;
var JSONB_TYPE_OPAQUE       = 15;

var JSONB_LITERALS = [ null, true, false ];

// node-mysql prefixes binary string values
var VAR_STRING_PREFIX = 'base64:type253:';

module.exports = function(input) {
  // Value must be JSON string to match node-mysql results
  // Related to this node-mysql PR:
  // https://github.com/felixge/node-mysql/pull/1299
  return JSON.stringify(parseBinaryBuffer(input));
};

/*
 * @func  parseBinaryBuffer
 * @param Buffer   input             Full binary JSON representation
 * @param Integer  offset            Position of the JSONB_TYPE to start
                                     reading (optional)
 * @param Integer  parentValueOffset Provided if reading value from nested
                                     object or array (required if offset
                                     specified)
 * @param Function readUInt          Provide alternative function to read
                                     pointer value for value offset (defaults
                                     to UInt16LE, large objects/arrays pass
                                     UInt32LE)
 * @return Varies
 */
function parseBinaryBuffer(input, offset, parentValueOffset, readUInt) {
  offset = offset || 0;
  readUInt = readUInt || input.readUInt16LE.bind(input);

  // Only used for types which use the value stored at a pointer position
  // If object is root (offset 0), the value is not offset by pointer
  var valueOffset = offset === 0 ? 0 :
    readUInt(offset + 1) + parentValueOffset;

  var result = null;
  var jsonType = input.readUInt8(offset);
  var low, high;
  var raw, fraction, yearMonth;
  switch (jsonType) {
    // Small enough types are inlined
    case JSONB_TYPE_INT16:
      result = input.readInt16LE(offset + 1);
      break;
    case JSONB_TYPE_UINT16:
      // XXX: No known instance of this type being used
      result = input.readUInt16LE(offset + 1);
      break;
    case JSONB_TYPE_LITERAL:
      var inlineValue = input.readUInt8(offset + 1);
      result = JSONB_LITERALS[inlineValue];
      break;
    // All other types are retrieved from pointer
    case JSONB_TYPE_STRING:
      var strLen, strLenSize = 0, curStrLenByte;
      // If the high bit is 1, the string length continues to the next byte
      while (strLenSize === 0 || (curStrLenByte & 128) === 128) {
        strLenSize++;
        curStrLenByte = input.readUInt8(valueOffset + strLenSize);
        if (strLenSize === 1) {
          strLen = curStrLenByte;
        } else {
          strLen = (strLen ^ Math.pow(128, strLenSize - 1))
            + (curStrLenByte * Math.pow(2, 7 * (strLenSize - 1)));
        }
      }
      result = input.toString('utf8',
        valueOffset + strLenSize + 1,
        valueOffset + strLenSize + 1 + strLen);
      break;
    case JSONB_TYPE_LARGE_OBJECT:
      result = readObject(input, valueOffset, true);
      break;
    case JSONB_TYPE_SMALL_OBJECT:
      result = readObject(input, valueOffset, false);
      break;
    case JSONB_TYPE_LARGE_ARRAY:
      result = readArray(input, valueOffset, true);
      break;
    case JSONB_TYPE_SMALL_ARRAY:
      result = readArray(input, valueOffset, false);
      break;
    case JSONB_TYPE_DOUBLE:
      low = input.readUInt32LE(valueOffset + 1);
      high = input.readUInt32LE(valueOffset + 5);
      result = common.parseIEEE754Float(high, low);
      break;
    case JSONB_TYPE_INT32:
      result = input.readInt32LE(valueOffset + 1);
      break;
    case JSONB_TYPE_UINT32:
      // XXX: No known instance of this type being used
      result = input.readUInt32LE(valueOffset + 1);
      break;
    case JSONB_TYPE_INT64:
      low = input.readUInt32LE(valueOffset + 1);
      high = input.readUInt32LE(valueOffset + 5);
      if (high & (1 << 31)) {
        // Javascript integers only support 2^53 not 2^64, must trim bits!
        // 64-53 = 11, 32-11 = 21, so grab first 21 bits of high word only
        var mask = Math.pow(2, 32) - 1;
        high = common.sliceBits(high ^ mask, 0, 21);
        low = low ^ mask;
        result =
          ((high * Math.pow(2, 32)) * - 1) - common.getUInt32Value(low) - 1;
      } else {
        result = (high * Math.pow(2,32)) + low;
      }
      break;
    case JSONB_TYPE_UINT64:
      low = input.readUInt32LE(valueOffset + 1);
      high = input.readUInt32LE(valueOffset + 5);
      result = (high * Math.pow(2,32)) + low;
      break;
    case JSONB_TYPE_OPAQUE:
      var customType = input.readUInt8(valueOffset + 1);
      var dataLen, dataLenSize = 0, curDataLenByte;
      // If the high bit is 1, the string length continues to the next byte
      while (dataLenSize === 0 || (curDataLenByte & 128) === 128) {
        dataLenSize++;
        curDataLenByte = input.readUInt8(valueOffset + 1 + dataLenSize);
        if (dataLenSize === 1) {
          dataLen = curDataLenByte;
        } else {
          dataLen = (dataLen ^ Math.pow(128, dataLenSize - 1))
            + (curDataLenByte * Math.pow(2, 7 * (dataLenSize - 1)));
        }
      }

      // Configure parser and metadata if using standard readMysqlValue
      // from common.js, otherwise set result for custom decoding
      var parser = new Parser();
      var metadata = {};
      var parseType = customType;

      parser.append(input.slice(
        valueOffset + dataLenSize + 2,
        valueOffset + dataLenSize + 2 + dataLen));

      switch (customType) {
        case common.MysqlTypes.DATE:
          raw = parser._buffer.readInt32LE(4);
          yearMonth = common.sliceBits(raw, 14, 31);
          result =
            common.zeroPad(Math.floor(yearMonth / 13), 4) + '-' +  // year
            common.zeroPad(yearMonth % 13, 2) + '-' +              // month
            common.zeroPad(common.sliceBits(raw, 9, 14), 2);        // day
          break;
        case common.MysqlTypes.TIME:
          raw = parser._buffer.readUInt32LE(3);
          fraction = common.sliceBits(parser._buffer.readInt32LE(0), 0, 24);

          var isNegative = (raw & (1 << 23)) !== 0;
          if (isNegative) {
            raw = (raw ^ ((1 << 24) - 1)) + 1; // flip all bits
            // If fraction exists, last bit adjustment goes to microseconds
            if (fraction) {
              fraction = (fraction ^ ((1 << 24) - 1)) + 1;
              raw--;
            }
          }

          var hour = common.sliceBits(raw, 12, 22);
          var minute = common.sliceBits(raw, 6, 12);
          var second = common.sliceBits(raw, 0, 6);

          result = (isNegative ? '-' : '') +
                   common.zeroPad(hour, hour > 99 ? 3 : 2) + ':' +
                   common.zeroPad(minute, 2) + ':' +
                   common.zeroPad(second, 2) +
                   '.' + common.zeroPad(fraction, 6);
          break;
        case common.MysqlTypes.DATETIME:
          // Overlapping high-low to get all data in 32-bit numbers
          var rawHigh = parser._buffer.readUInt32LE(3);
          var rawLow = parser._buffer.readUInt32LE(4);
          fraction = common.sliceBits(parser._buffer.readInt32LE(0), 0, 24);

          yearMonth = common.sliceBits(rawLow, 14, 31);
          result =
            common.zeroPad(Math.floor(yearMonth / 13), 4) + '-' +        // year
            common.zeroPad(yearMonth % 13, 2) + '-' +                    // month
            common.zeroPad(common.sliceBits(rawLow, 9, 14), 2) + ' ' +   // day
            common.zeroPad(common.sliceBits(rawHigh, 12, 17), 2) + ':' + // hour
            common.zeroPad(common.sliceBits(rawHigh, 6, 12), 2) + ':' +  // minutes
            common.zeroPad(common.sliceBits(rawHigh, 0, 6), 2) + '.' +   // seconds
            common.zeroPad(fraction, 6);
          break;
        case common.MysqlTypes.NEWDECIMAL:
          metadata = {
            precision: parser.parseUnsignedNumber(1),
            decimals: parser.parseUnsignedNumber(1),
          };
          break;
        case common.MysqlTypes.VAR_STRING:
          result = VAR_STRING_PREFIX + parser._buffer.toString('base64');
          break;
        default:
          throw new Error('JSON Opaque Type Not Implemented: ' + customType);
      }

      // If a result has not already been returned, try using the
      // value reader for normal binary values (not JSON)
      if (result === null) {
        result = common.readMysqlValue(parser, {
          type: parseType,
          metadata: metadata
        });
      }
      break;
    default:
      throw new Error('JSON Type Not Implemented: ' + jsonType);
  }
  return result;
}

function readObject(input, valueOffset, isLarge) {
  var readUInt, intSize;
  if (isLarge) {
    readUInt = input.readUInt32LE.bind(input);
    intSize = 4;
  } else {
    readUInt = input.readUInt16LE.bind(input);
    intSize = 2;
  }

  var result = {};
  var memberCount = readUInt(valueOffset + 1); // +1 = JSON type byte

  // Position where key entries start
  // Key entry: Key offset (int16/32) + Key length (int16)
  var memberKeyStart =
    valueOffset + 1 + // Beginning of definition
    (intSize * 2);   // memberCount + binarySize

  // Value entries (or pointers to such) begin after key entries
  var memberValueStart = memberKeyStart + (memberCount * (intSize + 2));

  for (var pointerPos = 0; pointerPos < memberCount; pointerPos++) {
    var keyEntryPos = memberKeyStart + (pointerPos * (intSize + 2));

    var keyStart = valueOffset + 1 + readUInt(keyEntryPos);
    var keyEnd = keyStart + input.readUInt16LE(keyEntryPos + intSize);

    var thisKey = input.toString('utf8', keyStart, keyEnd);
    var memberValueOffset = memberValueStart + (pointerPos * (intSize + 1));

    result[thisKey] =
      parseBinaryBuffer(input, memberValueOffset, valueOffset, readUInt);
  }

  return result;
}

function readArray(input, valueOffset, isLarge) {
  var readUInt, intSize;
  if (isLarge) {
    readUInt = input.readUInt32LE.bind(input);
    intSize = 4;
  } else {
    readUInt = input.readUInt16LE.bind(input);
    intSize = 2;
  }

  var result = [];
  var memberCount = readUInt(valueOffset + 1); // +1 = JSON type byte

  for (var pointerPos = 0; pointerPos < memberCount; pointerPos++) {
    var memberValueOffset =
      valueOffset + 1 + // Beginning of definition
      (intSize * 2) +   // memberCount + binarySize
      (pointerPos * (1 + intSize)); // value type + value offset

    result.push(
      parseBinaryBuffer(input, memberValueOffset, valueOffset, readUInt));
  }
  return result;
}
