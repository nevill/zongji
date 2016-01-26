var hexdump = require('buffer-hexdump');
var common = require('./common');

var JSONB_TYPES = {
  SMALL_OBJECT: 0,
  LARGE_OBJECT: 1,
  SMALL_ARRAY: 2,
  LARGE_ARRAY: 3,
  LITERAL: 4,
  INT16: 5,
  UINT16: 6,
  INT32: 7,
  UINT32: 8,
  INT64: 9,
  UINT64: 10,
  DOUBLE: 11,
  STRING: 12,
  OPAQUE: 13
};

var JSONB_LITERALS = [ null, true, false ];

var debugLog = process.env.DEBUG ? console.log : function() {};

module.exports = function(input) {
  debugLog('\n\n' + hexdump(input));

  // Value must be JSON string to match node-mysql results
  // Related to this node-mysql PR:
  // https://github.com/felixge/node-mysql/pull/1299
  return JSON.stringify(parseBinaryBuffer(input));
}

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
  switch(jsonType) {
    // Small enough types are inlined
    case JSONB_TYPES.INT16:
      result = input.readInt16LE(offset + 1);
      break;
    case JSONB_TYPES.UINT16:
      // XXX: when would this ever be used???
      debugLog('UInt16!!!');
      result = input.readUInt16LE(offset + 1);
      break;
    case JSONB_TYPES.LITERAL:
      var inlineValue = input.readUInt8(offset + 1);
      result = JSONB_LITERALS[inlineValue];
      break;
    // All other types are retrieved from pointer
    case JSONB_TYPES.STRING:
      var strLen, strLenSize = 0, curStrLenByte;
      // If the high bit is 1, the string length continues to the next byte
      while(strLenSize === 0 || (curStrLenByte & 128) === 128) {
        strLenSize++;
        curStrLenByte = input.readUInt8(valueOffset + strLenSize);
        if(strLenSize === 1) {
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
    case JSONB_TYPES.LARGE_OBJECT:
      result = readObject(input, valueOffset, true);
      break;
    case JSONB_TYPES.SMALL_OBJECT:
      result = readObject(input, valueOffset, false);
      break;
    case JSONB_TYPES.LARGE_ARRAY:
      result = readArray(input, valueOffset, true);
      break;
    case JSONB_TYPES.SMALL_ARRAY:
      result = readArray(input, valueOffset, false);
      break;
    case JSONB_TYPES.DOUBLE:
      var low = input.readUInt32LE(valueOffset + 1);
      var high = input.readUInt32LE(valueOffset + 5);
      result = common.parseIEEE754Float(high, low);
      break;
    case JSONB_TYPES.INT32:
      result = input.readInt32LE(valueOffset + 1);
      break;
    case JSONB_TYPES.UINT32:
      // XXX: when would this ever be used???
      debugLog('UInt32!!!');
      result = input.readUInt32LE(valueOffset + 1);
      break;
    case JSONB_TYPES.INT64:
      var low = input.readUInt32LE(valueOffset + 1);
      var high = input.readUInt32LE(valueOffset + 5);
      if(high & (1 << 31)) {
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
    case JSONB_TYPES.UINT64:
      var low = input.readUInt32LE(valueOffset + 1);
      var high = input.readUInt32LE(valueOffset + 5);
      result = (high * Math.pow(2,32)) + low;
      break;
    default:
      debugLog('type nyi', jsonType);
  }
  return result;
}

function readObject(input, valueOffset, isLarge) {
  if(isLarge) {
    var readUInt = input.readUInt32LE.bind(input);
    var intSize = 4;
  } else {
    var readUInt = input.readUInt16LE.bind(input);
    var intSize = 2;
  }

  var result = {};
  var memberCount = readUInt(valueOffset + 1); // +1 = JSON type byte
  var binarySize = readUInt(valueOffset + 1 + intSize);

  // Position where key entries start
  // Key entry: Key offset (int16/32) + Key length (int16)
  var memberKeyStart =
    valueOffset + 1 + // Beginning of definition
    (intSize * 2);   // memberCount + binarySize

  // Value entries (or pointers to such) begin after key entries
  var memberValueStart = memberKeyStart + (memberCount * (intSize + 2));

  for(var pointerPos = 0; pointerPos < memberCount; pointerPos++) {
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
  if(isLarge) {
    var readUInt = input.readUInt32LE.bind(input);
    var intSize = 4;
  } else {
    var readUInt = input.readUInt16LE.bind(input);
    var intSize = 2;
  }

  var result = [];
  var memberCount = readUInt(valueOffset + 1); // +1 = JSON type byte
  var binarySize = readUInt(valueOffset + 1 + intSize);

  for(var pointerPos = 0; pointerPos < memberCount; pointerPos++) {
    var memberValueOffset =
      valueOffset + 1 + // Beginning of definition
      (intSize * 2) +   // memberCount + binarySize
      (pointerPos * (1 + intSize)); // value type + value offset

    result.push(
      parseBinaryBuffer(input, memberValueOffset, valueOffset, readUInt));
  }
  return result;
}
