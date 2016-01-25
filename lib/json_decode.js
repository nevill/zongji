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

function parseBinaryBuffer(input, offset, parentValueOffset) {
  offset = offset || 0;

  // Only used for types which use the value stored at a pointer position
  // If object is root (offset 0), the value is not offset by pointer
  var valueOffset = offset === 0 ? 0 :
    input.readUInt16LE(offset + 1) + parentValueOffset;

  debugLog('offset', offset, valueOffset);
  var result = null;
  var jsonType = input.readUInt8(offset);
  switch(jsonType) {
    // Small enough types are inlined
    case JSONB_TYPES.INT16:
      result = input.readInt16LE(offset + 1);
      break;
    case JSONB_TYPES.UINT16:
      // XXX: when would this ever be used???
      result = input.readUInt16LE(offset + 1);
      break;
    case JSONB_TYPES.LITERAL:
      var inlineValue = input.readUInt8(offset + 1);
      result = JSONB_LITERALS[inlineValue];
      break;
    // All other types are retrieved from pointer
    case JSONB_TYPES.STRING:
      debugLog('readstr', valueOffset);
      var strLen, strLenSize = 0, curStrLenByte;
      // If the high bit is 1, the string length continues to the next byte
      while(strLenSize === 0 || (curStrLenByte & 128) === 128) {
        strLenSize++;
        curStrLenByte = input.readUInt8(valueOffset + strLenSize);
        debugLog('lensize', strLenSize, 'curbyte', curStrLenByte);
        if(strLenSize === 1) {
          strLen = curStrLenByte;
        } else {
          strLen = (strLen ^ Math.pow(128, strLenSize - 1))
            + (curStrLenByte * Math.pow(2, 7 * (strLenSize - 1)));
        }
      }
      debugLog('strlen', strLen);
      result = input.toString('utf8',
        valueOffset + strLenSize + 1,
        valueOffset + strLenSize + 1 + strLen);
      break;
    case JSONB_TYPES.SMALL_OBJECT:
      result = {};
      debugLog('is small obj', valueOffset);
      var memberCount = input.readUInt16LE(valueOffset + 1);
      var binarySize = input.readUInt16LE(valueOffset + 3);
      debugLog('members:', memberCount, 'size:', binarySize);
      var pointerPos = 0;
      var memberValueStart = valueOffset + 5 + (memberCount * 4);
      var keyStart, keyEnd;
      var thisKey, memberValueOffset;
      while(pointerPos < memberCount) {
        keyStart = valueOffset + input.readUInt16LE(valueOffset + 5 + (pointerPos * 4)) + 1;
        keyEnd = keyStart + input.readUInt16LE(valueOffset + 7 + (pointerPos * 4));
        thisKey = input.toString('utf8', keyStart, keyEnd);
        memberValueOffset = memberValueStart + (pointerPos * 3);
        debugLog('thiskey', thisKey, keyStart, keyEnd, memberValueOffset);
        result[thisKey] = parseBinaryBuffer(input, memberValueOffset, valueOffset);
        pointerPos++;
      }
      break;
    case JSONB_TYPES.SMALL_ARRAY:
      result = [];
      debugLog('is small array');
      var memberCount = input.readUInt16LE(valueOffset + 1);
      var binarySize = input.readUInt16LE(valueOffset + 3);
      debugLog('members:', memberCount, 'size:', binarySize);
      var pointerPos = 0;
      var memberValueOffset;
      while(pointerPos < memberCount) {
        memberValueOffset = valueOffset + 5 + (pointerPos * 3);
        result.push(parseBinaryBuffer(input, memberValueOffset, valueOffset));
        pointerPos++;
      }
      break;
    case JSONB_TYPES.DOUBLE:
      var low = input.readUInt32LE(valueOffset + 1);
      var high = input.readUInt32LE(valueOffset + 5);
      debugLog('DOUBLE', low, high);
      result = common.parseIEEE754Float(high, low);
      break;
    case JSONB_TYPES.INT32:
      debugLog('INt32???');
      result = input.readInt32LE(valueOffset + 1);
      break;
    case JSONB_TYPES.UINT32:
      debugLog('UINt32!!!');
      // XXX: when would this ever be used???
      result = input.readUInt32LE(valueOffset + 1);
      break;
    case JSONB_TYPES.INT64:
      debugLog('INt64???');
      var low = input.readUInt32LE(valueOffset + 1);
      var high = input.readUInt32LE(valueOffset + 5);
      if(high & (1 << 31)) {
        // Javascript integers only support 2^53 not 2^64, must trim bits!
        // 64-53 = 11, 32-11 = 21, so grab first 21 bits of high word only
        var mask = Math.pow(2, 32) - 1;
        high = common.sliceBits(high ^ mask, 0, 21);
        low = low ^ mask;
        result = ((high * Math.pow(2, 32)) * - 1) - common.getUInt32Value(low) - 1;
      } else {
        debugLog('positive int64');
        result = (high * Math.pow(2,32)) + low;
      }
      break;
    case JSONB_TYPES.UINT64:
      debugLog('UINt64!!!');
      var low = input.readUInt32LE(valueOffset + 1);
      var high = input.readUInt32LE(valueOffset + 5);
      result = (high * Math.pow(2,32)) + low;
      break;
    default:
      debugLog('type nyi', jsonType);
  }
  debugLog('spittin', result);
  return result;
}
