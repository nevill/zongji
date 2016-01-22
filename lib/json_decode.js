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

module.exports = function(input) {
  // Hexdump fails on really long outputs ~ >16384 butes
//   console.log('\n\n' + hexdump(input));
  console.log('heyso');
  var jsonType = input.readUInt8(0);
  console.log('youtype', jsonType);
  switch(jsonType) {
    case JSONB_TYPES.SMALL_OBJECT:
      console.log('is small obj');
      var memberCount = input.readUInt16LE(1);
      var binarySize = input.readUInt16LE(3);
      console.log('members:', memberCount, 'size:', binarySize);
      var keyPointers = [];
      var keyLengths = [];
      var valueTypes = [];
      var valuePointers = [];
      var pointerPos = 0;
      var valueStart = 5 + (memberCount * 4);
      var members = {};
      var keyStart, keyEnd;
      var thisKey, thisType, thisValuePtr, thisValue;
      var strLen, strLenSize, curStrLenByte;
      while(pointerPos < memberCount) {
        keyPointers.push(input.readUInt16LE(5 + (pointerPos * 4)));
        keyLengths.push(input.readUInt16LE(7 + (pointerPos * 4)));
        valueTypes.push(input.readUInt8(valueStart + (pointerPos * 3)));
        valuePointers.push(input.readUInt16LE(valueStart + 1 + (pointerPos * 3)));
        keyStart = input.readUInt16LE(5 + (pointerPos * 4)) + 1;
        keyEnd = keyStart + input.readUInt16LE(7 + (pointerPos * 4));
        thisKey = input.toString('utf8', keyStart, keyEnd);
        thisType = input.readUInt8(valueStart + (pointerPos * 3));
        thisValuePtr = input.readUInt16LE(valueStart + 1 + (pointerPos * 3));
        switch(thisType) {
          // Small enough types are inlined
          case JSONB_TYPES.INT16:
            thisValue = input.readInt16LE(valueStart + 1 + (pointerPos * 3));
            break;
          case JSONB_TYPES.UINT16:
            // XXX: when would this ever be used???
            thisValue = thisValuePtr;
            break;
          case JSONB_TYPES.LITERAL:
            thisValue = JSONB_LITERALS[thisValuePtr];
            break;
          // All other types are retrieved from pointer
          case JSONB_TYPES.STRING:
            strLenSize = 0;
            // If the high bit is 1, the string length continues to the next byte
            while(strLenSize === 0 || (curStrLenByte & 128) === 128) {
              strLenSize++;
              curStrLenByte = input.readUInt8(thisValuePtr + strLenSize);
              if(strLenSize === 1) {
                strLen = curStrLenByte;
              } else {
                strLen = (strLen ^ Math.pow(128, strLenSize - 1))
                  + (curStrLenByte * Math.pow(2, 7 * (strLenSize - 1)));
              }
            }
            console.log('strLen', strLen);
            thisValue = input.toString('utf8',
              thisValuePtr + strLenSize + 1,
              thisValuePtr + strLenSize + 1 + strLen);
            break;
          default:
            thisValue = undefined;
        }
        console.log(pointerPos, keyStart, keyEnd, thisKey, // thisValue,
          5 + (pointerPos * 4), 7 + (pointerPos * 4),
          valueStart + (pointerPos * 3), valueStart + 1 + (pointerPos * 3));
        pointerPos++;
      }
      console.log(keyPointers);
      console.log(keyLengths);
      console.log(valueTypes);
      console.log(valuePointers);
      break;
    case JSONB_TYPES.SMALL_ARRAY:
      console.log('is small array');
      break;
    default:
      console.log('type nyi');
  }
  return 'nyi';
}
