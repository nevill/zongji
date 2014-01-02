var BufferReader = require('../lib/reader').BufferReader;

exports.readBitArray = function(test) {
  var buf = new Buffer([0x01, 0x03]);
  var reader = new BufferReader(buf);
  test.deepEqual(
    reader.readBitArray(11),
    [true, false, false, false, false, false, false, false,
      true, true,false]
  );
  test.done();
};
