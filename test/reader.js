var BufferReader = require('../lib/reader').BufferReader;

exports.readVariant = function(test) {
  var buf = new Buffer([0x02, 0x03, 0x0f, 0x02, 0xfd, 0x02, 0x03]);
  var reader = new BufferReader(buf);
  test.equal(reader.readVariant(), 2);
  test.done();
};

exports.readBitArray = function(test) {
  var buf = new Buffer([0x01, 0x03]);
  var reader = new BufferReader(buf);
  test.deepEqual(
    reader.readBitArray(11), [
      true, false, false, false, false,
      false, false, false, true, true, false
    ]
  );
  test.done();
};

exports.readBytesArray = function(test) {
  var data = [0x7c, 0x01, 0x03, 0x98];
  var reader = new BufferReader(new Buffer(data));
  test.deepEqual(reader.readBytesArray(data.length), data);
  test.done();
};
