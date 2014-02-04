var Buffer = require('buffer').Buffer;
var binlog = require('../lib');

/*
 * @param data an array of binary data to be parsed into binlog event
 */
function createEvent(data) {
  var buf = new Buffer(data);
  return binlog.create(buf);
}

exports.rotateEvent = function(test) {
  var data = [ 0x00,
    0x00, 0x00, 0x00, 0x00, 0x04, 0x01, 0x00, 0x00, 0x00, 0x2b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00,
    0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x6d, 0x79, 0x73, 0x71, 0x6c, 0x2d, 0x62, 0x69, 0x6e, 0x2e, 0x30, 0x30, 0x30, 0x30, 0x30, 0x31 ];

  var anEvent = createEvent(data);

  test.ok(anEvent instanceof binlog.Rotate);
  test.equal(anEvent.getEventName(), 'rotate');
  test.equal(anEvent.getTypeName(), 'Rotate');
  test.equal(anEvent.size, 0x2b - 19);
  test.equal(anEvent.position, 4);
  test.equal(anEvent.binlogName, 'mysql-bin.000001');
  test.done();
};

exports.XidEvent = function(test) {
  var data = [ 0x00,
    0xa9, 0xf2, 0x91, 0x52, 0x10, 0x01, 0x00, 0x00, 0x00, 0x1b, 0x00, 0x00, 0x00, 0x13, 0x02, 0x00, 0x00, 0x00, 0x00,
    0x44, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 ];

  var anEvent = createEvent(data);

  test.ok(anEvent instanceof binlog.Xid);
  test.equal(anEvent.getEventName(), 'xid');
  test.equal(anEvent.getTypeName(), 'Xid');
  test.equal(anEvent.xid, 0x44);
  test.done();
};

exports.formatEventHeader = function(test) {
  var data = [ 0x00,
    0xec, 0x14, 0x8e, 0x52, 0x0f, 0x01, 0x00, 0x00, 0x00, 0x74, 0x00, 0x00, 0x00, 0x78, 0x00, 0x00, 0x00, 0x00, 0x00
  ];

  var buf = new Buffer(data);
  var params = binlog.parseHeader(buf);

  test.equal(params[0], 0x0f);
  test.equal(params[2], 1385043180000);

  test.done();
};

exports.queryEventHeader = function(test) {
  var data = [ 0x00,
    0x4b, 0x58, 0x93, 0x52, // timestamp in seconds
    0x02, // event type
    0x01, 0x00, 0x00, 0x00, // server id
    0x4f, 0x01, 0x00, 0x00, // event length
    0xe0, 0x03, 0x00, 0x00, // next position
    0x00, 0x00
  ];

  var buf = new Buffer(data);
  var params = binlog.parseHeader(buf);
  test.equal(params[0], 2);
  test.equal(params[2], 1385388107000);
  test.equal(params[3], 992);
  test.equal(params[4], 316);
  test.done();
};

exports.tablemapEvent = function(test) {
  var data = [ 0x00,
    0x19, 0x69, 0xf0, 0x52, // timestamp in seconds
    0x13, // event type
    0x01, 0x00, 0x00, 0x00, // server id
    0x32, 0x00, 0x00, 0x00, // event length
    0xc8, 0x01, 0x00, 0x00, // next position
    0x00, 0x00, // header end
    0x4c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x09, 0x65, 0x6d,
    0x70, 0x6c, 0x6f, 0x79, 0x65, 0x65, 0x73, 0x00, 0x03, 0x61, 0x62,
    0x63, 0x00, 0x02, 0x03, 0x0f, 0x02, 0xfd, 0x02, 0x03
  ];

  var buf = new Buffer(data);
  var params = binlog.parseHeader(buf);
  test.equal(params[0], 0x13);
  test.equal(params[2], 1391487257000); // should return in milliseconds
  test.equal(params[3], 456);
  test.equal(params[4], 31);

  var anEvent = createEvent(data);
  test.ok(anEvent instanceof binlog.TableMap);
  test.equal(anEvent.getEventName(), 'tablemap');
  test.equal(anEvent.getTypeName(), 'TableMap');
  test.equal(anEvent.schemaName, 'employees');
  test.equal(anEvent.tableId, 76);
  test.equal(anEvent.tableName, 'abc');
  test.equal(anEvent.columnCount, 2);
  test.deepEqual(anEvent.columnTypes, [3, 15]);

  test.done();
};
