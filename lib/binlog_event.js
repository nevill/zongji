var util = require('util');

var BufferReader = require('./reader').BufferReader;

function BinlogEvent(buffer, typeCode, timestamp, nextPosition, size) {
  this.buffer = buffer;
  this.typeCode = typeCode;
  this.timestamp = timestamp;
  this.nextPosition = nextPosition;
  this.size = size;

  this.reader = new BufferReader(this.buffer);
}

BinlogEvent.prototype.getEventName = function() {
  return this.getTypeName().toLowerCase();
};

BinlogEvent.prototype.getTypeName = function() {
  return this.constructor.name;
};

BinlogEvent.prototype.getTypeCode = function() {
  return this.typeCode;
};

BinlogEvent.prototype.dump = function() {
  console.log('=== %s ===', this.getTypeName());
  console.log('Date: %s', new Date(this.timestamp));
  console.log('Next log position: %d', this.nextPosition);
  console.log('Event size:', this.size);
  console.log('Type code:', this.getTypeCode());
  console.log('Buffer:', this.buffer);
};

/* Change MySQL bin log file
 * Attributes:
 *   position: Position inside next binlog
 *   binlogName: Name of next binlog file
 */
function Rotate(buffer, typeCode, timestamp, nextPosition, size) {
  if (this instanceof Rotate) {
    BinlogEvent.apply(this, arguments);
    this.position = this.reader.readUInt64();
    this.binlogName = this.reader.readString();
  } else {
    return new Rotate(buffer, typeCode, timestamp, nextPosition, size);
  }
}
util.inherits(Rotate, BinlogEvent);

Rotate.prototype.dump = function() {
  console.log('=== %s ===', this.getTypeName());
  console.log('Event size: %d', (this.size));
  console.log('Position: %d', this.position);
  console.log('Next binlog file: %s', this.binlogName);
};

function Format(buffer, typeCode, timestamp, nextPosition, size) {
  if (this instanceof Format) {
    BinlogEvent.apply(this, arguments);
  } else {
    return new Format(buffer, typeCode, timestamp, nextPosition, size);
  }
}
util.inherits(Format, BinlogEvent);

/* A COMMIT event
 * Attributes:
 *   xid: Transaction ID for 2PC
 */
function Xid(buffer, typeCode, timestamp, nextPosition, size) {
  if (this instanceof Xid) {
    BinlogEvent.apply(this, arguments);

    this.xid = this.reader.readUInt64();
  } else {
    return new Xid(buffer, typeCode, timestamp, nextPosition, size);
  }
}
util.inherits(Xid, BinlogEvent);

/*
 * Attributes:
 *  (post-header)
 *    slaveProxyId
 *    executionTime
 *    schemaLength
 *    errorCode
 *    statusVarsLength
 *
 *  (payload)
 *    statusVars
 *    schema
 *    [00]
 *    query
 */
function Query(buffer, typeCode, timestamp, nextPosition, size) {
  if (this instanceof Query) {
    BinlogEvent.apply(this, arguments);

    this.slaveProxyId = this.reader.readUInt32();
    this.executionTime = this.reader.readUInt32();
    this.schemaLength = this.reader.readUInt8();
    this.errorCode = this.reader.readUInt16();
    this.statusVarsLength = this.reader.readUInt16();

    this.statusVars = this.reader
      .readStringInBytes(this.statusVarsLength);
    this.schema = this.reader.readStringInBytes(this.schemaLength);
    this.reader.readUInt8();
    this.query = this.reader.readString();
  } else {
    return new Query(buffer, typeCode, timestamp, nextPosition, size);
  }
}
util.inherits(Query, BinlogEvent);

Query.prototype.dump = function() {
  console.log('=== %s ===', this.getTypeName());
  console.log('Date: %s', new Date(this.timestamp));
  console.log('Next log position: %d', this.nextPosition);
  console.log('Schema: %s', this.schema);
  console.log('Execution time: %d', this.executionTime);
  console.log('Query: %s', this.query);
};

function Unknown(buffer, typeCode, timestamp, nextPosition, size) {
  if (this instanceof Unknown) {
    BinlogEvent.apply(this, arguments);
  } else {
    return new Unknown(buffer, typeCode, timestamp, nextPosition, size);
  }
}
util.inherits(Unknown, BinlogEvent);

exports.Rotate = Rotate;
exports.Format = Format;
exports.Query = Query;
exports.Xid = Xid;
exports.Unknown = Unknown;
