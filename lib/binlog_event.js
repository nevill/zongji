var util = require('util');

var EventCodeMap = {
  UNKNOWN_EVENT: 0x00,
  START_EVENT_V3: 0x01,
  QUERY_EVENT: 0x02,
  STOP_EVENT: 0x03,
  ROTATE_EVENT: 0x04,
  INTVAR_EVENT: 0x05,
  LOAD_EVENT: 0x06,
  SLAVE_EVENT: 0x07,
  CREATE_FILE_EVENT: 0x08,
  APPEND_BLOCK_EVENT: 0x09,
  EXEC_LOAD_EVENT: 0x0a,
  DELETE_FILE_EVENT: 0x0b,
  NEW_LOAD_EVENT: 0x0c,
  RAND_EVENT: 0x0d,
  USER_VAR_EVENT: 0x0e,
  FORMAT_DESCRIPTION_EVENT: 0x0f,
  XID_EVENT: 0x10,
  BEGIN_LOAD_QUERY_EVENT: 0x11,
  EXECUTE_LOAD_QUERY_EVENT: 0x12,
  TABLE_MAP_EVENT: 0x13,
  PRE_GA_DELETE_ROWS_EVENT: 0x14,
  PRE_GA_UPDATE_ROWS_EVENT: 0x15,
  PRE_GA_WRITE_ROWS_EVENT: 0x16,
  DELETE_ROWS_EVENT_V1: 0x19,
  UPDATE_ROWS_EVENT_V1: 0x18,
  WRITE_ROWS_EVENT_V1: 0x17,
  INCIDENT_EVENT: 0x1a,
  HEARTBEAT_LOG_EVENT: 0x1b,
  IGNORABLE_LOG_EVENT: 0x1c,
  ROWS_QUERY_LOG_EVENT: 0x1d,
  WRITE_ROWS_EVENT_V2: 0x1e,
  UPDATE_ROWS_EVENT_V2: 0x1f,
  DELETE_ROWS_EVENT_V2: 0x20,
  GTID_LOG_EVENT: 0x21,
  ANONYMOUS_GTID_LOG_EVENT: 0x22,
  PREVIOUS_GTIDS_LOG_EVENT: 0x23
};

var BufferReader = require('./reader').BufferReader;

function BinlogEvent(buffer, timestamp, nextPosition, size) {
  this.buffer = buffer;
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

BinlogEvent.prototype.dump = function() {
  console.log('=== %s ===', this.getTypeName());
  console.log('Date: %s', new Date(this.timestamp));
  console.log('Next log position: %d', this.nextPosition);
  console.log('Event size:', this.size);
  console.log('Buffer:', this.buffer);
};

/* Change MySQL bin log file
 * Attributes:
 *   position: Position inside next binlog
 *   binlogName: Name of next binlog file
 */
function Rotate(buffer, timestamp, nextPosition, size) {
  if (this instanceof Rotate) {
    BinlogEvent.apply(this, arguments);
    this.position = this.reader.readUInt64();
    this.binlogName = this.reader.readString();
  } else {
    return new Rotate(buffer, timestamp, nextPosition, size);
  }
}
util.inherits(Rotate, BinlogEvent);

Rotate.Code = EventCodeMap.ROTATE_EVENT;

Rotate.prototype.dump = function() {
  console.log('=== %s ===', this.getTypeName());
  console.log('Event size: %d', (this.size));
  console.log('Position: %d', this.position);
  console.log('Next binlog file: %s', this.binlogName);
};

function Format(buffer, timestamp, nextPosition, size) {
  if (this instanceof Format) {
    BinlogEvent.apply(this, arguments);
  } else {
    return new Format(buffer, timestamp, nextPosition, size);
  }
}
util.inherits(Format, BinlogEvent);

Format.Code = EventCodeMap.FORMAT_DESCRIPTION_EVENT;

/* A COMMIT event
 * Attributes:
 *   xid: Transaction ID for 2PC
 */
function Xid(buffer, timestamp, nextPosition, size) {
  if (this instanceof Xid) {
    BinlogEvent.apply(this, arguments);

    this.xid = this.reader.readUInt64();
  } else {
    return new Xid(buffer, timestamp, nextPosition, size);
  }
}
util.inherits(Xid, BinlogEvent);

Xid.Code = EventCodeMap.XID_EVENT;

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
function Query(buffer, timestamp, nextPosition, size) {
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
    return new Query(buffer, timestamp, nextPosition, size);
  }
}
util.inherits(Query, BinlogEvent);

Query.Code = EventCodeMap.QUERY_EVENT;

Query.prototype.dump = function() {
  console.log('=== %s ===', this.getTypeName());
  console.log('Date: %s', new Date(this.timestamp));
  console.log('Next log position: %d', this.nextPosition);
  console.log('Schema: %s', this.schema);
  console.log('Execution time: %d', this.executionTime);
  console.log('Query: %s', this.query);
};

function Unknown(buffer, timestamp, nextPosition, size) {
  if (this instanceof Unknown) {
    BinlogEvent.apply(this, arguments);
  } else {
    return new Unknown(buffer, timestamp, nextPosition, size);
  }
}
util.inherits(Unknown, BinlogEvent);

Unknown.Code = EventCodeMap.UNKNOWN_EVENT;

exports.Rotate = Rotate;
exports.Format = Format;
exports.Query = Query;
exports.Xid = Xid;
exports.Unknown = Unknown;
