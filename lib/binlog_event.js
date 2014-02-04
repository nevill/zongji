var util = require('util');

var EventCodeMap = require('./code_map');
var BufferReader = require('./reader').BufferReader;

function BinlogEvent(buffer, timestamp, nextPosition, size) {
  if (this instanceof BinlogEvent) {
    this.buffer = buffer;
    this.timestamp = timestamp;
    this.nextPosition = nextPosition;
    this.size = size;

    this.reader = new BufferReader(this.buffer);
  } else {
    return new BinlogEvent(buffer, timestamp, nextPosition, size);
  }
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

BinlogEvent.prototype._readTableId = function() {
  var lowBytes = this.reader.readUInt32();
  var highBytes = this.reader.readUInt16();
  // jshint bitwise: false
  this.tableId = (highBytes << 32) + lowBytes;
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

/**
 * This evenement describe the structure of a table.
 * It's send before a change append on a table.
 * A end user of the lib should have no usage of this
 *
 * see http://dev.mysql.com/doc/internals/en/table-map-event.html
 **/

function TableMap(buffer, timestamp, nextPosition, size) {
  if (this instanceof TableMap) {
    BinlogEvent.apply(this, arguments);
    var reader = this.reader;

    // post-header
    this._readTableId();
    this.flags = reader.readUInt16();

    // payload
    var schemaNameLength = reader.readUInt8();
    this.schemaName = reader.readStringInBytes(schemaNameLength);
    reader.readUInt8();

    var tableNameLength = reader.readUInt8();
    this.tableName = reader.readStringInBytes(tableNameLength);
    reader.readUInt8();

    this.columnCount = reader.readVariant();
    this.columnTypes = reader.readBytesArray(this.columnCount);

    // ignore the rest
  } else {
    return new TableMap(buffer, timestamp, nextPosition, size);
  }
}

util.inherits(TableMap, BinlogEvent);
TableMap.Code = EventCodeMap.TABLE_MAP_EVENT;

TableMap.prototype.dump = function() {
  BinlogEvent.prototype.dump.apply(this);
  console.log('Table id: %d', this.tableId);
  console.log('Schema: %s', this.schemaName);
  console.log('Table: %s', this.tableName);
  console.log('Columns: %s', this.columnCount);
  console.log('Column types:', this.columnTypes);
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

exports.BinlogEvent = BinlogEvent;
exports.Rotate = Rotate;
exports.Format = Format;
exports.Query = Query;
exports.Xid = Xid;
exports.TableMap = TableMap;
exports.Unknown = Unknown;
