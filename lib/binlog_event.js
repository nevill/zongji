var util = require('util');
var BufferReader = require('./reader').BufferReader;

function BinlogEvent(buffer, options) {
  if (this instanceof BinlogEvent) {
    this.buffer = buffer;
    this.timestamp = options.timestamp;
    this.nextPosition = options.nextPosition;
    this.size = options.size;

    this.reader = new BufferReader(this.buffer);
  } else {
    return new BinlogEvent(buffer, options);
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

function Rotate(buffer, options) {
  if (this instanceof Rotate) {
    BinlogEvent.apply(this, arguments);
    this.position = this.reader.readUInt64();
    this.binlogName = this.reader.readString();
  } else {
    return new Rotate(buffer, options);
  }
}
util.inherits(Rotate, BinlogEvent);

Rotate.prototype.dump = function() {
  console.log('=== %s ===', this.getTypeName());
  console.log('Event size: %d', (this.size));
  console.log('Position: %d', this.position);
  console.log('Next binlog file: %s', this.binlogName);
};

function Format(buffer, options) {
  if (this instanceof Format) {
    BinlogEvent.apply(this, arguments);
  } else {
    return new Format(buffer, options);
  }
}
util.inherits(Format, BinlogEvent);

/* A COMMIT event
 * Attributes:
 *   xid: Transaction ID for 2PC
 */

function Xid(buffer, options) {
  if (this instanceof Xid) {
    BinlogEvent.apply(this, arguments);

    this.xid = this.reader.readUInt64();
  } else {
    return new Xid(buffer, options);
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

function Query(buffer, options) {
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
    return new Query(buffer, options);
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

/**
 * This evenement describe the structure of a table.
 * It's send before a change append on a table.
 * A end user of the lib should have no usage of this
 *
 * see http://dev.mysql.com/doc/internals/en/table-map-event.html
 **/

function TableMap(buffer, options) {
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
    return new TableMap(buffer, options);
  }
}

util.inherits(TableMap, BinlogEvent);

TableMap.prototype.dump = function() {
  BinlogEvent.prototype.dump.apply(this);
  console.log('Table id: %d', this.tableId);
  console.log('Schema: %s', this.schemaName);
  console.log('Table: %s', this.tableName);
  console.log('Columns: %s', this.columnCount);
  console.log('Column types:', this.columnTypes);
};

function Unknown(buffer, options) {
  if (this instanceof Unknown) {
    BinlogEvent.apply(this, arguments);
  } else {
    return new Unknown(buffer, options);
  }
}
util.inherits(Unknown, BinlogEvent);

exports.BinlogEvent = BinlogEvent;
exports.Rotate = Rotate;
exports.Format = Format;
exports.Query = Query;
exports.Xid = Xid;
exports.TableMap = TableMap;
exports.Unknown = Unknown;
