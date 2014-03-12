var util = require('util');
var Common = require('./common');

function BinlogEvent(parser, options) {
  this.timestamp = options.timestamp;
  this.nextPosition = options.nextPosition;
  this.size = options.size;
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
};

BinlogEvent.prototype._readTableId = function(parser) {
  this.tableId = Common.parseUInt48(parser);
};

/* Change MySQL bin log file
 * Attributes:
 *   position: Position inside next binlog
 *   binlogName: Name of next binlog file
 */

function Rotate(parser) {
  BinlogEvent.apply(this, arguments);
  this.position = Common.parseUInt64(parser);
  this.binlogName = parser.parsePacketTerminatedString();
}
util.inherits(Rotate, BinlogEvent);

Rotate.prototype.dump = function() {
  console.log('=== %s ===', this.getTypeName());
  console.log('Event size: %d', (this.size));
  console.log('Position: %d', this.position);
  console.log('Next binlog file: %s', this.binlogName);
};

function Format() {
  BinlogEvent.apply(this, arguments);
}
util.inherits(Format, BinlogEvent);

/* A COMMIT event
 * Attributes:
 *   xid: Transaction ID for 2PC
 */

function Xid(parser) {
  BinlogEvent.apply(this, arguments);
  this.xid = Common.parseUInt64(parser);
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

function Query(parser) {
  BinlogEvent.apply(this, arguments);

  this.slaveProxyId = parser.parseUnsignedNumber(4);
  this.executionTime = parser.parseUnsignedNumber(4);
  this.schemaLength = parser.parseUnsignedNumber(1);
  this.errorCode = parser.parseUnsignedNumber(2);
  this.statusVarsLength = parser.parseUnsignedNumber(2);

  this.statusVars = parser.parseString(this.statusVarsLength);
  this.schema = parser.parseString(this.schemaLength);
  parser.parseUnsignedNumber(1);
  this.query = parser.parsePacketTerminatedString();
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

function TableMap(parser) {
  BinlogEvent.apply(this, arguments);
  // post-header
  this._readTableId(parser);
  this.flags = parser.parseUnsignedNumber(2);

  // payload
  var schemaNameLength = parser.parseUnsignedNumber(1);
  this.schemaName = parser.parseString(schemaNameLength);
  parser.parseUnsignedNumber(1);

  var tableNameLength = parser.parseUnsignedNumber(1);
  this.tableName = parser.parseString(tableNameLength);
  parser.parseUnsignedNumber(1);

  this.columnCount = parser.parseLengthCodedNumber();
  this.columnTypes = Common.parseBytesArray(parser, this.columnCount);
  // column meta info
  parser.parseLengthCodedBuffer();
  // ignore the rest
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

function Unknown() {
  BinlogEvent.apply(this, arguments);
}
util.inherits(Unknown, BinlogEvent);

exports.BinlogEvent = BinlogEvent;
exports.Rotate = Rotate;
exports.Format = Format;
exports.Query = Query;
exports.Xid = Xid;
exports.TableMap = TableMap;
exports.Unknown = Unknown;
