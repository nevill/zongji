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
  this.binlogName = parser.parseString(this.size - 8);
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

  // all the left is the query
  this.query = parser.parseString(this.size - 13 - this.statusVarsLength - this.schemaLength - 1);
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

function TableMap(parser, options, zongji) {
  BinlogEvent.apply(this, arguments);
  this.tableMap = options.tableMap;

  // post-header
  this._readTableId(parser);
  this.flags = parser.parseUnsignedNumber(2);

  // payload
  var schemaNameLength = parser.parseUnsignedNumber(1);
  this.schemaName = parser.parseString(schemaNameLength);
  parser.parseUnsignedNumber(1);

  var tableNameLength = parser.parseUnsignedNumber(1);
  this.tableName = parser.parseString(tableNameLength);

  if(zongji._skipSchema(this.schemaName, this.tableName)){
    // This event has been filtered out because of its database/table
    parser._offset = parser._packetEnd;
    this._filtered = true;
  }else{
    parser.parseUnsignedNumber(1);

    this.columnCount = parser.parseLengthCodedNumber();
    this.columnTypes = Common.parseBytesArray(parser, this.columnCount);
    // column meta data length
    parser.parseLengthCodedNumber();
    this._readColumnMetadata(parser);
    // ignore the rest
  }
}

util.inherits(TableMap, BinlogEvent);

TableMap.prototype.updateColumnInfo = function() {
  var columnsMetadata = this.columnsMetadata;
  for (var i = 0; i < this.columnCount; i++) {
    if (columnsMetadata[i] && columnsMetadata[i].type) {
      this.columnTypes[i] = columnsMetadata[i].type;
      delete columnsMetadata[i].type;
    }
  }
  var tableMap = this.tableMap[this.tableId];

  var columnSchemas = tableMap.columnSchemas;
  var columns = [];
  for (var j = 0; j < this.columnCount; j++) {
    columns.push({
      name: columnSchemas[j].COLUMN_NAME,
      charset: columnSchemas[j].CHARACTER_SET_NAME,
      type: this.columnTypes[j],
      // nullable:
      metadata: columnsMetadata[j]
    });
  }

  tableMap.columns = columns;
};

TableMap.prototype._readColumnMetadata = function(parser) {
  this.columnsMetadata = this.columnTypes.map(function(code) {
    var mysqlType = Common.convertToMysqlType(code);
    var result;

    //jshint indent: false
    switch (mysqlType) {
      case 'FLOAT':
      case 'DOUBLE':
        result = {
          size: parser.parseUnsignedNumber(1)
        };
        break;
      case 'VARCHAR':
        result = {
          'max_length': parser.parseUnsignedNumber(2)
        };
        break;
      case 'BIT':
        var bits = parser.parseUnsignedNumber(1);
        var bytes = parser.parseUnsignedNumber(1);
        result = {
          bits: bytes * 8 + bits
        };
        break;
      case 'NEWDECIMAL':
        result = {
          precision: parser.parseUnsignedNumber(1),
          decimals: parser.parseUnsignedNumber(1),
        };
        break;
      case 'BLOB':
      case 'GEOMETRY':
        result = {
          'length_size': parser.parseUnsignedNumber(1)
        };
        break;
      case 'STRING':
      case 'VAR_STRING':
        // The STRING type sets a 'real_type' field to indicate the
        // actual type which is fundamentally incompatible with STRING
        // parsing. Setting a 'type' key in this hash will cause
        // TableMap event to override the main field 'type' with the
        // provided 'type' here.
        var metadata = (parser.parseUnsignedNumber(1) << 8) + parser.parseUnsignedNumber(1);
        var realType = metadata >> 8;
        var typeName = Common.convertToMysqlType(realType);
        if (typeName === 'ENUM' || typeName === 'SET') {
          result = {
            type: realType,
            size: metadata & 0x00ff
          };
        } else {
          result = {
            'max_length': ((
              (metadata >> 4) & 0x300) ^ 0x300) + (metadata & 0x00ff)
          };
        }
        break;
      case 'TIMESTAMP2':
      case 'DATETIME2':
      case 'TIME2':
        result = {
          decimals: parser.parseUnsignedNumber(1)
        };
        break;
    }

    return result;
  });
};

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
