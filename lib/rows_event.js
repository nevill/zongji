var util = require('util');
var BinlogEvent = require('./binlog_event').BinlogEvent;
var Common = require('./common');

var Version2Events = [
  0x1e, // WRITE_ROWS_EVENT_V2,
  0x1f, // UPDATE_ROWS_EVENT_V2,
  0x20, // DELETE_ROWS_EVENT_V2
];

var CHECKSUM_SIZE = 4;

/**
 * Generic RowsEvent class
 * Attributes:
 *   position: Position inside next binlog
 *   binlogName: Name of next binlog file
 *   zongji: ZongJi instance
 **/

function RowsEvent(parser, options, zongji) {
  BinlogEvent.apply(this, arguments);
  this._zongji = zongji;
  this._readTableId(parser);
  this.flags = parser.parseUnsignedNumber(2);
  this.useChecksum = zongji.useChecksum;

  // Version 2 Events
  if (Version2Events.indexOf(options.eventType) !== -1) {
    this.extraDataLength = parser.parseUnsignedNumber(2);
    // skip extra data
    parser.parseBuffer(this.extraDataLength - 2);
  }

  // Body
  this.numberOfColumns = parser.parseLengthCodedNumber();

  this.tableMap = options.tableMap;

  var tableData = this.tableMap[this.tableId];
  if (tableData === undefined) {
    // TableMap event was filtered
    parser._offset = parser._packetEnd;
    this._filtered = true;
  } else {
    var columnsPresentBitmapSize = Math.floor((this.numberOfColumns + 7) / 8);
    // Columns present bitmap exceeds 4 bytes with >32 rows
    // And is not handled anyways so just skip over its space
    parser._offset += columnsPresentBitmapSize;
    if (this._hasTwoRows) {
      // UpdateRows event slightly different, has new and old rows represented
      parser._offset += columnsPresentBitmapSize;
    }

    if (this.useChecksum) {
      // Ignore the checksum at the end of this packet
      parser._packetEnd -= CHECKSUM_SIZE;
    }

    this.rows = [];
    while (!parser.reachedPacketEnd()) {
      this.rows.push(this._fetchOneRow(parser));
    }

    if (this.useChecksum) {
      // Skip past the checksum at the end of the packet
      parser._packetEnd += CHECKSUM_SIZE;
      parser._offset += CHECKSUM_SIZE;
    }
  }
}

util.inherits(RowsEvent, BinlogEvent);

RowsEvent.prototype.setTableMap = function(tableMap) {
  this.tableMap = tableMap;
};

RowsEvent.prototype.dump = function() {
  BinlogEvent.prototype.dump.apply(this);
  console.log('Affected columns:', this.numberOfColumns);
  console.log('Changed rows:', this.rows.length);
  console.log('Values:');
  this.rows.forEach(function(row) {
    console.log('--');
    Object.keys(row).forEach(function(name) {
      console.log('Column: %s, Value: %s', name, row[name]);
    });
  });
};

RowsEvent.prototype._fetchOneRow = function(parser) {
  return readRow(this.tableMap[this.tableId], parser, this._zongji);
};

var readRow = function(tableMap, parser, zongji) {
  var row = {}, column, columnSchema;
  var nullBitmapSize = Math.floor((tableMap.columns.length + 7) / 8);
  var nullBuffer = parser._buffer.slice(parser._offset,
                                        parser._offset + nullBitmapSize);
  var curNullByte, curBit;
  parser._offset += nullBitmapSize;

  for (var i = 0; i < tableMap.columns.length; i++) {
    curBit = i % 8;
    if (curBit === 0) curNullByte = nullBuffer.readUInt8(Math.floor(i / 8));
    column = tableMap.columns[i];
    columnSchema = tableMap.columnSchemas[i];
    if ((curNullByte & (1 << curBit)) === 0) {
      row[column.name] =
        Common.readMysqlValue(parser, column, columnSchema, tableMap, zongji);
    } else {
      row[column.name] = null;
    }
  }
  return row;
};

// Subclasses
function WriteRows(parser, options) { // eslint-disable-line
  RowsEvent.apply(this, arguments);
}

util.inherits(WriteRows, RowsEvent);

// eslint
function DeleteRows(parser, options) { // eslint-disable-line
  RowsEvent.apply(this, arguments);
}

util.inherits(DeleteRows, RowsEvent);

function UpdateRows(parser, options) { // eslint-disable-line
  this._hasTwoRows = true;
  RowsEvent.apply(this, arguments);
}

util.inherits(UpdateRows, RowsEvent);

UpdateRows.prototype._fetchOneRow = function(parser) {
  var tableMap = this.tableMap[this.tableId];
  return {
    before: readRow(tableMap, parser, this._zongji),
    after: readRow(tableMap, parser, this._zongji)
  };
};

UpdateRows.prototype.dump = function() {
  BinlogEvent.prototype.dump.apply(this);
  console.log('Affected columns:', this.numberOfColumns);
  console.log('Changed rows:', this.rows.length);
  console.log('Values:');
  this.rows.forEach(function(row) {
    console.log('--');
    Object.keys(row.before).forEach(function(name) {
      console.log('Column: %s, Value: %s => %s', name, row.before[name], row.after[name]);
    });
  });
};

exports.WriteRows = WriteRows;
exports.DeleteRows = DeleteRows;
exports.UpdateRows = UpdateRows;
