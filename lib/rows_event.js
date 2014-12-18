var util = require('util');
var BinlogEvent = require('./binlog_event').BinlogEvent;
var Common = require('./common');

var Version2Events = [
  0x1e, // WRITE_ROWS_EVENT_V2,
  0x1f, // UPDATE_ROWS_EVENT_V2,
  0x20, // DELETE_ROWS_EVENT_V2
];

/**
 * Generic RowsEvent class
 * Attributes:
 *   position: Position inside next binlog
 *   binlogName: Name of next binlog file
 **/

function RowsEvent(parser, options) {
  BinlogEvent.apply(this, arguments);
  this._readTableId(parser);
  this.flags = parser.parseUnsignedNumber(2);

  // Version 2 Events
  if (Version2Events.indexOf(options.eventType) !== -1) {
    this.extraDataLength = parser.parseUnsignedNumber(2);
    // skip extra data
    parser.parseBuffer(this.extraDataLength - 2);
  }

  // Body
  this.numberOfColumns = parser.parseLengthCodedNumber();
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

var readRow = function(tableMap, parser) {
  var row = {}, column, columnSchema;
  for (var i = 0; i < tableMap.columns.length; i++) {
    column = tableMap.columns[i];
    columnSchema = tableMap.columnSchemas[i];
    row[column.name] = Common.readMysqlValue(parser, column, columnSchema);
  }
  return row;
};

function WriteRows(parser, options) {
  RowsEvent.apply(this, arguments);
  this.columnsPresentBitmap = parser.parseUnsignedNumber(
    Math.floor((this.numberOfColumns + 7) / 8));

  this.tableMap = options.tableMap;

  this.rows = [];
  while (!parser.reachedPacketEnd()) {
    this.rows.push(this._fetchOneRow(parser));
  }
}

util.inherits(WriteRows, RowsEvent);

WriteRows.prototype._fetchOneRow = function(parser) {
  var nullBitmap = parser.parseUnsignedNumber(
    Math.floor((this.numberOfColumns + 7) / 8));

  return readRow(this.tableMap[this.tableId], parser);
};

function DeleteRows(parser, options) {
  RowsEvent.apply(this, arguments);
  this.columnsPresentBitmap = parser.parseUnsignedNumber(
    Math.floor((this.numberOfColumns + 7) / 8));

  this.tableMap = options.tableMap;
  this.rows = [];
  while (!parser.reachedPacketEnd()) {
    this.rows.push(this._fetchOneRow(parser));
  }
}

util.inherits(DeleteRows, RowsEvent);

DeleteRows.prototype._fetchOneRow = function(parser) {
  var nullBitmap = parser.parseUnsignedNumber(
    Math.floor((this.numberOfColumns + 7) / 8));

  return readRow(this.tableMap[this.tableId], parser);
};

function UpdateRows(parser, options) {
  RowsEvent.apply(this, arguments);

  this.tableMap = options.tableMap;

  this.columnsPresentBitmap = parser.parseUnsignedNumber(
    Math.floor((this.numberOfColumns + 7) / 8));

  this.columnsPresentBitmap2 = parser.parseUnsignedNumber(
    Math.floor((this.numberOfColumns + 7) / 8));

  this.rows = [];
  while (!parser.reachedPacketEnd()) {
    this.rows.push(this._fetchOneRow(parser));
  }
}

util.inherits(UpdateRows, RowsEvent);

UpdateRows.prototype._fetchOneRow = function(parser) {
  var tableMap = this.tableMap[this.tableId];

  // null bitmap
  parser.parseUnsignedNumber(
    Math.floor((this.numberOfColumns + 7) / 8));

  var row = {};
  row.before = readRow(tableMap, parser);

  // null bitmap 2
  parser.parseUnsignedNumber(
    Math.floor((this.numberOfColumns + 7) / 8));

  row.after = readRow(tableMap, parser);

  return row;
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
