const util = require('util');
const BinlogEvent = require('./binlog_event').BinlogEvent;
const Common = require('./common');

const Version2Events = [
  0x1e, // WRITE_ROWS_EVENT_V2,
  0x1f, // UPDATE_ROWS_EVENT_V2,
  0x20, // DELETE_ROWS_EVENT_V2
];

const CHECKSUM_SIZE = 4;

// A quick way to know how many bits set in a given byte
// e.g. Given 3 => 0000 0011, it has 2 bits set
const BIT_COUNT_MAP_IN_ONE_BYTE = [
  0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
  3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
  4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
];

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

  this.tableMap = zongji.tableMap;

  const tableData = this.tableMap[this.tableId];
  if (tableData === undefined) {
    // TableMap event was filtered
    parser._offset = parser._packetEnd;
    this._filtered = true;
  } else {
    const columnsPresentBitmapSize = Math.floor((this.numberOfColumns + 7) / 8);
    this.columns_present_bitmap = parser.parseBuffer(columnsPresentBitmapSize);
    if (this._hasTwoRows) {
      // UpdateRows event slightly different, has new and old rows represented
      this.columns_present_bitmap2 = parser.parseBuffer(columnsPresentBitmapSize);
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
  const tablemap = this.tableMap[this.tableId];
  return readRow(parser, tablemap, this.columns_present_bitmap, this._zongji);
};

const countBits = function(buff) {
  let bits = 0;
  for (let i = 0; i < buff.length; i++) {
    bits += BIT_COUNT_MAP_IN_ONE_BYTE[buff[i]];
  }
  return bits;
};

const getBit = function(buff, position) {
  let byte = buff[Math.floor(position / 8)];
  return byte & (1 << (position % 8));
};

const readRow = function(parser, tablemap, bitmap, zongji) {
  const nullBitmapSize = Math.floor((countBits(bitmap) + 7) / 8);
  const nullBitmap = parser.parseBuffer(nullBitmapSize);

  let row = {};
  for (let i = 0, nullBitIndex = 0; i < tablemap.columns.length; i++) {
    let column = tablemap.columns[i];

    if (getBit(bitmap, i) == 0) {
      row[column.name] = null;
      continue;
    }

    if (getBit(nullBitmap, nullBitIndex) != 0) {
      row[column.name] = null;
    } else {
      let columnSchema = tablemap.columnSchemas[i];
      row[column.name] = Common.readMysqlValue(
        parser, column, columnSchema, tablemap, zongji
      );
    }

    nullBitIndex += 1;
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
  const tablemap = this.tableMap[this.tableId];
  return {
    before: readRow(parser, tablemap, this.columns_present_bitmap, this._zongji),
    after: readRow(parser, tablemap, this.columns_present_bitmap2, this._zongji),
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
