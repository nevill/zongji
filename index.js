var mysql = require('mysql');
var util = require('util');
var EventEmitter = require('events').EventEmitter;

var capture = require('./lib/capture');
var patch = require('./lib/patch');

// to send table info query
var ctrlConnection;

function ZongJi(connection, options) {
  EventEmitter.call(this);
  this.connection = connection;
  this.ready = false;
  this.tableMap = options.tableMap;
}

util.inherits(ZongJi, EventEmitter);

var queryTemplate = 'SELECT ' +
    'COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME, ' +
    'COLUMN_COMMENT, COLUMN_TYPE ' +
    'FROM columns ' + 'WHERE table_schema="%s" AND table_name="%s"';

ZongJi.prototype._fetchTableInfo = function(tableMapEvent, next) {
  var sql = util.format(queryTemplate,
    tableMapEvent.schemaName, tableMapEvent.tableName);

  var self = this;

  ctrlConnection.query(sql, function(err, rows) {
    if (err) {
      throw err;
    }

    self.tableMap[tableMapEvent.tableId] = {
      columnSchemas: rows
    };

    next();
  });
};

ZongJi.prototype.start = function(options) {
  var self = this;
  var connection = this.connection;

  if (!this.ready) {
    connection.connect();
    this.ready = true;
  }

  var emitBinlog = function(binlog) {
    self.emit('binlog', binlog);
  };

  if (options && options.filter) {
    emitBinlog = function(binlog) {
      if (options.filter.indexOf(binlog.getEventName()) > -1) {
        self.emit('binlog', binlog);
      }
    };
  }

  connection.dumpBinlog(function(err, binlog) {
    if (binlog.getTypeName() === 'TableMap') {
      var tableMap = self.tableMap[binlog.tableId];

      if (!tableMap) {
        connection.pause();
        self._fetchTableInfo(binlog, function() {
          // merge the column info with metadata
          binlog.updateColumnInfo();
          emitBinlog(binlog);
          connection.resume();
        });
        return;
      }
    }

    emitBinlog(binlog);
  });
};

exports.connect = function(dsn) {
  var connection = mysql.createConnection(dsn);

  ctrlConnection = mysql.createConnection({
    host: dsn.host,
    user: dsn.user,
    password: dsn.password,
    database: 'information_schema',
  });

  ctrlConnection.connect();

  var options = {
    tableMap: {},
  };

  patch(capture(connection), options);
  return new ZongJi(connection, options);
};
