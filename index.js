var mysql = require('mysql');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var generateBinlog = require('./lib/sequence/binlog');

function ZongJi(dsn) {
  EventEmitter.call(this);

  // to send table info query
  this.ctrlConnection = mysql.createConnection({
    host: dsn.host,
    user: dsn.user,
    password: dsn.password,
    database: 'information_schema',
  });
  this.ctrlConnection.connect();
  this.ctrlCallbacks = [];

  this.connection = mysql.createConnection(dsn);

  this.tableMap = {};
  this.ready = false;
  this.useChecksum = false;

  this._init();
}

util.inherits(ZongJi, EventEmitter);

ZongJi.prototype._init = function() {
  var self = this;

  this._isChecksumEnabled(function(checksumEnabled) {
    self.useChecksum = checksumEnabled;
    var options = {
      tableMap: self.tableMap,
      useChecksum: checksumEnabled,
    };

    self.binlog = generateBinlog.call(self, options);
    self.ready = true;

    self._executeCtrlCallbacks();
  });
};

ZongJi.prototype._isChecksumEnabled = function(next) {
  var sql = 'select @@GLOBAL.binlog_checksum as checksum';
  var ctrlConnection = this.ctrlConnection;
  var connection = this.connection;

  ctrlConnection.query(sql, function(err, rows) {
    if (err) {
      if(err.toString().match(/ER_UNKNOWN_SYSTEM_VARIABLE/)){
        // MySQL < 5.6.2 does not support @@GLOBAL.binlog_checksum
        return next(false);
      }
      throw err;
    }

    var checksumEnabled = true;
    if (rows[0].checksum === 'NONE') {
      checksumEnabled = false;
    }

    var setChecksumSql = 'set @master_binlog_checksum=@@global.binlog_checksum';
    if (checksumEnabled) {
      connection.query(setChecksumSql, function(err) {
        if (err) {
          throw err;
        }
        next(checksumEnabled);
      });
    } else {
      next(checksumEnabled);
    }
  });
};

ZongJi.prototype._executeCtrlCallbacks = function() {
  if (this.ctrlCallbacks.length > 0) {
    this.ctrlCallbacks.forEach(function(cb) {
      setImmediate(cb);
    });
  }
};

var tableInfoQueryTemplate = 'SELECT ' +
  'COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME, ' +
  'COLUMN_COMMENT, COLUMN_TYPE ' +
  'FROM columns ' + 'WHERE table_schema="%s" AND table_name="%s"';

ZongJi.prototype._fetchTableInfo = function(tableMapEvent, next) {
  var self = this;
  var sql = util.format(tableInfoQueryTemplate,
    tableMapEvent.schemaName, tableMapEvent.tableName);

  this.ctrlConnection.query(sql, function(err, rows) {
    if (err) throw err;

    self.tableMap[tableMapEvent.tableId] = {
      columnSchemas: rows,
      parentSchema: tableMapEvent.schemaName,
      tableName: tableMapEvent.tableName
    };

    next();
  });
};

ZongJi.prototype.set = function(options){
  this.options = options || {};
};

ZongJi.prototype.start = function(options) {
  var self = this;
  self.set(options);

  var _start = function() {
    self.connection._implyConnect();
    self.connection._protocol._enqueue(new self.binlog(function(error, event){
      if(error) return self.emit('error', error);
      if(event === undefined) return; // Filtered out

      if (event.getTypeName() === 'TableMap') {
        var tableMap = self.tableMap[event.tableId];

        if (!tableMap) {
          self.connection.pause();
          self._fetchTableInfo(event, function() {
            // merge the column info with metadata
            event.updateColumnInfo();
            self.emit('binlog', event);
            self.connection.resume();
          });
          return;
        }
      }
      self.emit('binlog', event);
    }));
  };

  if (this.ready) {
    _start();
  } else {
    this.ctrlCallbacks.push(_start);
  }
};

ZongJi.prototype._skipEvent = function(eventName){
  var include = this.options.includeEvents;
  var exclude = this.options.excludeEvents;
  return !(
   (include === undefined ||
    (include instanceof Array && include.indexOf(eventName) !== -1)) &&
   (exclude === undefined ||
    (exclude instanceof Array && exclude.indexOf(eventName) === -1)));
};

ZongJi.prototype._skipSchema = function(database, table){
  var include = this.options.includeSchema;
  var exclude = this.options.excludeSchema;
  return !(
   (include === undefined ||
    (database !== undefined && (database in include) &&
     (include[database] === true ||
      (include[database] instanceof Array &&
       include[database].indexOf(table) !== -1)))) &&
   (exclude === undefined ||
      (database !== undefined && 
       (!(database in exclude) || 
        (exclude[database] !== true &&
          (exclude[database] instanceof Array &&
           exclude[database].indexOf(table) === -1))))));
};

module.exports = ZongJi;
