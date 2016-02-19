var mysql = require('mysql');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var generateBinlog = require('./lib/sequence/binlog');

function ZongJi(dsn, options) {
  var self = this;

  this.set(options);

  EventEmitter.call(this);

  // to send table info query
  var ctrlDsn = cloneObjectSimple(dsn);
  ctrlDsn.database = 'information_schema';
  this.ctrlConnection = mysql.createConnection(ctrlDsn);

  this.ctrlConnection.on('error', function (error) {
    self.emit('error', error);
  });

  this.ctrlConnection.on('unhandledError', function (error) {
    self.emit('error', error);
  });

  this.ctrlConnection.connect();
  this.ctrlCallbacks = [];

  this.connection = mysql.createConnection(dsn);

  this.tableMap = {};
  this.ready = false;
  this.useChecksum = false;

  this._init();
}

function cloneObjectSimple (obj){
  var out = {};
  for(var i in obj){
    if(obj.hasOwnProperty(i)){
      out[i] = obj[i];
    }
  }
  return out;
}

util.inherits(ZongJi, EventEmitter);

ZongJi.prototype._init = function() {
  var self = this;
  var binlogOptions = {
    tableMap: self.tableMap
  };

  var asyncMethods = [
    {
      name: '_isChecksumEnabled',
      callback: function(checksumEnabled) {
        self.useChecksum = checksumEnabled;
        binlogOptions.useChecksum = checksumEnabled
      }
    },
    {
      name: '_findBinlogEnd',
      callback: function(result){
        if(result && self.options.startAtEnd){
          binlogOptions.filename = result.Log_name;
          binlogOptions.position = result.File_size;
        }
      }
    },
    {
      name: '_getUtcOffset',
      callback: function(utcOffset) {
        self.utcOffset = utcOffset * 1000;
        require('./lib/common').setUtcOffset(self.utcOffset);
      }
    },
  ];

  var methodIndex = 0;
  var nextMethod = function(){
    var method = asyncMethods[methodIndex];
    self[method.name](function(/* args */){
      method.callback.apply(this, arguments);
      methodIndex++;
      if(methodIndex < asyncMethods.length){
        nextMethod();
      }else{
        ready();
      }
    });
  };
  nextMethod();

  var ready = function(){
    // Run asynchronously from _init(), as serverId option set in start()
    if(self.options.serverId !== undefined){
      binlogOptions.serverId = self.options.serverId;
    }

    self.binlog = generateBinlog.call(self, binlogOptions);
    self.ready = true;
    self._executeCtrlCallbacks();
  };
};

ZongJi.prototype._getUtcOffset = function(next) {
  var self = this;
  var sql = 'SELECT TIMESTAMPDIFF(SECOND, NOW(), UTC_TIMESTAMP) AS offset;';
  var ctrlConnection = self.ctrlConnection;

  ctrlConnection.query(sql, function(err, rows) {
    var nodeOffsetInSeconds = new Date().getTimezoneOffset() * 60,
        mysqlOffsetInSeconds;

    if (err) {
      self.emit('error', err);
      return next(false);
    }

    mysqlOffsetInSeconds = rows[0].offset;

    next(mysqlOffsetInSeconds - nodeOffsetInSeconds);
  });
};

ZongJi.prototype._isChecksumEnabled = function(next) {
  var self = this;
  var sql = 'select @@GLOBAL.binlog_checksum as checksum';
  var ctrlConnection = self.ctrlConnection;
  var connection = self.connection;

  ctrlConnection.query(sql, function(err, rows) {
    if (err) {
      if(err.toString().match(/ER_UNKNOWN_SYSTEM_VARIABLE/)){
        // MySQL < 5.6.2 does not support @@GLOBAL.binlog_checksum
        return next(false);
      } else {
        // Any other errors should be emitted
        self.emit('error', err);
        return;
      }
    }

    var checksumEnabled = true;
    if (rows[0].checksum === 'NONE') {
      checksumEnabled = false;
    }

    var setChecksumSql = 'set @master_binlog_checksum=@@global.binlog_checksum';
    if (checksumEnabled) {
      connection.query(setChecksumSql, function(err) {
        if (err) {
          // Errors should be emitted
          self.emit('error', err);
          return;
        }
        next(checksumEnabled);
      });
    } else {
      next(checksumEnabled);
    }
  });
};

ZongJi.prototype._findBinlogEnd = function(next) {
  var self = this;
  self.ctrlConnection.query('SHOW BINARY LOGS', function(err, rows) {
    if (err) {
      // Errors should be emitted
      self.emit('error', err);
      return;
    }
    next(rows.length > 0 ? rows[rows.length - 1] : null);
  });
};

ZongJi.prototype._executeCtrlCallbacks = function() {
  if (this.ctrlCallbacks.length > 0) {
    this.ctrlCallbacks.forEach(function(cb) {
      setImmediate(cb);
    });
  }
};

var tableInfoQueryTemplate = [
  '   SELECT c.ORDINAL_POSITION,' +
  '          c.COLUMN_NAME,',
  '          COLLATION_NAME,',
  '          CHARACTER_SET_NAME,',
  '          COLUMN_COMMENT,',
  '          COLUMN_TYPE,',
  '          IS_NULLABLE,',
  '          t.CONSTRAINT_TYPE,',
  '          k.CONSTRAINT_NAME',
  '     FROM information_schema.columns c',
  'LEFT JOIN information_schema.KEY_COLUMN_USAGE k',
  '       ON k.TABLE_NAME = c.TABLE_NAME',
  '      AND k.TABLE_SCHEMA = c.TABLE_SCHEMA',
  '      AND k.COLUMN_NAME = c.COLUMN_NAME',
  'LEFT JOIN information_schema.TABLE_CONSTRAINTS t',
  '       ON t.TABLE_NAME = c.TABLE_NAME',
  '      AND t.CONSTRAINT_SCHEMA = c.TABLE_SCHEMA',
  '      AND t.CONSTRAINT_NAME = k.CONSTRAINT_NAME',
  '    WHERE c.TABLE_SCHEMA = "%s"',
  '      AND c.TABLE_NAME = "%s"'
].join(' ').replace(/\s{2,}/g, ' ');

ZongJi.prototype._fetchTableInfo = function(tableMapEvent, next) {
  var self = this;
  var sql = util.format(tableInfoQueryTemplate,
    tableMapEvent.schemaName, tableMapEvent.tableName);

  this.ctrlConnection.query(sql, function(err, rows) {
    var columns = [],
        column,
        rowLen,
        idx,
        x;

    if (err) {
      // Errors should be emitted
      self.emit('error', err);
      // This is a fatal error, no additional binlog events will be processed as next() will never be called
      return;
    }

    rowLen = rows.length;

    if (rowLen === 0) {
      self.emit('error',
          new Error('Insufficient permissions to access: ' + tableMapEvent.schemaName + '.' + tableMapEvent.tableName)
      );
      // This is a fatal error, no additional binlog events will be processed as next() will never be called
      return;
    }

    for (x = 0; x < rowLen; x++) {
      column = rows[x];
      idx = column.ORDINAL_POSITION - 1;

      column.UNIQUE = column.CONSTRAINT_TYPE === 'UNIQUE';
      column.PK = column.CONSTRAINT_TYPE === 'PRIMARY KEY';

      if (x === 0 || columns[idx] === undefined) {
        columns.push(column);
      } else {
        columns[idx].CONSTRAINT_TYPES || (columns[idx].CONSTRAINT_TYPES = [columns[idx].CONSTRAINT_TYPE]);
        columns[idx].CONSTRAINT_NAMES || (columns[idx].CONSTRAINT_NAMES = [columns[idx].CONSTRAINT_NAME]);

        columns[idx].CONSTRAINT_TYPES.push(column.CONSTRAINT_TYPE);
        columns[idx].CONSTRAINT_NAMES.push(column.CONSTRAINT_NAME);

        if (column.UNIQUE) {
          columns[idx].UNIQUE = true;
        } else if (column.PK) {
          columns[idx].PK = true;
        }
      }
    }

    self.tableMap[tableMapEvent.tableId] = {
      columnSchemas: columns,
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
      // Do not emit events that have been filtered out
      if(event === undefined || event._filtered === true) return;

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

ZongJi.prototype.stop = function(){
  var self = this;
  // Binary log connection does not end with destroy()
  self.connection.destroy();
  self.ctrlConnection.query(
    'KILL ' + self.connection.threadId,
    function(error, results){
      self.ctrlConnection.destroy();
    }
  );
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
