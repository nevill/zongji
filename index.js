const mysql = require('mysql');
const util = require('util');
const EventEmitter = require('events').EventEmitter;
const initBinlogClass = require('./lib/sequence/binlog');

const ConnectionConfigMap = {
  'Connection': obj => obj.config,
  'Pool': obj => obj.config.connectionConfig,
};

const TableInfoQueryTemplate = 'SELECT ' +
  'COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME, ' +
  'COLUMN_COMMENT, COLUMN_TYPE ' +
  'FROM information_schema.columns ' + "WHERE table_schema='%s' AND table_name='%s'";

function ZongJi(dsn, options) {
  this.set(options);

  EventEmitter.call(this);

  this.ctrlCallbacks = [];
  this.tableMap = {};
  this.ready = false;
  this.useChecksum = false;
  // Include 'rotate' events to keep these properties updated
  this.binlogName = null;
  this.binlogNextPos = null;

  this._establishConnection(dsn);
  this._init();
}

util.inherits(ZongJi, EventEmitter);

// dsn - can be one instance of Connection or Pool / object / url string
ZongJi.prototype._establishConnection = function(dsn) {
  let binlogDsn;
  let configFunc = ConnectionConfigMap[dsn.constructor.name];

  if (typeof dsn === 'object' && configFunc) {
    let conn = dsn;
    // reuse as ctrlConnection
    this.ctrlConnection = conn;
    this.ctrlConnectionOwner = false;
    binlogDsn = Object.assign({}, configFunc(conn));
  }

  let createConnection = (options) => {
    let connection = mysql.createConnection(options);
    connection.on('error', this.emit.bind(this, 'error'));
    connection.on('unhandledError', this.emit.bind(this, 'error'));
    // don't need to call connection.connect() here
    // we use implicitly established connection
    // see https://github.com/mysqljs/mysql#establishing-connections
    return connection;
  };

  if (!binlogDsn) {
    // assuming that the object passed is the connection settings
    this.ctrlConnectionOwner = true;
    this.ctrlConnection = createConnection(dsn);
    binlogDsn = dsn;
  }

  this.connection = createConnection(binlogDsn);
};

ZongJi.prototype._init = function() {
  let binlogOptions = {};

  let ready = () => {
    // Run asynchronously from _init(), as serverId option set in start()
    if (this.options.serverId !== undefined) {
      binlogOptions.serverId = this.options.serverId;
    }

    if (('binlogName' in this.options) && ('binlogNextPos' in this.options)) {
      binlogOptions.filename = this.options.binlogName;
      binlogOptions.position = this.options.binlogNextPos;
    }

    this.BinlogClass = initBinlogClass(this, binlogOptions);
    this.ready = true;
    this._executeCtrlCallbacks();
  };

  let testChecksum = new Promise((resolve, reject) => {
    this._isChecksumEnabled((err, checksumEnabled) => {
      if (err) {
        reject(err);
      }
      else {
        this.useChecksum = checksumEnabled;
        binlogOptions.useChecksum = checksumEnabled;
        resolve();
      }
    });
  });

  let findBinlogEnd = new Promise((resolve, reject) => {
    this._findBinlogEnd((err, result) => {
      if (err) {
        return reject(err);
      }

      if (result && this.options.startAtEnd) {
        binlogOptions.filename = result.Log_name;
        binlogOptions.position = result.File_size;
      }

      resolve();
    });
  });

  Promise.all([testChecksum, findBinlogEnd])
    .then(ready)
    .catch(err => {
      this.emit('error', err);
    });
};

ZongJi.prototype._isChecksumEnabled = function(next) {
  const SelectChecksumParamSql = 'select @@GLOBAL.binlog_checksum as checksum';
  const SetChecksumSql = 'set @master_binlog_checksum=@@global.binlog_checksum';

  let query = (conn, sql) => {
    return new Promise(
      (resolve, reject) => {
        conn.query(sql, (err, result) => {
          if (err) {
            reject(err);
          }
          else {
            resolve(result);
          }
        });
      }
    );
  };

  let checksumEnabled = true;

  query(this.ctrlConnection, SelectChecksumParamSql)
    .then(rows => {
      if (rows[0].checksum === 'NONE') {
        checksumEnabled = false;
      }

      if (checksumEnabled) {
        return query(this.connection, SetChecksumSql);
      }
    })
    .catch(err => {
      if (err.toString().match(/ER_UNKNOWN_SYSTEM_VARIABLE/)) {
        checksumEnabled = false;
        // a simple query to open this.connection
        return query(this.connection, 'SELECT 1');
      }
      else {
        next(err);
      }
    })
    .then(() => {
      next(null, checksumEnabled);
    });
};

ZongJi.prototype._findBinlogEnd = function(next) {
  this.ctrlConnection.query('SHOW BINARY LOGS', function(err, rows) {
    if (err) {
      // Errors should be emitted
      next(err);
    }
    else {
      next(null, rows.length > 0 ? rows[rows.length - 1] : null);
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

ZongJi.prototype._fetchTableInfo = function(tableMapEvent, next) {
  var self = this;
  var sql = util.format(TableInfoQueryTemplate,
    tableMapEvent.schemaName, tableMapEvent.tableName);

  this.ctrlConnection.query(sql, function(err, rows) {
    if (err) {
      // Errors should be emitted
      self.emit('error', err);
      // This is a fatal error, no additional binlog events will be
      // processed since next() will never be called
      return;
    }

    if (rows.length === 0) {
      self.emit('error', new Error(
        'Insufficient permissions to access: ' +
        tableMapEvent.schemaName + '.' + tableMapEvent.tableName));
      // This is a fatal error, no additional binlog events will be
      // processed since next() will never be called
      return;
    }

    self.tableMap[tableMapEvent.tableId] = {
      columnSchemas: rows,
      parentSchema: tableMapEvent.schemaName,
      tableName: tableMapEvent.tableName
    };

    next();
  });
};

ZongJi.prototype.set = function(options) {
  this.options = options || {};
};

ZongJi.prototype.start = function(options) {
  var self = this;
  self.set(options);

  var _start = function() {
    self.connection._protocol._enqueue(new self.BinlogClass(function(error, event) {
      if (error) return self.emit('error', error);
      // Do not emit events that have been filtered out
      if (event === undefined || event._filtered === true) return;

      switch (event.getTypeName()) {
        case 'TableMap':
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
          break;
        case 'Rotate':
          if (self.binlogName !== event.binlogName) {
            self.binlogName = event.binlogName;
          }
          break;
      }
      self.binlogNextPos = event.nextPosition;
      self.emit('binlog', event);
    }));
  };

  if (this.ready) {
    _start();
  }
  else {
    this.ctrlCallbacks.push(_start);
  }
};

ZongJi.prototype.stop = function() {
  var self = this;
  // Binary log connection does not end with destroy()
  self.connection.destroy();
  self.ctrlConnection.query(
    'KILL ' + self.connection.threadId,
    function() {
      if (self.ctrlConnectionOwner)
        self.ctrlConnection.destroy();
    }
  );
};

ZongJi.prototype._skipEvent = function(eventName) {
  var include = this.options.includeEvents;
  var exclude = this.options.excludeEvents;
  return !(
   (include === undefined ||
    (include instanceof Array && include.indexOf(eventName) !== -1)) &&
   (exclude === undefined ||
    (exclude instanceof Array && exclude.indexOf(eventName) === -1)));
};

ZongJi.prototype._skipSchema = function(database, table) {
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
