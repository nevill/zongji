var mysql = require('mysql');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var generateBinlog = require('./lib/sequence/binlog');

function ZongJi(dsn, options) {
  this.set(options);

  EventEmitter.call(this);

  // to send table info query
  this.dsn = dsn;

  var ctrlDsn = cloneObjectSimple(dsn);
  ctrlDsn.database = 'information_schema';
  this.ctrlDsn = ctrlDsn;

//  this.ctrlConnection = mysql.createConnection(ctrlDsn);
//  this.ctrlConnection.connect();
  this.interval = null;
  this.interval2 = null;
  this.binlogName = null;
  this.nextPosition = 0;
  this.listenerAdded = false;
  this._handleDisconnect();

  this.ctrlCallbacks = [];

//  this.connection = mysql.createConnection(dsn);

  this.tableMap = {};
  this.ready = false;
  this.useChecksum = false;

  this._init();
}

var cloneObjectSimple = function(obj){
  var out = {};
  for(var i in obj){
    if(obj.hasOwnProperty(i)){
      out[i] = obj[i];
    }
  }
  return out;
}

util.inherits(ZongJi, EventEmitter);

ZongJi.prototype._handleDisconnect = function() {
  var self = this;
  self.ctrlConnection = mysql.createConnection(self.ctrlDsn);

  self.ctrlConnection.connect(function(err) {
    if(err) {
      setTimeout(function() {
          self._handleDisconnect();
      }, 5000);
    } else {
      if (self.connection && self.connection.state && self.connection.state !== 'disconnected' && self.disconnect()) {
        self.connection.state = 'disconnected';
      }
      self.connection = mysql.createConnection(self.dsn);
      if (self.ready && self.ctrlConnection.state==='authenticated' && self.connection.state==='disconnected') {
       console.log("** Reconnected **");
       self._init();
       self.start(self.options);
      }
      //polling the server to see the connection is active, if error, then the handleDisconnect will kick into action
      self.interval = setInterval(function(){
        self.ctrlConnection.query("SELECT 1 AS ctrlConnection", function(err, rows) {
          if (err) throw err;
          //console.log(rows);
        });
      }, 30000);
    }
  });

  self.ctrlConnection.on('error', function(err) {
    clearInterval(self.interval);
    if(err.code === 'PROTOCOL_CONNECTION_LOST' || err.code === 'ECONNREFUSED' || err.code === 'ETIMEDOUT') {
      self._handleDisconnect();
    } else {
      throw err;
    }
  });
};

ZongJi.prototype._handleConnectionDisconnect = function(){
  var self = this;
  if (self.connection.threadId !== null) {
    console.log("self.connection.threadId: "+self.connection.threadId);
  }
  if (self.interval2 === null) {
    console.log("_handleConnectionDisconnect called");
    var triggerTimeout = (Date.now()/1000|0)+600;
    self.interval2 = setInterval(function(){
      if (triggerTimeout < Date.now()/1000|0) {
        console.log("Triggered Event TimeOut, disconnecting self.connection");
        self.disconnect()
      }
    }, 10000);
  }
  if (self.listenerAdded === false) {
    self.listenerAdded = true;
    console.log("*******listenerAdded*********");
    self.on('binlog', function(evt){
      if (evt.getEventName() !== 'tablemap') {
        triggerTimeout = (Date.now()/1000|0)+600;
        console.log("triggerTimeout: " +triggerTimeout+', for the event '+evt.getEventName());
      }
    });
  }
};

ZongJi.prototype._init = function() {
  var self = this;
  var binlogOptions = {
    tableMap: self.tableMap,
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
    }
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

ZongJi.prototype._findBinlogEnd = function(next) {
  var self = this;
  self.ctrlConnection.query('SHOW BINARY LOGS', function(err, rows) {
    if(err) throw err;
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
    self._handleConnectionDisconnect();
    self.connection._protocol._enqueue(new self.binlog(function(error, event){
      if(error) return self.emit('error', error);
      // Do not emit events that have been filtered out
      if(event === undefined || event._filtered === true) return;

      //Manage Server Restarts
      //log gets rotated when mysqld restarts, each new binlogfile starts with position 0
      // if currently watching binlogName != new binlogName, then it shows server restarted, this is required, because if the script is running and mysqld restarts, the whole events from last file is returned, we need to skip the events that we already processed, we use nextPosition for this purpose.
      if (event.getTypeName() === 'Rotate') {
       //console.log("Rotate: \n", event);//{ timestamp: 0,nextPosition: 0,size: 24,position: 4,binlogName: 'mysql-bin.000107' }
        if (self.binlogName !== event.binlogName) {
          self.binlogName = event.binlogName;
          self.nextPosition = 0;
        }
      }

      // Skip the processed events
      if (self.nextPosition >= event.nextPosition) {
        return;
      }
      else {
        self.nextPosition = event.nextPosition;
      }

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
    function(error, rows){
      self.ctrlConnection.destroy();
    }
  );
};

ZongJi.prototype.disconnect = function(){
  var self = this;
  // Binary log connection does not end with destroy()
  //self.connection._protocol.quit();
  self.connection.destroy();
  self.ctrlConnection.query(
    'KILL '+self.connection.threadId,
    function(err, rows){
      if (err) throw err;
      console.log('KILLED MySQL self.connection.threadId: ' + self.connection.threadId);
      self.reconnect();
    }
  );
};

ZongJi.prototype.reconnect = function(){
  var self =  this;
  clearInterval(self.interval2);
  self.interval2 = null;
  self.connection = mysql.createConnection(self.dsn);
  self.start(self.options);
  console.log("Started Again");
};

ZongJi.prototype._skipEvent = function(eventName){
  var include = this.options.includeEvents;
  var exclude = this.options.excludeEvents;
  if (include.indexOf('rotate') === -1) {
    include.push('rotate');
  }
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
