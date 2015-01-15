var ZongJi = require('./../../');
var mysql = require('mysql');
var querySequence = require('./querySequence');

module.exports = function(settings, callback){
  var self = this;
  var db =       self.db =       mysql.createConnection(settings.connection);
  var esc =      self.esc =      db.escape.bind(db);
  var escId =    self.escId =    db.escapeId;
  var eventLog = self.eventLog = [];
  var errorLog = self.errorLog = [];

  self.dbName = settings.database;
  self.testCount = 0;

  // Perform initialization queries sequentially
  querySequence(db, [
    'DROP DATABASE IF EXISTS ' + escId(settings.database),
    'CREATE DATABASE ' + escId(settings.database),
    'USE ' + escId(settings.database),
    'RESET MASTER',
    'SELECT VERSION() AS version'
  ], function(results){
    
    self.mysqlVersion = results[results.length - 1][0].version
      .split('-')[0]
      .split('.')
      .map(function(part){
        return parseInt(part, 10);
      });

    var zongji = self.zongji = new ZongJi(settings.connection);

    zongji.on('binlog', function(event) {
      eventLog.push(event);
    });

    zongji.on('error', function(error) {
      errorLog.push(error);
    });

    zongji.start({
      includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows']
    });

    callback();
  });

  // Extra methods on connector object
  self.incCount = function(){
    self.testCount++;
  };

  self.closeIfInactive = function(interval){
    var startCount = self.testCount;
    setTimeout(function(){
      if(startCount === self.testCount){
        self.zongji.stop();
        self.db.destroy();
      }
    }, interval);
  };

  return self;
}
