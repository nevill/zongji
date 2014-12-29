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

  // Perform initialization queries sequentially
  querySequence(db, [
    'DROP DATABASE IF EXISTS ' + escId(settings.database),
    'CREATE DATABASE ' + escId(settings.database),
    'USE ' + escId(settings.database),
    'RESET MASTER',
  ], function(){
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

  return self;
}
