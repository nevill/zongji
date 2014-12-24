var ZongJi = require('./../../');
var mysql = require('mysql');
var querySequence = require('./querySequence');

module.exports = function(settings, callback){
  var self = this;
  var db =       self.db =       mysql.createConnection(settings.connection);
  var esc =      self.esc =      db.escape.bind(db);
  var escId =    self.escId =    db.escapeId;
  var eventLog = self.eventLog = [];

  // Perform initialization queries sequentially
  querySequence(db, [
    'DROP DATABASE IF EXISTS ' + escId(settings.database),
    'CREATE DATABASE ' + escId(settings.database),
    'USE ' + escId(settings.database),
    'RESET MASTER',
  ], function(){
    zongji = new ZongJi(settings.connection);

    zongji.on('binlog', function(evt) {
      eventLog.push(evt);
    });

    zongji.start({
      filter: ['tablemap', 'writerows', 'updaterows', 'deleterows']
    });

    callback();
  });

  return self;
}
