const ZongJi = require('./../../');
const mysql = require('mysql');
const querySequence = require('./querySequence');

module.exports = function(settings, callback) {
  const db =       this.db =       mysql.createConnection(settings.connection);
  const escId =    this.escId =    db.escapeId;
  const eventLog = this.eventLog = [];
  const errorLog = this.errorLog = [];

  this.dbName = settings.database;
  this.testCount = 0;

  // Perform initialization queries sequentially
  querySequence(db, [
    'SET GLOBAL sql_mode = \'' + settings.sessionSqlMode + '\'',
    'DROP DATABASE IF EXISTS ' + escId(settings.database),
    'CREATE DATABASE ' + escId(settings.database),
    'USE ' + escId(settings.database),
    'RESET MASTER',
    'SELECT VERSION() AS version'
  ], (error, results) => {
    if (error) console.error(error);

    this.mysqlVersion = results[results.length - 1][0].version
      .split('-')[0]
      .split('.')
      .map(function(part) {
        return parseInt(part, 10);
      });

    const zongji = this.zongji = new ZongJi(settings.connection);

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
  this.incCount = function() {
    this.testCount++;
  };

  this.closeIfInactive = function(interval) {
    const startCount = this.testCount;
    setTimeout(function() {
      if (startCount === this.testCount) {
        this.zongji.stop();
        this.db.destroy();
      }
    }, interval);
  };

  return this;
};
