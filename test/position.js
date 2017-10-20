var ZongJi = require('./../');
var getEventClass = require('./../lib/code_map').getEventClass;
var settings = require('./settings/mysql');
var connector = require('./helpers/connector');
var querySequence = require('./helpers/querySequence');
var util = require('util')

var conn = process.testZongJi || {};

module.exports = {
  setUp: function (done) {
    if (!conn.db) {
      process.testZongJi = connector.call(conn, settings, done);
    } else {
      conn.incCount();
      done();
    }
  },
  tearDown: function (done) {
    if (conn) {
      conn.eventLog.splice(0, conn.eventLog.length);
      conn.errorLog.splice(0, conn.errorLog.length);
      conn.closeIfInactive(1000);
    }
    done();
  },
  position: function (test) {
    var TEST_TABLE = 'binlog_position';
    var events = [];

    var zongji = new ZongJi(settings.connection);

    var positions = []
    zongji.on('binlog', function(event){
      positions.push({
        filename: zongji.binlogName,
        position: zongji.binlogNextPos
      })
    });
    zongji.start({
      startAtEnd: true,
      serverId: 10, // Second instance must not use same server ID
      subscribePosition: true,
      includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows', 'rotate']
    });

    setTimeout(function() {
      querySequence(conn.db, [
        'FLUSH LOGS',
        'DROP TABLE IF EXISTS ' + conn.escId(TEST_TABLE),
        'CREATE TABLE ' + conn.escId(TEST_TABLE) + ' (col INT UNSIGNED)',
        'INSERT INTO ' + conn.escId(TEST_TABLE) + ' (col) VALUES (1)',
        'UPDATE ' + conn.escId(TEST_TABLE) + ' SET col=2',
        'DELETE FROM ' + conn.escId(TEST_TABLE),
        'FLUSH LOGS',
      ], function (error, results) {
        if (error) {
          console.error(error);
          zongji.stop();
          test.done();
          return;
        }
        // newer position should bigger than previous position
        var lastPos;
        for(var n=0; n<positions.length; n++){
          var item = positions[n];
          if(lastPos){
            if(item.filename == lastPos.filename){
              test.ok(item.position >= lastPos.position, util.format('same filename, position should increase, but from %s to %s', lastPos.position, item.position));
            }else{
              test.ok(item.filename > lastPos.filename, util.format('filename should increase, but from %s to %s', lastPos.filename, item.filename));
            }
          }
          lastPos = item;
        }
        zongji.stop();
        test.done();
      })
    }, 1000);
  },
  subscribe_position: function (test) {
    var TEST_TABLE = 'binlog_position';
    var events = [];

    var zongji = new ZongJi(settings.connection);

    var positions = []
    zongji.on('binlog_position', function(pos){
      positions.push(pos)
    });
    zongji.start({
      startAtEnd: true,
      serverId: 11, // Second instance must not use same server ID
      subscribePosition: true,
      includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows', 'rotate']
    });

    setTimeout(function() {
      querySequence(conn.db, [
        'FLUSH LOGS',
        'DROP TABLE IF EXISTS ' + conn.escId(TEST_TABLE),
        'CREATE TABLE ' + conn.escId(TEST_TABLE) + ' (col INT UNSIGNED)',
        'INSERT INTO ' + conn.escId(TEST_TABLE) + ' (col) VALUES (1)',
        'UPDATE ' + conn.escId(TEST_TABLE) + ' SET col=2',
        'DELETE FROM ' + conn.escId(TEST_TABLE),
        'FLUSH LOGS',
      ], function (error, results) {
        if (error) {
          console.error(error);
          zongji.stop();
          test.done();
          return;
        }
        // newer position should bigger than previous position
        var lastPos;
        for(var n=0; n<positions.length; n++){
          var item = positions[n];
          if(lastPos){
            if(item.filename == lastPos.filename){
              test.ok(item.position > lastPos.position, util.format('same filename, position should increase, but from %s to %s', lastPos.position, item.position));
            }else{
              test.ok(item.filename > lastPos.filename, util.format('filename should increase, but from %s to %s', lastPos.filename, item.filename));
            }
          }
          lastPos = item;
        }
        zongji.stop();
        test.done();
      })
    }, 1000);
  }
}
