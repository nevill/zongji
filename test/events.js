var settings = require('./settings/mysql');
var connector =  require('./helpers/connector');
var querySequence = require('./helpers/querySequence');
var expectEvents = require('./helpers/expectEvents');
var ZongJi = require('./../');

var conn = process.testZongJi || {};

var checkTableMatches = function(tableName){
  return function(test, event){
    var tableDetails = event.tableMap[event.tableId];
    test.strictEqual(tableDetails.parentSchema, settings.database);
    test.strictEqual(tableDetails.tableName, tableName);
  };
};

// For use with expectEvents()
var tableMapEvent = function(tableName){
  return {
    _type: 'TableMap',
    tableName: tableName,
    schemaName: settings.database
  };
};

var cloneObjectSimple = function(obj){
  var out = {};
  for(var i in obj){
    if(obj.hasOwnProperty(i)){
      out[i] = obj[i];
    }
  }
  return out;
}

module.exports = {
  setUp: function(done){
    if(!conn.db){
      process.testZongJi = connector.call(conn, settings, done);
    }else{
      conn.incCount();
      done();
    }
  },
  tearDown: function(done){
    if(conn){
      conn.eventLog.splice(0, conn.eventLog.length);
      conn.errorLog.splice(0, conn.errorLog.length);
      conn.closeIfInactive(1000);
    }
    done();
  },
  testStartAtEnd: function(test){
    var testTable = 'start_at_end_test';
    querySequence(conn.db, [
      'FLUSH LOGS', // Ensure Zongji perserveres through a rotation event
      'DROP TABLE IF EXISTS ' + conn.escId(testTable),
      'CREATE TABLE ' + conn.escId(testTable) + ' (col INT UNSIGNED)',
      'INSERT INTO ' + conn.escId(testTable) + ' (col) VALUES (10)',
    ], function(results){
      // Start second ZongJi instance
      var zongji = new ZongJi(settings.connection);
      var events = [];

      zongji.on('binlog', function(event) {
        events.push(event);
      });

      zongji.start({
        startAtEnd: true,
        serverId: 10, // Second instance must not use same server ID
        includeEvents: ['tablemap', 'writerows']
      });

      // Give enough time to initialize
      setTimeout(function(){
        querySequence(conn.db, [
          'INSERT INTO ' + conn.escId(testTable) + ' (col) VALUES (10)',
        ], function(results){
          // Should only have 2 events since ZongJi start
          test.equal(events.length, 2);
          test.equal(events[1].rows[0].col, 10);
          zongji.stop();
          test.done();
        });
      }, 200);

    });
  },
  testWriteUpdateDelete: function(test){
    var testTable = 'events_test';
    querySequence(conn.db, [
      'DROP TABLE IF EXISTS ' + conn.escId(testTable),
      'CREATE TABLE ' + conn.escId(testTable) + ' (col INT UNSIGNED)',
      'INSERT INTO ' + conn.escId(testTable) + ' (col) VALUES (10)',
      'UPDATE ' + conn.escId(testTable) + ' SET col = 15',
      'DELETE FROM ' + conn.escId(testTable)
    ], function(){
      expectEvents(test, conn.eventLog, [
        tableMapEvent(testTable),
        {
          _type: 'WriteRows',
          _checkTableMap: checkTableMatches(testTable),
          rows: [ { col: 10 } ]
        },
        tableMapEvent(testTable),
        {
          _type: 'UpdateRows',
          _checkTableMap: checkTableMatches(testTable),
          rows: [ { before: { col: 10 }, after: { col: 15 } } ]
        },
        tableMapEvent(testTable),
        {
          _type: 'DeleteRows',
          _checkTableMap: checkTableMatches(testTable),
          rows: [ { col: 15 } ]
        }
      ]);
      test.equal(conn.errorLog.length, 0);
      test.done();
    });
  },
  testManyColumns: function(test){
    var testTable = '33_columns';
    querySequence(conn.db, [
      'DROP TABLE IF EXISTS ' + conn.escId(testTable),
      'CREATE TABLE ' + conn.escId(testTable) + ' (' +
        'col1 INT SIGNED NULL, ' +
        'col2 BIGINT SIGNED NULL, ' +
        'col3 TINYINT SIGNED NULL, ' +
        'col4 SMALLINT SIGNED NULL, ' +
        'col5 MEDIUMINT SIGNED NULL, ' +
        'col6 INT SIGNED NULL, ' +
        'col7 BIGINT SIGNED NULL, ' +
        'col8 TINYINT SIGNED NULL, ' +
        'col9 SMALLINT SIGNED NULL, ' +
        'col10 INT SIGNED NULL, ' +
        'col11 BIGINT SIGNED NULL, ' +
        'col12 TINYINT SIGNED NULL, ' +
        'col13 SMALLINT SIGNED NULL, ' +
        'col14 INT SIGNED NULL, ' +
        'col15 BIGINT SIGNED NULL, ' +
        'col16 TINYINT SIGNED NULL, ' +
        'col17 SMALLINT SIGNED NULL, ' +
        'col18 INT SIGNED NULL, ' +
        'col19 BIGINT SIGNED NULL, ' +
        'col20 TINYINT SIGNED NULL, ' +
        'col21 SMALLINT SIGNED NULL, ' +
        'col22 INT SIGNED NULL, ' +
        'col23 BIGINT SIGNED NULL, ' +
        'col24 TINYINT SIGNED NULL, ' +
        'col25 SMALLINT SIGNED NULL, ' +
        'col26 INT SIGNED NULL, ' +
        'col27 BIGINT SIGNED NULL, ' +
        'col28 TINYINT SIGNED NULL, ' +
        'col29 SMALLINT SIGNED NULL, ' +
        'col30 INT SIGNED NULL, ' +
        'col31 BIGINT SIGNED NULL, ' +
        'col32 TINYINT SIGNED NULL, ' +
        'col33 SMALLINT SIGNED NULL)',
      'INSERT INTO ' + conn.escId(testTable) +
        ' (col1, col2, col3, col4, col5, col33) VALUES ' +
          '(2147483647, null, 127, 32767, 8388607, 12), ' +
          '(-2147483648, -9007199254740992, -128, -32768, -8388608, 10), ' +
          '(-2147483645, -9007199254740990, -126, -32766, -8388606, 6), ' +
          '(-1, -1, -1, -1, null, -6), ' +
          '(123456, 100, 96, 300, 1000, null), ' +
          '(-123456, -100, -96, -300, -1000, null)',
       'SELECT * FROM ' + conn.escId(testTable)
    ], function(results){
      test.deepEqual(conn.eventLog[1].rows, results[results.length - 1]);
      test.done();
    });
  },
};

