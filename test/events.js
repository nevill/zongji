var mysql = require('mysql');
var settings = require('./settings/mysql');
var connector =  require('./helpers/connector');
var querySequence = require('./helpers/querySequence');
var expectEvents = require('./helpers/expectEvents');
var ZongJi = require('./../');

var conn = process.testZongJi || {};

var checkTableMatches = function(tableName) {
  return function(test, event) {
    var tableDetails = event.tableMap[event.tableId];
    test.strictEqual(tableDetails.parentSchema, settings.database);
    test.strictEqual(tableDetails.tableName, tableName);
  };
};

// For use with expectEvents()
var tableMapEvent = function(tableName) {
  return {
    _type: 'TableMap',
    tableName: tableName,
    schemaName: settings.database
  };
};

module.exports = {
  setUp: function(done) {
    if (!conn.db) {
      process.testZongJi = connector.call(conn, settings, done);
    } else {
      conn.incCount();
      done();
    }
  },
  tearDown: function(done) {
    if (conn) {
      conn.eventLog.splice(0, conn.eventLog.length);
      conn.errorLog.splice(0, conn.errorLog.length);
      conn.closeIfInactive(1000);
    }
    done();
  },
  testStartAtEnd: function(test) {
    var testTable = 'start_at_end_test';
    querySequence(conn.db, [
      'FLUSH LOGS', // Ensure Zongji perserveres through a rotation event
      'DROP TABLE IF EXISTS ' + conn.escId(testTable),
      'CREATE TABLE ' + conn.escId(testTable) + ' (col INT UNSIGNED)',
      'INSERT INTO ' + conn.escId(testTable) + ' (col) VALUES (10)',
    ], function(error) {
      if (error) console.error(error);
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
      setTimeout(function() {
        querySequence(conn.db, [
          'INSERT INTO ' + conn.escId(testTable) + ' (col) VALUES (10)',
        ], function(error) {
          if (error) console.error(error);
          // Should only have 2 events since ZongJi start
          expectEvents(test, events, [
            { /* do not bother testing anything on first event */ },
            { rows: [ { col: 10 } ] }
          ], 1, function() {
            zongji.stop();
            test.done();
          });
        });
      }, 200);

    });
  },
  testPassedConnectionObj: function(test) {
    var testTable = 'conn_obj_test';
    var connObjs = [
      { create: mysql.createConnection, end: function(obj) { obj.destroy(); } },
      { create: mysql.createPool, end: function(obj) { obj.end(); } }
    ];
    querySequence(conn.db, [
      'DROP TABLE IF EXISTS ' + conn.escId(testTable),
      'CREATE TABLE ' + conn.escId(testTable) + ' (col INT UNSIGNED)',
      'INSERT INTO ' + conn.escId(testTable) + ' (col) VALUES (10)',
    ], function(error) {
      if (error) console.error(error);
      // Start second ZongJi instance
      connObjs.forEach(function(connObj, index) {
        var ctrlConn = connObj.create(settings.connection);
        var zongji = new ZongJi(ctrlConn);
        var events = [];

        zongji.on('binlog', function(event) {
          events.push(event);
        });

        zongji.start({
          startAtEnd: true,
          serverId: 12 + index, // Second instance must not use same server ID
          includeEvents: ['tablemap', 'writerows']
        });

        connObj.zongji = zongji;
        connObj.events = events;
      });

      // Give enough time to initialize
      setTimeout(function() {
        querySequence(conn.db, [
          'INSERT INTO ' + conn.escId(testTable) + ' (col) VALUES (10)',
        ], function(error) {
          if (error) console.error(error);
          // Should only have 2 events since ZongJi start
          var finishedCount = 0;
          connObjs.forEach(function(connObj) {
            expectEvents(test, connObj.events, [
              { /* do not bother testing anything on first event */ },
              { rows: [ { col: 10 } ] }
            ], 1, function() {
              connObj.zongji.stop();
              // When passing connection object, connection doesn't end on stop
              connObj.end(connObj.zongji.ctrlConnection);
              if (++finishedCount === connObjs.length - 1) test.done();
            });
          });
        });
      }, 200);

    });
  },
  testWriteUpdateDelete: function(test) {
    var testTable = 'events_test';
    querySequence(conn.db, [
      'DROP TABLE IF EXISTS ' + conn.escId(testTable),
      'CREATE TABLE ' + conn.escId(testTable) + ' (col INT UNSIGNED)',
      'INSERT INTO ' + conn.escId(testTable) + ' (col) VALUES (10)',
      'UPDATE ' + conn.escId(testTable) + ' SET col = 15',
      'DELETE FROM ' + conn.escId(testTable)
    ], function(error) {
      if (error) console.error(error);
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
      ], 1, function() {
        test.equal(conn.errorLog.length, 0);
        test.done();
      });
    });
  },
  testManyColumns: function(test) {
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
    ], function(error, results) {
      if (error) console.error(error);
      expectEvents(test, conn.eventLog, [
        { /* do not bother testing anything on first event */ },
        { rows: results[results.length - 1] }
      ], 1, test.done);
    });
  },
  testIntvar: function(test) {
    var testTable = 'intvar_test';
    querySequence(conn.db, [
      'DROP TABLE IF EXISTS ' + conn.escId(testTable),
      'CREATE TABLE ' + conn.escId(testTable) + ' (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY , col INT)',
    ], function(error) {
      if (error) console.error(error);
      // Start second ZongJi instance
      var zongji = new ZongJi(settings.connection);
      var events = [];

      zongji.on('binlog', function(event) {
        if (event.getTypeName() === 'Query' && event.query === 'BEGIN')
          return;
        events.push(event);
      });

      zongji.start({
        startAtEnd: true,
        serverId: 12, // Second instance must not use same server ID
        includeEvents: ['intvar', 'query']
      });

      // Give enough time to initialize
      setTimeout(function() {
        querySequence(conn.db, [
          'SET SESSION binlog_format=STATEMENT',
          'INSERT INTO ' + conn.escId(testTable) + ' (col) VALUES (10)',
          'INSERT INTO ' + conn.escId(testTable) + ' (col) VALUES (11)',
          'INSERT INTO ' + conn.escId(testTable) + ' (id, col) VALUES (100, LAST_INSERT_ID())',
          // Other tests expect row-based replication, so reset here
          'SET SESSION binlog_format=ROW',
        ], function(error) {
          if (error) console.error(error);
          expectEvents(test, events, [
            { _type: 'IntVar', type: 2, value: 1 },
            { _type: 'Query' },
            { _type: 'IntVar', type: 2, value: 2 },
            { _type: 'Query' },
            { _type: 'IntVar', type: 1, value: 2 },
            { _type: 'Query' },
          ], 1, function() {
            zongji.stop();
            test.done();
          });
        });
      }, 200);
    });
  },
};

