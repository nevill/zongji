var settings = require('./settings/mysql');
var connector =  require('./helpers/connector');
var querySequence = require('./helpers/querySequence');
var expectEvents = require('./helpers/expectEvents');

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

module.exports = {
  setUp: function(done){
    if(!conn.db) process.testZongJi = connector.call(conn, settings, done);
    else done();
  },
  tearDown: function(done){
    conn && conn.eventLog.splice(0, conn.eventLog.length);
    done();
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
      test.done();
    });
  }
};

