var SETTINGS = require('./settings/mysql');
var querySequence = require('./helpers/querySequence');
var expectEvents = require('./helpers/expectEvents');

var ZongJi = require('./../');
var mysql = require('mysql');

var eventLog = [];
var zongji, db, esc, escId;


module.exports = {
  setUp: function(done){
    db = mysql.createConnection(SETTINGS.connection);
    esc = db.escape.bind(db);
    escId = db.escapeId;

    // Perform initialization queries sequentially
    querySequence(db, [
      'DROP DATABASE IF EXISTS ' + escId(SETTINGS.database),
      'CREATE DATABASE ' + escId(SETTINGS.database),
      'USE ' + escId(SETTINGS.database),
      'RESET MASTER',
    ], function(){
      zongji = new ZongJi(SETTINGS.connection);

      zongji.on('binlog', function(evt) {
        eventLog.push(evt);
      });

      zongji.start({
        filter: ['tablemap', 'writerows', 'updaterows', 'deleterows']
      });

      done();
    });
  },
  tearDown: function(done){
    done();
  },
  testTypeSet: function(test){
    var testTable = 'type_set';
    querySequence(db, [
      'DROP TABLE IF EXISTS ' + escId(testTable),
      'CREATE TABLE ' + escId(testTable) + ' (col SET(' +
         '"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", ' +
         '"n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"));',
      'INSERT INTO ' + escId(testTable) + ' (col) VALUES ' +
        '("a,d"), ("d,a,b"), ("a,d,i,z"), ("a,j,d"), ("d,a,p")'
    ], function(){
      expectEvents(test, eventLog.splice(0, eventLog.length), [
        {
          _type: 'TableMap',
          tableName: testTable,
          schemaName: SETTINGS.database
        },
        {
          _type: 'WriteRows',
          _custom: function(test, event){
            var tableDetails = event.tableMap[event.tableId]; 
            test.strictEqual(tableDetails.parentSchema, SETTINGS.database);
            test.strictEqual(tableDetails.tableName, testTable);
          },
          rows: [
            { col: [ 'a', 'd' ] },
            { col: [ 'a', 'b', 'd' ] },
            { col: [ 'a', 'd', 'i', 'z' ] },
            { col: [ 'a', 'd', 'j' ] },
            { col: [ 'a', 'd', 'p' ] }
          ]
        }
      ]);
      test.done();
    });
  }
}
