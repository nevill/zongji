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
    conn && conn.errorLog.splice(0, conn.errorLog.length);
    done();
  },
  testTypeSet: function(test){
    var testTable = 'type_set';
    querySequence(conn.db, [
      'DROP TABLE IF EXISTS ' + conn.escId(testTable),
      'CREATE TABLE ' + conn.escId(testTable) + ' (col SET(' +
         '"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", ' +
         '"n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"));',
      'INSERT INTO ' + conn.escId(testTable) + ' (col) VALUES ' +
        '("a,d"), ("d,a,b"), ("a,d,i,z"), ("a,j,d"), ("d,a,p"), (null)'
    ], function(){
      expectEvents(test, conn.eventLog, [
        tableMapEvent(testTable),
        {
          _type: 'WriteRows',
          _checkTableMap: checkTableMatches(testTable),
          rows: [
            { col: [ 'a', 'd' ] },
            { col: [ 'a', 'b', 'd' ] },
            { col: [ 'a', 'd', 'i', 'z' ] },
            { col: [ 'a', 'd', 'j' ] },
            { col: [ 'a', 'd', 'p' ] },
            { col: null }
          ]
        }
      ]);
      test.equal(conn.errorLog.length, 0);
      test.done();
    });
  },
  testTypeIntSigned: function(test){
    var testTable = 'type_int_signed';
    querySequence(conn.db, [
      'DROP TABLE IF EXISTS ' + conn.escId(testTable),
      'CREATE TABLE ' + conn.escId(testTable) + ' (' +
        'col1 INT SIGNED NULL, ' +
        'col2 BIGINT SIGNED NULL, ' +
        'col3 TINYINT SIGNED NULL, ' +
        'col4 SMALLINT SIGNED NULL, ' +
        'col5 MEDIUMINT SIGNED NULL)',
      'INSERT INTO ' + conn.escId(testTable) +
        ' (col1, col2, col3, col4, col5) VALUES ' +
          '(2147483647, 9007199254740992, 127, 32767, 8388607), ' +
          '(-2147483648, -9007199254740992, -128, -32768, -8388608), ' +
          '(-2147483645, -9007199254740990, -126, -32766, -8388606), ' +
          '(-1, -1, -1, -1, -1), ' +
          '(123456, 100, 96, 300, 1000), ' +
          '(-123456, -100, -96, -300, -1000)'
    ], function(){
      expectEvents(test, conn.eventLog, [
        tableMapEvent(testTable),
        {
          _type: 'WriteRows',
          _checkTableMap: checkTableMatches(testTable),
          rows: [
            { col1: 2147483647,
              col2: 9007199254740992,
              col3: 127,
              col4: 32767,
              col5: 8388607 },
            { col1: -2147483648,
              col2: -9007199254740992,
              col3: -128,
              col4: -32768,
              col5: -8388608 },
            { col1: -2147483645,
              col2: -9007199254740990,
              col3: -126,
              col4: -32766,
              col5: -8388606 },
            { col1: -1,
              col2: -1,
              col3: -1,
              col4: -1,
              col5: -1 },
            { col1: 123456,
              col2: 100,
              col3: 96,
              col4: 300,
              col5: 1000 },
            { col1: -123456,
              col2: -100,
              col3: -96,
              col4: -300,
              col5: -1000 }
          ]
        }
      ]);
      test.equal(conn.errorLog.length, 0);
      test.done();
    });
  },
  testTypeIntUnsigned: function(test){
    var testTable = 'type_int_unsigned';
    querySequence(conn.db, [
      'DROP TABLE IF EXISTS ' + conn.escId(testTable),
      'CREATE TABLE ' + conn.escId(testTable) + ' (' +
        'col1 INT UNSIGNED NULL, ' +
        'col2 BIGINT UNSIGNED NULL, ' +
        'col3 TINYINT UNSIGNED NULL, ' +
        'col4 SMALLINT UNSIGNED NULL, ' +
        'col5 MEDIUMINT UNSIGNED NULL)',
      'INSERT INTO ' + conn.escId(testTable) +
        ' (col1, col2, col3, col4, col5) VALUES ' +
          '(4294967295, 9007199254740992, 255, 65535, 16777215), ' +
          '(1, 1, 1, 1, 1), ' +
          '(1, 8589934591, 1, 1, 1), ' +
          '(123456, 100, 96, 300, 1000)'
    ], function(){
      expectEvents(test, conn.eventLog, [
        tableMapEvent(testTable),
        {
          _type: 'WriteRows',
          _checkTableMap: checkTableMatches(testTable),
          rows: [
            { col1: 4294967295,
              col2: 9007199254740992,
              col3: 255,
              col4: 65535,
              col5: 16777215 },
            { col1: 1,
              col2: 1,
              col3: 1,
              col4: 1,
              col5: 1 },
            { col1: 1,
              col2: 8589934591,
              col3: 1,
              col4: 1,
              col5: 1 },
            { col1: 123456,
              col2: 100,
              col3: 96,
              col4: 300,
              col5: 1000 }
          ]
        }
      ]);
      test.equal(conn.errorLog.length, 0);
      test.done();
    });
  },
  testTypeDouble: function(test){
    var testTable = 'type_double';
    querySequence(conn.db, [
      'DROP TABLE IF EXISTS ' + conn.escId(testTable),
      'CREATE TABLE ' + conn.escId(testTable) + ' (col DOUBLE NULL)',
      'INSERT INTO ' + conn.escId(testTable) + ' (col) VALUES ' +
        '(1.0), (-1.0), (123.456), (-13.47), (0.00005), (-0.00005), ' +
        '(44441231231231231223999.123), (-44441231231231231223999.123), (null)'
    ], function(){
      expectEvents(test, conn.eventLog, [
        tableMapEvent(testTable),
        {
          _type: 'WriteRows',
          _checkTableMap: checkTableMatches(testTable),
          rows: [
            { col: 1 },
            { col: -1 },
            { col: 123.456 },
            { col: -13.47 },
            { col: 0.00005 },
            { col: -0.00005 },
            { col: 44441231231231231223999.123 }, // > 2^32 (not actual value)
            { col: -44441231231231231223999.123 },
            { col: null }
          ]
        }
      ]);
      test.equal(conn.errorLog.length, 0);
      test.done();
    });
  },
  testTypeFloat: function(test){
    var testTable = 'type_float';
    querySequence(conn.db, [
      'DROP TABLE IF EXISTS ' + conn.escId(testTable),
      'CREATE TABLE ' + conn.escId(testTable) + ' (col FLOAT NULL)',
      'INSERT INTO ' + conn.escId(testTable) + ' (col) VALUES ' +
        '(1.0), (-1.0), (123.456), (-13.47), (3999.123)'
    ], function(){
      expectEvents(test, conn.eventLog, [
        tableMapEvent(testTable),
        {
          _type: 'WriteRows',
          _checkTableMap: checkTableMatches(testTable),
          _fuzzy: function(test, event){
            // Ensure sum of differences is very low
            var rowsExp = [ 1, -1, 123.456, -13.47, 3999.123 ];
            var diff = event.rows.reduce(function(prev, cur, index){
              return prev + Math.abs(cur.col - rowsExp[index]);
            }, 0);
            test.ok(diff < 0.0001);
          }
        }
      ]);
      test.equal(conn.errorLog.length, 0);
      test.done();
    });
  },
  testTypeDecimal: function(test){
    var testTable = 'type_decimal';
    querySequence(conn.db, [
      'DROP TABLE IF EXISTS ' + conn.escId(testTable),
      'CREATE TABLE ' + conn.escId(testTable) + ' (col DECIMAL(30, 10) NULL)',
      'INSERT INTO ' + conn.escId(testTable) + ' (col) VALUES ' +
        '(1.0), (-1.0), (123.456), (-13.47),' +
        '(123456789.123), (-123456789.123), (null)'
    ], function(){
      expectEvents(test, conn.eventLog, [
        tableMapEvent(testTable),
        {
          _type: 'WriteRows',
          _checkTableMap: checkTableMatches(testTable),
          rows: [
            { col: 1 },
            { col: -1 },
            { col: 123.456 },
            { col: -13.47 },
            { col: 123456789.123 },
            { col: -123456789.123 },
            { col: null }
          ]
        }
      ]);
      test.equal(conn.errorLog.length, 0);
      test.done();
    });
  },
  testTypeBlob: function(test){
    var testTable = 'type_blob';
    querySequence(conn.db, [
      'DROP TABLE IF EXISTS ' + conn.escId(testTable),
      'CREATE TABLE ' + conn.escId(testTable) + ' (' +
        'col1 BLOB NULL, ' +
        'col2 TINYBLOB NULL, ' +
        'col3 MEDIUMBLOB NULL, ' +
        'col4 LONGBLOB NULL)',
      'INSERT INTO ' + conn.escId(testTable) + ' (col1, col2, col3, col4) VALUES ' +
        '("something here", "tiny", "medium", "long"), ' +
        '("nothing there", "small", "average", "huge"), ' +
        '(null, null, null, null)'
    ], function(){
      expectEvents(test, conn.eventLog, [
        tableMapEvent(testTable),
        {
          _type: 'WriteRows',
          _checkTableMap: checkTableMatches(testTable),
          rows: [
            { col1: 'something here',
              col2: 'tiny',
              col3: 'medium',
              col4: 'long' },
            { col1: 'nothing there',
              col2: 'small',
              col3: 'average',
              col4: 'huge' },
            { col1: null,
              col2: null,
              col3: null,
              col4: null }
          ]
        }
      ]);
      test.equal(conn.errorLog.length, 0);
      test.done();
    });
  }
}
