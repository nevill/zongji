var settings = require('./settings/mysql');
var connector =  require('./helpers/connector');
var querySequence = require('./helpers/querySequence');
var expectEvents = require('./helpers/expectEvents');

var conn = process.testZongJi || {};

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
  }
};

// @param {string} name - unique identifier of this test [a-zA-Z0-9]
// @param {[string]} fields - MySQL field description e.g. `BIGINT NULL`
// @param {[[any]]} testRows - 2D array of rows and fields to insert and test
// @param {func} customTest - optional, instead of exact row check
// @param {string} minVersion - optional, e.g. '5.6.4'
var defineTypeTest = function(name, fields, testRows, customTest, minVersion){
  // Allow skipping customTest argument and passing minVersion in its place
  if(typeof customTest === 'string'){
    minVersion = customTest;
    customTest = undefined;
  }

  module.exports[name] = function(test){
    var testTable = 'type_' + name;
    var fieldText = fields.map(function(field, index){
      return 'col' + index + ' ' + field;
    }).join(', ');
    var insertColumns = fields.map(function(field, index){
      return 'col' + index;
    }).join(', ');
    var insertRows = testRows.map(function(row){
      return '(' + row.map(function(field){
        return field === null ? 'null' : field;
      }).join(', ') + ')';
    }).join(', ');

    if(!minVersion || checkVersion(minVersion, conn.mysqlVersion)){
      querySequence(conn.db, [
        'DROP TABLE IF EXISTS ' + conn.escId(testTable),
        'CREATE TABLE ' + conn.escId(testTable) + ' (' + fieldText + ')',
        'INSERT INTO ' + conn.escId(testTable) +
          ' (' + insertColumns + ') VALUES ' + insertRows,
        'SELECT * FROM ' + conn.escId(testTable)
      ], function(results){
        var selectResult = results[results.length - 1];
        var expectedWrite = {
          _type: 'WriteRows',
          _checkTableMap: function(test, event){
            var tableDetails = event.tableMap[event.tableId];
            test.strictEqual(tableDetails.parentSchema, settings.database);
            test.strictEqual(tableDetails.tableName, testTable);
          }
        };

        if(customTest){
          expectedWrite._custom = customTest.bind(selectResult);
        }else{
          expectedWrite.rows = selectResult.map(function(row){
            for(var field in row){
              if(row.hasOwnProperty(field) &&
                  row[field] instanceof Buffer &&
                  name === 'blob'){
                // Special case where blobs return as String instead of Buffer
                row[field] = row[field].toString();
              }
            }
            return row;
          });
        };

        expectEvents(test, conn.eventLog, [
          {
            _type: 'TableMap',
            tableName: testTable,
            schemaName: settings.database
          },
          expectedWrite
        ]);

        test.equal(conn.errorLog.length, 0);
        conn.errorLog.length &&
          console.log('Type Test Error: ', name, conn.errorLog);
        if(conn.errorLog.length){
          throw conn.errorLog[0];
        }

        test.done();
      });
    }else{
      // Skip running test when version doesn't meet minVersion
      test.done();
    }
  }
};

var checkVersion = function(check, actual){
  var parts = check.split('.').map(function(part){
    return parseInt(part, 10);
  });
  for(var i = 0; i < parts.length; i++){
    if(actual[i] > parts[i]) return true;
    else if(actual[i] < parts[i]) return false;
  }
};

// Begin test case definitions

defineTypeTest('set', [
  'SET(' +
    '"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", ' +
    '"n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", ' +
    '"a2", "b2", "c2", "d2", "e2", "f2", "g2", "h2", "i2", "j2", "k2", ' +
    '"l2", "m2", "n2", "o2", "p2", "q2", "r2", "s2", "t2", "u2", "v2", ' +
    '"w2", "x2", "y2", "z2")',
  'SET("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m")'
], [
  ['"a,d"', '"a,d"'],
  ['"d,a,b"', '"d,a,b"'],
  ['"a,d,i,z2"', '"a,d,i,k,l,m,c"'],
  ['"a,j,d"', '"a,j,d"'],
  ['"d,a,p"', '"d,a,m"'],
  ['""', '""'],
  [null, null]
]);

defineTypeTest('bit', [
  'BIT(64) NULL',
  'BIT(32) NULL',
], [
  ["b'111'", "b'111'"],
  ["b'100000'", "b'100000'"],
  [
    // 64th position
    "b'1000000000000000000000000000000000000000000000000000000000000000'",
    // 32nd position
    "b'10000000000000000000000000000000'"
  ],
  [null, null]
]);

defineTypeTest('enum', [
  'ENUM(' +
    '"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", ' +
    '"n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", ' +
    '"a2", "b2", "c2", "d2", "e2", "f2", "g2", "h2", "i2", "j2", "k2", ' +
    '"l2", "m2", "n2", "o2", "p2", "q2", "r2", "s2", "t2", "u2", "v2", ' +
    '"w2", "x2", "y2", "z2")',
  'ENUM("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m")'
], [
  ['"a"', '"b"'],
  ['"z2"', '"l"'],
  [null, null]
]);

defineTypeTest('int_signed', [
  'INT SIGNED NULL',
  'BIGINT SIGNED NULL',
  'TINYINT SIGNED NULL',
  'SMALLINT SIGNED NULL',
  'MEDIUMINT SIGNED NULL'
], [
  [2147483647, 9007199254740992, 127, 32767, 8388607],
  [-2147483648, -9007199254740992, -128, -32768, -8388608],
  [-2147483645, -9007199254740990, -126, -32766, -8388606],
  [-1, -1, -1, -1, -1],
  [123456, 100, 96, 300, 1000],
  [-123456, -100, -96, -300, -1000]
]);

defineTypeTest('int_unsigned', [
  'INT UNSIGNED NULL',
  'BIGINT UNSIGNED NULL',
  'TINYINT UNSIGNED NULL',
  'SMALLINT UNSIGNED NULL',
  'MEDIUMINT UNSIGNED NULL'
], [
  [4294967295, 9007199254740992, 255, 65535, 16777215],
  [1, 1, 1, 1, 1],
  [1, 8589934591, 1, 1, 1],
  [123456, 100, 96, 300, 1000]
]);

defineTypeTest('double', [
  'DOUBLE NULL'
], [
  [1.0], [-1.0], [123.456], [-13.47], [0.00005], [-0.00005],
  [8589934592.123], [-8589934592.123], [null]
]);

defineTypeTest('float', [
  'FLOAT NULL'
], [
  [1.0], [-1.0], [123.456], [-13.47], [3999.12]
], function(test, event){
  // Ensure sum of differences is very low
  var diff = event.rows.reduce(function(prev, cur, index){
    return prev + Math.abs(cur.col0 - this[index].col0);
  }.bind(this), 0);
  test.ok(diff < 0.001);
});

defineTypeTest('decimal', [
  'DECIMAL(30, 10) NULL'
], [
  [1.0], [-1.0], [123.456], [-13.47],
  [123456789.123], [-123456789.123], [null]
]);

defineTypeTest('blob', [
  'BLOB NULL',
  'TINYBLOB NULL',
  'MEDIUMBLOB NULL',
  'LONGBLOB NULL'
], [
  ['"something here"', '"tiny"', '"medium"', '"long"'],
  ['"nothing there"', '"small"', '"average"', '"huge"'],
  [null, null, null, null]
]);

defineTypeTest('geometry', [
  'GEOMETRY',
], [
  ['GeomFromText("POINT(1 1)")'],
  ['GeomFromText("POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7, 5 5))")']
]);

defineTypeTest('time_no_fraction', [
  'TIME NULL'
], [
  ['"-00:00:01"'],
  ['"00:00:00"'],
  ['"00:07:00"'],
  ['"20:00:00"'],
  ['"19:00:00"'],
  ['"04:00:00"'],
  ['"-838:59:59"'],
  ['"838:59:59"'],
  ['"01:07:08"'],
  ['"01:27:28"'],
  ['"-01:07:08"'],
  ['"-01:27:28"'],
]);

defineTypeTest('time_fraction', [
  'TIME(0) NULL',
  'TIME(1) NULL',
  'TIME(3) NULL',
  'TIME(6) NULL'
], [
  ['"-00:00:01"', '"-00:00:01.1"', '"-00:00:01.002"', '"-00:00:01.123456"'],
  ['"00:00:00"',  '"00:00:00.2"',  '"00:00:00.123"',  '"-00:00:00.000001"'],
  ['"00:07:00"',  '"00:07:00.3"',  '"00:07:00.654"',  '"00:07:00.010203"'],
  ['"20:00:00"',  '"20:00:00.4"',  '"20:00:00.090"',  '"20:00:00.987654"'],
  ['"19:00:00"',  '"19:00:00.5"',  '"19:00:00.999"',  '"19:00:00.000001"'],
  ['"04:00:00"',  '"04:00:00.0"',  '"04:00:00.01"',  '"04:00:00.1"'],
], '5.6.4');

defineTypeTest('datetime_no_fraction', [
  'DATETIME NULL'
], [
  ['"1000-01-01 00:00:00"'],
  ['"9999-12-31 23:59:59"'],
  ['"2014-12-27 01:07:08"']
]);

defineTypeTest('datetime_fraction', [
  'DATETIME(0) NULL',
  'DATETIME(1) NULL',
  'DATETIME(4) NULL',
  'DATETIME(6) NULL'
], [
  ['"1000-01-01 00:00:00"', '"1000-01-01 00:00:00.5"',
   '"1000-01-01 00:00:00.9999"',  '"1000-01-01 00:00:00.123456"'],
  ['"9999-12-31 23:59:59"', '"9999-12-31 23:59:59.9"',
   '"9999-12-31 23:59:59.6543"',  '"9999-12-31 23:59:59.000001"'],
  ['"9999-12-31 23:59:59"', '"9999-12-31 23:59:59.1"',
   '"9999-12-31 23:59:59.1234"',  '"9999-12-31 23:59:59.4326"'  ],
  ['"2014-12-27 01:07:08"', '"2014-12-27 01:07:08.0"',
   '"2014-12-27 01:07:08.0001"',  '"2014-12-27 01:07:08.05"'    ]
], '5.6.4');

defineTypeTest('timestamp_fractional', [
  'TIMESTAMP(3) NULL',
], [
  ['"1970-01-01 00:00:01.123"'],
  ['"2038-01-18 03:14:07.900"'],
  ['"2014-12-27 01:07:08.001"'],
], '5.6.4');

defineTypeTest('temporal_other', [
  'DATE NULL',
  'TIMESTAMP NULL',
  'YEAR NULL'
], [
  ['"1000-01-01"', '"1970-01-01 00:00:01"', 1901],
  ['"9999-12-31"', '"2038-01-18 03:14:07"', 2155],
  ['"2014-12-27"', '"2014-12-27 01:07:08"', 2014]
]);

defineTypeTest('string', [
  'VARCHAR(250) NULL',
  'CHAR(20) NULL',
  'BINARY(3) NULL',
  'VARBINARY(10) NULL'
], [
  ['"something here"', '"tiny"', '"a"', '"binary"'],
  ['"nothing there"', '"small"', '"b"', '"test123"'],
  [null, null, null, null]
]);

defineTypeTest('text', [
  'TINYTEXT NULL',
  'MEDIUMTEXT NULL',
  'LONGTEXT NULL',
  'TEXT NULL'
], [
  ['"something here"', '"tiny"', '"a"', '"binary"'],
  ['"nothing there"', '"small"', '"b"', '"test123"'],
  [null, null, null, null]
]);

