const tap = require('tap');

const ZongJi = require('../');
const expectEvents = require('./helpers/expectEvents');
const testDb = require('./helpers');
const settings = require('./settings/mysql');

const checkTableMatches = function(tableName) {
  return function(test, event) {
    const tableDetails = event.tableMap[event.tableId];
    test.strictEqual(tableDetails.parentSchema, testDb.SCHEMA_NAME);
    test.strictEqual(tableDetails.tableName, tableName);
  };
};

// For use with expectEvents()
const tableMapEvent = function(tableName) {
  return {
    _type: 'TableMap',
    tableName: tableName,
    schemaName: testDb.SCHEMA_NAME,
  };
};

tap.test('Initialise testing db', test => {
  testDb.init(err => {
    if (err) {
      return test.threw(err);
    }
    test.end();
  });
});

tap.test('Binlog option startAtEnd', test => {
  const TEST_TABLE = 'start_at_end_test';

  test.test(`prepare new table ${TEST_TABLE}`, test => {
    testDb.execute([
      'FLUSH LOGS', // Ensure ZongJi perserveres through a rotation event
      `DROP TABLE IF EXISTS ${TEST_TABLE}`,
      `CREATE TABLE ${TEST_TABLE} (col INT UNSIGNED)`,
      `INSERT INTO ${TEST_TABLE} (col) VALUES (12)`,
    ], err => {
      if (err) {
        return test.fail(err);
      }
      test.end();
    });
  });

  test.test('start', test => {
    const events = [];

    const zongji = new ZongJi(settings.connection);
    test.tearDown(() => zongji.stop());

    zongji.on('binlog', evt => events.push(evt));
    zongji.start({
      startAtEnd: true,
      includeEvents: ['tablemap', 'writerows'],
    });

    zongji.on('ready', () => {
      testDb.execute([
        `INSERT INTO ${TEST_TABLE} (col) VALUES (9)`,
      ], err => {
        if (err) {
          return test.fail(err);
        }

        // Should only have 2 events since ZongJi start
        expectEvents(test, events,
          [
            { /* do not bother testing anything on first event */ },
            { rows: [ { col: 9 } ] }
          ], 1,
          () => test.end()
        );
      });
    });


  });

  test.end();
});

tap.test('Class constructor', test => {
  const TEST_TABLE = 'conn_obj_test';

  test.test(`prepare table ${TEST_TABLE}`, test => {
    testDb.execute([
      `DROP TABLE IF EXISTS ${TEST_TABLE}`,
      `CREATE TABLE ${TEST_TABLE} (col INT UNSIGNED)`,
      `INSERT INTO ${TEST_TABLE} (col) VALUES (10)`,
    ], err => {
      if (err) {
        return test.fail(err);
      }

      test.end();
    });
  });

  function run(test, zongji) {
    test.tearDown(() => zongji.stop());

    const events = [];
    zongji.on('binlog', evt => events.push(evt));
    zongji.start({
      startAtEnd: true,
      serverId: testDb.serverId(),
      includeEvents: ['tablemap', 'writerows'],
    });
    zongji.on('ready', () => {
      let value = Math.round(Math.random() *  100);
      testDb.execute([
          `INSERT INTO ${TEST_TABLE} (col) VALUES (${value})`,
        ], err => {
          if (err) {
            return test.fail(err);
          }
          // Should only have 2 events since ZongJi start

          expectEvents(test, events, [
            { /* do not bother testing anything on first event */ },
            { rows: [ { col: value } ] }
          ], 1, () => test.end());
        });
    });
  }

  const mysql = require('mysql');

  test.test('pass a mysql connection instance', test => {
    const conn = mysql.createConnection(settings.connection);
    const zongji = new ZongJi(conn);
    zongji.on('stopped', () => conn.destroy());
    run(test, zongji);
  });

  test.test('pass a mysql pool', test => {
    const pool = mysql.createConnection(settings.connection);
    const zongji = new ZongJi(pool);
    zongji.on('stopped', () => pool.end());
    run(test, zongji);
  });

  test.end();
});

tap.test('Write events', test => {
  const TEST_TABLE = 'write_events_test';

  test.test(`prepare table ${TEST_TABLE}`, test => {
    testDb.execute([
      `DROP TABLE IF EXISTS ${TEST_TABLE}`,
      `CREATE TABLE ${TEST_TABLE} (col INT UNSIGNED)`,
    ], err => {
      if (err) {
        return test.fail(err);
      }

      test.end();
    });
  });

  test.test('write a record', test => {
    const events = [];
    const zongji = new ZongJi(settings.connection);
    test.tearDown(() => zongji.stop());

    zongji.start({
      startAtEnd: true,
      serverId: testDb.serverId(),
      includeEvents: ['tablemap', 'writerows'],
    });

    zongji.on('ready', () => {
      testDb.execute([
        `INSERT INTO ${TEST_TABLE} (col) VALUES (14)`,
      ], err => {
        if (err) {
          return test.fail(err);
        }
      });
    });

    zongji.on('binlog', evt => {
      events.push(evt);

      if (events.length == 2) {
        expectEvents(test, events,
          [
            tableMapEvent(TEST_TABLE),
            {
              _type: 'WriteRows',
              _checkTableMap: checkTableMatches(TEST_TABLE),
              rows: [ { col: 14 } ],
            }
          ], 1,
          () => test.end()
        );
      }
    });
  });

  test.test('update a record', test => {
    const events = [];
    const zongji = new ZongJi(settings.connection);
    test.tearDown(() => zongji.stop());

    zongji.start({
      startAtEnd: true,
      serverId: testDb.serverId(),
      includeEvents: ['tablemap', 'updaterows'],
    });

    zongji.on('ready', () => {
      testDb.execute([
        `UPDATE ${TEST_TABLE} SET col=15`,
      ], err => {
        if (err) {
          return test.fail(err);
        }
      });
    });

    zongji.on('binlog', evt => {
      events.push(evt);

      if (events.length == 2) {
        expectEvents(test, events,
          [
            tableMapEvent(TEST_TABLE),
            {
              _type: 'UpdateRows',
              _checkTableMap: checkTableMatches(TEST_TABLE),
              rows: [ { before: { col: 14 }, after: { col: 15 } } ],
            }
          ], 1,
          () => test.end()
        );
      }
    });
  });

  test.test('delete a record', test => {
    const events = [];
    const zongji = new ZongJi(settings.connection);
    test.tearDown(() => zongji.stop());

    zongji.start({
      startAtEnd: true,
      serverId: testDb.serverId(),
      includeEvents: ['tablemap', 'deleterows'],
    });

    zongji.on('ready', () => {
      testDb.execute([
        `DELETE FROM ${TEST_TABLE}`,
      ], err => {
        if (err) {
          return test.fail(err);
        }
      });
    });

    zongji.on('binlog', evt => {
      events.push(evt);

      if (events.length == 2) {
        expectEvents(test, events,
          [
            tableMapEvent(TEST_TABLE),
            {
              _type: 'DeleteRows',
              _checkTableMap: checkTableMatches(TEST_TABLE),
              rows: [ { col: 15 } ],
            }
          ], 1,
          () => test.end()
        );
      }
    });
  });

  test.end();
});

tap.test('Intvar / Query event', test => {
  const TEST_TABLE = 'intvar_test';

  test.test(`prepare table ${TEST_TABLE}`, test => {
    testDb.execute([
      `DROP TABLE IF EXISTS ${TEST_TABLE}`,
      `CREATE TABLE ${TEST_TABLE} (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, col INT)`,
    ], err => {
      if (err) {
        return test.fail(err);
      }

      test.end();
    });
  });

  test.test('begin', test => {
    const events = [];
    const zongji = new ZongJi(settings.connection);
    test.tearDown(() => zongji.stop());

    zongji.on('binlog', event => {
      if (event.getTypeName() === 'Query' && event.query === 'BEGIN') {
        return;
      }
      events.push(event);

      if (events.length === 6) {
        expectEvents(test, events, [
            { _type: 'IntVar', type: 2, value: 1 },
            { _type: 'Query' },
            { _type: 'IntVar', type: 2, value: 2 },
            { _type: 'Query' },
            { _type: 'IntVar', type: 1, value: 2 },
            { _type: 'Query' },
          ], 1, () => test.end()
        );
      }
    });

    zongji.start({
      startAtEnd: true,
      serverId: testDb.serverId(),
      includeEvents: ['intvar', 'query'],
    });

    zongji.on('ready', () => {
      testDb.execute([
        'SET SESSION binlog_format=STATEMENT',
        `INSERT INTO ${TEST_TABLE} (col) VALUES (10)`,
        `INSERT INTO ${TEST_TABLE} (col) VALUES (11)`,
        `INSERT INTO ${TEST_TABLE} (id, col) VALUES (100, LAST_INSERT_ID())`,
        // Other tests expect row-based replication, so reset here
        'SET SESSION binlog_format=ROW',
      ], err => {
        if (err) {
          test.fail(err);
        }
      });
    });

  });

  test.end();
});

tap.test('With many columns', test => {
  const TEST_TABLE = '33_columns';
  const events = [];

  const zongji = new ZongJi(settings.connection);

  test.tearDown(() => zongji.stop());
  zongji.on('binlog', evt => events.push(evt));
  zongji.start({
    startAtEnd: true,
    serverId: testDb.serverId(),
    includeEvents: ['tablemap', 'writerows'],
  });

  zongji.on('ready', () => {
    testDb.execute([
      `DROP TABLE IF EXISTS ${TEST_TABLE}`,
      `CREATE TABLE ${TEST_TABLE} (
        col1 INT SIGNED NULL, col2 BIGINT SIGNED NULL,
        col3 TINYINT SIGNED NULL, col4 SMALLINT SIGNED NULL,
        col5 MEDIUMINT SIGNED NULL, col6 INT SIGNED NULL,
        col7 BIGINT SIGNED NULL, col8 TINYINT SIGNED NULL,
        col9 SMALLINT SIGNED NULL, col10 INT SIGNED NULL,
        col11 BIGINT SIGNED NULL, col12 TINYINT SIGNED NULL,
        col13 SMALLINT SIGNED NULL, col14 INT SIGNED NULL,
        col15 BIGINT SIGNED NULL, col16 TINYINT SIGNED NULL,
        col17 SMALLINT SIGNED NULL, col18 INT SIGNED NULL,
        col19 BIGINT SIGNED NULL, col20 TINYINT SIGNED NULL,
        col21 SMALLINT SIGNED NULL, col22 INT SIGNED NULL,
        col23 BIGINT SIGNED NULL, col24 TINYINT SIGNED NULL,
        col25 SMALLINT SIGNED NULL, col26 INT SIGNED NULL,
        col27 BIGINT SIGNED NULL, col28 TINYINT SIGNED NULL,
        col29 SMALLINT SIGNED NULL, col30 INT SIGNED NULL,
        col31 BIGINT SIGNED NULL, col32 TINYINT SIGNED NULL,
        col33 SMALLINT SIGNED NULL)`,
      `INSERT INTO ${TEST_TABLE} (col1, col2, col3, col4, col5, col33) VALUES
          (null, null, null, null, null, null),
          (-1, -1, -1, -1, -1, -1),
          (2147483647, 9007199254740993, 127, 32767, 8388607, 12),
          (-2147483648, -9007199254740993, -128, -32768, -8388608, 10),
          (-2147483645, -1, -126, -32766, -8388606, 6),
          (-1, 9223372036854775809, -1, -1, null, -6),
          (123456, -9223372036854775809, 96, 300, 1000, null),
          (-123456, 9223372036854775807, -96, -300, -1000, null)`,
      `SELECT * FROM ${TEST_TABLE}`,
    ], (err, result) => {
      if (err) {
        return test.fail(err);
      }

      expectEvents(test, events, [
        { _type: 'TableMap' },
        { rows: result[result.length - 1], _type: 'WriteRows' }
      ], 1, test.end);
    });
  });
});
