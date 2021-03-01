const tap = require('tap');

const ZongJi = require('../');
const testDb = require('./helpers');
const expectEvents = require('./helpers/expectEvents');
const settings = require('./settings/mysql');

tap.test('Initialise testing db', test => {
  testDb.init(err => {
    if (err) {
      return test.threw(err);
    }
    test.end();
  });
});

testDb.requireVersion('5.6.2', () => {

  tap.test('Update with binlog_row_image=minmal', test => {
    const TEST_TABLE = 'row_image_minimal_test';

    test.test(`prepare table ${TEST_TABLE}`, test => {
      testDb.execute([
        'SET GLOBAL binlog_row_image=minimal',
        `DROP TABLE IF EXISTS ${TEST_TABLE}`,
        `CREATE TABLE ${TEST_TABLE} (
          id int primary key auto_increment,
          name varchar(20),
          age tinyint,
          height mediumint
        )`,
        `INSERT INTO ${TEST_TABLE} (name, age) VALUES ('Tom', 2)`,
      ], err => {
        if (err) {
          return test.fail(err);
        }

        test.end();
      });
    });

    test.test('update a record', test => {
      const events = [];
      const zongji = new ZongJi(settings.connection);
      test.tearDown(() => zongji.stop());

      zongji.on('ready', () => {
        testDb.execute([
          `UPDATE ${TEST_TABLE} SET age=age+1 WHERE id=1`,
        ], err => {
          if (err) {
            test.fail(err);
          }
        });
      });

      zongji.on('binlog', evt => {
        events.push(evt);

        if (events.length == 2) {
          expectEvents(test, events,
            [
              {
                _type: 'TableMap',
                tableName: TEST_TABLE,
                schemaName: testDb.SCHEMA_NAME,
              },
              {
                _type: 'UpdateRows',
                rows: [
                  {
                    before: { id: 1, age: null, name: null, height: null },
                    after: { id: null, age: 3, name: null, height: null },
                  },
                ],
              }
            ], 1, () => test.end()
          );
        }
      });

      zongji.start({
        startAtEnd: true,
        serverId: testDb.serverId(),
        includeEvents: ['tablemap', 'updaterows'],
      });
    });

    test.end();
  });

  tap.test('Update with binlog_row_image=noblob', test => {
    const TEST_TABLE = 'row_image_noblob_test';

    test.test(`prepare table ${TEST_TABLE}`, test => {
      testDb.execute([
        'SET GLOBAL binlog_row_image=noblob',
        `DROP TABLE IF EXISTS ${TEST_TABLE}`,
        `CREATE TABLE ${TEST_TABLE} (
          id int primary key auto_increment,
          summary text
        )`,
        `INSERT INTO ${TEST_TABLE} (summary) VALUES ('Hello world')`,
      ], err => {
        if (err) {
          return test.fail(err);
        }

        test.end();
      });
    });

    test.test('update a record', test => {
      const events = [];
      const zongji = new ZongJi(settings.connection);
      test.tearDown(() => zongji.stop());

      zongji.on('ready', () => {
        testDb.execute([
          `UPDATE ${TEST_TABLE} SET summary='hello again' WHERE id=1`,
        ], err => {
          if (err) {
            test.fail(err);
          }
        });
      });

      zongji.on('binlog', evt => {
        events.push(evt);

        if (events.length == 2) {
          expectEvents(test, events,
            [
              {
                _type: 'TableMap',
                tableName: TEST_TABLE,
                schemaName: testDb.SCHEMA_NAME,
              },
              {
                _type: 'UpdateRows',
                rows: [
                  {
                    before: { id: 1, summary: null },
                    after: { id: 1, summary: 'hello again' },
                  },
                ],
              }
            ], 1, () => test.end()
          );
        }
      });

      zongji.start({
        startAtEnd: true,
        serverId: testDb.serverId(),
        includeEvents: ['tablemap', 'updaterows'],
      });
    });

    test.end();
  });
});
