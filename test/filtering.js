const tap = require('tap');
const ZongJi = require('../');
const settings = require('./settings/mysql');
const testDb = require('./helpers');

// this test is only used for initialization
tap.test('Initialise testing db', test => {
  testDb.init(err => {
    if (err) {
      return test.fail(err);
    }

    test.end();
  });
});

tap.test('Unit test', test => {
  const zongji = new ZongJi(settings.connection);

    test.test('Check that exclude overrides include', test => {
      zongji._filters({
        includeEvents: ['tablemap', 'writerows', 'updaterows', 'rotate'],
        excludeEvents: ['rotate'],
        includeSchema: {db1: true, db2: ['one_table'], db3: true},
        excludeSchema: {db3: true}
      });
      test.ok(!zongji._skipEvent('tablemap'));
      test.ok(zongji._skipEvent('rotate'));
      test.ok(!zongji._skipSchema('db1', 'any_table'));
      test.ok(!zongji._skipSchema('db2', 'one_table'));
      test.ok(zongji._skipSchema('db2', 'another_table'));
      test.ok(zongji._skipSchema('db3', 'any_table'));

      test.end();
    });

    test.test(test => {
      zongji._filters({
        includeSchema: {db1: ['just_me']}
      });
      test.ok(!zongji._skipSchema('db1', 'just_me'));
      test.ok(zongji._skipSchema('db2', 'anything_else'));
      test.ok(zongji._skipSchema('db1', 'not_me'));

      test.end();
    });

    test.test(test => {
      zongji._filters({
        excludeSchema: {db1: ['not_me']}
      });

      test.ok(!zongji._skipSchema('db1', 'anything_else'));
      test.ok(!zongji._skipSchema('db2', 'anything_else'));
      test.ok(zongji._skipSchema('db1', 'not_me'));

      test.end();
    });

    test.test(test =>{
      zongji._filters({
        excludeEvents: ['rotate']
      });
      test.ok(!zongji._skipEvent('tablemap'));
      test.ok(zongji._skipEvent('rotate'));

      test.end();
    });

    test.test(test =>{
      test.plan(2);
      zongji._filters({
        includeEvents: ['rotate'],
      });
      test.ok(zongji._skipEvent('tablemap'));
      test.ok(!zongji._skipEvent('rotate'));
    });

    test.end();
});

tap.test('Exclue all the schema', test => {
  const zongji = new ZongJi(settings.connection);

  const eventLog = [];
  const errorLog = [];

  zongji.on('binlog', event => eventLog.push(event));
  zongji.on('error', error => errorLog.push(error));

  test.tearDown(() => zongji.stop());

  // Set includeSchema to not include anything, recieve no row events
  // Ensure that filters are applied
  const includeSchema = {};
  zongji.start({
    includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows'],
    includeSchema: includeSchema
  });

  zongji.on('ready', () => {
    const testTable = 'filter_test';
    testDb.execute([
      `DROP TABLE IF EXISTS ${testTable}`,
      `CREATE TABLE ${testTable} (col INT UNSIGNED)`,
      `INSERT INTO ${testTable} (col) VALUES (10)`,
      `UPDATE ${testTable} SET col = 15`,
      `DELETE FROM ${testTable}`,
    ], (error) => {
      if (error) {
        return test.fail(error);
      }

      // Give 1 second to see if any events are emitted, they should not be!
      setTimeout(() => {
        test.equal(eventLog.length, 0);
        test.equal(errorLog.length, 0);
        test.end();
      }, 1000);
    });
  });
});

tap.test('Change filter when ZongJi is running', test => {
  // Set includeSchema to skip table after the tableMap has already been
  // cached once, recieve no row events afterwards
  const testTable = 'after_init_test';
  const includeSchema = {};
  includeSchema[settings.connection.database] = [ testTable ];

  const zongji = new ZongJi(settings.connection);
  const eventLog = [];

  zongji.on('binlog', event => eventLog.push(event));
  zongji.on('error', error => test.fail(error));

  zongji.start({
    includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows'],
    includeSchema: includeSchema
  });

  test.tearDown(() => zongji.stop());

  testDb.execute(
    [
    `DROP TABLE IF EXISTS ${testTable}`,
    `CREATE TABLE ${testTable} (col INT UNSIGNED)`,
    `INSERT INTO ${testTable} (col) VALUES (10)`,
    ],
    err => {
      if (err) {
        return test.fail(err);
      }

      setTimeout(() => {
        test.equal(eventLog.length, 2);

        test.test('update filter', test => {
          // reset for next test
          eventLog.splice(0, eventLog.length);

          zongji._filters({
            includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows'],
            includeSchema: {},
          });

          testDb.execute(
            [
              `UPDATE ${testTable} SET col = 15`,
              `DELETE FROM ${testTable}`,
            ],
            (error) => {
              if (error) {
                return test.fail(error);
              }

              setTimeout(() => {
                test.equal(eventLog.length, 0);
                test.end();
              }, 1000);
            }
          );
        });

        test.end();
      }, 1000);
    }
  );
});
