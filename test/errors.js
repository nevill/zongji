const tap = require('tap');

const ZongJi = require('../');
const settings = require('./settings/mysql');
const testDb = require('./helpers');

tap.test('Connect to an invalid host', test => {
  const zongji = new ZongJi({
    host: 'wronghost',
    user: 'wronguser',
    password: 'wrongpass'
  });

  zongji.on('error', function(error) {
    test.ok(['ENOTFOUND', 'ETIMEDOUT'].indexOf(error.code) !== -1);
    test.end();
  });

  test.tearDown(() => zongji.stop());
  zongji.start();
});

tap.test('Initialise testing db', test => {
  testDb.init(err => {
    if (err) {
      return test.threw(err);
    }
    test.end();
  });
});

const ACCEPTABLE_ERRORS = [
  'PROTOCOL_CONNECTION_LOST',
  // MySQL 5.1 emits a packet sequence error when the binlog disconnected
  'PROTOCOL_INCORRECT_PACKET_SEQUENCE'
];

tap.test('Disconnect binlog connection', test => {
  const zongji = new ZongJi(settings.connection);

  zongji.start({
    includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows'],
    serverId: testDb.serverId(),
  });

  zongji.on('ready', () => {
    let threadId = zongji.connection.threadId;
    test.ok(!isNaN(threadId));
    testDb.execute([`kill ${threadId}`], err => {
      if (err) {
        test.threw(err);
      }
    });
  });

  zongji.on('error', err => {
    if (ACCEPTABLE_ERRORS.indexOf(err.code) > -1) {
      zongji.stop();
      test.end();
    } else {
      test.threw(err);
    }
  });
});

tap.test('Disconnect control connection', test => {
  const zongji = new ZongJi(settings.connection);

  zongji.start({
    includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows'],
    serverId: testDb.serverId(),
  });

  zongji.on('ready', () => {
    let threadId = zongji.ctrlConnection.threadId;
    test.ok(!isNaN(threadId));
    testDb.execute([`kill ${threadId}`], err => {
      if (err) {
        test.threw(err);
      }
    });
  });

  zongji.on('error', err => {
    if (ACCEPTABLE_ERRORS.indexOf(err.code) > -1) {
      zongji.stop();
      test.end();
    } else {
      test.threw(err);
    }
  });
});


tap.test('Events come through in sequence', test => {
  const NEW_INST_TIMEOUT = 1000;
  const UPDATE_INTERVAL = 300;
  const UPDATE_COUNT = 5;
  const TEST_TABLE = 'reconnect_at_pos';

  test.test(`prepare table ${TEST_TABLE}`, test => {
    testDb.execute([
      `DROP TABLE IF EXISTS ${TEST_TABLE}`,
      `CREATE TABLE ${TEST_TABLE} (col INT UNSIGNED)`,
      `INSERT INTO ${TEST_TABLE} (col) VALUES (10)`,
    ], err =>{
      if (err) {
        return test.threw(err);
      }
      test.end();
    });
  });

  test.test('when reconnect', test => {
    const result = [];

    function startPeriodicallyWriting() {
      let sequences = Array.from(
        {length: UPDATE_COUNT},
        (_, i) => `INSERT INTO ${TEST_TABLE} (col) VALUES (${i})`
      );

      let updateInterval = setInterval(() => {
        testDb.execute([sequences.shift()], error => {
          if (error) {
            clearInterval(updateInterval);
            test.threw(error);
          }
        });

        if (sequences.length === 0) {
          clearInterval(updateInterval);
        }
      }, UPDATE_INTERVAL);
    }

    function newInstance(options) {
      const zongji = new ZongJi(settings.connection);

      zongji.start({
        ...options,
        // Must include rotate events for filename and position properties
        includeEvents: [
          'rotate', 'tablemap', 'writerows', 'updaterows', 'deleterows'
        ]
      });

      zongji.on('binlog', function(event) {
        if (event.getTypeName() === 'WriteRows') {
          result.push(event.rows[0].col);
        }

        if (result.length === UPDATE_COUNT) {
          test.strictSame(
            result,
            Array.from({length: UPDATE_COUNT}, (_, i) => i)
          );
          test.end();
        }
      });

      return zongji;
    }

    let first = newInstance({
      serverId: testDb.serverId(),
      startAtEnd: true,
    });

    first.on('ready', () => {
      startPeriodicallyWriting();

      first.on('stopped', () => {
        // Start new ZongJi instance where the previous was when stopped
        let second = newInstance({
          serverId: testDb.serverId(),
          filename: first.get('filename'),
          position: first.get('position'),
        });

        test.tearDown(() => second.stop());
      });
      setTimeout(() => first.stop(), NEW_INST_TIMEOUT);
    });
  });

  test.end();
});
