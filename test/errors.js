var ZongJi = require('./../');
var getEventClass = require('./../lib/code_map').getEventClass;
var settings = require('./settings/mysql');
var connector =  require('./helpers/connector');
var querySequence = require('./helpers/querySequence');

var conn = process.testZongJi || {};

function generateDisconnectionCase(readyKillIdFun, cleanupKillIdFun) {
  return function(test) {
    var zongji = new ZongJi(settings.connection);
    var errorTrapped = false;
    var ACCEPTABLE_ERRORS = [
      'PROTOCOL_CONNECTION_LOST',
      // MySQL 5.1 emits a packet sequence error when the binlog disconnected
      'PROTOCOL_INCORRECT_PACKET_SEQUENCE'
    ];

    zongji.on('error', function(error) {
      if (!errorTrapped && ACCEPTABLE_ERRORS.indexOf(error.code) > -1) {
        errorTrapped = true;
        killThread(cleanupKillIdFun);
        test.done();
      }
    });

    zongji.start({
      includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows'],
      // Anything other than default (1) as used in helpers/connector
      serverId: 12
    });

    function killThread(argFun) {
      var threadId = argFun(zongji);
      test.ok(!isNaN(threadId));
      conn.db.query('KILL ' + threadId);
    }

    function isZongjiReady() {
      setTimeout(function() {
        if (zongji.ready) {
          killThread(readyKillIdFun);
        } else {
          isZongjiReady();
        }
      }, 100);
    }
    isZongjiReady();

  };
}

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
  binlogConnection_disconnect: generateDisconnectionCase(
    function onReady(zongji) { return zongji.connection.threadId; },
    function onCleanup(zongji) { return zongji.ctrlConnection.threadId; }),
  ctrlConnection_disconnect: generateDisconnectionCase(
    function onReady(zongji) { return zongji.ctrlConnection.threadId; },
    function onCleanup(zongji) { return zongji.connection.threadId; }),

  reconnect_at_pos: function(test) {
    // Test that binlog events come through in correct sequence after
    // reconnect using the filename and position properties
    const NEW_INST_TIMEOUT = 1000;
    const UPDATE_INTERVAL = 300;
    const UPDATE_COUNT = 5;
    const TEST_TABLE = 'reconnect_at_pos';

    let first;
    let second;

    let result = [];

    // Create a new ZongJi instance using some default options that will count
    // using the values in the new rows inserted
    function startNewZongJi(options) {
      let zongji = new ZongJi(settings.connection);

      zongji.start(
        Object.assign(
          {
            // Must include rotate events for filename and position properties
            includeEvents: [
              'rotate', 'tablemap', 'writerows', 'updaterows', 'deleterows'
            ]
          },
          options
        )
      );

      zongji.on('binlog', function(event) {
        if (event.getTypeName() === 'WriteRows') {
          result.push(event.rows[0].col);

          if (result.length === UPDATE_COUNT) {
            exitTest();
          }
        }
      });
      return zongji;
    }

    function exitTest() {
      first.stop && first.stop();
      second.stop && second.stop();

      test.deepEqual(
        result,
        Array.from({length: UPDATE_COUNT}, (_, i) => i)
      );
      test.done();
    }

    function startPeriodicallyWriting() {
      const INSERT_QUERY = 'INSERT INTO ' + conn.escId(TEST_TABLE) + ' (col) VALUES ';
      let sequences = Array.from(
        {length: UPDATE_COUNT},
        (_, i) => INSERT_QUERY + `(${i})`
      );
      let updateInterval;

      let doInsert = () => {
        querySequence(conn.db, [sequences.shift()], error => {
          if (error) {
            clearInterval(updateInterval);
            test.done(error);
          }
        });

        if (sequences.length === 0) {
          clearInterval(updateInterval);
        }
      };

      updateInterval = setInterval(doInsert, UPDATE_INTERVAL);
    }

    function killFirstWhenTimeout() {
      setTimeout(function() {
        // Start new ZongJi instance where the previous was when stopped
        first.stop();
        second = startNewZongJi({
          serverId: 16,
          filename: first.get('filename'),
          position: first.get('position'),
        });
      }, NEW_INST_TIMEOUT);
    }

    querySequence(conn.db, [
      'DROP TABLE IF EXISTS ' + conn.escId(TEST_TABLE),
      'CREATE TABLE ' + conn.escId(TEST_TABLE) + ' (col INT UNSIGNED)',
      'INSERT INTO ' + conn.escId(TEST_TABLE) + ' (col) VALUES (10)',
    ], function(error) {
      if (error) {
        return test.done(error);
      }

      first = startNewZongJi({
        serverId: 14,
        startAtEnd: true
      });

      first.on('ready', () => {
        startPeriodicallyWriting();
        killFirstWhenTimeout();
      });
    });
  },

  invalid_host: function(test) {
    var zongji = new ZongJi({
      host: 'wronghost',
      user: 'wronguser',
      password: 'wrongpass'
    });
    zongji.on('error', function(error) {
      test.ok([ 'ENOTFOUND', 'ETIMEDOUT' ].indexOf(error.code) !== -1);
      test.done();
    });
  },
  code_map: function(test) {
    test.equal(getEventClass(2).name, 'Query');
    test.equal(getEventClass(490).name, 'Unknown');
    test.done();
  }
};
