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
    // reconnect using the binlogName and binlogNextPos properties
    var NEW_INST_TIMEOUT = 1000;
    var UPDATE_INTERVAL = 300;
    var UPDATE_COUNT = 5;
    var TEST_TABLE = 'reconnect_at_pos';

    var updatesSent = 0, updateEvents = 0;

    // Create a new ZongJi instance using some default options that will count
    // using the values in the new rows inserted
    function startNewZongJi(options) {
      var zongji = new ZongJi(settings.connection);

      zongji.start(Object.keys(options || {}).reduce(function(opts, setKey) {
        // Object.assign-like to support node 0.10
        opts[setKey] = options[setKey];
        return opts;
      }, {
        // Must include rotate events for binlogName and binlogNextPos properties
        includeEvents: ['rotate', 'tablemap', 'writerows', 'updaterows', 'deleterows']
      }));
      zongji.on('binlog', function(event) {
        if (event.getTypeName() === 'WriteRows') {
          if (updateEvents++ !== event.rows[0].col) {
            exitTest('Events in the wrong order');
          } else if (updateEvents === UPDATE_COUNT) {
            exitTest();
          }
        }
      });
      return zongji;
    }

    var firstZongJi;
    var secondZongJi;

    querySequence(conn.db, [
      'DROP TABLE IF EXISTS ' + conn.escId(TEST_TABLE),
      'CREATE TABLE ' + conn.escId(TEST_TABLE) + ' (col INT UNSIGNED)',
      'INSERT INTO ' + conn.escId(TEST_TABLE) + ' (col) VALUES (10)',
    ], function(error) {
      if (error)
        return exitTest(error);

      firstZongJi = startNewZongJi({
        serverId: 14,
        startAtEnd: true
      });

      setTimeout(function() {
        // Start new ZongJi instance where the previous was when stopped
        firstZongJi.stop();
        secondZongJi = startNewZongJi({
          serverId: 16,
          binlogName: firstZongJi.binlogName,
          binlogNextPos: firstZongJi.binlogNextPos
        });

      }, NEW_INST_TIMEOUT);
    });

    function exitTest(error) {
      test.ifError(error);
      firstZongJi.stop && firstZongJi.stop();
      secondZongJi.stop && secondZongJi.stop();
      test.done();
    }

    var updateInterval = setInterval(function() {
      if (updatesSent++ < UPDATE_COUNT) {
        querySequence(conn.db, [
          'INSERT INTO ' + conn.escId(TEST_TABLE) + ' (col) VALUES (' + updateEvents + ')',
        ], function(error) { error && exitTest(error); });
      } else {
        clearInterval(updateInterval);
      }
    }, UPDATE_INTERVAL);

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
