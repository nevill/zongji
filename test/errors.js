var ZongJi = require('./../');
var Pool = require('mysql/lib/Pool');
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
      if(!errorTrapped && ACCEPTABLE_ERRORS.indexOf(error.code) > -1) {
        errorTrapped = true;
        killThread(cleanupKillIdFun);
      }
    });

    zongji.start({
      includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows'],
      // Anything other than default (1) as used in helpers/connector
      serverId: 12
    });

    function killThread(argFun) {
      var threadId = argFun(zongji);
      if (typeof threadId === 'object') {
        test.done();
        threadId.end();
      } else {
        test.ok(!isNaN(threadId));
        conn.db.query('KILL ' + threadId);
      }
    }

    function isZongjiReady() {
      setTimeout(function() {
        if(zongji.ready) {
          killThread(readyKillIdFun);
        } else {
          isZongjiReady();
        }
      }, 100);
    }
    isZongjiReady();

  }
}

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
  },
  binlogConnection_disconnect: generateDisconnectionCase(
    function onReady(zongji) { return zongji.connection.threadId },
    function onCleanup(zongji) { return zongji.ctrlPool }),
  ctrlPool_disconnect: generateDisconnectionCase(
    function onReady(zongji) { return zongji.ctrlPool },
    function onCleanup(zongji) { return zongji.connection.threadId }),
  ctrlPool_prototype: function(test) {
    test.ok(conn.zongji.ctrlPool instanceof Pool);
    test.done();
  },
  invalid_host: function(test) {
    var zongji = new ZongJi({
      host: 'wronghost',
      user: "wronguser",
      password: "wrongpass"
    });
    zongji.on('error', function(error) {
      test.equal(error.code, 'ENOTFOUND');
      test.done();
    });
  },
  code_map: function(test) {
    test.equal(getEventClass(2).name, 'Query');
    test.equal(getEventClass(490).name, 'Unknown');
    test.done();
  }
}
