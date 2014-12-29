var settings = require('./settings/mysql');
var connector =  require('./helpers/connector');

var codeMap = require('../lib/code_map');
var initBinlogHeader = require('../lib/packet/binlog_header');
var rowEvents = require('../lib/rows_event');

var conn = process.testZongJi || {};

module.exports = {
  setUp: function(done){
    if(!conn.db) process.testZongJi = connector.call(conn, settings, done);
    else done();
  },
  tearDown: function(done){
    done();
  },
  testFilter: function(test){
    var origOptions = conn.zongji.options;

    conn.zongji.set({
      includeEvents: ['tablemap', 'writerows', 'updaterows', 'rotate'],
      excludeEvents: ['rotate'],
      includeSchema: {db1: true, db2: ['one_table'], db3: true},
      excludeSchema: {db3: true}
    });
    // Check that exclude overrides include
    test.ok(!conn.zongji._skipEvent('tablemap'));
    test.ok(conn.zongji._skipEvent('rotate'));
    test.ok(!conn.zongji._skipSchema('db1', 'any_table'));
    test.ok(!conn.zongji._skipSchema('db2', 'one_table'));
    test.ok(conn.zongji._skipSchema('db2', 'another_table'));
    test.ok(conn.zongji._skipSchema('db3', 'any_table'));


    conn.zongji.set({
      includeSchema: {db1: ['just_me']}
    });
    test.ok(!conn.zongji._skipSchema('db1', 'just_me'));
    test.ok(conn.zongji._skipSchema('db2', 'anything_else'));
    test.ok(conn.zongji._skipSchema('db1', 'not_me'));


    conn.zongji.set({
      excludeSchema: {db1: ['not_me']}
    });
    test.ok(!conn.zongji._skipSchema('db1', 'anything_else'));
    test.ok(!conn.zongji._skipSchema('db2', 'anything_else'));
    test.ok(conn.zongji._skipSchema('db1', 'not_me'));


    conn.zongji.set({
      excludeEvents: ['rotate']
    });
    test.ok(!conn.zongji._skipEvent('tablemap'));
    test.ok(conn.zongji._skipEvent('rotate'));


    conn.zongji.set({
      includeEvents: ['rotate']
    });
    test.ok(conn.zongji._skipEvent('tablemap'));
    test.ok(!conn.zongji._skipEvent('rotate'));

    // Restore original emitter
    delete conn.zongji.emit;
    conn.zongji.set(origOptions);
    
    test.done();
  }
}
