var settings = require('./settings/mysql');
var connector =  require('./helpers/connector');
var querySequence = require('./helpers/querySequence');

var conn = process.testZongJi || {};

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
  unitTestFilter: function(test) {
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
  },
  integrationTestFilter: function(test) {
    // Set includeSchema to not include anything, recieve no row events
    // Ensure that filters are applied
    var origOptions = conn.zongji.options;
    var testTable = 'filter_test';
    var includeSchema = {};
    // Uncomment the following line to manually test this test:
    // includeSchema[settings.database] = [ testTable ];
    conn.zongji.set({
      includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows'],
      includeSchema: includeSchema
    });
    querySequence(conn.db, [
      'DROP TABLE IF EXISTS ' + conn.escId(testTable),
      'CREATE TABLE ' + conn.escId(testTable) + ' (col INT UNSIGNED)',
      'INSERT INTO ' + conn.escId(testTable) + ' (col) VALUES (10)',
      'UPDATE ' + conn.escId(testTable) + ' SET col = 15',
      'DELETE FROM ' + conn.escId(testTable)
    ], function(error) {
      if (error) console.error(error);
      // Give 1 second to see if any events are emitted, they should not be!
      setTimeout(function() {
        conn.zongji.set(origOptions);
        test.equal(conn.eventLog.length, 0);
        test.equal(conn.errorLog.length, 0);
        test.done();
      }, 1000);
    });
  },
  changeAfterInit: function(test) {
    // Set includeSchema to skip table after the tableMap has already been
    // cached once, recieve no row events afterwards
    var origOptions = conn.zongji.options;
    var testTable = 'after_init_test';
    var includeSchema = {};
    includeSchema[settings.database] = [ testTable ];
    conn.zongji.set({
      includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows'],
      includeSchema: includeSchema
    });
    querySequence(conn.db, [
      'DROP TABLE IF EXISTS ' + conn.escId(testTable),
      'CREATE TABLE ' + conn.escId(testTable) + ' (col INT UNSIGNED)',
      'INSERT INTO ' + conn.escId(testTable) + ' (col) VALUES (10)',
    ], function(error) {
      if (error) console.error(error);
      // Give 1 second to see if any events are emitted, they should not be!
      setTimeout(function() {
        // Expect 2 events, TableMap and WriteRows from the INSERT query
        test.equal(conn.eventLog.length, 2);
        // Reset eventLog
        conn.eventLog.splice(0, conn.eventLog.length);
        // Skip all events from all tables
        conn.zongji.set({
          includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows'],
          includeSchema: {}
        });
        querySequence(conn.db, [
          'UPDATE ' + conn.escId(testTable) + ' SET col = 15',
          'DELETE FROM ' + conn.escId(testTable)
        ], function(error) {
          if (error) console.error(error);
          setTimeout(function() {
            conn.zongji.set(origOptions);
            test.equal(conn.eventLog.length, 0);
            test.equal(conn.errorLog.length, 0);
            test.done();
          }, 500);
        });
      }, 500);
    });
  }
};
