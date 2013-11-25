var zongji = require('../');

exports.parseDSN = function(test) {
  var dsn1 = 'mysql://zongji:foobar@localhost';
  var params1 = zongji.parseDSN(dsn1);
  test.deepEqual(params1, ['zongji', 'foobar', 'localhost', 3306]);

  var dsn2 = 'mysql://rooter@localhost';
  var params2 = zongji.parseDSN(dsn2);
  test.deepEqual(params2, ['rooter', '', 'localhost', 3306]);

  var dsn3 = 'mysql://localhost';
  var params3 = zongji.parseDSN(dsn3);
  test.deepEqual(params3, ['', '', 'localhost', 3306]);

  test.done();
};

exports.connectSucceeded = function(test) {
  var dsn = 'mysql://zongji:zongji@localhost';
  var zj = zongji.connect(dsn);

  test.ok(zj, 'connection established');
  test.done();
};

exports.connectFailed = function(test) {
  var dsn = 'mysql://foo:bar@localhost';
  test.throws(function() {
    zongji.connect(dsn);
  });
  test.done();
};

exports.beginDumpSucceeded = function(test) {
  var dsn = 'mysql://zongji:zongji@localhost';

  test.doesNotThrow(function() {
      var zj = zongji.connect(dsn);
  //   zj.start();
  });
  test.done();
};
