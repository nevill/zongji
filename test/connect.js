var zongji = require('../');

exports.connectSucceeded = function(test) {
  var dsn = 'mysql://zongji:zongji@localhost';
  var zj = zongji.connect(dsn);

  test.ok(zj, 'connection established');
  test.ok(zj.connection.waitForNextEvent, 'has method waitForNextEvent');
  test.done();
};

exports.connectFailed = function(test) {
  var dsn = 'mysql://foo:bar@localhost';
  test.throws(function() {
    zongji.connect(dsn);
  });
  test.done();
};
