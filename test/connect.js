var zongji = require('../');

exports.connect = function(test) {
  var dsn = 'mysql://zongji:zongji@localhost';
  var zj = zongji.connect(dsn);

  test.ok(zj, 'connection established');
  test.ok(zj.connection.waitForNextEvent, 'has method waitForNextEvent');
  test.done();
};
