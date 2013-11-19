var zongji = require('../');

exports.connect = function(test) {
  var zj = zongji.connect();
  test.ok(zj, 'connection established');
  test.ok(zj.connection.waitForNextEvent, 'has method waitForNextEvent');
  test.done();
};
