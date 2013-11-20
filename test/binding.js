var binding = require('../build/Release/zongji');

exports.methods = function(test) {
  var connection = binding.init();
  test.ok(connection.connect, "has method 'connect'");
  test.ok(connection.waitForNextEvent, "has method 'waitForNextEvent'");
  test.done();
};
