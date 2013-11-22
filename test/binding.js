var binding = require('../build/Release/zongji');

exports.methods = function(test) {
  var connection = binding.init();
  test.ok(connection.connect, "has method 'connect'");
  test.ok(connection.beginBinlogDump, "has method 'beginBinlogDump'");
  test.ok(connection.waitForNextEvent, "has method 'waitForNextEvent'");
  test.done();
};

exports.connect = function(test) {
  var connection = binding.init();
  test.doesNotThrow(function() {
    connection.connect('zongji', 'zongji', 'localhost', 3306);
    connection.beginBinlogDump();
  });
  test.done();
};
