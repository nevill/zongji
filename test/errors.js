var ZongJi = require('./../');
var getEventClass = require('./../lib/code_map').getEventClass;

module.exports = {
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
