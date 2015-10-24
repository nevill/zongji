var ZongJi = require('./../');


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
  }
}
