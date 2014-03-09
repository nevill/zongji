// Client code
var ZongJi = require('./');

var zongji = ZongJi.connect({
  host     : 'localhost',
  user     : 'zongji',
  password : 'zongji',
  // debug: true
});

zongji.on('binlog', function(evt) {
  evt.dump();
});

zongji.start();

process.on('SIGINT', function() {
  console.log('Got SIGINT.');
  process.exit();
});
