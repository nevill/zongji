// Client code
var ZongJi = require('./');

var zongji = new ZongJi({
  host     : 'localhost',
  user     : 'zongji',
  password : 'zongji',
  // debug: true
});

zongji.on('binlog', function(err, evt) {
  if(err) throw err;
  evt.dump();
});

zongji.start({
  filter: ['tablemap', 'writerows', 'updaterows', 'deleterows']
  // TODO: filterEvents, filterSchema
  // Perform schema filter before parsing fields for extra speed
});

process.on('SIGINT', function() {
  console.log('Got SIGINT.');
  process.exit();
});
