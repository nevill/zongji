// Client code
var ZongJi = require('./');

var connection = ZongJi.connect({
  host     : 'localhost',
  user     : 'zongji',
  password : 'zongji',
  // debug: true
});

connection.connect();

connection.dumpBinlog(function(err, packet) {
  if (err) {
    throw err;
  }

  console.log('binlog dump ====>');
  console.log('=== %s ===', packet.eventName);
  console.log('Date: %s', new Date(packet.timestamp));
  console.log('Next log position: %d', packet.nextPosition);
});

process.on('exit', function() {
  console.log('about to exit');
});

process.on('SIGINT', function() {
  // connection.end(process.exit);
  console.log('Got SIGINT.');
  connection.destroy();
});
