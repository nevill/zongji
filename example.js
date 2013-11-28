// Client code
var ZongJi = require('./');

var mysqlUrl = 'mysql://zongji:zongji@localhost';
var listener = ZongJi.connect(mysqlUrl);

// listener.setOption({
//   logLevel: 'debug',
//   retryLimit: 100,
//   retryTimeout: 3 });

listener.on('rotate', function(event) {
  event.dump();
});

listener.on('format', function(event) {
  event.dump();
});

listener.on('query', function(event) {
  event.dump();
});

listener.on('xid', function(event) {
  event.dump();
});

listener.on('unknown', function(event) {
  event.dump();
});

listener.start();
