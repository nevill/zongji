// Client code
// use docker-compose up to start numtel example db
const ZongJi = require('./');

const zongji = new ZongJi({
  host : 'localhost',
  user : 'root',
  password : 'numtel',
  charset : 'utf8mb4_unicode_ci',
  debug: true
});

zongji.on('binlog', function(evt) {
  evt.dump();
  let database = evt.tableMap[evt.tableId].parentSchema;
  let table = evt.tableMap[evt.tableId].tableName;
  let columns = evt.tableMap[evt.tableId].columns;
  console.log(database);
  console.log(table);
  console.log(columns);
});

zongji.start({
  includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows']
});

process.on('SIGINT', function() {
  console.log('Got SIGINT.');
  zongji.stop();
  process.exit();
});
