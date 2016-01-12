// Replication logs will be cleared!
// Database will be recreated!
module.exports = {
  connection: {
    host     : 'localhost',
    user     : 'root',
    password : 'numtel',
    serverId : 111,
    // debug: true
  },
  database: 'zongji_test'
}

if(process.env.TRAVIS){
  // Travis CI database root user does not have a password
  module.exports.connection.password = '';
}
