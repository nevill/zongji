// Replication logs will be cleared!
// Database will be recreated!
module.exports = {
  connection: {
    host     : 'localhost',
    user     : 'root',
    password : 'numtel',
    debug : false // setting debug to true will output all packets
  },
  database: 'zongji_test'
};

if(process.env.TRAVIS){
  // Port to use is passed as variable
  module.exports.connection.port = process.env.TEST_MYSQL_PORT;
}
