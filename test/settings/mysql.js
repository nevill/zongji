// Replication logs will be cleared!
// Database will be recreated!
module.exports = {
  connection: {
    host     : 'localhost',
    user     : 'root',
    password : 'numtel',
    charset  : 'utf8mb4_unicode_ci',
    port     : process.env.TEST_MYSQL_PORT,
    dateStrings : process.env.TEST_MODE === 'date_strings',
    supportBigNumbers : [ 'big_numbers', 'big_number_strings' ].indexOf(process.env.TEST_MODE) !== -1,
    bigNumberStrings : process.env.TEST_MODE === 'big_number_strings',
    // debug: true
  },
  database: 'zongji_test',
  sessionSqlMode: process.env.TEST_SESSION_SQL_MODE || ''
}

