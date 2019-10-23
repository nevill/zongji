// Execute a sequence of queries on a node-mysql database connection
// @param {object} connection - Node-Mysql Connection, Connected
// @param {boolean} debug - Print queries as they execute (optional)
// @param {[string]} queries - Queries to execute, in order
// @param {function} callback - Call when complete
module.exports = function(connection, debug, queries, callback) {
  if (debug instanceof Array) {
    callback = queries;
    queries = debug;
    debug = false;
  }
  const results = [];
  const sequence = queries.map(function(queryStr, index) {
    return function() {
      debug && console.log('Query Sequence', index, queryStr);
      connection.query(queryStr, function(err, rows) {
        if (err) callback(err);
        results.push(rows);
        if (index < sequence.length - 1) {
          sequence[index + 1]();
        }
        else {
          callback(null, results);
        }
      });
    };
  });
  sequence[0]();
};
