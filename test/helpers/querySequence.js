// Execute a sequence of queries on a node-mysql database connection
// @param {object} connection - Node-Mysql Connection, Connected
// @param {[string]} queries - Queries to execute, in order
// @param {function} callback - Call when complete
module.exports = function(connection, queries, callback){
  var sequence = queries.map(function(queryStr, index, initQueries){
    return function(){
      connection.query(queryStr, function(err, rows, fields){
        if(err) throw err;
        if(index < sequence.length - 1){
          sequence[index + 1]();
        }else{
          callback();
        }
      });
    }
  });
  sequence[0]();
};
