var mysql = require('mysql');

var query = mysql.createQuery('select 1', function() {});

var Query = query.constructor;
// `super_` is set by calling util.inherits
// see http://nodejs.org/api/util.html#util_util_inherits_constructor_superconstructor
var Sequence = Query.super_;

var QueryPrototype = Object.getPrototypeOf(query);
var SequencePrototype = Object.getPrototypeOf(QueryPrototype);

var classes = {
  Query: Query,
  Sequence: Sequence,
  QueryPrototype: QueryPrototype,
  SequencePrototype: SequencePrototype
};

var captured = false;


var captureIt = function(connection) {
  var ConnectionPrototype = Object.getPrototypeOf(connection);
  ConnectionPrototype._patch = function() {
    classes.ProtocolPrototype = Object.getPrototypeOf(this._protocol);
    classes.ConnectionPrototype = ConnectionPrototype;
    classes.Connection = connection.constructor;
    classes.Protocol = this._protocol.constructor;
    captured = true;
  };

  connection._patch();
  delete ConnectionPrototype._patch;
};

// connection {Connection} an instance created by mysql.createConnection
module.exports = function(connection) {
  if (!captured) {
    captureIt(connection);
  }
  return classes;
};
