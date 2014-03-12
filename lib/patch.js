var patched = false;

var decorateProtocol = function(protocolPrototype, Binlog) {
  protocolPrototype.dumpBinlog = function(cb) {
    return this._enqueue(new Binlog(cb));
  };
};

var decorateConnection = function(connectionPrototype) {
  connectionPrototype.dumpBinlog = function(cb) {
    this._implyConnect();
    return this._protocol.dumpBinlog(cb);
  };
};

var patchIt = function(claz, options) {
  var Binlog = require('./sequence/binlog')(claz, options);
  decorateConnection(claz.ConnectionPrototype);
  decorateProtocol(claz.ProtocolPrototype, Binlog);
};

module.exports = function(claz, options) {
  if (!patched) {
    patchIt(claz, options);
  }
};
