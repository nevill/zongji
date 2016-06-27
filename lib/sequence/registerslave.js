var Util = require('util');
var Packet = require('../packet');
var capture = require('../capture');

module.exports = function(options) {
  var self = this; // ZongJi instance
  var Sequence = capture(self.connection).Sequence;

  function RegisterSlave(callback) {
    Sequence.call(this, callback);
  }

  Util.inherits(RegisterSlave, Sequence);

  RegisterSlave.prototype.start = function() {
    this.emit('packet', new Packet.ComRegisterSlave(options));
  };

  return RegisterSlave;
};
