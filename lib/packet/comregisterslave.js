// https://dev.mysql.com/doc/internals/en/com-register-slave.html
function ComRegisterSlave(options) {
  options = options || {};
  this.command = 0x15;

  this.serverId = options.serverId || 1;
}

ComRegisterSlave.prototype.write = function(writer) {
  writer.writeUnsignedNumber(1, this.command);
  writer.writeUnsignedNumber(4, this.serverId);
  writer.writeUnsignedNumber(1, 0); // slave_hostname
  writer.writeUnsignedNumber(1, 0); // slave_user
  writer.writeUnsignedNumber(1, 0); // slave_password
  writer.writeUnsignedNumber(2, 0); // slave_port
  writer.writeUnsignedNumber(4, 0); // replication_rank
  writer.writeUnsignedNumber(4, 0); // master_id
}


ComRegisterSlave.prototype.parse = function() {
  throw new Error('should never be called');
};

module.exports = ComRegisterSlave;
