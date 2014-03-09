var getEventClass = require('../code_map').getEventClass;

function BinlogHeader() {}

BinlogHeader.prototype.parse = function(parser) {
  // uint8_t  marker; // always 0 or 0xFF
  // uint32_t timestamp;
  // uint8_t  type_code;
  // uint32_t server_id;
  // uint32_t event_length;
  // uint32_t next_position;
  // uint16_t flags;
  parser.parseUnsignedNumber(1);

  var timestamp = parser.parseUnsignedNumber(4) * 1000;
  var eventType = parser.parseUnsignedNumber(1);
  var serverId = parser.parseUnsignedNumber(4);
  var eventLength = parser.parseUnsignedNumber(4);
  var nextPosition = parser.parseUnsignedNumber(4);
  var flags = parser.parseUnsignedNumber(2);

  // headerLength doesn't count marker
  var headerLength = 19;
  // for MySQL 5.6 and binlog-checksum = CRC32
  // if (useChecksum) {
  //   headerLength += 4;
  // }
  var eventSize = eventLength - headerLength;

  var options = {
    timestamp: timestamp,
    nextPosition: nextPosition,
    size: eventSize
  };

  var EventClass = getEventClass(eventType);
  this._event = new EventClass(parser, options);
};

BinlogHeader.prototype.getEvent = function() {
  return this._event;
};

module.exports = BinlogHeader;
