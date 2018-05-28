var getEventClass = require('../code_map').getEventClass;

module.exports = function generateBinlogHeader(options) {
  var zongji = this;
  var tableMap = options.tableMap;
  var useChecksum = options.useChecksum;

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
    var serverId = parser.parseUnsignedNumber(4); // eslint-disable-line
    var eventLength = parser.parseUnsignedNumber(4);
    var nextPosition = parser.parseUnsignedNumber(4);
    var flags = parser.parseUnsignedNumber(2); // eslint-disable-line

    // headerLength doesn't count marker
    var headerLength = 19;
    // for MySQL 5.6 and binlog-checksum = CRC32
    if (useChecksum) {
      headerLength += 4;
    }
    var eventSize = eventLength - headerLength;

    var options = {
      timestamp: timestamp,
      nextPosition: nextPosition,
      size: eventSize,
      eventType: eventType,
      tableMap: tableMap,
    };

    var EventClass = getEventClass(eventType);
    // Check event filtering
    if (!zongji._skipEvent(EventClass.name.toLowerCase())) {
      try {
        this._event = new EventClass(parser, options, zongji);
      } catch (err) {
        // Record error occurence but suppress until handled
        this._error = err;
      }
    }
  };

  BinlogHeader.prototype.getEvent = function() {
    // Ready to handle the error now
    if (this._error) throw this._error;
    return this._event;
  };

  return BinlogHeader;
};

