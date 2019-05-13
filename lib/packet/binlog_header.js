var getEventClass = require('../code_map').getEventClass;

//TODO Don't depend on zongji instance here
module.exports = function initBinlogHeaderClass(zongji) {

  function BinlogHeader() {}

  // header length doesn't count marker
  BinlogHeader.Length = 19;

  if (zongji.useChecksum) {
    BinlogHeader.Length = 19 + 4;
  }

  // interface will be called, see mysql/lib/protocol/Protocol
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

    var options = {
      timestamp: timestamp,
      nextPosition: nextPosition,
      size: eventLength - BinlogHeader.Length,
      eventType: eventType,
    };

    var EventClass = getEventClass(eventType);
    this.eventName = EventClass.name;
    try {
      this._event = new EventClass(parser, options, zongji);
    } catch (err) {
      // Record error occurence but suppress until handled
      this._error = err;
    }
  };

  BinlogHeader.prototype.getEvent = function() {
    // Ready to handle the error now
    if (this._error) throw this._error;

    // Check event filtering
    if (zongji._skipEvent(this.eventName.toLowerCase())) {
      delete this._event;
    }
    return this._event;
  };

  return BinlogHeader;
};

