var getEventClass = require('../code_map').getEventClass;

//TODO Don't depend on zongji instance here
module.exports = function initBinlogPacketClass(zongji) {

  class BinlogPacket {
    // interface will be called, see mysql/lib/protocol/Protocol
    parse(parser) {
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
      parser.parseUnsignedNumber(4); // serverId
      var eventLength = parser.parseUnsignedNumber(4);
      var nextPosition = parser.parseUnsignedNumber(4);
      parser.parseUnsignedNumber(2); // flags

      var options = {
        timestamp: timestamp,
        nextPosition: nextPosition,
        size: eventLength - BinlogPacket.Length,
        eventType: eventType,
      };

      var EventClass = getEventClass(eventType);
      this.eventName = EventClass.name;

      //TODO use generator to split this step, if it's a event to be filtered, shouldn't continue
      try {
        this._event = new EventClass(parser, options, zongji);
      } catch (err) {
        // Record error occurence but suppress until handled
        this._error = err;
      }
    }

    getEvent() {
      // Ready to handle the error now
      if (this._error) throw this._error;

      // Check event filtering
      if (zongji._skipEvent(this.eventName.toLowerCase())) {
        delete this._event;
      }
      return this._event;
    }
  }

  // header length doesn't count marker
  BinlogPacket.Length = 19;

  if (zongji.useChecksum) {
    BinlogPacket.Length = 19 + 4;
  }

  return BinlogPacket;
};
