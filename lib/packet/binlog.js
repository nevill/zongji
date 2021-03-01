const getEventClass = require('../code_map').getEventClass;

//TODO Don't depend on zongji instance here
module.exports = function initBinlogPacketClass(zongji) {

  class BinlogPacket {

    *_process(parser) {
      // uint8_t  marker; // always 0 or 0xFF
      // uint32_t timestamp;
      // uint8_t  type_code;
      // uint32_t server_id;
      // uint32_t event_length;
      // uint32_t next_position;
      // uint16_t flags;
      parser.parseUnsignedNumber(1);

      const timestamp = parser.parseUnsignedNumber(4) * 1000;
      const eventType = parser.parseUnsignedNumber(1);
      parser.parseUnsignedNumber(4); // serverId
      const eventLength = parser.parseUnsignedNumber(4);
      const nextPosition = parser.parseUnsignedNumber(4);
      parser.parseUnsignedNumber(2); // flags

      const options = {
        timestamp: timestamp,
        nextPosition: nextPosition,
        size: eventLength - BinlogPacket.Length,
        eventType: eventType,
      };

      const EventClass = getEventClass(eventType);
      this.eventName = EventClass.name;

      yield;

      try {
        this._event = new EventClass(parser, options, zongji);
      } catch (err) {
        // Record error occurence but suppress until handled
        this._error = err;
      }
    }

    // interface will be called, see mysql/lib/protocol/Protocol
    parse(parser) {
      this._processor = this._process(parser);
      this._processor.next();
    }

    getEvent() {
      this._processor.next();
      // Ready to handle the error now
      if (this._error) throw this._error;
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
