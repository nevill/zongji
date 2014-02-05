var binlogEvent = require('./binlog_event');
var BufferReader = require('./reader').BufferReader;
var EventCode = require('./code_map').EventCode;
var EventClass = require('./code_map').EventClass;

Object.keys(binlogEvent).forEach(function(name) {
  exports[name] = binlogEvent[name];
});

function getEventName(code) {
  var result = 'UNKNOWN_EVENT';
  Object.keys(EventCode).forEach(function(name) {
    if (EventCode[name] === code) {
      result = name;
      return;
    }
  });
  return result;
}

function getEventClass(code) {
  var eventName = getEventName(code);
  var result = '';
  Object.keys(EventClass).forEach(function(className) {
    if (eventName === className) {
      result = EventClass[className];
      return;
    }
  });
  return result;
}

function parseHeader(buffer) {
  // uint8_t  marker; // always 0 or 0xFF
  // uint32_t timestamp;
  // uint8_t  type_code;
  // uint32_t server_id;
  // uint32_t event_length;
  // uint32_t next_position;
  // uint16_t flags;

  var reader = new BufferReader(buffer);
  reader.readUInt8();

  // convert from milliseconds to seconds
  var timestamp = reader.readUInt32() * 1000;
  var eventType = reader.readUInt8();
  var serverId = reader.readUInt32();
  var eventLength = reader.readUInt32();
  var nextPosition = reader.readUInt32();
  var flags = reader.readUInt16();

  // headerLength doesn't count marker
  var headerLength = reader.position - 1;
  // for MySQL 5.6 and binlog-checksum = CRC32
  // if (useChecksum) {
  //   headerLength += 4;
  // }
  var eventSize = eventLength - headerLength;
  var binlogBuffer = buffer.slice(reader.position);

  return [eventType, binlogBuffer, timestamp, nextPosition, eventSize];
}

exports.parseHeader = parseHeader;

exports.create = function(buffer) {
  var params = parseHeader(buffer);
  var typeCode = params.shift();
  var className = getEventClass(typeCode);
  return binlogEvent[className].apply(null, params);
};
