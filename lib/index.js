var binlogEvent = require('./binlog_event');
var BufferReader = require('./reader').BufferReader;

var Rotate = binlogEvent.Rotate,
    Format = binlogEvent.Format,
    Query = binlogEvent.Query,
    Xid = binlogEvent.Xid,
    Unknown = binlogEvent.Unknown;

var UNKNOWN_EVENT = 0x00,
    START_EVENT_V3 = 0x01,
    QUERY_EVENT = 0x02,
    STOP_EVENT = 0x03,
    ROTATE_EVENT = 0x04,
    INTVAR_EVENT = 0x05,
    LOAD_EVENT = 0x06,
    SLAVE_EVENT = 0x07,
    CREATE_FILE_EVENT = 0x08,
    APPEND_BLOCK_EVENT = 0x09,
    EXEC_LOAD_EVENT = 0x0a,
    DELETE_FILE_EVENT = 0x0b,
    NEW_LOAD_EVENT = 0x0c,
    RAND_EVENT = 0x0d,
    USER_VAR_EVENT = 0x0e,
    FORMAT_DESCRIPTION_EVENT = 0x0f,
    XID_EVENT = 0x10,
    BEGIN_LOAD_QUERY_EVENT = 0x11,
    EXECUTE_LOAD_QUERY_EVENT = 0x12,
    TABLE_MAP_EVENT = 0x13,
    PRE_GA_DELETE_ROWS_EVENT = 0x14,
    PRE_GA_UPDATE_ROWS_EVENT = 0x15,
    PRE_GA_WRITE_ROWS_EVENT = 0x16,
    DELETE_ROWS_EVENT_V1 = 0x19,
    UPDATE_ROWS_EVENT_V1 = 0x18,
    WRITE_ROWS_EVENT_V1 = 0x17,
    INCIDENT_EVENT = 0x1a,
    HEARTBEAT_LOG_EVENT = 0x1b,
    IGNORABLE_LOG_EVENT = 0x1c,
    ROWS_QUERY_LOG_EVENT = 0x1d,
    WRITE_ROWS_EVENT_V2 = 0x1e,
    UPDATE_ROWS_EVENT_V2 = 0x1f,
    DELETE_ROWS_EVENT_V2 = 0x20,
    GTID_LOG_EVENT = 0x21,
    ANONYMOUS_GTID_LOG_EVENT = 0x22,
    PREVIOUS_GTIDS_LOG_EVENT = 0x23;

var eventMap = [
  { code: ROTATE_EVENT, type: Rotate },
  { code: FORMAT_DESCRIPTION_EVENT, type: Format },
  { code: QUERY_EVENT, type: Query },
  { code: XID_EVENT, type: Xid },
];

function getEventTypeByCode(code) {
  var result = Unknown;
  for (var i = eventMap.length - 1; i >= 0; i--) {
    if (eventMap[i].code === code) {
      result = eventMap[i].type;
      break;
    }
  }
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

  return [binlogBuffer, eventType, timestamp, nextPosition, eventSize];
}

exports.parseHeader = parseHeader;

exports.create = function(buffer) {
  var params = parseHeader(buffer);
  var claz = getEventTypeByCode(params[1]);
  return claz.apply(null, params);
};

Object.keys(binlogEvent).forEach(function(name) {
  exports[name] = binlogEvent[name];
});
