var EventCode = {
  UNKNOWN_EVENT: 0x00,
  START_EVENT_V3: 0x01,
  QUERY_EVENT: 0x02,
  STOP_EVENT: 0x03,
  ROTATE_EVENT: 0x04,
  INTVAR_EVENT: 0x05,
  LOAD_EVENT: 0x06,
  SLAVE_EVENT: 0x07,
  CREATE_FILE_EVENT: 0x08,
  APPEND_BLOCK_EVENT: 0x09,
  EXEC_LOAD_EVENT: 0x0a,
  DELETE_FILE_EVENT: 0x0b,
  NEW_LOAD_EVENT: 0x0c,
  RAND_EVENT: 0x0d,
  USER_VAR_EVENT: 0x0e,
  FORMAT_DESCRIPTION_EVENT: 0x0f,
  XID_EVENT: 0x10,
  BEGIN_LOAD_QUERY_EVENT: 0x11,
  EXECUTE_LOAD_QUERY_EVENT: 0x12,
  TABLE_MAP_EVENT: 0x13,
  PRE_GA_DELETE_ROWS_EVENT: 0x14,
  PRE_GA_UPDATE_ROWS_EVENT: 0x15,
  PRE_GA_WRITE_ROWS_EVENT: 0x16,
  DELETE_ROWS_EVENT_V1: 0x19,
  UPDATE_ROWS_EVENT_V1: 0x18,
  WRITE_ROWS_EVENT_V1: 0x17,
  INCIDENT_EVENT: 0x1a,
  HEARTBEAT_LOG_EVENT: 0x1b,
  IGNORABLE_LOG_EVENT: 0x1c,
  ROWS_QUERY_LOG_EVENT: 0x1d,
  WRITE_ROWS_EVENT_V2: 0x1e,
  UPDATE_ROWS_EVENT_V2: 0x1f,
  DELETE_ROWS_EVENT_V2: 0x20,
  GTID_LOG_EVENT: 0x21,
  ANONYMOUS_GTID_LOG_EVENT: 0x22,
  PREVIOUS_GTIDS_LOG_EVENT: 0x23
};

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

function BinlogHeader() {
}

BinlogHeader.prototype.parse = function(parser) {
  console.log('parsing binlog event header ...');
  parser.parseUnsignedNumber(1);
  this.timestamp = parser.parseUnsignedNumber(4) * 1000;
  this.eventType = parser.parseUnsignedNumber(1);
  this.serverId = parser.parseUnsignedNumber(4);
  this.eventLength = parser.parseUnsignedNumber(4);
  this.nextPosition = parser.parseUnsignedNumber(4);

  this.eventName = getEventName(this.eventType);
};

module.exports = BinlogHeader;
