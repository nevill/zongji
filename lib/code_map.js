const events = require('./binlog_event');
const rowsEvents = require('./rows_event');

const CodeEvent = [
  'UNKNOWN_EVENT',
  'START_EVENT_V3',
  'QUERY_EVENT',
  'STOP_EVENT',
  'ROTATE_EVENT',
  'INTVAR_EVENT',
  'LOAD_EVENT',
  'SLAVE_EVENT',
  'CREATE_FILE_EVENT',
  'APPEND_BLOCK_EVENT',
  'EXEC_LOAD_EVENT',
  'DELETE_FILE_EVENT',
  'NEW_LOAD_EVENT',
  'RAND_EVENT',
  'USER_VAR_EVENT',
  'FORMAT_DESCRIPTION_EVENT',
  'XID_EVENT',
  'BEGIN_LOAD_QUERY_EVENT',
  'EXECUTE_LOAD_QUERY_EVENT',
  'TABLE_MAP_EVENT',
  'PRE_GA_DELETE_ROWS_EVENT',
  'PRE_GA_UPDATE_ROWS_EVENT',
  'PRE_GA_WRITE_ROWS_EVENT',
  'WRITE_ROWS_EVENT_V1',
  'UPDATE_ROWS_EVENT_V1',
  'DELETE_ROWS_EVENT_V1',
  'INCIDENT_EVENT',
  'HEARTBEAT_LOG_EVENT',
  'IGNORABLE_LOG_EVENT',
  'ROWS_QUERY_LOG_EVENT',
  'WRITE_ROWS_EVENT_V2',
  'UPDATE_ROWS_EVENT_V2',
  'DELETE_ROWS_EVENT_V2',
  'GTID_LOG_EVENT',
  'ANONYMOUS_GTID_LOG_EVENT',
  'PREVIOUS_GTIDS_LOG_EVENT'
];

const EventClass = {
  UNKNOWN_EVENT: events.Unknown,
  QUERY_EVENT: events.Query,
  INTVAR_EVENT: events.IntVar,
  ROTATE_EVENT: events.Rotate,
  FORMAT_DESCRIPTION_EVENT: events.Format,
  XID_EVENT: events.Xid,

  TABLE_MAP_EVENT: events.TableMap,
  DELETE_ROWS_EVENT_V1: rowsEvents.DeleteRows,
  UPDATE_ROWS_EVENT_V1: rowsEvents.UpdateRows,
  WRITE_ROWS_EVENT_V1: rowsEvents.WriteRows,
  WRITE_ROWS_EVENT_V2: rowsEvents.WriteRows,
  UPDATE_ROWS_EVENT_V2: rowsEvents.UpdateRows,
  DELETE_ROWS_EVENT_V2: rowsEvents.DeleteRows,
};

exports.getEventClass = function(code) {
  return EventClass[CodeEvent[code]] || events.Unknown;
};
