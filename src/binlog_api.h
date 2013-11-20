#ifndef BINLOG_API_H_
#define BINLOG_API_H_

/**
 * Error codes.
 */
enum Error_code {
  ERR_OK = 0,                                   /* All OK */
  ERR_EOF,                                      /* End of file */
  ERR_FAIL,                                     /* Unspecified failure */
  ERR_CHECKSUM_ENABLED,
  ERR_CHECKSUM_QUERY_FAIL,
  ERR_CONNECT,
  ERR_BINLOG_VERSION,
  ERR_PACKET_LENGTH,
  ERR_MYSQL_QUERY_FAIL,
  ERROR_CODE_COUNT
};

#endif
