# ZongJi [![Build Status](https://travis-ci.org/nevill/zongji.svg?branch=master)](https://travis-ci.org/nevill/zongji)
A MySQL binlog listener running on Node.js.

ZongJi (踪迹) is pronounced as `zōng jì` in Chinese.

This package is a "pure JS" implementation based on [`node-mysql`](https://github.com/felixge/node-mysql). Since v0.2.0, The native part (which was written in C++) has been dropped.

This package has been tested with MySQL server 5.5.40 and 5.6.19. All MySQL server versions >= 5.1.15 are supported.

## Quick Start

```javascript
var zongji = new ZongJi({ /* ... MySQL Connection Settings ... */ });

// Each change to the replication log results in an event
zongji.on('binlog', function(evt) {
  evt.dump();
});

// Binlog must be started, optionally pass in filters
zongji.start({
  includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows']
});
```

For a complete implementation see [`example.js`](example.js)...

## Installation

* Requires Node.js v0.10+

  ```bash
  $ npm install zongji
  ```

* Enable MySQL binlog in `my.cnf`, restart MySQL server after making the changes.
  > From [MySQL 5.6](https://dev.mysql.com/doc/refman/5.6/en/replication-options-binary-log.html), binlog checksum is enabled by default. Zongji can work with it, but it doesn't really verify it.

  ```
  # binlog config
  server-id        = 1
  log_bin          = /var/log/mysql/mysql-bin.log
  expire_logs_days = 10            # optional
  max_binlog_size  = 100M          # optional

  # Very important if you want to receive write, update and delete row events
  binlog_format    = row
  ```
* Create an account with replication privileges, e.g. given privileges to account `zongji` (or any account that you use to read binary logs)

  ```sql
  GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'zongji'@'localhost'
  ```

## ZongJi Class

The `ZongJi` constructor accepts one argument: an object containg MySQL connection details in the same format as used by `node-mysql`.

Each instance includes the following methods:

Method Name | Arguments | Description
------------|-----------|------------------------
`start`     | `options` | Start receiving replication events
`stop`      | *None*    | Disconnect from MySQL server, stop receiving events
`set`       | `options` | Change options after `start()`
`on`        | `eventName`, `handler` | Add a listener to the `binlog` or `error` event. Each handler function accepts one argument.

**Options available:**

Option Name | Type | Description
------------|------|-------------------------------
`serverId`  | `integer` | [Unique number (1 - 2<sup>32</sup>)](http://dev.mysql.com/doc/refman/5.0/en/replication-options.html#option_mysqld_server-id) to identify this replication slave instance. Must be specified if running more than one instance of ZongJi. Must be used in `start()` method for effect.<br>**Default:** `1`
`startAtEnd` | `boolean` | Pass `true` to only emit binlog events that occur after ZongJi's instantiation. Must be used in `start()` method for effect.<br>**Default:** `false`
`includeEvents` | `[string]` | Array of event names to include<br>**Example:** `['writerows', 'updaterows', 'deleterows']`
`excludeEvents` | `[string]` | Array of event names to exclude<br>**Example:** `['rotate', 'tablemap']`
`includeSchema` | `object` | Object describing which databases and tables to include (Only for row events). Use database names as the key and pass an array of table names or `true` (for the entire database).<br>**Example:** ```{ 'my_database': ['allow_table', 'another_table'], 'another_db': true }```
`excludeSchema` | `object` | Object describing which databases and tables to exclude (Same format as `includeSchema`)<br>**Example:** ```{ 'other_db': ['disallowed_table'], 'ex_db': true }```

* By default, all events and schema are emitted.
* `excludeSchema` and `excludeEvents` take precedence over `includeSchema` and `includeEvents`, respectively.

**Supported Events:**

Event name  | Description
------------|---------------
`unknown`   | Catch any other events
`query`     | [Insert/Update/Delete Query](http://dev.mysql.com/doc/internals/en/query-event.html)
`rotate`    | [New Binlog file](http://dev.mysql.com/doc/internals/en/rotate-event.html) (not required to be included to rotate to new files)
`format`    | [Format Description](http://dev.mysql.com/doc/internals/en/format-description-event.html)
`xid`       | [Transaction ID](http://dev.mysql.com/doc/internals/en/xid-event.html)
`tablemap`  | Before any row event (must be included for any other row events)
`writerows` | Rows inserted
`updaterows` | Rows changed
`deleterows` | Rows deleted

**Event Methods**

Neither method requires any arguments.

Name   | Description
-------|---------------------------
`dump` | Log a description of the event to the console
`getEventName` | Return the name of the event

## Important Notes

* :star2: [All types allowed by `node-mysql`](https://github.com/felixge/node-mysql#type-casting) are supported by this package.
* :speak_no_evil: While 64-bit integers in MySQL (`BIGINT` type) allow values in the range of 2<sup>64</sup> (± ½ × 2<sup>64</sup> for signed values), Javascript's internal storage of numbers limits values to 2<sup>53</sup>, making the allowed range of `BIGINT` fields only `-9007199254740992` to `9007199254740992`. Unsigned 64-bit integers must also not exceed `9007199254740992`.
* :point_right: `TRUNCATE` statement does not cause corresponding `DeleteRows` event. Use unqualified `DELETE FROM` for same effect.
* When using fractional seconds with `DATETIME` and `TIMESTAMP` data types in MySQL > 5.6.4, only millisecond precision is available due to the limit of Javascript's `Date` object.

## Run Tests

* Configure MySQL in `test/settings/mysql.js`
* Run `npm test`

## Reference

I learnt many things from following resources while making ZongJi.

* https://github.com/felixge/node-mysql
* https://github.com/felixge/faster-than-c/
* http://intuitive-search.blogspot.co.uk/2011/07/binary-log-api-and-replication-listener.html
* https://github.com/Sannis/node-mysql-libmysqlclient
* https://kkaefer.com/node-cpp-modules/
* http://dev.mysql.com/doc/internals/en/replication-protocol.html
* http://www.cs.wichita.edu/~chang/lecture/cs742/program/how-mysql-c-api.html
* https://github.com/jeremycole/mysql_binlog (Ruby implemenation of MySQL binlog parser)
* http://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html

## License
MIT
