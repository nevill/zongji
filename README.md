# ZongJi [![Build Status](https://travis-ci.org/nevill/zongji.svg?branch=master)](https://travis-ci.org/nevill/zongji)
A MySQL binlog listener running on Node.js.

ZongJi (踪迹) is pronounced as `zōng jì` in Chinese.

This package is a pure JS implementation based on [`mysql`](https://github.com/mysqljs/mysql). It has been tested to work in MySQL 5.5, 5.6, and 5.7.

# Latest Release

The latest release is v0.5.0, only supports Node.js from v8.

v0.4.7 is the last release which supports Node.js v4.x.

## Quick Start

```javascript
let zongji = new ZongJi({ /* ... MySQL Connection Settings ... */ });

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

* Requires Node.js v8+

  ```bash
  $ npm install zongji
  ```

* Enable MySQL binlog in `my.cnf`, restart MySQL server after making the changes.
  > From [MySQL 5.6](https://dev.mysql.com/doc/refman/5.6/en/replication-options-binary-log.html), binlog checksum is enabled by default. Zongji can work with it, but it doesn't really verify it.

  ```
  # Must be unique integer from 1-2^32
  server-id        = 1
  # Row format required for ZongJi
  binlog_format    = row
  # Directory must exist. This path works for Linux. Other OS may require
  #   different path.
  log_bin          = /var/log/mysql/mysql-bin.log

  binlog_do_db     = employees   # Optional, limit which databases to log
  expire_logs_days = 10          # Optional, purge old logs
  max_binlog_size  = 100M        # Optional, limit log size
  ```
* Create an account with replication privileges, e.g. given privileges to account `zongji` (or any account that you use to read binary logs)

  ```sql
  GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'zongji'@'localhost'
  ```

## ZongJi Class

The `ZongJi` constructor accepts one argument of either:

* An object containing MySQL connection details in the same format as used by [package mysql](https://npm.im/mysql)
* Or, a [mysql](https://npm.im/mysql) `Connection` or `Pool` object that will be used for querying column information.

If a `Connection` or `Pool` object is passed to the constructor, it will not be destroyed/ended by Zongji's `stop()` method.

If there is a `dateStrings` `mysql` configuration option in the connection details or connection, `ZongJi` will follow it.

Each instance includes the following methods:

Method Name | Arguments | Description
------------|-----------|------------------------
`start`     | `options` | Start receiving replication events, see options listed below
`stop`      | *None*    | Disconnect from MySQL server, stop receiving events
`on`        | `eventName`, `handler` | Add a listener to the `binlog` or `error` event. Each handler function accepts one argument.

Some events can be emitted in different phases:

Event Name | Description
-----------|------------------------
`ready`    | This event is occurred right after ZongJi successfully established a connection, setup slave status, and set binlog position.
`binlog`   | Once a binlog is received and passes the filter, it will bubble up with this event.
`error`    | Every error will be caught by this event.
`stopped`  | Emitted when ZongJi connection is stopped (ZongJi#stop is called).

**Options available:**

Option Name | Type | Description
------------|------|-------------------------------
`serverId`  | `integer` | [Unique number (1 - 2<sup>32</sup>)](http://dev.mysql.com/doc/refman/5.0/en/replication-options.html#option_mysqld_server-id) to identify this replication slave instance. Must be specified if running more than one instance of ZongJi. Must be used in `start()` method for effect.<br>**Default:** `1`
`startAtEnd` | `boolean` | Pass `true` to only emit binlog events that occur after ZongJi's instantiation. Must be used in `start()` method for effect.<br>**Default:** `false`
`filename` | `string` | Begin reading events from this binlog file. If specified together with `position`, will take precedence over `startAtEnd`.
`position` | `integer` | Begin reading events from this position. Must be included with `filename`.
`includeEvents` | `[string]` | Array of event names to include<br>**Example:** `['writerows', 'updaterows', 'deleterows']`
`excludeEvents` | `[string]` | Array of event names to exclude<br>**Example:** `['rotate', 'tablemap']`
`includeSchema` | `object` | Object describing which databases and tables to include (Only for row events). Use database names as the key and pass an array of table names or `true` (for the entire database).<br>**Example:** ```{ 'my_database': ['allow_table', 'another_table'], 'another_db': true }```
`excludeSchema` | `object` | Object describing which databases and tables to exclude (Same format as `includeSchema`)<br>**Example:** ```{ 'other_db': ['disallowed_table'], 'ex_db': true }```

* By default, all events and schema are emitted.
* `excludeSchema` and `excludeEvents` take precedence over `includeSchema` and `includeEvents`, respectively.

**Supported Binlog Events:**

Event name  | Description
------------|---------------
`unknown`   | Catch any other events
`query`     | [Insert/Update/Delete Query](http://dev.mysql.com/doc/internals/en/query-event.html)
`intvar`    | [Autoincrement and LAST_INSERT_ID](https://dev.mysql.com/doc/internals/en/intvar-event.html)
`rotate`    | [New Binlog file](http://dev.mysql.com/doc/internals/en/rotate-event.html) Not required to be included to rotate to new files, but it is required to be included in order to keep the `filename` and `position` properties updated with current values for [graceful restarting on errors](https://gist.github.com/numtel/5b37b2a7f47b380c1a099596c6f3db2f).
`format`    | [Format Description](http://dev.mysql.com/doc/internals/en/format-description-event.html)
`xid`       | [Transaction ID](http://dev.mysql.com/doc/internals/en/xid-event.html)
`tablemap`  | Before any row event (must be included for any other row events)
`writerows` | Rows inserted, row data array available as `rows` property on event object
`updaterows` | Rows changed, row data array available as `rows` property on event object
`deleterows` | Rows deleted, row data array available as `rows` property on event object

**Event Methods**

Neither method requires any arguments.

Name   | Description
-------|---------------------------
`dump` | Log a description of the event to the console
`getEventName` | Return the name of the event

## Important Notes

* :star2: [All types allowed by `mysql`](https://github.com/mysqljs/mysql#type-casting) are supported by this package.
* :speak_no_evil: 64-bit integer is supported via package big-integer(see #108). If an integer is within the safe range of JS number (-2^53, 2^53), a Number object will returned, otherwise, will return as String.
* :point_right: `TRUNCATE` statement does not cause corresponding `DeleteRows` event. Use unqualified `DELETE FROM` for same effect.
* When using fractional seconds with `DATETIME` and `TIMESTAMP` data types in MySQL > 5.6.4, only millisecond precision is available due to the limit of Javascript's `Date` object.

## Run Tests

* install [Docker](https://www.docker.com/community-edition#download)
* run `docker-compose up` and then `./docker-test.sh`

## Reference

I learnt many things from following resources while making ZongJi.

* https://github.com/mysqljs/mysql
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
