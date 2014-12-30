# ZongJi
A MySQL binlog listener running on Node.js.

ZongJi (踪迹) is pronounced as `zōng jì` in Chinese.

This package is a "pure JS" implementation based on [`node-mysql`](https://github.com/felixge/node-mysql). Since v0.2.0, The native part (which was written in C++) has been dropped.

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
  log_bin          = /usr/local/var/log/mysql/mysql-bin.log
  binlog_do_db     = employees   # optional
  expire_logs_days = 10          # optional
  max_binlog_size  = 100M        # optional

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
`set`       | `options` | Change options after `start()`
`on`        | `eventName`, `handler` | Add a listener to the `binlog` or `error` event. Each handler function accepts one argument.

**Options available:**

Option Name | Type | Description
------------|------|-------------------------------
`includeEvents` | `[string]` | Array of event names to include<br><br>**Example:** `['writerows', 'updaterows', 'deleterows']`
`excludeEvents` | `[string]` | Array of event names to exclude<br><br>**Example:** `['rotate', 'tablemap']`
`includeSchema` | `object` | Object describing which databases and tables to include (Only for row events). Use database names as the key and pass an array of table names or `true` (for the entire database).<br><br>**Example:** ```{ 'my_database': ['allow_table', 'another_table'], 'another_db': true }```
`excludeSchema` | `object` | Object describing which databases and tables to exclude (Same format as `includeSchema`)<br><br>**Example:** ```{ 'other_db': ['disallowed_table'], 'ex_db': true }```

* `excludeSchema` and `excludeEvents` take precedence over `includeSchema` and `includeEvents`, respectively.

## Important Notes

* :star2: [All types allowed by `node-mysql`](https://github.com/felixge/node-mysql#type-casting) are supported by this package.
* :speak_no_evil: While 64-bit integers in MySQL (`BIGINT` type) allow values in the range of 2<sup>64</sup> (± ½ × 2<sup>64</sup> for signed values), Javascript's internal storage of numbers limits values to 2<sup>53</sup>, making the allowed range of `BIGINT` fields only `-9007199254740992` to `9007199254740992`. Unsigned 64-bit integers must also not exceed `9007199254740992`.
* :point_right: `TRUNCATE` statement does not cause corresponding `DeleteRows` event. Use unqualified `DELETE FROM` for same effect.

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

## License
MIT
