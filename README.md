# ZongJi
A MySQL binlog listener running on Node.js.

ZongJi (踪迹) is pronounced as `zōng jì` in Chinese.

## Work In Progress

Not all the data types in Mysql are supported, I will keep working on that.

Implemented | Still To Do
------------|-------------------------------------
<ul><li>TINY<li>SHORT<li>LONG<li>**INT24**<li>**LONGLONG**<li>**FLOAT**<li>**DOUBLE**<li>**NEWDECIMAL**<li>**SET**<li>VAR_STRING✱<li>VARCHAR✱<li>STRING✱<li>**TINY_BLOB**<li>**MEDIUM_BLOB**<li>**LONG_BLOB**<li>**BLOB**</ul> | <ul><li>~~DECIMAL~~ *Deprecated as of MySQL 5.0.3*<li>~~NULL~~ *Supported otherwise*<li>TIMESTAMP<li>DATE<li>TIME<li>DATETIME<li>YEAR<li>NEWDATE<li>BIT<li>ENUM<li>GEOMETRY</ul>

✱ Still needs test

**Notes**

* `NULL` value support requires a bitmap to each field. Due to current usage of Javascript's bitwise operators and their inability to handle integers greater than 32-bits, the current maximum number of fields on a table is 32.
* While 64-bit integers in MySQL (`bigint` type) allow values in the range of 2<sup>64</sup> (± ½ × 2<sup>64</sup> for signed values), Javascript's internal storage of numbers limits values to 2<sup>53</sup>, making the allowed range of `bigint` fields only `-9007199254740992` to `9007199254740992`. Unsigned 64-bit integers must also not exceed `9007199254740992`.

## Rewrite

Since v0.2.0, The native part(which is written in C++) has been dropped. It is now a pure JS implementation based on [node-mysql](https://github.com/felixge/node-mysql), or you can say it is a patch on `node-mysql`.

## Prerequisite

* Node.js v0.10+
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
