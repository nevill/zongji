# ZongJi
A MySQL binlog listener running on Node.js.

ZongJi (踪迹) is pronounced as `zōng jì` in Chinese.

## Work In Progress

Not all the data types in Mysql are supported, I will keep working on that.

Implemented | Still To Do
------------|-------------------------------------
<ul><li>TINY✱<li>SHORT✱<li>**YEAR**✱<li>LONG✱<li>**INT24**✱<li>**LONGLONG**✱<li>**FLOAT**<li>**DOUBLE**<li>**NEWDECIMAL**<li>**SET**<li>VAR_STRING✱<li>VARCHAR✱<li>STRING✱<li>**TINY_BLOB**<li>**MEDIUM_BLOB**<li>**LONG_BLOB**<li>**BLOB**</ul> | <ul><li>~~DECIMAL~~ *Deprecated as of MySQL 5.0.3*<li>NULL<li>TIMESTAMP<li>DATE<li>TIME<li>DATETIME<li>NEWDATE<li>BIT<li>ENUM<li>GEOMETRY</ul>

✱ Still needs test

## Rewrite

Since v0.2.0, The native part(which is written in C++) has been dropped. It is now a pure JS implementation based on [node-mysql](https://github.com/felixge/node-mysql), or you can say it is a patch on `node-mysql`.

## Prerequisite

* Node.js v0.10+
* enable MySQL binlog in `my.cnf`, restart MySQL server after making the changes.
  > From [MySQL 5.6](https://dev.mysql.com/doc/refman/5.6/en/replication-options-binary-log.html), binlog checksum is enabled by default. Zongji can work with it, but it doesn't really verify it.

  ```
  # binlog config
  server-id = 1
  log_bin = /usr/local/var/log/mysql/mysql-bin.log
  binlog_do_db = employees
  expire_logs_days = 10
  max_binlog_size  = 100M

  #Very important if you want to receive write, update and delete row events
  binlog_format    = row
  ```
* create an account with replication privileges, e.g. given privileges to account 'zongji'
  ```GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'zongji'@'localhost'```

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

## License
MIT
