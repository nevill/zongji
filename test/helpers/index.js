const mysql = require('mysql');

const settings = require('../settings/mysql');
const querySequence = require('./querySequence');

const SCHEMA_NAME = settings.connection.database;

exports.SCHEMA_NAME = SCHEMA_NAME;

exports.init = function(done) {
  const connObj = {...settings.connection};
  // database doesn't exist at this time
  delete connObj.database;
  const conn = mysql.createConnection(connObj);

  querySequence(
    conn,
    [
      'SET GLOBAL sql_mode = \'' + settings.sessionSqlMode + '\'',
      `DROP DATABASE IF EXISTS ${SCHEMA_NAME}`,
      `CREATE DATABASE ${SCHEMA_NAME}`,
      `USE ${SCHEMA_NAME}`,
      'RESET MASTER',
      // 'SELECT VERSION() AS version'
    ],
    error => {
      conn.destroy();
      done(error);
    }
  );
};

exports.execute = function(queries, done) {
  const conn = mysql.createConnection(settings.connection);
  querySequence(
    conn,
    queries,
    (error, result) => {
      conn.destroy();
      done(error, result);
    }
  );
};

const checkVersion = function(expected, actual) {
  const parts = expected.split('.').map(part => parseInt(part, 10));
  for (let i = 0; i < parts.length; i++) {
    if (actual[i] == parts[i]) {
      continue;
    }
    return actual[i] > parts[i];
  }
  return true;
};

exports.requireVersion = function(expected, done) {
  const connObj = {...settings.connection};
  // database doesn't exist at this time
  delete connObj.database;
  const conn = mysql.createConnection(connObj);
  querySequence(conn, ['SELECT VERSION() AS version'], (err, results) => {
    conn.destroy();

    if (err) {
      throw err;
    }

    let ver = results[results.length - 1][0]
      .version.split('-')[0]
      .split('.')
      .map(part => parseInt(part, 10));

    if (checkVersion(expected, ver)) {
      done();
    }
  });
};

let id = 100;
exports.serverId = function() {
  id ++;
  return id;
};

exports.strRepeat = function (pattern, count) {
  if (count < 1) return '';
  let result = '';
  let pos = 0;
  while (pos < count) {
    result += pattern.replace(/##/g, pos);
    pos++;
  }
  return result;
};
