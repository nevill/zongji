// This file contains functions useful for converting bare numbers into
// JavaScript Date objects, or DATE/DATETIME/TIMESTAMP strings, according to the
// dateStrings option.  The dateStrings option should be read from
// zongji.connection.config, where zongji is the current instance of the ZongJi
// object.  The dateStrings option is interpreted the same as in node-mysql.
var common = require('./common'); // used only for common.zeroPad

// dateStrings are used only if the dateStrings option is true, or is an array
// containing the sql type name string, 'DATE', 'DATETIME', or 'TIMESTAMP'.
// This follows the documentation of the dateStrings option in node-mysql.
var useDateStringsForType = function(dateStrings, sqlTypeString) {
  return dateStrings &&
           (dateStrings === true ||
            dateStrings.indexOf && dateStrings.indexOf(sqlTypeString) > -1);
};

// fraction is the fractional second object from readTemporalFraction().
// returns '' or a '.' followed by fraction.precision digits, like '.123'
var getFractionString = exports.getFractionString = function(fraction) {
  return fraction ?
         '.' + common.zeroPad(fraction.value, fraction.precision) :
         '';
};

// 1950-00-00 and the like are perfectly valid Mysql dateStrings.  A 0 portion
// of a date is essentially a null part of the date, so we should keep it.
// year, month, and date must be integers >= 0.  January is month === 1.
var getDateString = exports.getDateString = function(year, month, date) {
  return common.zeroPad(year, 4) + '-' +
         common.zeroPad(month, 2) + '-' +
         common.zeroPad(date, 2);
};

// Date object months are 1 less than Mysql months, and we need to filter 0.
// If we don't filter 0, 2017-00-01 will become the javascript Date 2016-12-01,
// which is not what it means.  It means 2017-NULL-01, but the Date object
// cannot handle it, so we want to return an invalid month, rather than a
// subtracted month.
var jsMonthFromMysqlMonth = function(month) {
  return month > 0 ? month - 1 : undefined;
};

// Returns a new Date object or Mysql dateString, following the dateStrings
// option.  With the dateStrings option, it can output valid Mysql DATE strings
// representing values that cannot be represented by a Date object, such as
// values with a null part like '1950-00-04', or a zero date '0000-00-00'.
exports.getDate = function(dateStrings, // node-mysql dateStrings option
                           year,
                           month,       // January === 1
                           date
                          )
{
  if (!useDateStringsForType(dateStrings, 'DATE')) {
    return new Date(year,
                    jsMonthFromMysqlMonth(month),
                    date);
  }
  return getDateString(year, month, date);
};

// Returns a new Date object or Mysql dateString, following the dateStrings
// option.  Fraction is an optional parameter that comes from
// readTemporalFraction().  Mysql dateStrings are needed for microsecond
// precision, or to represent '0000-00-00 00:00:00'.
exports.getDateTime = function(dateStrings, // node-mysql dateStrings option
                               year,
                               month,       // January === 1
                               date,
                               hour,
                               minute,
                               second,
                               fraction     // optional fractional second object
                              )
{
  if (!useDateStringsForType(dateStrings, 'DATETIME')) {
    return new Date(year,
                    jsMonthFromMysqlMonth(month),
                    date,
                    hour,
                    minute,
                    second,
                    fraction ? fraction.milliseconds : 0);
  }
  return getDateString(year, month, date) + ' ' +
         common.zeroPad(hour, 2) + ':' +
         common.zeroPad(minute, 2) + ':' +
         common.zeroPad(second, 2) +
         getFractionString(fraction);
};

// Returns a new Date object or Mysql dateString, following the dateStrings
// option.  Fraction is an optional parameter that comes from
// readTemporalFraction().  With the dateStrings option from node-mysql,
// this returns a mysql TIMESTAMP string, like '1975-03-01 23:03:20.38945' or
// '1975-03-01 00:03:20'.  Mysql strings are needed for precision beyond ms.
exports.getTimeStamp = function(dateStrings, // node-mysql dateStrings option
                                secondsFromEpoch, // an integer
                                fraction // optional fraction of second object
                               )
{
  var milliseconds = fraction ? fraction.milliseconds : 0;
  var dateObject = new Date(secondsFromEpoch * 1000 + milliseconds);
  if (!useDateStringsForType(dateStrings, 'TIMESTAMP')) {
    return dateObject;
  }
  if (secondsFromEpoch === 0 && (!fraction || fraction.value === 0)) {
    return '0000-00-00 00:00:00' + getFractionString(fraction);
  }
  return getDateString(dateObject.getFullYear(),
                       dateObject.getMonth() + 1,
                       dateObject.getDate()) + ' ' +
         common.zeroPad(dateObject.getHours(), 2) + ':' +
         common.zeroPad(dateObject.getMinutes(), 2) + ':' +
         common.zeroPad(dateObject.getSeconds(), 2) +
         getFractionString(fraction);
};
