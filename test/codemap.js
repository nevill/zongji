const tap = require('tap');
const getEventClass = require('./../lib/code_map').getEventClass;

tap.test('Codemap', test => {
  test.equal(getEventClass(2).name, 'Query');
  test.equal(getEventClass(490).name, 'Unknown');
  test.end();
});
