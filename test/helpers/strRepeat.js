module.exports = function (pattern, count) {
  if (count < 1) return '';
  var result = '';
  var pos = 0;
  while (pos < count) {
    result += pattern.replace(/##/g, pos);
    pos++;
  }
  return result;
};
