module.exports = function (pattern, count) {
  if (count < 1) return '';
  let result = '';
  let pos = 0;
  while (pos < count) {
    result += pattern.replace(/##/g, pos);
    pos++;
  }
  return result;
};
