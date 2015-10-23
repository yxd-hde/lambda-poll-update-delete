exports.newId = function() {
  return "#" + Math.random().toString(16).slice(2, 8);
};
