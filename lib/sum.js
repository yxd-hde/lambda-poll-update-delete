module.exports = Sum;

function Sum() {
}

Sum.stats = {};

Sum.add = function(body) {
  var domain = body.domain;
  var stats = Sum.stats;
  if (!(domain in stats)) {
    stats[domain] = {
      count: 0,
      size: 0
    };
  }

  stats[domain].count += body.count;
  stats[domain].size += body.size;
};
