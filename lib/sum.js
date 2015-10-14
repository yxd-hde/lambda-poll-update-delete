module.exports = function() {
  module.exports.prototype.stats = {};

  module.exports.prototype.add = function(msg) {
    var stats = this.stats;
    var body = JSON.parse(msg.Body);
    var domain = body.domain;
    if (!(domain in stats)) {
      stats[domain] = {
        'domain': domain,
        'count': 0,
        'size': 0,
        'ids': []
      };
    }

    stats[domain].count += body.count;
    stats[domain].size += body.size;
    stats[domain].ids.push({
      Id: msg.MessageId,
      ReceiptHandle: msg.ReceiptHandle
    });
  };
};
