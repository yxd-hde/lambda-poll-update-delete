module.exports = function() {
  module.exports.prototype.stats = {};

  module.exports.prototype.add = function(msg) {
    var stats = this.stats;
    var body = JSON.parse(msg.Body);
    var domain = body.domain;
    var date = body.date;
    var key = newKey(date, domain);
    if (!(key in stats)) {
      stats[key] = {
        'date': date,
        'domain': domain,
        'count': 0,
        'size': 0,
        'archivedSize': 0,
        'ids': []
      };
    }

    stats[key].count++;
    stats[key].size += body.size;
    stats[key].archivedSize += body.archivedSize;
    stats[key].ids.push({
      Id: msg.MessageId,
      ReceiptHandle: msg.ReceiptHandle
    });
  };
};

function newKey(date, domain) {
  return date + '-' + domain;
}
