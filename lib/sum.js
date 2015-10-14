var Promise = require('promise');

module.exports = function() {
  var stats = {};

  return {
    add: function(msg) {
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
    },
    stats: function() {
      return stats;
    }
  };
};
