var Promise = require('promise');

module.exports = function() {
  module.exports.prototype.deleteCount = 0;
  module.exports.prototype.messageCount = 0;

  module.exports.prototype.exec = function(sqs, queueUrl, ids) {
    var self = this;
    return groupBy10(ids).map(function(batch) {
      return new Promise(function(resolve, reject) {
        var params = makeParams(queueUrl, batch);
        self.deleteCount++;
        sqs.deleteMessageBatch(params, function(err, data) {
          if (err) {
            console.warn(err, err.stack);
            reject(err);
          } else {
            self.messageCount += data.Successful.length;
            if (data.Failed.length > 0) {
              console.warn(data.Failed);
              reject(data.Failed);
            } else {
              resolve(data.Successful);
            }
          }
        });
      });
    });
  };
};

function groupBy10(ids) {
  var ret = [];
  var origin = ids.slice();
  while (origin.length > 0) {
    ret.push(origin.splice(0, 10));
  }
  return ret;
}

function makeParams(queueUrl, ids) {
  return {
    Entries: ids,
    QueueUrl: queueUrl
  };
}
