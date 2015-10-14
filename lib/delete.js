var Promise = require('promise');

module.exports = function() {
  var deleteCount = 0;
  var messageCount = 0;

  return {
    exec: function(sqs, queueUrl, ids) {
      return groupBy10(ids).map(function(batch) {
        return new Promise(function(resolve, reject) {
          var params = makeParams(queueUrl, batch);
          deleteCount++;
          sqs.deleteMessageBatch(params, function(err, data) {
            if (err) {
              console.warn(err, err.stack);
              reject(err);
            } else {
              messageCount += data.Successful.length;
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
    },
    deleteCount: function() {
      return deleteCount;
    },
    messageCount: function() {
      return messageCount;
    }
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
