var Promise = require('promise');

exports.deleteCount = 0;
exports.messageCount = 0;

exports.exec = function(sqs, queueUrl, ids) {
  return groupBy10(ids).map(function(batch) {
    return new Promise(function(resolve, reject) {
      var params = makeParams(queueUrl, batch);
      exports.deleteCount++;
      sqs.deleteMessageBatch(params, function(err, data) {
        if (err) {
          console.log(err, err.stack);
          reject(err);
        } else {
          exports.messageCount += data.Successful.length;
          if (data.Failed.length > 0) {
            console.log(data.Failed);
            reject(data.Failed);
          } else {
            resolve(data.Successful);
          }
        }
      });
    });
  });
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
