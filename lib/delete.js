var Promise = require('promise');

module.exports = Delete;

function Delete() {
}

Delete.deleteCount = 0;

Delete.messageCount = 0;

Delete.exec = function(sqs, queueUrl, ids) {
  return groupBy10(ids).map(function(batch) {
    return new Promise(function(resolve, reject) {
      var params = makeParams(queueUrl, batch);
      Delete.deleteCount++;

      var deleteId = 'delete' + id();
      console.time(deleteId);
      sqs.deleteMessageBatch(params, function(err, data) {
        console.timeEnd(deleteId);
        if (err) {
          console.log(err);
          reject(err);
        } else {
          Delete.messageCount += data.Successful.length;
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

function id() {
  return "#" + Math.random().toString(16).slice(2, 8);
}
