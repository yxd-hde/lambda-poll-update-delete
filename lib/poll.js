var Rx = require('rx');

exports.fetchCount = 0;
exports.messageCount = 0;

exports.messages = function(sqs, queueUrl, messageCount) {
  var params = {
    QueueUrl: queueUrl,
    MaxNumberOfMessages: 10,
    WaitTimeSeconds: 1
  };

  var ret = Rx.Observable.empty();

  var numberOfCalls = messageCount / 10 | 0;
  for (var i = 0; i < numberOfCalls; i++) {
    ret = Rx.Observable.merge(ret, oneRequest(sqs, params));
  }

  return ret;
};

var oneRequest = function(sqs, params) {
  return Rx.Observable.create(function(observer) {
    fetch(sqs, params, observer);
  });
};

var fetch = function(sqs, params, observer) {
  exports.fetchCount++;
  sqs.receiveMessage(params, function(err, data) {
    if (err) {
      console.log(err, err.stack);
    } else {
      if (data.Messages) {
        exports.messageCount += data.Messages.length;
        for (var i in data.Messages) {
          observer.onNext(data.Messages[i]);
        }
      }
    }

    observer.onCompleted();
  });
};
