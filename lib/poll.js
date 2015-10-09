var Rx = require('rx');

module.exports = Poll;

function Poll() {
}

Poll.fetchCount = 0;

Poll.messageCount = 0;

Poll.messages = function(sqs, queueUrl) {
  var params = {
    QueueUrl: queueUrl,
    MaxNumberOfMessages: 10,
    WaitTimeSeconds: 1
  };

  var ret = Rx.Observable.empty();
  for (var i = 0; i < 102; i++) {
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
  Poll.fetchCount++;
  sqs.receiveMessage(params, function(err, data) {
    if (err) {
      console.log(err);
    } else {
      if (data.Messages) {
        Poll.messageCount += data.Messages.length;
        for (var i in data.Messages) {
          observer.onNext(data.Messages[i]);
        }
      }
    }

    observer.onCompleted();
  });
};
