var Rx = require('rx');

module.exports = Poll;

function Poll() {
}

Poll.messages = function(sqs, queueUrl) {
  var params = {
    QueueUrl: queueUrl,
    MaxNumberOfMessages: 10,
    WaitTimeSeconds: 1
  };

  var ret = Rx.Observable.empty();
  for (var i = 0; i < 100; i++) {
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
  sqs.receiveMessage(params, function(err, data) {
    if (err) {
      console.log(err);
      observer.onCompleted();
    } else {
      if (data.Messages) {
        for (var i in data.Messages) {
          observer.onNext(data.Messages[i]);
        }
      }

      fetch(sqs, params, observer);
    }
  });
};
