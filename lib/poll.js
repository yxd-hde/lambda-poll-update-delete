var Rx = require('rx');

const batchCount = 10;
const waitTime = 1;

module.exports = function() {
  var fetchCount = 0;
  var messageCount = 0;

  function oneRequest(sqs, params) {
    return Rx.Observable.create(function(observer) {
      fetchCount++;
      sqs.receiveMessage(params, function(err, data) {
        if (err) {
          console.warn(err, err.stack);
        } else {
          if (data.Messages) {
            messageCount += data.Messages.length;
            for (var i in data.Messages) {
              observer.onNext(data.Messages[i]);
            }
          }
        }

        observer.onCompleted();
      });
    });
  }

  return {
    messages: function(sqs, queueUrl, messageCount) {
      var params = {
        QueueUrl: queueUrl,
        MaxNumberOfMessages: batchCount,
        WaitTimeSeconds: waitTime
      };

      var ret = Rx.Observable.empty();

      var numberOfCalls = messageCount / batchCount | 0;
      for (var i = 0; i < numberOfCalls; i++) {
        ret = Rx.Observable.merge(ret, oneRequest(sqs, params));
      }

      return ret;
    },
    fetchCount: function() {
      return fetchCount;
    },
    messageCount: function() {
      return messageCount;
    }
  };
};
