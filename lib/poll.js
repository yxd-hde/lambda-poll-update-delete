var Rx = require('rx');

const batchCount = 10;
const waitTime = 1;

module.exports = function() {
  module.exports.prototype.fetchCount = 0;
  module.exports.prototype.messageCount = 0;

  module.exports.prototype._oneRequest = function(sqs, params) {
    var self = this;
    return Rx.Observable.create(function(observer) {
      self.fetchCount++;
      sqs.receiveMessage(params, function(err, data) {
        if (err) {
          console.warn(err, err.stack);
        } else {
          if (data.Messages) {
            self.messageCount += data.Messages.length;
            for (var i in data.Messages) {
              observer.onNext(data.Messages[i]);
            }
          }
        }

        observer.onCompleted();
      });
    });
  };

  module.exports.prototype.messages = function(sqs, queueUrl, messageCount) {
    var self = this;
    var params = {
      QueueUrl: queueUrl,
      MaxNumberOfMessages: batchCount,
      WaitTimeSeconds: waitTime
    };

    var ret = Rx.Observable.empty();

    var numberOfCalls = messageCount / batchCount | 0;
    for (var i = 0; i < numberOfCalls; i++) {
      ret = Rx.Observable.merge(ret, self._oneRequest(sqs, params));
    }

    return ret;
  };
};
