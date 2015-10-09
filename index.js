var Rx = require('rx');
var aws = require('aws-sdk');
aws.config.update({
  region: 'ap-northeast-1'
});
var sqs = new aws.SQS();
var queueUrl = 'https://sqs.ap-northeast-1.amazonaws.com/164201395711/xudong-nobita-mailstat-test';

var poll = require('./lib/poll.js');

exports.handler = function(event, context) {
  var source = poll.messages(sqs, queueUrl);
  var count = 0;

  var observer = Rx.Observer.create(function(x) {
    count++;
  }, function(e) {
    console.log(e);
  }, function() {
    console.log('Fetched messages: ' + count);
    context.done();
  });

  var subscription = source.take(1000).subscribe(observer);
};
