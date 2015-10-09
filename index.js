var Rx = require('rx');
var aws = require('aws-sdk');
aws.config.update({
  region: 'ap-northeast-1'
});
var sqs = new aws.SQS();
var queueUrl = 'https://sqs.ap-northeast-1.amazonaws.com/164201395711/xudong-nobita-mailstat-test';

var poll = require('./lib/poll.js');
var sum = require('./lib/sum.js');
var del = require('./lib/delete.js');

exports.handler = function(event, context) {
  var source = poll.messages(sqs, queueUrl);
  var count = 0;

  var observer = Rx.Observer.create(function(msg) {
    count++;

    var body = JSON.parse(msg.Body);
    sum.add(body);
    del.append(body.domain, msg.MessageId);
  }, function(e) {
    console.log(e);
    context.done(e);
  }, function() {
    console.log('Receive API count: ' + poll.fetchCount);
    console.log('Fetched messages: ' + count);

    console.log('Statistics: ');
    console.log(sum.stats);

    console.log('Message IDs: ');
    console.log(del.ids);
    context.done();
  });

  var subscription = source.take(1000).subscribe(observer);
};
