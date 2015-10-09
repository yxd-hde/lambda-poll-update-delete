var Rx = require('rx');
var aws = require('aws-sdk');
aws.config.update({
  region: 'ap-northeast-1'
});
var sqs = new aws.SQS();
var db = new aws.DynamoDB();
const queueUrl = 'https://sqs.ap-northeast-1.amazonaws.com/164201395711/xudong-nobita-mailstat-test';
const table = 'xudong-nobita-mailstat-sample';

var poll = require('./lib/poll.js');
var sum = require('./lib/sum.js');
var update = require('./lib/update.js');
var del = require('./lib/delete.js');

exports.handler = function(event, context) {
  startPoll(event, context);
};

function startPoll(event, context) {
  var messages = poll.messages(sqs, queueUrl);
  var count = 0;

  var msgObserver = Rx.Observer.create(function(msg) {
    count++;
    sum.add(msg);
  }, function(e) {
    console.log(e);
    context.done(e);
  }, function() {
    console.log('Receive API count: ' + poll.fetchCount);
    console.log('Fetched messages: ' + count);

    updateAndDelete(context);
  });

  var subscription = messages.take(1000).subscribe(msgObserver);
}

function updateAndDelete(context) {
  var results = [];
  for (var domain in sum.stats) {
    var stats = sum.stats[domain];
    var updateResult = update.exec(db, table, stats);
    var delResult = updateResult.then(function() {
      return del.exec(sqs, queueUrl, stats.ids);
    }, function(e) {
      console.log(e);
    });
    results.push(delResult);
  }
  Promise.all(results).then(function() {
    console.log("Update API count: " + update.updateCount);
    console.log("Delete API count: " + del.deleteCount);
    console.log("Delete Message count: " + del.messageCount);
    context.done();
  });
}
