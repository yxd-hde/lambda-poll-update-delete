var Promise = require('promise');
var Rx = require('rx');
var aws = require('aws-sdk');
aws.config.update({
  region: 'ap-northeast-1'
});
var sqs = new aws.SQS();
var db = new aws.DynamoDB();

var poll = require('./lib/poll.js');
var sum = require('./lib/sum.js');
var update = require('./lib/update.js');
var del = require('./lib/delete.js');

var queueUrl = 'https://sqs.ap-northeast-1.amazonaws.com/164201395711/xudong-nobita-mailstat-test';
var table = 'xudong-nobita-mailstat-sample';
var messageCount = 1000;

exports.handler = function(event, context) {
  console.log('Start!');
  console.time('job');

  messageCount = event.messageCount || messageCount;
  queueUrl = event.queueUrl || queueUrl;
  table = event.table || table;

  startPoll(context);
};

function startPoll(context) {
  var messages = poll.messages(sqs, queueUrl, messageCount);

  var msgObserver = Rx.Observer.create(function(msg) {
    sum.add(msg);
  }, function(e) {
    console.log(e);
    context.done(e);
  }, function() {
    console.log('Receive API count: ' + poll.fetchCount);
    console.log('Fetched messages: ' + poll.messageCount);

    var results = Rx.Observable.forkJoin(updateAndDelete());
    results.subscribe(function() {}, function(e) {
      console.timeEnd('job');
      context.done(e);
    }, function() {
      console.log("Update API count: " + update.updateCount);
      console.log("Delete API count: " + del.deleteCount);
      console.log("Delete Message count: " + del.messageCount);

      console.timeEnd('job');
      context.done(null, 'Lambda function finished successfully.');
    });
  });

  var subscription = messages.subscribe(msgObserver);
}

function updateAndDelete() {
  var results = [];
  for (var domain in sum.stats) {
    var stats = sum.stats[domain];
    var updateResult = update.exec(db, table, stats);
    var delResult = updateResult.then(function(ids) {
      return new Promise(function(resolve, reject) {
        var results = Rx.Observable.forkJoin(del.exec(sqs, queueUrl, ids));
        results.subscribe(function() {}, function(e) {
          console.log(e);
          reject(e);
        }, function() {
          resolve();
        });
      });
    }, function(e) {
      console.log(e);
      return Promise.resolve('dynamodb update error');
    });
    results.push(delResult);
  }
  return results;
}
