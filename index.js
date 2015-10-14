var Promise = require('promise');
var Rx = require('rx');
var aws = require('aws-sdk');
aws.config.update({
  region: 'ap-northeast-1'
});

var Poll = require('./lib/poll.js');
var Sum = require('./lib/sum.js');
var Update = require('./lib/update.js');
var Del = require('./lib/delete.js');

var sqs = new aws.SQS();
var db = new aws.DynamoDB();

const job = 'Lambda Job';

exports.handler = function(event, context) {
  var poll = new Poll();
  var sum = new sum();
  var update = new Update();
  var del = new Del();

  var messageCount;
  var queueUrl;
  var table;

  var done;

  function before(event, context) {
    console.time(job);
    console.info('Job start!');

    prepareSingletons();
    prepareDone(context);

    if (!event.messageCount || !event.table || !event.queueUrl) {
      done(new Error('Event is malformed.'));
    }
    console.info(event);

    messageCount = event.messageCount;
    queueUrl = event.queueUrl;
    table = event.table;
  }

  function prepareDone(context) {
    done = function(error, result) {
      if (error) {
        console.error(error, error.stack);
      }
      console.timeEnd(job);
      context.done(error, result);
    };
  }

  function prepareSingletons() {
    poll = Poll();
    sum = Sum();
    update = Update();
    del = new Del();
  }

  function start(context) {
    var messages = poll.messages(sqs, queueUrl, messageCount);

    var msgObserver = Rx.Observer.create(function(msg) {
      sum.add(msg);
    }, function(e) {
      done(e);
    }, function() {
      console.info('Receive API count: ' + poll.fetchCount);
      console.info('Fetched messages: ' + poll.messageCount);

      var results = Rx.Observable.forkJoin(updateAndDelete());
      results.subscribe(function() {}, function(e) {
        done(e);
      }, function() {
        console.info("Update API count: " + update.updateCount);
        console.info("Delete API count: " + del.deleteCount);
        console.info("Delete Message count: " + del.messageCount);

        done(null, 'Lambda job finished successfully.');
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
            console.warn(e, e.stack);
            reject(e);
          }, function() {
            resolve();
          });
        });
      }, function(e) {
        console.warn(e, e.stack);
        return Promise.resolve('dynamodb update error');
      });
      results.push(delResult);
    }
    return results;
  }

  before(event, context);
  start(context);
};
