var Promise = require('promise');

module.exports = Update;

function Update() {
}

Update.updateCount = 0;

Update.exec = function(db, table, stats) {
  return new Promise(function(resolve, reject) {
    Update.updateCount++;
    var requestId = id();
    console.log(requestId, 'update start');
    db.updateItem(makeParams(table, stats), function(err, data) {
      console.log(requestId, 'update stop');
      if (err) {
        console.log(err, err.stack);
        reject(err);
      } else {
        resolve(stats.ids);
      }
    });
  });
};

function makeParams(table, stats) {
  return {
    Key: {
      domain: {
        'S': stats.domain
      }
    },
    TableName: table,
    UpdateExpression: 'ADD msg_count :c, msg_size :s',
    ExpressionAttributeValues: {
      ':c': {
        'N': stats.count.toString()
      },
      ':s': {
        'N': stats.size.toString()
      }
    },
    ReturnValues: 'NONE'
  };
}

function id() {
  return "#" + Math.random().toString(16).slice(2, 8);
}
