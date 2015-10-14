var Promise = require('promise');

exports.updateCount = 0;

exports.exec = function(db, table, stats) {
  return new Promise(function(resolve, reject) {
    exports.updateCount++;
    db.updateItem(makeParams(table, stats), function(err, data) {
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
