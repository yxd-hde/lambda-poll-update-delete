var Promise = require('promise');

module.exports = function() {
  module.exports.prototype.updateCount = 0;

  module.exports.prototype.exec = function(db, table, stats) {
    var self = this;
    return new Promise(function(resolve, reject) {
      self.updateCount++;
      db.updateItem(makeParams(table, stats), function(err, data) {
        if (err) {
          console.warn(err, err.stack);
          reject(err);
        } else {
          resolve(stats.ids);
        }
      });
    });
  };
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
