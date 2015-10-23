class Update(object):
    def __init__(self):
        self.update_count = 0

    def execute(self, db, table, stats):
        self.update_count += 1

        db.update_item(
            TableName=table,
            Key={
                'date': stats.date,
                'domain': stats.domain
            },
            UpdateExpression="ADD msg_count :c, msg_size :ms, archived_size :as",
            ExpressionAttributeValues={
                ':c': stats.count,
                ':ms': stats.size,
                ':as':  stats.archived_size
            },
            ReturnValues='NONE')

        return stats.ids
