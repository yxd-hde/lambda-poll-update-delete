import trollius as asyncio
from trollius import From

from delete import Delete


class UpdateAndDelete(object):

    def __init__(self, loop, executor):
        self.update_count = 0
        self.delete_count = 0
        self.deleted_message_count = 0

        self.loop = loop
        self.executor = executor

    @asyncio.coroutine
    def _one_key_request(self, sqs, db, queue_url, table, stats):
        self.update_count += 1
        deleted = yield From(self.loop.run_in_executor(
            None, update_and_delete_one, sqs, db, queue_url, table, stats,
            self.executor))
        self.delete_count += deleted.delete_count
        self.deleted_message_count += deleted.message_count

    def execute(self, sqs, db, queue_url, table, stats):
        tasks = []
        for key in stats:
            stats_per_key = stats[key]
            tasks.append(self._one_key_request(
                sqs, db, queue_url, table, stats_per_key))
        self.loop.run_until_complete(asyncio.wait(tasks))


def update(db, table, stats):
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


def update_and_delete_one(sqs, db, queue_url, table, stats, executor):
    ids = update(db, table, stats)
    delete = Delete(executor)
    delete.execute(sqs, queue_url, ids)
    return delete
