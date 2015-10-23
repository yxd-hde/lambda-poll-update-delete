from tornado import gen

import uuid
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class UpdateAndDelete(object):

    def __init__(self, loop, delete):
        self.update_count = 0

        self.loop = loop
        self.delete = delete

    @gen.coroutine
    def _one_key_request(self, sqs_delete_message_batch, db_update_item,
                         queue_url, table,
                         stats):
        self.update_count += 1
        ids = yield update(db_update_item, table, stats)
        yield self.delete.execute(
            sqs_delete_message_batch, queue_url, ids)

    def execute(self, sqs_delete_message_batch, db_update_item,
                queue_url, table, stats):
        @gen.coroutine
        def update_and_delete():
            r = []
            for key in stats:
                stats_per_key = stats[key]
                r.append(self._one_key_request(
                    sqs_delete_message_batch, db_update_item,
                    queue_url, table,
                    stats_per_key))
            yield r

        self.loop.run_sync(update_and_delete)


@gen.coroutine
def update(db_update_item, table, stats):
    yield gen.Task(
        db_update_item.call,
        TableName=table,
        Key={
            'date': {
                'S': stats.date
            },
            'domain': {
                'S': stats.domain
            }
        },
        UpdateExpression="ADD msg_count :c, msg_size :ms, archived_size :as",
        ExpressionAttributeValues={
            ':c': {
                'N': str(stats.count)
            },
            ':ms': {
                'N': str(stats.size)
            },
            ':as': {
                'N': str(stats.archived_size)
            }
        },
        ReturnValues='NONE')

    raise gen.Return(stats.ids)
