import trollius as asyncio
from trollius import From
from itertools import izip_longest
from functools import partial
from operator import is_not


class Delete(object):
    def __init__(self, executor):
        self.delete_count = 0
        self.message_count = 0

        self.executor = executor

    def execute(self, sqs, queue_url, ids):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.set_default_executor(self.executor)

        id_groups = group_by_10(ids)
        tasks = []
        for id_group in id_groups:
            tasks.append(self._one_request(loop, sqs, queue_url, id_group))
        loop.run_until_complete(asyncio.wait(tasks))

    @asyncio.coroutine
    def _one_request(self, loop, sqs, queue_url, ids):
        self.delete_count += 1
        result = yield From(loop.run_in_executor(
            None, one_request, sqs, queue_url, ids))
        self.message_count += result['successful']
        if result['failed'] > 0:
            raise Exception('failed to delete messages')


def one_request(sqs, queue_url, ids):
    response = sqs.delete_message_batch(
        QueueUrl=queue_url,
        Entries=ids)

    failed = 0
    successful = 0

    if 'Failed' in response:
        failed = len(response['Failed'])
    if 'Successful' in response:
        successful = len(response['Successful'])

    return {
        'failed': failed,
        'successful': successful
    }


def group_by_10(ids):
    def grouper(iterable, n):
        args = [iter(iterable)] * n
        return izip_longest(*args)

    def convert(t):
        return list(filter(partial(is_not, None), t))

    return map(convert, grouper(ids, 10))
