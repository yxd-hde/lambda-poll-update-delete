from tornado import gen
from itertools import izip_longest
from functools import partial
from operator import is_not


class Delete(object):
    def __init__(self):
        self.delete_count = 0
        self.message_count = 0

    @gen.coroutine
    def _one_request(self, sqs_delete_message_batch, queue_url, ids):
        self.delete_count += 1
        resp = yield gen.Task(sqs_delete_message_batch.call,
                              QueueUrl=queue_url,
                              Entries=ids)
        if 'Successful' in resp:
            self.message_count += len(resp['Successful'])
        if 'Failed' in resp:
            raise Exception('failed to delete messages')

    @gen.coroutine
    def execute(self, sqs_delete_message_batch, queue_url, ids):
        id_groups = group_by_10(ids)

        r = []
        for id_group in id_groups:
            r.append(self._one_request(sqs_delete_message_batch,
                                       queue_url, id_group))
        yield r


def group_by_10(ids):
    def grouper(iterable, n):
        args = [iter(iterable)] * n
        return izip_longest(*args)

    def convert(t):
        return list(filter(partial(is_not, None), t))

    return map(convert, grouper(ids, 10))
