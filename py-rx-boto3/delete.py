from itertools import izip_longest
from functools import partial
from operator import is_not
import rx


class Delete(object):
    def __init__(self):
        self.delete_count = 0
        self.message_count = 0

    def execute(self, sqs, queue_url, ids):
        id_groups = group_by_10(ids)
        results = []
        for id_group in id_groups:
            results.append(self._one_request(sqs, queue_url, id_group))

        o = rx.Observable.merge(results)
        o.subscribe(on_next=self._on_result)

        def block(r):
            pass

        o.to_blocking().for_each(block)

        return []

    def _on_result(self, result):
        self.message_count += result['successful']
        if result['failed'] > 0:
            raise Exception('failed to delete messages')

    def _one_request(self, sqs, queue_url, ids):
        self.delete_count += 1
        return rx.Observable.to_async(one_request)(sqs, queue_url, ids)


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
