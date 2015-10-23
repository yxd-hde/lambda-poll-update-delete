import trollius as asyncio
from trollius import From

from calculate import Sum


batch_count = 10
wait_time = 1


class Poll(object):

    def __init__(self, loop):
        self.fetch_count = 0
        self.message_count = 0

        self.loop = loop
        self.cal = Sum()

    @asyncio.coroutine
    def _one_request(self, queue):
        self.fetch_count += 1
        result = yield From(self.loop.run_in_executor(
            None, one_request, queue))
        self.message_count += len(result)
        for msg in result:
            self.cal.add(msg)

    def messages(self, sqs, queue_url, message_count):
        queue = sqs.Queue(queue_url)
        num_of_calls = message_count // batch_count

        tasks = []
        for i in range(num_of_calls):
            tasks.append(self._one_request(queue))
        self.loop.run_until_complete(asyncio.wait(tasks))


def one_request(queue):
    return queue.receive_messages(
        MaxNumberOfMessages=batch_count,
        WaitTimeSeconds=wait_time)
