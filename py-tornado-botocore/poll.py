from tornado import gen

from calculate import Sum


batch_count = 10
wait_time = 1


class Poll(object):

    def __init__(self, loop):
        self.fetch_count = 0
        self.message_count = 0

        self.loop = loop
        self.cal = Sum()

    @gen.coroutine
    def _one_request(self, sqs_receive_message, queue_url):
        self.fetch_count += 1
        resp = yield gen.Task(sqs_receive_message.call,
                              QueueUrl=queue_url,
                              MaxNumberOfMessages=batch_count,
                              WaitTimeSeconds=wait_time)
        messages = resp['Messages']
        self.message_count += len(messages)
        for msg in messages:
            self.cal.add(msg)

    def messages(self, sqs_receive_message, queue_url, message_count):
        num_of_calls = message_count // batch_count

        @gen.coroutine
        def poll():
            r = []
            for i in range(num_of_calls):
                r.append(self._one_request(sqs_receive_message,
                                           queue_url))
            yield r

        self.loop.run_sync(poll)
