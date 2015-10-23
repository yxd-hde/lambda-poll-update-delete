import rx


batch_count = 10
wait_time = 1


class Poll(object):

    def __init__(self):
        self.fetch_count = 0
        self.message_count = 0

    def _one_request(self, queue):
        self.fetch_count += 1
        o = rx.Observable.to_async(one_request)(queue)
        messages_o = o.select_many(rx.Observable.from_)
        messages_o.subscribe(on_next=self._add_message_count)
        return messages_o

    def _add_message_count(self, _):
        self.message_count += 1

    def messages(self, sqs, queue_url, message_count):
        queue = sqs.Queue(queue_url)
        num_of_calls = message_count // batch_count

        requests = []
        for i in range(num_of_calls):
            requests.append(self._one_request(queue))

        return rx.Observable.merge(requests)


def one_request(queue):
    return queue.receive_messages(
        MaxNumberOfMessages=batch_count,
        WaitTimeSeconds=wait_time)
