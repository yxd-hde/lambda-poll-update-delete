import boto3
from multiprocessing.pool import ThreadPool as Pool

from calculate import Sum

import logging

logging.getLogger(
    'botocore.vendored.requests.packages.urllib3.connectionpool'
).setLevel(logging.CRITICAL)
logging.getLogger('boto3.resources.action').setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

session = boto3.session.Session(region_name='ap-northeast-1')
sqs = session.resource('sqs')

batch_count = 10
wait_time = 1


def handler(event, contest):
    logger.info("Start!")

    executor = Pool(1000)

    cal = Sum()

    queue_url = event['queueUrl']
    message_count = event['messageCount']

    queue = sqs.Queue(queue_url)
    num_of_calls = message_count // batch_count

    queues = []
    for i in range(num_of_calls):
        queues.append(queue)

    message_count = 0

    responses = executor.map(one_request, queues)
    for response in responses:
        message_count += len(response)
        for msg in response:
            cal.add(msg)

    logger.info("Receive API count: {}".format(num_of_calls))
    logger.info("Fetched messages: {}".format(message_count))

    executor.close()


def one_request(queue):
    return queue.receive_messages(
        MaxNumberOfMessages=batch_count,
        WaitTimeSeconds=wait_time)
