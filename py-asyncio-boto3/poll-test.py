import trollius as asyncio
import boto3
from concurrent.futures import ThreadPoolExecutor

from poll import Poll

import logging

logging.getLogger(
    'botocore.vendored.requests.packages.urllib3.connectionpool'
).setLevel(logging.CRITICAL)
logging.getLogger('boto3.resources.action').setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

session = boto3.session.Session(region_name='ap-northeast-1')
sqs = session.resource('sqs')


def handler(event, contest):
    logger.info("Start!")

    executor = ThreadPoolExecutor(max_workers=1000)
    main_loop = asyncio.new_event_loop()
    main_loop.set_default_executor(executor)
    asyncio.set_event_loop(main_loop)

    poll = Poll(main_loop)

    queue_url = event['queueUrl']
    message_count = event['messageCount']

    poll.messages(sqs, queue_url, message_count)

    logger.info("Receive API count: {}".format(poll.fetch_count))
    logger.info("Fetched messages: {}".format(poll.message_count))

    main_loop.close()
    executor.shutdown()
