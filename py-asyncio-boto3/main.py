import trollius as asyncio
import boto3
from concurrent.futures import ThreadPoolExecutor

from poll import Poll
from update_and_delete import UpdateAndDelete

import logging

logging.getLogger(
    'botocore.vendored.requests.packages.urllib3.connectionpool'
).setLevel(logging.CRITICAL)
logging.getLogger('boto3.resources.action').setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

session = boto3.session.Session(region_name='ap-northeast-1')
sqs = session.resource('sqs')
sqs_client = sqs.meta.client
db = session.resource('dynamodb').meta.client


def handler(event, contest):
    logger.info("Start!")

    executor = ThreadPoolExecutor(max_workers=1000)
    main_loop = asyncio.new_event_loop()
    main_loop.set_default_executor(executor)
    asyncio.set_event_loop(main_loop)

    poll = Poll(main_loop)
    cal = poll.cal
    update_and_delete = UpdateAndDelete(main_loop, executor)

    table = event['table']
    queue_url = event['queueUrl']
    message_count = event['messageCount']

    poll.messages(sqs, queue_url, message_count)

    logger.info("Receive API count: {}".format(poll.fetch_count))
    logger.info("Fetched messages: {}".format(poll.message_count))

    update_and_delete.execute(sqs_client, db, queue_url, table, cal.stats)

    logger.info("Update API count: {}".format(update_and_delete.update_count))
    logger.info("Delete API count: {}".format(update_and_delete.delete_count))
    logger.info("Delete Message count: {}".format(
        update_and_delete.deleted_message_count))

    main_loop.close()
    executor.shutdown()

    return "Lambda job finished successfully."
