from tornado.ioloop import IOLoop
from tornado_botocore import Botocore
import botocore

from poll import Poll

import logging

logging.getLogger(
    'botocore.vendored.requests.packages.urllib3.connectionpool'
).setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


session = botocore.session.get_session()
sqs_receive_message = Botocore(service='sqs',
                               operation='ReceiveMessage',
                               region_name='ap-northeast-1',
                               session=session)


def handler(event, contest):
    logger.info("Start!")

    main_loop = IOLoop.instance()

    poll = Poll(main_loop)

    queue_url = event['queueUrl']
    message_count = event['messageCount']

    poll.messages(sqs_receive_message, queue_url, message_count)

    logger.info("Receive API count: {}".format(poll.fetch_count))
    logger.info("Fetched messages: {}".format(poll.message_count))
