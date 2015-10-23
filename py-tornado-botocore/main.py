from tornado.ioloop import IOLoop
from tornado_botocore import Botocore
import botocore

from poll import Poll
from update_and_delete import UpdateAndDelete
from delete import Delete

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
sqs_delete_message_batch = Botocore(service='sqs',
                                    operation='DeleteMessageBatch',
                                    region_name='ap-northeast-1',
                                    session=session)
db_update_item = Botocore(service='dynamodb',
                          operation='UpdateItem',
                          region_name='ap-northeast-1',
                          session=session)


def handler(event, contest):
    logger.info("Start!")

    main_loop = IOLoop.instance()

    poll = Poll(main_loop)
    cal = poll.cal
    delete = Delete()
    update_and_delete = UpdateAndDelete(main_loop, delete)

    table = event['table']
    queue_url = event['queueUrl']
    message_count = event['messageCount']

    poll.messages(sqs_receive_message, queue_url, message_count)

    logger.info("Receive API count: {}".format(poll.fetch_count))
    logger.info("Fetched messages: {}".format(poll.message_count))

    update_and_delete.execute(sqs_delete_message_batch, db_update_item,
                              queue_url, table, cal.stats)

    logger.info("Update API count: {}".format(update_and_delete.update_count))
    logger.info("Delete API count: {}".format(delete.delete_count))
    logger.info("Delete Message count: {}".format(delete.message_count))

    return "Lambda job finished successfully."
