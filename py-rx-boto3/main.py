#!/usr/bin/env python2.7

import boto3
import rx

from poll import Poll
from calculate import Sum
from update import Update
from delete import Delete

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


def block(r):
    pass


def handler(event, context):
    logger.info("Start!")

    poll = Poll()
    cal = Sum()
    update = Update()
    delete = Delete()

    table = event['table']
    queue_url = event['queueUrl']
    message_count = event['messageCount']

    def on_error(e):
        raise e

    def on_poll_completed():
        logger.info("Receive API count: {}".format(poll.fetch_count))
        logger.info("Fetched messages: {}".format(poll.message_count))

        update_and_delete()

    def update_and_delete_one(key):
        updated_message_ids = update.execute(db, table, cal.stats[key])
        return delete.execute(sqs_client, queue_url, updated_message_ids)

    def update_and_delete():
        delete_results = []
        async_one = rx.Observable.to_async(update_and_delete_one)
        for key in cal.stats:
            delete_results.append(async_one(key))

        rx.Observable.merge(delete_results).to_blocking().for_each(block)

    on_next_message = cal.add

    messages = poll.messages(sqs, queue_url, message_count).to_blocking()
    messages_observer = rx.Observer(on_next_message,
                                    on_error,
                                    on_poll_completed)
    messages.subscribe(messages_observer)
    messages.for_each(block)

    logger.info("Update API count: {}".format(update.update_count))
    logger.info("Delete API count: {}".format(delete.delete_count))
    logger.info("Delete Message count: {}".format(delete.message_count))
