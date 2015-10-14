#!/usr/bin/env python

import boto.sqs.connection
import boto.sqs.message
import json
import random

QUEUE_NAME = 'nobita-mailstat-test'
NUM_REQ = 3000

DOMAIN_LIST = ['foo', 'bar', 'buz']
DATE_LIST = ['20151014', '20151015', '20151016', '20151017']


def put_reqs_to_sqs_main():
    conn = boto.sqs.connection.SQSConnection(region=boto.sqs.SQSRegionInfo(name='ap-northeast-1', endpoint='sqs.ap-northeast-1.amazonaws.com'))
    q = conn.get_queue(QUEUE_NAME)
    for i in range(NUM_REQ):
        s = random.randint(100, 1000000)
        a = random.randint(100, s)
        m = {'date': random_date(),
             'domain': random_domain(),
             'size': s,
             'archivedSize': a}
        r = boto.sqs.message.RawMessage(body=json.dumps(m))
        q.write(r)


def random_domain():
    return DOMAIN_LIST[random.randint(0, 2)]


def random_date():
    return DATE_LIST[random.randint(0, 3)]


if __name__ == '__main__':
    put_reqs_to_sqs_main()
