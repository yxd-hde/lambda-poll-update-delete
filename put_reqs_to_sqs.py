#!/usr/bin/env python

import boto.sqs.connection
import boto.sqs.message
import json
import random

QUEUE_NAME = 'xudong-nobita-mailstat-test'
NUM_REQ = 1000

DOMAIN_LIST = ['foo', 'bar', 'buz']


def put_reqs_to_sqs_main():
    conn = boto.sqs.connection.SQSConnection(region=boto.sqs.SQSRegionInfo(name='ap-northeast-1', endpoint='sqs.ap-northeast-1.amazonaws.com'))
    q = conn.get_queue(QUEUE_NAME)
    for i in range(NUM_REQ):
        di = random.randint(0, 2)
        d = DOMAIN_LIST[di]
        c = 1
        s = random.randint(100, 1000000)
        m = {'domain': d,
             'count': c,
             'size': s}
        r = boto.sqs.message.RawMessage(body=json.dumps(m))
        q.write(r)


if __name__ == '__main__':
    put_reqs_to_sqs_main()
