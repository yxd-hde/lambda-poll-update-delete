import json


class Sum(object):
    def __init__(self):
        self.stats = {}

    def add(self, msg):
        body = json.loads(msg['Body'])
        domain = body['domain']
        date = body['date']
        key = new_key(date, domain)
        if key not in self.stats:
            self.stats[key] = Stats(date, domain)

        stats = self.stats[key]
        stats.count += 1
        stats.size += body['size']
        stats.archived_size += body['archivedSize']
        stats.ids.append({
            'Id': msg['MessageId'],
            'ReceiptHandle': msg['ReceiptHandle']
        })


class Stats(object):
    def __init__(self, date, domain):
        self.date = date
        self.domain = domain
        self.count = 0
        self.size = 0
        self.archived_size = 0
        self.ids = []

    def __repr__(self):
        return str(self.__dict__)


def new_key(date, domain):
    return date + '-' + domain
