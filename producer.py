import json
import logging
import threading
from collections import deque

import time

import sys
from kafka import KafkaProducer


class Producer(threading.Thread):
    daemon = True

    def __init__(self, server, topic):
        super(Producer, self).__init__()
        self._kafka_server = server
        self._kafka_topic = topic
        self._messages = deque()
        self._lock = threading.Lock()

    def add_message(self, message):
        self._lock.acquire()
        self._messages.appendleft(message)
        self._lock.release()

    def _producer_optimistic_init(self):
        try:
            producer = KafkaProducer(bootstrap_servers=self._kafka_server,
                                     value_serializer=lambda v: json.dumps(v,
                                                                           default=convert_to_builtin_type).encode(
                                         'utf-8'))
            return producer
        except:
            time.sleep(1)
            print "Unexpected error:", sys.exc_info()[0]
            return self._producer_optimistic_init()

    def run(self):
        producer = self._producer_optimistic_init()
        while True:
            messages = self._messages
            try:
                if messages:
                    msg = messages.pop()
                    producer.send(self._kafka_topic, msg)
            except:
                logging.info("Exception on send message")
                logging.info(sys.exc_info())


def convert_to_builtin_type(obj):
    d = {}
    d.update(obj.__dict__)
    return d
