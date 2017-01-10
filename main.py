import logging
import os
import sys
import time

from bs4 import BeautifulSoup

from consumer import Consumer
from dto.link_dto import LinkDto
from producer import Producer

ADDR = os.environ['ADDRESS']
KAFKA_ADDR = os.environ['KAFKA_ADDRESS']
RAW_QUEUE_TOPIC = os.environ['RAW_QUEUE_TOPIC']
LINK_QUEUE_TOPIC = os.environ['LINK_QUEUE_TOPIC']


class LinkService:
    URL_SKIP_LIST = list(['', '#', '/', 'javascript:void(0)', 'javascript:void(0);'])

    def __init__(self, kafka_addr, raw_queue_topic, link_queue_topic):
        self._resource_consumer = Consumer(kafka_addr, raw_queue_topic)
        self._link_producer = Producer(kafka_addr, link_queue_topic)
        self.serve()

    def serve(self):
        self._resource_consumer.add_handler(self.handle_resource)

        self._link_producer.start()
        self._resource_consumer.start()

    def handle_resource(self, raw_data):
        parsed_resource = BeautifulSoup(raw_data)
        link_tags = parsed_resource('a')
        for link in link_tags:
            try:
                href = str(link.get("href"))
                logging.log(logging.INFO, "Link href: " + href)
                if href not in self.URL_SKIP_LIST:
                    # TODO: synthetic delay for debug. Remove it
                    time.sleep(3)
                    link_dto = LinkDto(href)
                    self._link_producer.add_message(link_dto)
            except:
                logging.info("Exception on add message")
                logging.log(logging.INFO, sys.exc_info())


def main():
    LinkService(KAFKA_ADDR, RAW_QUEUE_TOPIC, LINK_QUEUE_TOPIC)
    # TODO: change to long time leave
    while True:
        time.sleep(20)


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()
