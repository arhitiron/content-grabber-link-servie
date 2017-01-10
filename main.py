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
SKIP_LIST_PATH = os.environ['SKIP_LIST_PATH']


class LinkService:
    def __init__(self, kafka_addr, raw_queue_topic, link_queue_topic, skip_list_path=""):
        self._resource_consumer = Consumer(kafka_addr, raw_queue_topic)
        self._link_producer = Producer(kafka_addr, link_queue_topic)
        self._url_skip_list = list()
        if skip_list_path != "":
            self._url_skip_list = [line.strip() for line in open(skip_list_path, 'r')]

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
                if href is not None and href not in self._url_skip_list:
                    # TODO: synthetic delay for debug. Remove it
                    time.sleep(3)
                    link_dto = LinkDto(href)
                    self._link_producer.add_message(link_dto)
            except:
                logging.info("Exception on add message")
                logging.log(logging.INFO, sys.exc_info())


def main():
    LinkService(kafka_addr=KAFKA_ADDR, raw_queue_topic=RAW_QUEUE_TOPIC, link_queue_topic=LINK_QUEUE_TOPIC,
                skip_list_path=SKIP_LIST_PATH)
    # TODO: change to long time leave
    while True:
        time.sleep(20)


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()
