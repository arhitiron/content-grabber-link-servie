import threading

from kafka import KafkaConsumer


class Consumer(threading.Thread):
    daemon = True

    def __init__(self, server, topic):
        super(Consumer, self).__init__()
        self._kafka_server = server
        self._kafka_topic = topic
        self._handlers = []
        self._consumer = KafkaConsumer(bootstrap_servers=self._kafka_server,
                                       auto_offset_reset='earliest')

        self.run()

    def add_handler(self, handler):
        self._handlers.append(handler)

    def run(self):
        self._consumer.subscribe([self._kafka_topic])

        for message in self._consumer:
            # TODO: not sure that it's a good idea work without interface, maybe will be better change implementation
            for handler in self._handlers:
                handler(message)
