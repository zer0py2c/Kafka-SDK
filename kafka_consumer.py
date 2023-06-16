import logging
from kafka import KafkaConsumer, TopicPartition

from utils.helpers import msg_decode


class KafkaMsgConsumer(object):

    def __init__(self, server, topic, partitions, log_path):
        self._server = server
        self.consumer = None
        self.topic = topic
        self._partitions = partitions
        self.log_path = log_path

    def connect(self):
        if self.consumer is None:
            consumer = KafkaConsumer(
                bootstrap_servers=self._server,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=msg_decode
            )
            self.consumer = consumer

    def close(self):
        if self.consumer is not None:
            self.consumer.close()
            self.consumer = None

    def recv(self):
        logger = KafkaMsgConsumer.get_logger(self.log_path, "recv")
        partitions = []
        for num in self._partitions:
            partitions.append(TopicPartition(topic=self.topic, partition=num))
        self.consumer.assign(partitions=partitions)
        for msg in self.consumer:
            logger.info(f"{msg.partition}, {msg.offset}, {msg}")
            yield msg

    @classmethod
    def get_logger(cls, log_path, method_name):
        logging_name = "%s.%s" % (cls.__name__, method_name)
        logger = logging.getLogger(logging_name)
        logger.setLevel(level=logging.DEBUG)
        # file log
        handler = logging.FileHandler(log_path, encoding="utf-8")
        handler.setLevel(logging.INFO)
        format_ = "[%(asctime)s] [%(levelname)s] [%(name)s] : %(message)s"
        formatter = logging.Formatter(format_)
        handler.setFormatter(formatter)
        # console log
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        logger.addHandler(console)
        return logger

def get_kafka_consumer(server, topic, nums, log_path):
    consumer = KafkaMsgConsumer(server, topic, nums, log_path)
    consumer.connect()
    return consumer
