import os
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

from utils.helpers import msg_encode

base_dir = os.path.abspath(os.path.dirname(__file__))


class KafkaMsgProducer(object):

    def __init__(self, server, topic, log_path):
        self._server = server
        self.producer = None
        self.topic = topic
        self.log_path = log_path

    def connect(self):
        if self.producer is None:
            producer = KafkaProducer(
                bootstrap_servers=self._server,
                retries=3,
                max_request_size=3048576,
                compression_type="gzip",
                max_block_ms=120000,
                request_timeout_ms=3600000,
                value_serializer=msg_encode
            )
            self.producer = producer

    def close(self):
        if self.producer is not None:
            self.producer.close()
            self.producer = None

    def send(self, msg, partition):
        logger = KafkaMsgProducer.get_logger(self.log_path, "send")
        if self.producer is not None:
            result = self.producer.send(self.topic, value=msg, partition=partition)
            try:
                record_metadata = result.get(timeout=30)
                logger.info(record_metadata)
            except KafkaError as e:
                logger.error(e)
            # self.producer.send(topic=self.topic, value=msg, partition=partition) \
            #     .add_callback(self.on_send_success) \
            #     .add_errback(self.on_send_failed)
            # self.producer.flush()

    def on_send_success(self, record_metadata):
        logger = KafkaMsgProducer.get_logger(self.log_path, "on_send_success")
        logger.info(f"value: {record_metadata.value}, topic: {record_metadata.topic}, partition: {record_metadata.partition}, offset: {record_metadata.offset}")

    def on_send_failed(self, exception):
        logger = KafkaMsgProducer.get_logger(self.log_path, "on_send_failed")
        logger.error("error", exc_info=exception)

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


def get_kafka_producer(server, topic, log_path):
    producer = KafkaMsgProducer(server, topic, log_path)
    producer.connect()
    return producer
