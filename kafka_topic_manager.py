from kafka.admin import KafkaAdminClient, NewTopic


class TopicManager(object):

    admin_client = None

    def __new__(cls, *args, **kwargs):
        if not cls.admin_client:
            cls.admin_client = object.__new__(cls)
        return cls.admin_client

    def __init__(self, nodes):
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=nodes,
            request_timeout_ms=30000 * 3,
            reconnect_backoff_ms=3000,
            reconnect_backoff_max_ms=30000 * 3,
            retry_backoff_ms=30000 * 3,
            )

    def create_topic(self, name_list):
        created = self.admin_client.list_topics()
        topic_list = [
            NewTopic(name=topic_name, num_partitions=3, replication_factor=3)
            for topic_name in name_list if topic_name not in created
        ]
        self.admin_client.create_topics(topic_list)

    def delete_topic(self, name_list):
        created = self.admin_client.list_topics()
        topic_list = [topic_name for topic_name in name_list if topic_name in created]
        self.admin_client.delete_topics(topic_list)

    def close(self):
        if isinstance(self.admin_client, KafkaAdminClient):
            self.admin_client.close()
