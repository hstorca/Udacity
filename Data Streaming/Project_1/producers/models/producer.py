"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.broker_properties = {
            "BROKER_URL": "PLAINTEXT://localhost:9092", # "PLAINTEXT://kafka0:9092",
            "SCHEMA_REGISTRY_URL": "http://localhost:8081", # "http://schema-registry:8081",
        }

        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            # self.create_topic(self,self.topic_name)
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            {"bootstrap.servers": self.broker_properties["BROKER_URL"]},
            schema_registry=CachedSchemaRegistryClient(
                {"url": self.broker_properties["SCHEMA_REGISTRY_URL"]},
            )
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        client = AdminClient({"bootstrap.servers": self.broker_properties["BROKER_URL"]})
        client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                )
            ]
        )

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()
        logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
