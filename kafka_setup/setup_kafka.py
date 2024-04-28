import os
import json
from faker import Faker
from random import Random

from pydantic import BaseModel
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic


# Define a Pydantic model for your data
class Player(BaseModel):
    id: int
    name: str
    score: int


def create_topics():
    # Get the Kafka broker address from the environment variable
    kafka_broker = os.environ.get("KAFKA_BROKER", "localhost:9094")

    # Create Kafka topics
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)

    topics = admin_client.list_topics()
    print("existing topics", topics)

    if not topics == []:
        admin_client.delete_topics(topics)

    res = admin_client.create_topics(
        [
            NewTopic(
                name="test_topic",
                num_partitions=1,
                replication_factor=1,
            ),
        ]
    )

    print("created_topic", res)

    topics = admin_client.list_topics()
    print("all topics", topics)
    return


def generate_fixed_players(num_players, seed=42):
    fake = Faker()
    random = Random(seed)
    players = []
    for i in range(num_players):
        random.seed(seed + i)  # Ensure each player gets the same name and score
        player = Player(id=i + 1, name=fake.name(), score=random.randint(0, 100))
        players.append(player)
    return players


def send_data_to_kafka(data, producer, topic):
    for i, record in enumerate(data):
        print(i, record)
        producer.send(topic, value=record.json())
    return


def insert_data():
    # Get the Kafka broker address from the environment variable
    kafka_broker = os.environ.get("KAFKA_BROKER", "localhost:9094")

    # Create the Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda x: json.dumps(x).encode(),
    )

    # Generate fixed players with the same names and scores
    num_players = 10_000  # Adjust as needed
    players = generate_fixed_players(num_players)

    # Send players to Kafka
    send_data_to_kafka(players, producer, "test_topic")

    print("Data insertion completed.")


def setup_kafka():
    create_topics()
    insert_data()


if __name__ == "__main__":
    setup_kafka()
