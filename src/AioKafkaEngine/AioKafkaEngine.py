import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import json


class ConsumerEngine:
    def __init__(
        self, bootstrap_servers, group_id=None, report_interval=5, queue_size=None
    ):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.receive_queue = asyncio.Queue(maxsize=queue_size)
        self.consumer = None
        self.stop_event = asyncio.Event()
        self.consume_counter = 0
        self.report_interval = report_interval

    async def consume_messages(self, topics):
        self.consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
        )
        await self.consumer.start()
        try:
            async for msg in self.consumer:
                await self.receive_queue.put(msg)
                self.consume_counter += 1
                if self.stop_event.is_set():
                    break
        finally:
            await self.consumer.stop()

    async def start_engine(self, topics):
        consume_task = asyncio.create_task(self.consume_messages(topics))
        report_task = asyncio.create_task(self.report_stats())
        await asyncio.gather(consume_task, report_task)

    async def stop_engine(self):
        self.stop_event.set()
        if self.consumer:
            await self.consumer.stop()

    async def report_stats(self):
        while not self.stop_event.is_set():
            await asyncio.sleep(self.report_interval)
            print(f"Consumed {self.consume_counter} messages")

    def get_queue(self):
        return self.receive_queue


class ProducerEngine:
    def __init__(self, bootstrap_servers, report_interval=5, queue_size=None):
        self.bootstrap_servers = bootstrap_servers
        self.send_queue = asyncio.Queue(maxsize=queue_size)
        self.producer = None
        self.stop_event = asyncio.Event()
        self.produce_counter = 0
        self.report_interval = report_interval

    async def produce_messages(self, topic):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode(),
            acks="all",
        )
        await self.producer.start()
        try:
            while not self.stop_event.is_set():
                msg = await self.send_queue.get()
                await self.producer.send_and_wait(topic, msg.key, msg.value)
                self.produce_counter += 1
        finally:
            await self.producer.stop()

    async def start_engine(self, topic):
        produce_task = asyncio.create_task(self.produce_messages(topic))
        report_task = asyncio.create_task(self.report_stats())
        await asyncio.gather(produce_task, report_task)

    async def stop_engine(self):
        self.stop_event.set()
        if self.producer:
            await self.producer.stop()

    async def report_stats(self):
        while not self.stop_event.is_set():
            await asyncio.sleep(self.report_interval)
            print(f"Produced {self.produce_counter} messages")

    def get_queue(self):
        return self.send_queue


# Example usage:
async def main():
    consumer_engine = ConsumerEngine(
        bootstrap_servers="localhost:9092",
        group_id="my_group",
        report_interval=5,
        queue_size=100,
    )
    producer_engine = ProducerEngine(
        bootstrap_servers="localhost:9092", report_interval=5, queue_size=100
    )

    consume_topics = ["topic1", "topic2"]
    produce_topic = "topic3"

    consumer_task = asyncio.create_task(consumer_engine.start_engine(consume_topics))
    producer_task = asyncio.create_task(producer_engine.start_engine(produce_topic))

    await asyncio.sleep(20)  # Allow the engines to run for 20 seconds

    # Stop the engines after 20 seconds
    await consumer_engine.stop_engine()
    await producer_engine.stop_engine()


asyncio.run(main())
