import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import json
import logging

logger = logging.getLogger(__name__)


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
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            group_id=self.group_id,
            auto_offset_reset="earliest",
        )
        await self.consumer.start()
        try:
            async for msg in self.consumer:
                await self.receive_queue.put(msg.value)
                self.consume_counter += 1
                if self.stop_event.is_set():
                    break
        finally:
            await self.consumer.stop()

    async def start_engine(self, topics):
        asyncio.create_task(self.consume_messages(topics))
        asyncio.create_task(self.report_stats())

    async def stop_engine(self):
        self.stop_event.set()
        if self.consumer:
            await self.consumer.stop()

    async def report_stats(self):
        while not self.stop_event.is_set():
            await asyncio.sleep(self.report_interval)
            logger.debug(
                f"Consumed {self.consume_counter} messages in {self.report_interval} seconds {self.consume_counter/self.report_interval} msg/sec"
            )
            self.consume_counter = 0

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
                await self.producer.send(topic=topic, value=msg)
                self.produce_counter += 1
        finally:
            await self.producer.stop()

    async def start_engine(self, topic):
        asyncio.create_task(self.produce_messages(topic))
        asyncio.create_task(self.report_stats())

    async def stop_engine(self):
        self.stop_event.set()
        if self.producer:
            await self.producer.stop()

    async def report_stats(self):
        while not self.stop_event.is_set():
            await asyncio.sleep(self.report_interval)
            logger.debug(
                f"Produced {self.produce_counter} messages in {self.report_interval} seconds {self.produce_counter/self.report_interval} msg/sec"
            )
            self.produce_counter = 0

    def get_queue(self):
        return self.send_queue


async def test_receive(engine: ConsumerEngine):
    queue = engine.get_queue()
    while not engine.stop_event.is_set():
        msg = await queue.get()
        print("received", msg)
        queue.task_done()


async def test_produce(engine: ProducerEngine, messages: list[dict]):
    queue = engine.get_queue()
    for msg in messages:
        if engine.stop_event.is_set():
            break
        print("send", msg)
        await queue.put(item=msg)


# Example usage:
async def main():
    consumer_engine = ConsumerEngine(
        bootstrap_servers="localhost:9094",
        group_id="my_group",
        report_interval=5,
        queue_size=100,
    )
    producer_engine = ProducerEngine(
        bootstrap_servers="localhost:9094", report_interval=5, queue_size=100
    )

    consume_topics = ["test_topic"]
    produce_topic = "test_topic"

    await consumer_engine.start_engine(consume_topics)
    await producer_engine.start_engine(produce_topic)

    await asyncio.sleep(5)  # Allow the engines to run for 5 seconds
    asyncio.create_task(test_receive(engine=consumer_engine))

    await asyncio.sleep(20)  # Allow the engines to run for 20 seconds
    asyncio.create_task(
        test_produce(
            engine=producer_engine, messages=[{"test": "test"}, {"test2": "test"}]
        )
    )
    await asyncio.sleep(20)  # Allow the engines to run for 20 seconds
    # Stop the engines after 20 seconds
    await consumer_engine.stop_engine()
    await producer_engine.stop_engine()


asyncio.run(main())
