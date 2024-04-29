import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pytest
import asyncio

from src.AioKafkaEngine import ConsumerEngine, ProducerEngine


@pytest.mark.asyncio
async def test_main():
    producer = ProducerEngine(
        bootstrap_servers="localhost:9094", report_interval=5, queue_size=100
    )

    consumer = ConsumerEngine(
        bootstrap_servers="localhost:9094",
        group_id="my_group",
        report_interval=5,
        queue_size=100,
    )

    await producer.start_engine("test_topic")
    await consumer.start_engine(["test_topic"])

    async def test_receive(consumer_engine: ConsumerEngine):
        queue = consumer_engine.get_queue()
        while not consumer_engine.stop_event.is_set():
            if queue.empty():
                await asyncio.sleep(1)
                continue
            msg = await queue.get()
            print("received", msg)
            queue.task_done()

    async def test_produce(producer_engine: ProducerEngine):
        queue = producer_engine.get_queue()
        for msg in [{"test": "test"}, {"test2": "test"}]:
            if producer_engine.stop_event.is_set():
                break
            print("send", msg)
            await queue.put(item=msg)

    asyncio.create_task(test_receive(consumer))
    asyncio.create_task(test_produce(producer))

    await asyncio.sleep(5)  # Allow the engines to run for 20 seconds

    await asyncio.gather(
        consumer.stop_engine(), producer.stop_engine(), asyncio.sleep(5)
    )


# Ensure to call test_main() in your test suite or in a test runner.
