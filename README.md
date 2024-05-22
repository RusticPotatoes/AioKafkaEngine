
# AsyncKafkaEngine

AsyncKafkaEngine is an asynchronous Kafka consumer and producer package built using the aiokafka library. This package allows efficient and scalable message handling with Kafka by providing classes for consuming and producing messages asynchronously.
Features
- Asynchronous Kafka consumer and producer
- JSON message serialization and deserialization
- Internal message queue management
- Periodic logging of message throughput
- Graceful shutdown of consumer and producer

# Installation

Install the package using pip:

```bash
pip install AsyncKafkaEngine
```
# Usage
## ConsumerEngine

The ConsumerEngine class manages the consumption of messages from Kafka topics asynchronously and places them into an internal queue. It also logs consumption statistics periodically.
Example

```python
import asyncio
from AsyncKafkaEngine import ConsumerEngine

async def main():
    consumer = ConsumerEngine(
        bootstrap_servers='localhost:9092', 
        group_id='my-group', 
        report_interval=5
    )
    await consumer.start_engine(['my_topic'])

asyncio.run(main())
```
## ProducerEngine

The ProducerEngine class manages the production of messages to a Kafka topic asynchronously by retrieving messages from an internal queue. It also logs production statistics periodically.
Example

```python

import asyncio
from AsyncKafkaEngine import ProducerEngine

async def main():
    producer = ProducerEngine(
        bootstrap_servers='localhost:9092', 
        report_interval=5
    )
    await producer.start_engine('my_topic')

asyncio.run(main())
```
# API
## ConsumerEngine
```
    __init__(bootstrap_servers, group_id=None, report_interval=5, queue_size=None)
        bootstrap_servers: Kafka server addresses.
        group_id: Consumer group ID (optional).
        report_interval: Interval for logging consumption statistics.
        queue_size: Maximum size of the internal message queue (optional).

    async start_engine(topics)
        topics: List of Kafka topics to consume from.

    async stop_engine()
        Stops the consumer gracefully.

    get_queue()
        Returns the internal queue holding consumed messages.
```
## ProducerEngine
```
    __init__(bootstrap_servers, report_interval=5, queue_size=None)
        bootstrap_servers: Kafka server addresses.
        report_interval: Interval for logging production statistics.
        queue_size: Maximum size of the internal message queue (optional).

    async start_engine(topic)
        topic: Kafka topic to produce messages to.

    async stop_engine()
        Stops the producer gracefully.

    get_queue()
        Returns the internal queue holding messages to be sent.
```
# Logging

The package uses the logging module to log debug information about the number of messages consumed and produced per report interval. Configure logging in your application as needed.

# Contributing

Contributions are welcome! Please submit a pull request or open an issue on GitHub.
# License

This project is licensed under the BSD 2-Clause License.