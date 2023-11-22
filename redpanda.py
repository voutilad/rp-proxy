from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import ConsumerStoppedError

import asyncio
import logging
import ssl

LOG = logging.getLogger("redpanda")

class Redpanda:
    def __init__(self, brokers, username=None, password=None, mechanism=None, tls=False, topics=[]):
        self.brokers = brokers
        self.username = username
        self.password = password
        self.mechanism = mechanism
        self.tls = tls
        self._topics = set(topics)
        self._tps = set()
        self._consumer = None
        self._listener_map = {} # map of topic -> [listeners]

    async def connect(self):
        proto = "PLAINTEXT"
        context = None
        if self.tls:
            if self.mechanism:
                proto = "SASL_SSL"
            else:
                proto = "SSL"
            context = ssl.create_default_context()
        elif self.mechanism:
            proto = "SASL_PLAINTEXT"

        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=self.brokers,
            security_protocol=proto,
            sasl_mechanism=self.mechanism,
            sasl_plain_username=self.username,
            sasl_plain_password=self.password,
            ssl_context=context,
            auto_offset_reset="latest",
        )

        try:
            await self._consumer.start()
            topics = await self._consumer.topics()
            LOG.info(f"discovered topics: {topics}")
        except Exception as e:
            LOG.error(f"ERROR: {e}")
            import sys
            sys.exit(1)

    async def disconnect(self):
        try:
            await self._consumer.stop()
        except Exception as e:
            LOG.error(f"failed to cleanly stop consumer: {e}")

    async def subscribe(self, topic):
        # TODO: multi-topic support?
        # TODO: check if we're already subscribed
        topics = await self._consumer.topics()
        if topic in topics:
            # Waste of effort, but just brute force update every time for now.
            for pid in self._consumer.partitions_for_topic(topic):
                tp = TopicPartition(topic, pid)
                self._tps.add(tp)
            self._consumer.assign(self._tps)

            # Update our subscriber map.
            q = asyncio.Queue(10)
            queues = self._listener_map.get(topic, [])
            queues.append(q)
            self._listener_map[topic] = queues
            return q
        else:
            # TODO
            raise RuntimeError(f"Uhh not a good topic? topic={topic}, topics={topics}")


    def unsubscribe(self, topic, queue):
        # TODO: multi-topic support
        if topic in self._listener_map:
            self._listener_map[topic].remove(queue)

    async def poll(self):
        """
        Constantly polls for data. If nobody is subscribed, it's discarded.
        """
        try:
            while True:
                batch = await self._consumer.getmany(timeout_ms=50)
                # todo: spawn subtask to process batch?
                if len(batch) > 0:
                    LOG.info(f"got batch of {len(batch.items())} records")
                for tp, messages in batch.items():
                    qs = self._listener_map.get(tp.topic, [])
                    for msg in messages:
                        for q in qs:
                            q.put_nowait((msg.offset, msg.key, msg.value))
        except ConsumerStoppedError:
            print("stopping poll task")
