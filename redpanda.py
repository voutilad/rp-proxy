from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import ConsumerStoppedError

import asyncio
import logging
import ssl

import common

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
        ## 2-level Map of "Topics" -> b"key pattern" -> [queue]
        self._listener_map = {}

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

    async def subscribe(self, topic: str, key_filter=common.MATCH_ALL):
        """
        Subscribe to a topic with an optional key filter. Returns a queue the
        caller can consumer from to receive messages.
        """
        # TODO: multi-topic support?
        # TODO: check if we're already subscribed
        # TODO: key -> partition_id logic
        topics = await self._consumer.topics()
        if topic in topics:
            # Waste of effort, but just brute force update every time for now.
            for pid in self._consumer.partitions_for_topic(topic):
                tp = TopicPartition(topic, pid)
                self._tps.add(tp)
            self._consumer.assign(self._tps)

            # Update our subscriber map.
            pattern_map = self._listener_map.get(topic, {})
            key_filter_b = key_filter.encode("utf8") # Pre-encode.

            queues = pattern_map.get(key_filter_b, [])
            q = asyncio.Queue(10) # TODO: 10 was chosen via dice roll.
            queues.append(q)
            pattern_map[key_filter_b] = queues
            self._listener_map[topic] = pattern_map

            return q
        else:
            # TODO
            raise RuntimeError(f"Uhh not a good topic? topic={topic}, topics={topics}")


    def unsubscribe(self, topic, key_filter, queue):
        """
        Remove subscription to a topic/key_filter.
        """
        if topic in self._listener_map:
            if key_filter in self._listener_map[topic]:
                self._listener_map[topic][key_filter].remove(queue)

    async def poll(self):
        """
        Constantly polls for data. If nobody is subscribed, it's discarded.

        This is really where fine tuning of the data model and algorithm would
        help as latency will increase as the number of listeners increases.
        """
        try:
            while True:
                batch = await self._consumer.getmany(timeout_ms=50)
                # TODO: Spawn subtask to process batch? This is mainly CPU bound.
                if len(batch) > 0:
                    LOG.info(f"got batch of {len(batch.items())} records")

                # We get batches of: (TopicPartition, [Message])
                for tp, messages in batch.items():
                    key_patterns = self._listener_map.get(tp.topic, {})
                    if not key_patterns:
                        # No listeners? Drop the message fast.
                        continue

                    # Fire off our messages to our listeners.
                    for msg in messages:
                        for p in key_patterns.keys():
                            if p == common.MATCH_ALL_B or p == msg.key:
                                for q in key_patterns[p]:
                                    q.put_nowait((msg.offset, msg.key, msg.value))

        except ConsumerStoppedError:
            # Happens if we're interrupted/asked to shut down.
            LOG.info("stopping poll task")

        except Exception as e:
            # All other runtime chaos.
            LOG.error(f"unhandled exception: {e}")
