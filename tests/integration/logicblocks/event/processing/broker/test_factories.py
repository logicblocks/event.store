import asyncio
from datetime import timedelta

from logicblocks.event.processing import (
    DistributedEventBrokerSettings,
    EventBrokerStorageType,
    EventBrokerType,
    make_event_broker,
)
from logicblocks.event.sources.partitioner import (
    StreamNamePrefixEventSourcePartitioner,
)
from logicblocks.event.store.adapters import InMemoryEventStorageAdapter
from logicblocks.event.testsupport.subscribers import CapturingEventSubscriber
from logicblocks.event.types import (
    CategoryIdentifier,
    CategoryPartitionIdentifier,
    StreamNamePrefixPartitionIdentifier,
)


class TestEventBrokerFactory:
    async def test_broker_without_partitioner_assigns_exact_requested_sources(self):
        broker = make_event_broker(
            node_id="test-node-1",
            broker_type=EventBrokerType.Distributed,
            storage_type=EventBrokerStorageType.InMemory,
            settings=DistributedEventBrokerSettings(
                coordinator_distribution_interval=timedelta(milliseconds=20)
            ),
            adapter=InMemoryEventStorageAdapter(),
        )

        category = CategoryIdentifier(category="orders")
        subscriber = CapturingEventSubscriber(
            group="test-group",
            id="subscriber-1",
            subscription_requests=[category]
        )

        await broker.register(subscriber)
        
        try:
            await broker.start()
            
            while len(subscriber.sources) == 0:
                await asyncio.sleep(0)
            
            accepted_sources = [
                source.identifier for source in subscriber.sources
            ]

            assert accepted_sources == [category]
        finally:
            await broker.stop()

    async def test_broker_with_stream_prefix_partitioner_expands_category_into_partitions(self):
        partitioner = StreamNamePrefixEventSourcePartitioner(
            prefix_length=1, character_set="ab"
        )

        broker = make_event_broker(
            node_id="test-node-2",
            broker_type=EventBrokerType.Distributed,
            storage_type=EventBrokerStorageType.InMemory,
            settings=DistributedEventBrokerSettings(
                coordinator_distribution_interval=timedelta(milliseconds=20),
            ),
            adapter=InMemoryEventStorageAdapter(),
            partitioner=partitioner,
        )

        category = CategoryIdentifier(category="orders")
        subscriber = CapturingEventSubscriber(
            group="test-group",
            id="subscriber-1",
            subscription_requests=[category]
        )

        await broker.register(subscriber)
        
        try:
            await broker.start()
            
            while len(subscriber.sources) < 2:
                await asyncio.sleep(0.01)
            
            accepted_sources = [
                source.identifier for source in subscriber.sources
            ]
            
            expected_partitions = {
                CategoryPartitionIdentifier(
                    category="orders",
                    partition=StreamNamePrefixPartitionIdentifier(value="a")
                ),
                CategoryPartitionIdentifier(
                    category="orders",
                    partition=StreamNamePrefixPartitionIdentifier(value="b")
                )
            }
            assert set(accepted_sources) == expected_partitions
        finally:
            await broker.stop()

    async def test_multiple_subscribers_receive_distributed_partitions(self):
        partitioner = StreamNamePrefixEventSourcePartitioner(
            prefix_length=1, character_set="abc"
        )

        broker = make_event_broker(
            node_id="test-node-3",
            broker_type=EventBrokerType.Distributed,
            storage_type=EventBrokerStorageType.InMemory,
            settings=DistributedEventBrokerSettings(
                coordinator_distribution_interval=timedelta(milliseconds=20),
            ),
            adapter=InMemoryEventStorageAdapter(),
            partitioner=partitioner,
        )

        category = CategoryIdentifier(category="payments")
        subscriber1 = CapturingEventSubscriber(
            group="test-group",
            id="subscriber-1",
            subscription_requests=[category]
        )
        subscriber2 = CapturingEventSubscriber(
            group="test-group",
            id="subscriber-2",
            subscription_requests=[category]
        )

        await broker.register(subscriber1)
        await broker.register(subscriber2)

        try:
            await broker.start()
            
            while len(subscriber1.sources) + len(subscriber2.sources) < 3:
                await asyncio.sleep(0)
            
            all_assigned_sources = (
                [source.identifier for source in subscriber1.sources] +
                [source.identifier for source in subscriber2.sources]
            )

            expected_partitions = {
                CategoryPartitionIdentifier(
                    category="payments",
                    partition=StreamNamePrefixPartitionIdentifier(value="a")
                ),
                CategoryPartitionIdentifier(
                    category="payments",
                    partition=StreamNamePrefixPartitionIdentifier(value="b")
                ),
                CategoryPartitionIdentifier(
                    category="payments",
                    partition=StreamNamePrefixPartitionIdentifier(value="c")
                )
            }

            assert set(all_assigned_sources) == expected_partitions
            assert len(subscriber1.sources) > 0
            assert len(subscriber2.sources) > 0
        finally:
            await broker.stop()