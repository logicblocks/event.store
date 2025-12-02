class ConsumerRegistrar(ABC):
    subscriber_group: ClassVar[LiteralString]
    subscriber_state_category: ClassVar[LiteralString]
    category: ClassVar[CategoryDefinition[Any, Any]]

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"subscriber_group={self.subscriber_group}, "
            f"category={self.category}, "
            f"event_processor={repr(self.event_processor)}"
            ")"
        )

    def __init__(
        self,
        event_store: EventStore,
        event_processor: EventProcessor,
    ):
        self._event_store = event_store
        self._event_processor = event_processor

        self._state_store = EventConsumerStateStore(
            category=event_store.category(category=self.subscriber_group),
        )

    @property
    def event_processor(self) -> EventProcessor:
        return self._event_processor

    def _consumer_for_source(self, source: EventSource) -> EventSourceConsumer:
        return EventSourceConsumer(
            source=source,
            processor=self._event_processor,
            state_store=self._state_store,
        )

    async def consume_all(self) -> None:
        source = self._event_store.category(category=self.category.name)
        consumer = self._consumer_for_source(source)
        return await consumer.consume_all()

    async def register_as_service(
        self, event_broker: EventBroker, service_manager: ServiceManager
    ) -> None:
        subscriber = EventSubscriptionConsumer(
            group=self.subscriber_group,
            id=str(uuid4()),
            subscription_requests=[
                CategoryIdentifier(category=self.category.name)
            ],
            delegate_factory=self._consumer_for_source,
        )

        await event_broker.register(subscriber)

        service = PollingService(
            callable=subscriber.consume_all,
            poll_interval=timedelta(seconds=1),
        )

        service_manager.register(
            service,
            execution_mode=ExecutionMode.BACKGROUND,
        )


async def register_tasks(
    service_manager: ServiceManager,
    event_broker: EventBroker,
    registrars: Iterable[ConsumerRegistrar],
) -> None:
    async with asyncio.TaskGroup() as tg:
        service_manager.register(event_broker)
        logger.info("Registering consumers", registrars=registrars)
        for registrar in registrars:
            tg.create_task(
                registrar.register_as_service(
                    service_manager=service_manager,
                    event_broker=event_broker,
                )
            )
