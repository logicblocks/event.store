async def register_subscriber(
    subscriber_group: str,
    subscription_request: EventSourceIdentifier,
    subscriber_state_category_name: SubscriberStateCategory,
    processor: SupportedProcessors,
    event_broker: EventBroker,
    event_store: EventStore,
    service_manager: ServiceManager,
    retryable_exceptions: list[Type[BaseException]] = (),
):
    subscriber_id = str(uuid4())
    subscriber_state_category = event_store.category(
        category=subscriber_state_category_name
    )

    state_store = EventConsumerStateStore(
        category=subscriber_state_category,
        converter=StoredEventEventConsumerStateConverter(),
        persistence_interval=EventCount(1),
    )

    def delegate_factory[I: EventSourceIdentifier](
        source: EventSource[I, StoredEvent],
    ) -> EventSourceConsumer[I, StoredEvent]:
        return EventSourceConsumer(
            source=source,
            processor=processor,
            state_store=state_store,
        )

    subscriber = EventSubscriptionConsumer(
        group=subscriber_group,
        id=subscriber_id,
        subscription_requests=[subscription_request],
        delegate_factory=delegate_factory,
    )

    await event_broker.register(subscriber)

    error_handling_service = ErrorHandlingService(
        callable=subscriber.consume_all,
        error_handler=TypeMappingErrorHandler(
            type_mappings=error_handler_type_mappings(
                continue_execution=retryable_exceptions
            )
        ),
    )

    polling_service = PollingService(
        callable=error_handling_service.execute,
        poll_interval=timedelta(seconds=1),
    )

    service_manager.register(
        polling_service,
        execution_mode=ExecutionMode.BACKGROUND,
    )
