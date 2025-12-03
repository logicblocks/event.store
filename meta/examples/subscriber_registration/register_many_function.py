async def register_subscribers(
    event_broker: EventBroker,
    service_manager: ServiceManager,
    consumers: List[EventSubscriptionConsumer],
) -> None:
    for consumer in consumers:
        await event_broker.register(consumer)

        error_handling_service = LoggingErrorHandlingService(
            callable=consumer.consume_all,
            error_handler=RaiseErrorHandler(),
        )
        polling_service = PollingService(
            callable=error_handling_service.execute,
            poll_interval=timedelta(milliseconds=100),
        )

        service_manager.register(
            polling_service, execution_mode=ExecutionMode.BACKGROUND
        )


async def init_event_services(
    db_type: DBType,
    db_settings: DBSettings,
    connection_pool: AsyncConnectionPool[AsyncConnection],
    db: DB,
    publisher: EventPublisher,
    clock: Clock,
    metrics: Metrics
) -> ServiceManager:
    service_manager = ServiceManager()  # type: ignore[no-untyped-call]
    event_broker = _make_event_broker(
        db_type, db_settings, connection_pool, db.event_store_adapter
    )
    service_manager.register(event_broker)
    event_store = db.event_store
    subscription_consumers = [
        *approval_subscribers.make_subscribers(
            event_store, db, clock, metrics
        ),
        *payment_subscribers.make_subscribers(
            event_store, publisher, db, clock
        ),
        *settings_subscribers.make_subscribers(
            event_store, publisher, db, metrics,
        )
    ]
    await register_subscribers(
        event_broker, service_manager, subscription_consumers
    )
    return service_manager
