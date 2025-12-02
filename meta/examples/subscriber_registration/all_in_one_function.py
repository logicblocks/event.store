async def add_company_projector(
    event_store, projection_store, event_broker, service_manager
):
    projection_subscriber = make_subscriber(
        subscriber_group="search-company-projection",
        subscription_request=CategoryIdentifier(category="company"),
        subscriber_state_category=event_store.category(
            category="search-company-projection-state"
        ),
        subscriber_state_persistence_interval=EventCount(10000),
        event_processor=CompanySearchEventProcessor(
            projector=CompanySearchProjector(),
            projection_store=projection_store,
        ),
    )

    error_handling_service = ErrorHandlingService(
        callable=projection_subscriber.consume_all,
        error_handler=ContinueErrorHandler(),
    )

    projection_service = PollingService(
        callable=error_handling_service.execute,
        poll_interval=timedelta(seconds=1),
    )

    await event_broker.register(projection_subscriber)
    service_manager.register(
        projection_service,
        execution_mode=ExecutionMode.BACKGROUND,
    )
