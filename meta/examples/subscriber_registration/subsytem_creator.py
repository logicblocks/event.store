class ProviderService[EntityRecord: RecordBaseModel]:
    def __init__(
        self,
        event_store: EventStore,
        projection_store: ProjectionStore,
        loader: Loader[EntityRecord],
        record_type: type[EntityRecord],
        constraints: Sequence[Constraint],
    ):
        self._event_store = event_store
        self._projection_store = projection_store
        self._loader = loader
        self._constraints = constraints
        self._record_type = record_type

    async def _make_projection_service(
        self,
        config: ProjectionConfig,
        event_broker: EventBroker,
        service_manager: ServiceManager,
        enabled: bool,
        event_processor: EventProcessor,
    ):
        projection_subscriber = make_subscriber(
            subscriber_group=config.subscriber_config.subscriber_group,
            subscription_request=CategoryIdentifier(
                category=config.subscriber_config.subscription_request_category
            ),
            subscriber_state_category=self._event_store.category(
                category=config.subscriber_config.subscriber_state_category
            ),
            subscriber_state_persistence_interval=EventCount(10000),
            event_processor=event_processor,
        )

        error_handling_service = ErrorHandlingService(
            callable=projection_subscriber.consume_all,
            error_handler=ContinueErrorHandler(),
        )

        projection_service = PollingService(
            callable=error_handling_service.execute,
            poll_interval=timedelta(seconds=1),
        )

        if enabled:
            await event_broker.register(projection_subscriber)
            service_manager.register(
                projection_service,
                execution_mode=ExecutionMode.BACKGROUND,
            )

    def _make_event_processor(self, config: IngestionConfig):
        return ProjectionEventProcessor[RecordLog | None](
            projector=RecordLogProjector[EntityRecord](
                projection_name=config.projection_name,
                payload_type=self._record_type,
            ),
            projection_store=self._projection_store,
            state_type=RecordLog,
        )

    def _make_ingestion_service(
        self,
        config: IngestionConfig,
        service_manager: ServiceManager,
        enabled: bool,
    ):
        record_log_cache = RecordLogCache(
            event_store=self._event_store,
            projection_store=self._projection_store,
            projection_name=config.projection_name,
            record_category_name=config.record_category_name,
            projection_consumer_state_category_name=(
                config.projection_consumer_state_category_name
            ),
        )
        ingestion_service = IngestionService(
            event_store=self._event_store,
            projection_store=self._projection_store,
            loader=self._loader,
            record_log_cache=record_log_cache,
            constraints=self._constraints,
            projection_name=config.projection_name,
            event_name=config.event_name,
            category_name=config.record_category_name,
        )

        error_handling_ingestion_service = ErrorHandlingService(
            callable=ingestion_service.execute,
            error_handler=RetryErrorHandler(),
        )

        if enabled:
            service_manager.register(
                error_handling_ingestion_service,
                execution_mode=ExecutionMode.BACKGROUND,
            )

    async def make(
        self,
        event_broker: EventBroker,
        service_manager: ServiceManager,
        config: ProviderConfig,
    ):
        if config.ingestion_config is not None:
            self._make_ingestion_service(
                config=config.ingestion_config,
                service_manager=service_manager,
                enabled=config.ingestion_config.enabled,
            )

        if config.record_log_config is not None:
            await self._make_projection_service(
                config=config.record_log_config,
                event_broker=event_broker,
                service_manager=service_manager,
                event_processor=config.record_log_config.event_processor,
                enabled=config.record_log_config.enabled,
            )

        if config.changeset_config is not None:
            await self._make_projection_service(
                config=config.changeset_config,
                event_broker=event_broker,
                service_manager=service_manager,
                event_processor=config.changeset_config.event_processor,
                enabled=config.changeset_config.enabled,
            )

        if config.entity_config is not None:
            await self._make_projection_service(
                config=config.entity_config,
                event_broker=event_broker,
                service_manager=service_manager,
                event_processor=config.entity_config.event_processor,
                enabled=config.entity_config.enabled,
            )
