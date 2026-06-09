### Added

- Exported `make_event_store_subscriber` from `logicblocks.event.processing` and
  `logicblocks.event.processing.consumers`. This convenience wrapper around
  `make_subscriber` pre-fills the `subscriber_state_converter` for the common
  `StoredEvent` case.
