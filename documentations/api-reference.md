# API Reference

## Overview

This document provides a comprehensive reference for all public APIs in the event store library. Each section covers a specific module with detailed information about classes, methods, and parameters.

## Core Classes

### EventStore

The main entry point for interacting with the event store.

```python
class EventStore:
    def __init__(self, adapter: EventStorageAdapter):
        """
        Initialize the event store.
        
        Args:
            adapter: Storage adapter instance
        """
    
    def stream(self, 
               category: str, 
               stream: str, 
               log: str = "default") -> EventStream:
        """
        Get a reference to an event stream.
        
        Args:
            category: Category name
            stream: Stream name
            log: Log name (default: "default")
            
        Returns:
            EventStream instance
        """
    
    def category(self, 
                 category: str, 
                 log: str = "default") -> EventCategory:
        """
        Get a reference to an event category.
        
        Args:
            category: Category name
            log: Log name (default: "default")
            
        Returns:
            EventCategory instance
        """
    
    def log(self, log: str = "default") -> EventLog:
        """
        Get a reference to an event log.
        
        Args:
            log: Log name (default: "default")
            
        Returns:
            EventLog instance
        """
```

### EventStream

Represents a single event stream.

```python
class EventStream:
    @property
    def identifier(self) -> StreamIdentifier:
        """Get the stream identifier."""
    
    async def publish(self,
                     events: List[NewEvent],
                     expected_position: Optional[int] = None,
                     write_conditions: Optional[List[WriteCondition]] = None) -> List[StoredEvent]:
        """
        Publish events to the stream.
        
        Args:
            events: List of events to publish
            expected_position: Expected stream position for optimistic concurrency
            write_conditions: Additional conditions that must be met
            
        Returns:
            List of stored events
            
        Raises:
            UnmetWriteConditionError: If conditions are not met
        """
    
    async def scan(self,
                  from_position: Optional[int] = None,
                  to_position: Optional[int] = None,
                  limit: Optional[int] = None,
                  event_names: Optional[List[str]] = None) -> AsyncIterator[StoredEvent]:
        """
        Scan events from the stream.
        
        Args:
            from_position: Starting position (inclusive)
            to_position: Ending position (inclusive)
            limit: Maximum number of events to return
            event_names: Filter by event names
            
        Yields:
            StoredEvent instances
        """
    
    async def get_info(self) -> Dict[str, Any]:
        """
        Get stream information.
        
        Returns:
            Dictionary with stream metadata
        """
```

## Event Types

### NewEvent

Represents an event that hasn't been stored yet.

```python
@dataclass
class NewEvent:
    name: str
    payload: JSONType
    metadata: JSONType = field(default_factory=dict)
    occurred_at: Optional[datetime] = None
    
    def __post_init__(self):
        """Validate and set defaults."""
```

### StoredEvent

Represents an event that has been persisted.

```python
@dataclass
class StoredEvent:
    id: str
    stream: StreamIdentifier
    name: str
    payload: JSONType
    metadata: JSONType
    position: int
    global_position: int
    occurred_at: datetime
    recorded_at: datetime
```

### StreamIdentifier

Identifies a specific stream.

```python
@dataclass
class StreamIdentifier:
    log: str
    category: str
    stream: str
    
    def __str__(self) -> str:
        """String representation: log/category/stream"""
```

## Storage Adapters

### EventStorageAdapter (Abstract Base)

```python
class EventStorageAdapter(ABC):
    @abstractmethod
    async def append_to_stream(self,
                             stream: StreamIdentifier,
                             events: List[NewEvent],
                             expected_position: Optional[int] = None) -> List[StoredEvent]:
        """Append events to a stream."""
    
    @abstractmethod
    async def scan_stream(self,
                         stream: StreamIdentifier,
                         from_position: Optional[int] = None,
                         to_position: Optional[int] = None,
                         limit: Optional[int] = None) -> AsyncIterator[StoredEvent]:
        """Scan events from a stream."""
    
    @abstractmethod
    async def scan_category(self,
                           log: str,
                           category: str,
                           from_global_position: Optional[int] = None,
                           to_global_position: Optional[int] = None,
                           limit: Optional[int] = None) -> AsyncIterator[StoredEvent]:
        """Scan events from a category."""
    
    @abstractmethod
    async def scan_log(self,
                      log: str,
                      from_global_position: Optional[int] = None,
                      to_global_position: Optional[int] = None,
                      limit: Optional[int] = None) -> AsyncIterator[StoredEvent]:
        """Scan events from a log."""
    
    @abstractmethod
    async def get_stream_info(self,
                            stream: StreamIdentifier) -> Dict[str, Any]:
        """Get stream information."""
```

### InMemoryEventStorageAdapter

```python
class InMemoryEventStorageAdapter(EventStorageAdapter):
    def __init__(self):
        """Initialize in-memory storage."""
```

### PostgresEventStorageAdapter

```python
class PostgresEventStorageAdapter(EventStorageAdapter):
    def __init__(self,
                connection_source: ConnectionSource,
                serializer: Optional[Serializer] = None,
                serialization_constraint: Optional[SerializationConstraint] = None):
        """
        Initialize PostgreSQL adapter.
        
        Args:
            connection_source: Function that returns a database connection
            serializer: Custom serializer (optional)
            serialization_constraint: Write serialization strategy
        """
```

## Projections

### Projector

Base class for creating projectors.

```python
class Projector(Generic[TSource, TState, TMetadata]):
    @abstractmethod
    def initial_state_factory(self) -> TState:
        """Create initial state."""
    
    @abstractmethod
    def initial_metadata_factory(self) -> TMetadata:
        """Create initial metadata."""
    
    @abstractmethod
    def id_factory(self, state: TState, source: TSource) -> str:
        """Generate projection ID."""
    
    async def project(self, source: EventSource) -> Projection[TState, TMetadata]:
        """
        Project events from source.
        
        Args:
            source: Event source to project from
            
        Returns:
            Projection instance
        """
    
    def project_event(self, 
                     state: TState, 
                     metadata: TMetadata, 
                     event: StoredEvent) -> Tuple[TState, TMetadata]:
        """
        Project a single event.
        
        Args:
            state: Current state
            metadata: Current metadata
            event: Event to project
            
        Returns:
            Tuple of (new_state, new_metadata)
        """
```

### Projection

```python
@dataclass
class Projection(Generic[TState, TMetadata]):
    id: str
    state: TState
    metadata: TMetadata
```

### ProjectionStore

```python
class ProjectionStore:
    def __init__(self, adapter: ProjectionStorageAdapter):
        """Initialize projection store."""
    
    async def save(self, projection: Projection) -> None:
        """Save a projection."""
    
    async def get(self, id: str) -> Optional[Projection]:
        """Get projection by ID."""
    
    async def query(self, query: Query) -> AsyncIterator[Projection]:
        """Query projections."""
    
    async def delete(self, id: str) -> None:
        """Delete a projection."""
```

## Query System

### Query

```python
@dataclass
class Query:
    filter: Optional[FilterClause] = None
    order_by: Optional[List[Tuple[str, str]]] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
```

### where

Factory function for creating filter clauses.

```python
def where(field: str) -> FieldReference:
    """
    Create a field reference for filtering.
    
    Args:
        field: Field path (supports dot notation)
        
    Returns:
        FieldReference that supports comparison operations
        
    Example:
        filter = where("state.status") == "active"
    """
```

### FilterClause Operations

```python
# Equality
where("field") == value
where("field") != value

# Comparison
where("field") > value
where("field") >= value
where("field") < value
where("field") <= value

# Contains
where("field").contains(value)

# Null checks
where("field").is_null()
where("field").is_not_null()

# Logical operations
filter1 & filter2  # AND
filter1 | filter2  # OR
~filter           # NOT
```

## Transactions

### event_store_transaction

```python
@contextmanager
async def event_store_transaction(store: EventStore) -> EventStoreTransaction:
    """
    Create a transaction context.
    
    Args:
        store: Event store instance
        
    Yields:
        Transaction instance
        
    Example:
        async with event_store_transaction(store) as tx:
            await tx.stream("cat", "stream").publish(events)
    """
```

### Retry Decorators

```python
def retry_on_error(max_attempts: int = 3,
                  backoff_factor: float = 2.0,
                  max_delay: float = 60.0):
    """
    Retry on any error.
    
    Args:
        max_attempts: Maximum retry attempts
        backoff_factor: Exponential backoff factor
        max_delay: Maximum delay between retries
    """

def retry_on_unmet_condition_error(max_attempts: int = 3,
                                  backoff_factor: float = 2.0):
    """Retry only on write condition errors."""

def ignore_on_error():
    """Ignore all errors (use carefully)."""

def ignore_on_unmet_condition_error():
    """Ignore only write condition errors."""
```

## Write Conditions

### Built-in Conditions

```python
def stream_empty() -> WriteCondition:
    """Stream must be empty."""

def stream_not_empty() -> WriteCondition:
    """Stream must not be empty."""

def stream_exists() -> WriteCondition:
    """Stream must exist."""

def position_is(position: int) -> WriteCondition:
    """Stream must be at specific position."""

def position_less_than(position: int) -> WriteCondition:
    """Stream position must be less than value."""

def any_condition(*conditions: WriteCondition) -> WriteCondition:
    """At least one condition must be true."""

def all_conditions(*conditions: WriteCondition) -> WriteCondition:
    """All conditions must be true."""
```

## Testing Utilities

### EventBuilder

```python
class EventBuilder:
    def with_id(self, id: str) -> EventBuilder:
        """Set event ID."""
    
    def with_name(self, name: str) -> EventBuilder:
        """Set event name."""
    
    def with_stream(self, category: str, stream: str, log: str = "default") -> EventBuilder:
        """Set stream identifier."""
    
    def with_payload(self, payload: JSONType) -> EventBuilder:
        """Set payload."""
    
    def with_metadata(self, metadata: JSONType) -> EventBuilder:
        """Set metadata."""
    
    def with_position(self, position: int) -> EventBuilder:
        """Set stream position."""
    
    def with_global_position(self, global_position: int) -> EventBuilder:
        """Set global position."""
    
    def with_occurred_at(self, occurred_at: datetime) -> EventBuilder:
        """Set occurrence time."""
    
    def build(self) -> StoredEvent:
        """Build the event."""
    
    def build_many(self, count: int) -> List[StoredEvent]:
        """Build multiple events."""
```

### ProjectionBuilder

```python
class ProjectionBuilder:
    def with_id(self, id: str) -> ProjectionBuilder:
        """Set projection ID."""
    
    def with_state(self, state: Any) -> ProjectionBuilder:
        """Set state."""
    
    def with_metadata(self, metadata: Any) -> ProjectionBuilder:
        """Set metadata."""
    
    def build(self) -> Projection:
        """Build the projection."""
```

## Exceptions

### UnmetWriteConditionError

```python
class UnmetWriteConditionError(Exception):
    """Raised when write conditions are not met."""
```

### OptimisticConcurrencyError

```python
class OptimisticConcurrencyError(UnmetWriteConditionError):
    """Raised when expected version doesn't match."""
```

## Type Definitions

### JSONType

```python
JSONType = Union[None, bool, int, float, str, List['JSONType'], Dict[str, 'JSONType']]
```

### ConnectionSource

```python
ConnectionSource = Callable[[], Awaitable[Connection]]
```

## Utility Functions

### Serialization

```python
class Serializer(ABC):
    @abstractmethod
    def serialize(self, value: Any) -> str:
        """Serialize value to string."""
    
    @abstractmethod
    def deserialize(self, value: str) -> Any:
        """Deserialize string to value."""
```

### Time Utilities

```python
def now_utc() -> datetime:
    """Get current UTC time with timezone."""

def ensure_timezone(dt: datetime) -> datetime:
    """Ensure datetime has UTC timezone."""
```

## Usage Examples

### Basic Event Store Usage

```python
# Initialize
adapter = InMemoryEventStorageAdapter()
store = EventStore(adapter)

# Publish events
stream = store.stream(category="users", stream="user-123")
await stream.publish(events=[
    NewEvent(name="user-registered", payload={"email": "user@example.com"})
])

# Read events
async for event in stream.scan():
    print(f"{event.name}: {event.payload}")
```

### Projection Example

```python
# Define projector
class UserProjector(Projector[StreamIdentifier, dict, dict]):
    def initial_state_factory(self) -> dict:
        return {"events": []}
    
    def initial_metadata_factory(self) -> dict:
        return {"version": 0}
    
    def id_factory(self, state, source) -> str:
        return f"user:{source.stream}"
    
    def user_registered(self, state, event):
        state["events"].append(event.name)
        return state

# Use projector
projector = UserProjector()
projection = await projector.project(stream)
```

### Query Example

```python
# Query projections
query = Query(
    filter=(
        (where("state.status") == "active") &
        (where("state.score") > 100)
    ),
    order_by=[("state.created_at", "desc")],
    limit=10
)

async for projection in projection_store.query(query):
    print(projection.id, projection.state)
``` 