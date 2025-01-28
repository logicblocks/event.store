# Async consumers/workers

## Considerations

* Consumers are allocated "partitions" to consume
* Initially a category is a partition but could conceptually achieve competing 
  consumers by sharding streams within a category
* Need some sort of leadership election over work allocation so that each piece
  of work is allocated to a single consumer at a time most of the time
* Want work allocation to auto-recover in the case of a consumer failure
* Would like to be able to plug in Kafka as an alternative work provider to this
  postgres backed version without requiring changes to consumers
* May be able to get away with advisory locking for work allocation instead of
  true leadership election
* May be able to use postgres backed bully algorithm 
  (https://github.com/janbjorge/notifelect) implementation (if it is complete 
  enough)

## Abstractions

* Consumer
  * knows about event sequence and what work it wants to do 
  * needs a name to identify the type of work that it does
  * subscribes to consume an event sequence (log, category or stream, but predominantly 
    category)
  * may or may not be allocated that event sequence
  * some sort of poll interval for how frequently the consumer should check for 
    new work
  * some sort of position write frequency to keep track of where the consumer 
    is up to
  * keeps track of where it has reached within the event sequence it is working 
    on (say, using a consumer position store)

* Consumer position store
  * keeps track of where a consumer has got to with its work within an event 
    sequence
    * probably backed by the event store

* Work allocator
  * knows about event sequences, how to partition them
  * is told about types of work to be done and what event sequences that work 
    is associated with by consumers
  * distributes work to be done to consumers
  * there could be many work allocators online at a time (e.g., in different OS 
    processes, on different machines) but only one of them can be active at a 
    time
  * needs to keep track of work to be done and current allocation.
  * could this use a category or stream to store the work allocation state?

* Leader elector
  * Could use advisory locks to hold leadership (e.g., lock manager)
  * Could use a postgres backed bully algorithm implementation

## Subscription management

### Table Structure

2 node system

table: nodes
columns: node_id             | heartbeat_timestamp
         -----------------------------------------
         <uuid-1>            | <timestamp>
         <uuid-2>            | <timestamp>

table: subscribers
columns: subscriber_name             | subscriber_id   | node_id  | heartbeat_timestamp
         ------------------------------------------------------------------------------
         company-projection-consumer | <uuid-3>        | <uuid-1> | <timestamp>
         company-projection-consumer | <uuid-4>        | <uuid-2> | <timestamp>
         contact-projection-consumer | <uuid-5>        | <uuid-1> | <timestamp>
         contact-projection-consumer | <uuid-6>        | <uuid-2> | <timestamp>

table: subscriptions
columns: subscriber_name             | subscriber_id | subscriber_event_sources                                                      |
         -----------------------------------------------------------------------------------------------------------------------------
         company-projection-consumer | <uuid-3>      | { type: category, category: companies, partitions: [1, 2, 3, 4, 5, 6, 7, 8] } |
         company-projection-consumer | <uuid-4>      | { type: category, category: companies, partitions: [9, a, b, c, d, e, f] }    |
         contact-projection-consumer | <uuid-5>      | { type: category, category: contacts }                                        |
         contact-projection-consumer | <uuid-6>      |                                                                               |

### Components

EventBroker 
  - chooses strategy for managing subscribers and subscriptions based on 
    backing technology
  - maintains state on active nodes in the system
  - maintains state on health of subscribers in the system
EventSequencePartitioner
  - knows how to partitioner an event sequence into buckets of ordered streams
EventSubscriptionCoordinator 
  - manages subscriptions for subscribers to ensure minimal duplication of 
    effort and to allow parallelism
  - only one instance can be coordinating at a time
EventSubscriptionObserver
  - starts and stops subscribers from working on event sources
  - all instances (1 per node that has subscribers) can operate at the same
    time as they are readonly
EventSubscriber
  + accepts event sources into processing when asked
  + revokes event sources from processing when asked
  - keeps track of its own health, an exception causes a subscriber to enter an
    unhealthy state
  + has a name (representing the type of work that it does) and an ID (
    representing the specific subscriber instance)
EventSubscriberStore
  - a store for keeping track of the subscribers in the system and their health
    + in-memory
    - postgres
    - kafka
EventSubscriptionStore
  - a store for keeping track of the current allocations of event sources to
    subscriber instances
    + in-memory
    - postgres
    - kafka
LockManager
  - manages application locks to ensure exclusive access to some resource or 
    process
    - in-memory
    - postgres
NodeHeartbeat
  - keeps track of node health for each node in the processing group
    - in-memory
    + postgres
EventConsumerStateStore
  + keeps track of consumer progress through an event source 

### Questions

* How do we ensure that subscribers have been registered before allocating?
* Do we need `EventSubscriber.subscribe`?

## Archive

* 1 work allocator gets elected as leader, this is the only one allowed to add
  work to the table and assign workers to it
* each work allocator polls the table to determine bits of work that it should 
  distribute to its consumer processes
* each work allocator updates heartbeat timestamp against work it is still 
  working on

Example:
- 3 nodes start up -> 3 brokers
- Each node starts a consumer based on the config -> 3 brokers, 3 consumer 
  instances (all with the same name / belonging to same consumer group)
- Each broker has registered against it the consumer instance of its node 
  (brokers know that each node has the equivalent set of consumers in belonging 
  to the same consumer group(s))
- Each node is assigned (picks) an id, and starts writing heartbeats into the 
  worker_nodes table
- A broker leader is elected -> 1 broker is leader
- Broker leader assigns worker to consumer group partitions by replacing 
  contents of work_allocations table (need to think about avoiding double 
  consumers)
- Each broker polls the work_allocations table to determine what work should be 
  delegated or revoked from its subscribers

```python
class ConsumerService(Service):
    def __init__(self, consumer: EventConsumer, poll_interval: int):
        self.consumer = consumer
        self.poll_interval = poll_interval
        
    def execute(self):
        while True:
            await self.consumer.consume_all()
            asyncio.sleep(poll_interval)
            
class Consumer(ABC):
    def __init__(self, name: str, identifier: EventSequenceIdentifier):
    
    def consume_all(self):
        # read new events
        # for each, consume_event
        # store new position
        pass
        
    
class EventProcessor():
    def handle_event(self, event: StoredEvent):
        pass

class FenxSynchronisationConsumer(Consumer):
   def consume_event(self, event: StoredEvent):
       # do some work
       pass
```
