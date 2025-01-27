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
  * subscribes to consumer an event sequence (log, category or stream, but predominantly 
    category)
  * may or may not be allocated that event sequence
  * some sort of poll interval for how frequently the consumer should check for 
    new work
  * some sort of position write frequency to keep track of where the consumer 
    is up to
  * keeps track of where it has reached within the event sequence it is working 
    on (say, using a consumer position store)

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

3 node system

table: nodes
columns: node_id   | heartbeat_timestamp
         <uuid_1>  | <timestamp>
         <uuid_2>  | <timestamp>

table: consumers
columns: consumer_name | event_sequence_identifier | ~~partition~~ | worker_id |
         consumer_1    | companies                 | ~~default~~   | 1         |
         consumer_2    | companies                 | ~~default~~   | 2         |
         consumer_3    | contacts                  | ~~default~~   | 3         |

"broker"
registers itself at startup
work allocator x3, 1 has advisory lock at a time
work commencer x3

* 1 work allocator gets elected as leader, this is the only one allowed to add work to the table and assign workers to it
* each work allocator polls the table to determine bits of work that it should distribute to its consumer processes
* each work allocator updates heartbeat timestamp against work it is still working on

Example:
3 nodes start up -> 3 brokers
Each node starts a consumer based on the config -> 3 brokers, 3 consumer instances (all with the same name / belonging to same consumer group)
Each broker has registered against it the consumer instance of its node (brokers know that each node has the equivalent set of consumers in belonging to the same consumer group(s))
Each node is assigned (picks) an id, and starts writing heartbeats into the worker_nodes table
A broker leader is elected -> 1 broker is leader
Broker leader assigns worker to consumer group partitions by replacing contents of work_allocations table (need to think about avoiding double consumers)
Each broker polls the work_allocations table to determine what work should be delegated or revoked from its subscribers
