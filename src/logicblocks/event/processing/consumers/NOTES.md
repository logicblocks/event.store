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
class Consumer(Service, ABC):
    def __init__(self, name: str, identifier: EventSequenceIdentifier):
    
    def execute(self):
        # tell work allocator
        # start polling based on config
        # every interval consume all
    
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