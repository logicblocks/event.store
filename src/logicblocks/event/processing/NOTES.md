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

## Subscription management

### Table Structure

2 node system

table: nodes
columns: node_id             | heartbeat_timestamp
         -----------------------------------------
         <uuid-1>            | <timestamp>
         <uuid-2>            | <timestamp>

table: subscribers
columns: subscriber_name             | subscriber_id | node_id  | heartbeat_timestamp
         ----------------------------------------------------------------------------
         company-projection-consumer | <uuid-3>      | <uuid-1> | <timestamp>
         company-projection-consumer | <uuid-4>      | <uuid-2> | <timestamp>
         contact-projection-consumer | <uuid-5>      | <uuid-1> | <timestamp>
         contact-projection-consumer | <uuid-6>      | <uuid-2> | <timestamp>

table: subscriptions
columns: subscriber_name             | subscriber_id | node_id  | subscriber_event_sources                                                        |
         ------------------------------------------------------------------------------------------------------------------------------------------
         company-projection-consumer | <uuid-3>      | <uuid-1> | [{ type: category, category: companies, partitions: [1, 2, 3, 4, 5, 6, 7, 8] }] |
         company-projection-consumer | <uuid-4>      | <uuid-2> | [{ type: category, category: companies, partitions: [9, a, b, c, d, e, f] }]    |
         contact-projection-consumer | <uuid-5>      | <uuid-1> | [{ type: category, category: contacts }]                                        |
         contact-projection-consumer | <uuid-6>      | <uuid-2> |                                                                                 |

### Components

EventBroker 
  + chooses strategy for managing subscribers and subscriptions based on 
    backing technology
NodeManager
  + maintains state on active nodes in the system
EventSubscriberManager
  + maintains state on health of subscribers in the system
EventSequencePartitioner
  - knows how to partitioner an event sequence into buckets of ordered streams
EventSubscriptionCoordinator 
  + manages subscriptions for subscribers to ensure minimal duplication of 
    effort and to allow parallelism
  + only one instance can be coordinating at a time
EventSubscriptionObserver
  + starts and stops subscribers from working on event sources
  + all instances (1 per node that has subscribers) can operate at the same
    time as they are readonly
EventSubscriber
  + accepts event sources into processing when asked
  + revokes event sources from processing when asked
  ~ keeps track of its own health, an exception causes a subscriber to enter an
    unhealthy state
  + has a name (representing the type of work that it does) and an ID (
    representing the specific subscriber instance)
EventSubscriberStore
  + a store containing all the local subscriber instances
    + in-memory
EventSubscriberStateStore
  ~ a store for keeping track of the subscribers in the system and their health
    + in-memory
    + postgres
    - kafka
EventSubscriptionStateStore
  ~ a store for keeping track of the current allocations of event sources to
    subscriber instances
    + in-memory
    + postgres
    - kafka
EventSubscriptionSourceMappingStore
  + a store for keeping track of the full set of event sources that can be 
    subscribed to for a given subscriber group
    + in-memory
LockManager
  + manages application locks to ensure exclusive access to some resource or 
    process
    + in-memory
    + postgres
NodeStateStore
  + keeps track of node health for each node in the processing group
    + in-memory
    + postgres
EventConsumerStateStore
  + keeps track of consumer progress through an event source 

### Questions

* How do we ensure that subscribers have been registered before allocating?

## Todo

* Implement EventSubscriptionConsumer error management
* Only fetch subscriptions for this node in Observer
* Add logging
* Add support for partitioning
* Add rebalancing support to ensure even workloads
* Work out how to handle error handling in infinite processes
  * Coordinator
  * Observer
  * Broker
* Remove Postgres implementation duplication
* Create Table abstraction to simplify Postgres implementations
