---
title: How the distributed event broker works
description: How nodes, subscribers, the coordinator and observers share event sources between processes.
---

The distributed event broker lets several processes run the same
subscribers while each event source is consumed by one subscriber at a
time. If a subscriber or a whole process goes away, another one takes over
its sources. This page explains how that works. To set up a broker, see
[Processing events asynchronously](/event.store/guides/event-processing/).

## Nodes, subscribers and subscriptions

A **node** is a process that runs a broker. Each broker is created with a
node ID, which should be unique among the nodes sharing a database.

A **subscriber** is registered with the broker on its node. It has two
names:

- its **group** names the kind of work it does, for example
  `profile-projection`. Every node that runs that work registers a
  subscriber in the same group. Older design notes call this the
  subscriber name;
- its **ID** identifies one instance within the group. `make_subscriber`
  defaults it to a random UUID, so it changes each time a process starts.

Together, the group and ID form the subscriber's key.

Each subscriber lists its **subscription requests**: the identifiers of the
event sources it wants, such as a category or the log. A subscriber made
with `make_subscriber` requests exactly one source.

A **subscription** records which event sources are currently allocated to
one subscriber instance. It has the same group and ID as its subscriber. A
subscriber with no allocated sources can still have a subscription with an
empty list.

## What runs on each node

While it runs, every distributed broker runs three parts at once:

- the **subscriber manager** records the node's subscribers in shared
  state, and keeps that record fresh;
- the **coordinator** competes to become leader. Only the leader decides
  which subscriber gets which source;
- the **observer** reads the subscriptions and starts or stops the node's
  own subscribers to match.

```text
          node-1                                  node-2
 +--------------------------+           +--------------------------+
 | subscriber manager       |           | subscriber manager       |
 | coordinator (leader)     |           | coordinator (waiting)    |
 | observer                 |           | observer                 |
 | subscribers              |           | subscribers              |
 +--------------------------+           +--------------------------+
     |         |        ^                   |                  ^
     |         |        |                   |                  |
     | heart-  | writes | reads             | heartbeats       | reads
     | beats   |        |                   |                  |
     v         v        |                   v                  |
 +-----------------------------------------------------------------+
 | shared state                                                    |
 |   subscribers table     (who is alive, and what they request)   |
 |   subscriptions table   (which sources each subscriber has)     |
 |   coordinator lock      (which node is leader)                  |
 +-----------------------------------------------------------------+
```

The nodes never talk to each other directly. They only read and write the
shared state.

If any of the three parts fails, the broker stops the other two and removes
its subscribers from shared state. With the default error handler, it then
starts again and registers them afresh.

## Leader election

The coordinator on every node tries to take a lock named by
[`COORDINATOR_LOCK_NAME`](/event.store/reference/processing/#logicblocks.event.processing.broker.strategies.distributed.COORDINATOR_LOCK_NAME),
using a
[`LockManager`](/event.store/reference/processing/#logicblocks.event.processing.LockManager).
It doesn't wait for the lock. The node that gets it is the leader:

1. A node without the lock tries again every
   `coordinator_leadership_attempt_interval`.
2. The leader allocates sources, then waits
   `coordinator_distribution_interval`, and repeats.
3. Once it has been leader for longer than
   `coordinator_leadership_max_duration`, it releases the lock and waits the
   attempt interval before competing again. This gives other nodes a chance
   to lead.

With Postgres storage, the lock is a transaction-level advisory lock taken
on its own connection, made from the broker's `connection_settings`. The
leader keeps that transaction open while it leads. If the leader's process
dies, its connection closes, Postgres releases the lock and another node
takes it at its next attempt.

Every broker that uses the same database competes for the same lock and
reads the same tables, so they form one group of nodes.

## How the leader allocates sources

At each distribution, the leader reads all subscriptions and the
subscribers seen within `coordinator_subscriber_max_time_since_last_seen`.
It then works out the changes:

1. It removes the subscriptions of subscribers that are no longer live.
2. For each subscriber group, it takes the longest list of subscription
   requests among the group's live subscribers as the target sources.
   Subscribers whose requests differ from the target lose their
   subscription. This can happen during a rolling deployment, if old and
   new versions of a subscriber request different sources.
3. Sources already allocated to an eligible subscriber stay where they are.
   Sources no longer in the target are removed.
4. Target sources that aren't allocated to an eligible subscriber are
   shared out round-robin across the eligible subscribers.

The leader applies all the changes in a single call to the subscription
state store.

Allocations stick. The leader doesn't rebalance sources when a subscriber
joins, so a new subscriber only gets sources that are free. Because a
subscriber made with `make_subscriber` requests one source, one instance in
each group consumes it and the other instances wait as standbys. Work is
spread across nodes by having many groups, not many instances of one
group. The broker doesn't split a category into partitions.

## Observers

The observer on each node reads every subscription at each
`observer_synchronisation_interval`. It compares them with what it read
last time and works out which sources were allocated and which were
revoked. It then:

1. calls `withdraw` on the node's subscribers for each revoked source;
2. calls `accept` on the node's subscribers for each newly allocated
   source.

It skips subscriptions for subscribers that aren't on its own node. When a
node starts, the observer has read nothing yet, so every source already
allocated to its subscribers counts as new.

For a subscriber made with `make_subscriber`, `accept` creates a consumer
for the source and `withdraw` discards it. The subscriber only reads events
when its `consume_all` method is called, so the source is consumed at the
next poll.

## Heartbeats and health

When the broker starts, the subscriber manager adds each registered
subscriber to the subscriber state store, with the node ID, the
subscription requests and a `last_seen` time. Subscribers registered after
this are not added. Then, until the broker stops, it:

- every `subscriber_manager_heartbeat_interval`, asks each subscriber for
  its
  [`health`](/event.store/reference/processing/#logicblocks.event.processing.EventSubscriber.health).
  For each healthy subscriber, it updates `last_seen`. It logs an error for
  each unhealthy one and leaves its `last_seen` alone;
- every `subscriber_manager_purge_interval`, deletes every subscriber, on
  any node, not seen within `subscriber_manager_subscriber_max_age`.

A heartbeat only updates a subscriber that is already stored. Once a
subscriber has been purged, it stays out of shared state until its broker
registers it again, for example when the broker restarts.

The subscribers made by `make_subscriber` always report themselves as
healthy. Custom subscribers can report `EventSubscriberHealth.UNHEALTHY` to
give up their sources.

## Recovering from failures

When a broker stops cleanly, it deletes its subscribers from the subscriber
state store. At its next distribution, the leader removes their
subscriptions and hands their sources to live subscribers in the same
groups.

When a node fails without stopping cleanly, its subscribers stop sending
heartbeats. Once they haven't been seen for
`coordinator_subscriber_max_time_since_last_seen`, the leader stops
treating them as live and moves their sources at its next distribution.
Their rows stay in the table until a purge removes them. An unhealthy
subscriber loses its sources in the same way.

If the failed node was the leader, another node takes the lock at its next
attempt after Postgres closes the old leader's connection, as described in
[Leader election](#leader-election).

A move isn't instant on both sides. The old subscriber keeps its source
until its own observer next synchronises, and the new subscriber gets it
when its observer synchronises. If the old node is still running, both can
consume the source until the old node's observer catches up. Subscribers
save progress with a write
condition, so one of two competing writers fails rather than overwriting
the other's progress. The broker aims for one consumer per source most of
the time, not all of the time, so make processors safe to repeat.

## State stores and tables

The broker keeps shared state in two stores. Each has an in-memory and a
Postgres implementation.

The
[`EventSubscriberStateStore`](/event.store/reference/processing/#logicblocks.event.processing.broker.strategies.distributed.EventSubscriberStateStore)
holds one record per subscriber. The Postgres version uses the
`subscribers` table, created by `sql/create_subscribers_table.sql`:

| Column                  | Contents                                          |
| ----------------------- | ------------------------------------------------- |
| `group`                 | The subscriber group.                             |
| `id`                    | The subscriber ID. The key is (`group`, `id`).    |
| `node_id`               | The node the subscriber runs on.                  |
| `subscription_requests` | The requested sources, as a JSON list.            |
| `last_seen`             | When the subscriber was registered or last seen.  |

`sql/create_subscribers_indices.sql` adds an index on `last_seen`, which the
leader and purges filter on.

The
[`EventSubscriptionStateStore`](/event.store/reference/processing/#logicblocks.event.processing.broker.strategies.distributed.EventSubscriptionStateStore)
holds one record per subscription. The Postgres version uses the
`subscriptions` table, created by `sql/create_subscriptions_table.sql`:

| Column          | Contents                                              |
| --------------- | ----------------------------------------------------- |
| `id`            | The subscriber ID. The key is (`id`, `group`).        |
| `group`         | The subscriber group.                                 |
| `node_id`       | The node whose store wrote the row.                   |
| `event_sources` | The allocated sources, as a JSON list.                |

Only the leader writes subscriptions, so `node_id` in this table holds the
ID of the leader that created the row, not the node the subscriber runs on.
To find where a subscriber runs, use the `subscribers` table.
`sql/create_subscriptions_indices.sql` adds an index on `node_id`.

The Postgres store applies the leader's changes on one connection, so they
are committed together.

Subscriber progress is not kept in these tables. Each subscriber saves its
own progress as events in its state category, as described in
[Track and resume progress](/event.store/guides/event-processing/#track-and-resume-progress).

## In-memory storage

With `EventBrokerStorageType.InMemory`, the broker creates an in-memory
lock manager and in-memory state stores for itself. No other broker can see
them, even in the same process, so the broker always becomes leader and
only allocates sources to its own subscribers. This is useful for tests and
single-process deployments, but it doesn't coordinate between nodes.

## Settings

[`DistributedEventBrokerSettings`](/event.store/reference/processing/#logicblocks.event.processing.DistributedEventBrokerSettings)
controls the timings described above:

| Setting                                           | Default    |
| ------------------------------------------------- | ---------- |
| `subscriber_manager_heartbeat_interval`           | 10 seconds |
| `subscriber_manager_purge_interval`               | 1 minute   |
| `subscriber_manager_subscriber_max_age`           | 10 minutes |
| `coordinator_subscriber_max_time_since_last_seen` | 60 seconds |
| `coordinator_distribution_interval`               | 20 seconds |
| `coordinator_leadership_max_duration`             | 15 minutes |
| `coordinator_leadership_attempt_interval`         | 5 seconds  |
| `observer_synchronisation_interval`               | 20 seconds |

Keep `coordinator_subscriber_max_time_since_last_seen` several times longer
than the heartbeat interval, so that one slow heartbeat doesn't move a
source. Shorter distribution and synchronisation intervals move sources
sooner, at the cost of more queries.
