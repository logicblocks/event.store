---
type: "topic-research"
id: "04-what-hybrid-push-and-poll-patterns-reconcile-notification-ef-openalex"
title: "What hybrid push-and-poll patterns reconcile notification efficiency with polling durability — notify as a wake-up hint over a durable log, safety-net polling, adaptive and backoff polling, long-polling, and coalescing or debouncing of notification bursts — and how are missed notifications detected and recovered from?"
date: "2026-10-05T13:56:47+00:00"
author: "Toby Clemson"
producer: "research-topic"
status: "complete"
kind: "finding"
round: 1
depth: 3
question: "What hybrid push-and-poll patterns reconcile notification efficiency with polling durability — notify as a wake-up hint over a durable log, safety-net polling, adaptive and backoff polling, long-polling, and coalescing or debouncing of notification bursts — and how are missed notifications detected and recovered from?"
source_profile: "openalex"
tags: ["research", "topic-research", "finding"]
last_updated: "2026-10-05T13:56:47+00:00"
last_updated_by: "Toby Clemson"
schema_version: 1
---

# What hybrid push-and-poll patterns reconcile notification efficiency with polling durability — notify as a wake-up hint over a durable log, safety-net polling, adaptive and backoff polling, long-polling, and coalescing or debouncing of notification bursts — and how are missed notifications detected and recovered from?

## Question

A consumer reads a durable event log. Which designs combine push notification
(for low latency and almost no cost while idle) with polling (for durability
and tolerance of failure)? The patterns in scope are:

- treating a notification as a wake-up hint over the log;
- a safety-net poll;
- adaptive and backoff polling;
- long-polling;
- coalescing or debouncing bursts of notifications.

The question also asks how a consumer can detect that it has missed
notifications, and how it recovers.

## Findings

### The design in brief

No study in the scholarly record looks at PostgreSQL LISTEN/NOTIFY consumers
directly. Several neighbouring fields reach the same answer anyway:

- push-pull dissemination of changing data;
- interrupt and polling hybrids in operating-system network stacks;
- multicast where the receiver is responsible for reliability;
- lease-based cache consistency;
- failure detectors;
- the lost-wake-up problem in synchronisation;
- publish/subscribe;
- queueing control.

They all conclude that **push is a latency optimisation and pull is the
correctness mechanism**. A notification never carries data of record. It only
tells the consumer that reading the durable log is worthwhile now. Nothing is
lost because the consumer reads from its own checkpoint. A read on a timeout
caps how long a failure of the push path can go unnoticed.

The design that follows has seven parts:

1. **A durable, ordered log and a position the consumer owns.** The consumer
   always reads from its own checkpoint, so the log also serves as the
   retransmission buffer.
2. **A level-triggered wake-up hint.** A notification only marks the consumer
   runnable. Notifications collapse into a single pending flag and are never
   treated as work items.
3. **Subscribe, clear, read, wait, in that order.** The subscription is
   committed before the first read. Pending hints are cleared before each
   read, never after it. Only then does the consumer wait.
4. **A wait with a timeout.** This is a long poll, and its timeout is the
   safety-net poll. Two things set the interval: how stale the consumer may
   become when push fails, and the query budget of the whole fleet of
   consumers. The interval is widely jittered and adapts to evidence that push
   has failed.
5. **A switch to continuous batched polling with hysteresis.** The consumer
   switches when its reads show a backlog. It switches back to waiting only
   after a set time with no work.
6. **Explicit loss handling.** The consumer catches up unconditionally at
   start-up and after every reconnect. Heartbeats detect a listening
   connection that has died silently. The count of timeout-driven reads that
   find events measures how many notifications were missed.
7. **A safe read frontier instead of inferring loss from gaps.** When
   positions come from a sequence assigned before commit, the consumer reads
   only below a frontier taken from the database snapshot. It does not treat
   position gaps as evidence of loss.

Under this design a missed notification costs extra latency, at most the
current safety-net interval (or the heartbeat detection bound if that is
shorter). It never costs correctness.

**How to read the evidence:**

- **Inference.** Applying a general result to PostgreSQL, LISTEN/NOTIFY or
  psycopg 3 is this finding's own inference unless stated otherwise.
- **PostgreSQL internals.** Statements about how PostgreSQL works come from
  its documented behaviour, not from the cited records. This covers when
  LISTEN takes effect, how notifications are queued, how transaction IDs are
  assigned, timeout defaults and planner behaviour. Check them against the
  manual for the server version in use.
- **Formulas and numbers.** These are illustrations derived from the cited
  models. None is a measurement of PostgreSQL.

### Why combine push and pull: they fail in complementary ways

Bhide et al. compare push and pull for keeping clients coherent with
fast-changing data. They find the two complementary on four properties: the
coherency they achieve, communication overhead, the state the server must
hold, and the coherency lost when something fails.

- **Push** keeps coherency high and sends messages only when something
  changes. In exchange, the server holds state for every client, and a failure
  or a broken connection silently stops updates.
- **Pull** needs no server state and survives failures. Its interval, though,
  is either too short (wasted requests) or too long (stale data).

They propose two combined, adaptive schemes:

- **Push-and-Pull.** The client keeps pulling on an adaptively computed
  time-to-refresh, and the server pushes the changes those pulls would miss.
  If push fails, the pull schedule carries on, so coherency degrades gracefully
  rather than collapsing.
- **Push-or-Pull.** The server decides for each client whether to push or to
  leave it to pull, depending on available resources.

Bozdag and van Deursen likewise argue for switching between push and pull at
run time in AJAX applications, rather than fixing one approach.

Gray and Cheriton's leases show how to bound the damage when the push side
fails. A lease grants rights for a limited term. As a result, "non-Byzantine
failures affect performance, not correctness, with their effect minimized by
short leases". Their analytic model and V-system evaluation found that fairly
short terms already capture most of the benefit, and that the term matters
more as systems grow.

For an event store the mapping is direct:

- The append-only log is the authoritative state.
- The notification is the push path, and it is allowed to fail.
- The consumer's read from its checkpoint is the pull path, and only that read
  guarantees nothing is lost.

### A notification is a hint; the read is the work

Mogul and Ramakrishnan's work on receive livelock is the clearest precedent
for using a notification only to trigger polling. Interrupt-driven systems
"can provide low overhead and good latency at low offered load, but degrade
significantly at higher arrival rates". Their fix works like this:

1. An interrupt only schedules a polling routine, and further interrupts are
   suppressed.
2. The poller processes pending work to completion in bounded quotas,
   round-robin across sources.
3. Interrupts are re-enabled only when the backlog is empty.

The result is interrupt-driven behaviour at light load and polling at heavy
load. The switch follows the state of the queue, not a fixed rate.

Applied to a log consumer, this gives a precise contract:

- **The notification handler only marks the consumer runnable.** It neither
  parses nor trusts the payload, and does no work itself. Duplicate,
  out-of-order, merged or missing notifications therefore cannot cause a
  correctness error. The worst a missing one can do is delay a read.
- **On waking, the consumer drains the log from its checkpoint in bounded
  batches until a read comes back short or empty.** The batch limit is the
  quota. It bounds memory and latency per iteration and keeps the consumer
  responsive to shutdown and to other work.
- **Under sustained load, notifications become redundant.** When every read
  finds work, reacting to each notification only adds overhead.

Lampson and Redell's account of Mesa monitors sets out the same discipline for
synchronisation. A Mesa NOTIFY is a hint, WAIT accepts a timeout, and the
waiter re-checks its condition in a loop after every wake-up, whatever caused
it. A PostgreSQL notification should be read the same way: as "the log may
have advanced", which triggers a re-read by position.

**Long-polling is this loop seen from the transport.** Long-polling simulates
server push by holding a request open until an event arrives or a timeout
fires. Shuang and Kai compare plain polling, long polling, streaming and
WebSocket on delivery delay and on unnecessary connections. These are exactly
the two axes on which plain polling loses: its latency is bounded by the
interval, and its cost goes on empty requests.

Stratmann, Ousterhout and Madan's Vault shows that the hard part of
long-polling is not the mechanism. It is fitting it into application
frameworks and scaling it across clusters of hundreds of application servers.
For a library, this argues for hiding the wait-and-recheck loop inside the
consumer abstraction rather than exposing notification plumbing to users.

A consumer that blocks on a notification with a timeout *is* a long poll, and
the timeout *is* the safety-net lease. The same shape appears outside the
web:

- Zhai describes replacing short-interval database polling, which loaded the
  database, with a push service built from database triggers and a message
  queue.
- Apple's DNS Long-Lived Queries added change notification to a query
  protocol so clients need not poll. A standards-track push-notification
  design later superseded it.

One loop covers both the wake-up hint and the safety net:

```text
LISTEN, committed (autocommit)               # subscribe before the first read
at start-up and after every reconnect: re-LISTEN, then catch up unconditionally
loop:
    drain already-queued notifications without blocking   # clear hints first
    read from checkpoint in bounded batches until a short read
        (stay in continuous batched polling while batches are full)
    wait for a notification or a freshly jittered timeout T
    adapt T from what woke the consumer and what the read found
```

### Ordering subscribe, read and wait so no wake-up is lost

**The race.** Suppose the consumer's last read comes back empty and it is
about to wait. A producer commits and notifies in that gap. The consumer then
sleeps through the event until the safety net fires. This is the classic lost
wake-up. Luo Yigui's account of the Linux kernel describes the SMP version and
the kernel's fix. The sleeper first publishes its intent to sleep by setting
its task state, then checks the condition, and only then calls the scheduler.
A wake-up landing between the check and the call makes the task runnable
again, so the call returns at once. The general rule: register "I am waiting"
before the check, never after it.

**Three classic remedies, two of which carry over:**

1. **Atomic check-and-wait under a shared lock.** In Hoare's monitors, WAIT
   gives up the monitor atomically as the process suspends, and the signaller
   must enter the same monitor. Buhr, Fortier and Coffin's classification of
   monitor variants shows a point common to all of them: condition variables
   keep no memory, so the mutual exclusion around check and wait is what makes
   them correct. PostgreSQL has no lock spanning the consumer's read and the
   producer's commit-plus-notify, short of serialising every writer behind an
   advisory lock. This remedy does not carry over.
2. **A signal remembered until it is consumed.** Dijkstra's semaphores, from
   the THE multiprogramming system, count V operations. A V issued before the
   matching P is not lost. Once a subscription exists, PostgreSQL LISTEN
   behaves like this: a notification committed while the session is listening
   is queued for it until read, so a wait that starts later returns at once.
   The real hazard is a notification committed before the subscription exists.
3. **Waiting on a value instead of an occurrence.** Reed and Kanodia's
   eventcounts replace "wait for the next signal" with `await(E, v)`, which
   returns once the counter has reached `v`. The race disappears without mutual
   exclusion. For an event store, the log's committed head position and the
   consumer's cursor play the roles of `E` and `v`. Waiting on "head beyond
   cursor" is immune to ordering mistakes in a way that waiting on "a
   notification arrived" is not.

**The rule that fixes the order.** Chandy and Lamport's distributed-snapshot
algorithm states it most generally. A process records its state and starts
recording its incoming channels at the same instant, so every message is
reflected in one or the other:

- If channel recording starts after the state is captured, messages sent in
  between appear in neither.
- If it starts before, a message may appear in both.

For a consumer, the log read is the state capture and the subscription is the
channel recording. The correct sequence is:

1. **Subscribe first.** Issue LISTEN and make sure it has taken effect before
   reading anything. LISTEN takes effect when its transaction commits, which
   in autocommit mode means when the command completes. If the catch-up read
   uses a different connection, it must start after the LISTEN is
   acknowledged.
2. **Clear pending hints, then read.** Drain already-queued notifications
   without blocking, then read from the cursor until a read comes back short.
   This matches the kernel's "set state before checking". A hint cleared after
   the read might be the only record of an event that the read's snapshot
   missed.
3. **Wait with a timeout,** then go back to step 2. Anything committed after
   the drain is still queued, so the wait returns at once even if it arrived
   during the read.

Both wrong orderings fail:

- **Read, subscribe, wait.** An event committed between the read's snapshot
  and the LISTEN raises no notification for this session and is not in the
  read.
- **Subscribe, read, drain, wait.** Draining after the read discards exactly
  the hints the read could not see.

**Does PostgreSQL guarantee a notification is either delivered or visible?**
Yes, under conditions. The guarantee comes from PostgreSQL's implementation,
not from any published model.

The snapshot half is settled in the literature:

- Berenson et al. define snapshot isolation: a transaction sees every
  transaction committed before its snapshot and none after.
- Ports and Grittner describe how PostgreSQL implements this multiversion
  model and builds its serializable level on it.
- Kalay surveys what each of PostgreSQL's isolation levels lets a reader see.

Snapshots are monotone on one server: a transaction committed for an earlier
snapshot is committed for every later one.

The delivery half rests on PostgreSQL's implementation commentary, and is
unverified against any cited source. As described there, committing LISTEN
does three things:

- It adds the session to the shared listener set, so the queue can no longer
  discard messages the session has not read.
- It takes a registration snapshot S_L.
- It holds back messages from transactions still in progress at S_L,
  delivering each one if its transaction commits and dropping it if it aborts.

Every notifying transaction therefore falls on one side of S_L:

- **In progress, or not yet started, at S_L:** its message is delivered when
  it commits.
- **Already committed at S_L:** by monotonicity, its writes are visible to any
  snapshot taken after the LISTEN commit returns.

On a single primary there is no third case. The registration snapshot does
the job that the monitor lock does in Mesa.

The guarantee holds only under these conditions:

- **"Returned" must mean "committed".** A LISTEN inside an explicit
  transaction block returns before it takes effect. psycopg 3 connections are
  not in autocommit mode by default, so the listening connection must use
  autocommit or commit immediately after LISTEN. The implementation commentary
  gives the failure case: LISTEN followed by a SELECT in the same transaction
  can miss both the change and its notification.
- **The snapshot must be later than the LISTEN commit.** Under READ COMMITTED,
  any statement issued after the commit qualifies. Under REPEATABLE READ or
  SERIALIZABLE, the snapshot is fixed at the transaction's first statement, so
  the catch-up read's transaction must begin after the LISTEN commits.
- **Same primary only.** A streaming replica may lag, so reading from one
  does not qualify.
- **"Delivered" means queued for the session.** It does not mean handed to
  application code.
- **The guarantee lasts only as long as the session.** After a dropped
  connection, the consumer must commit a new LISTEN and then read again.

**The client library is part of the subscription.** No scholarly work covers
how psycopg 3 or libpq handle notifications that arrive while a query runs or
while no `notifies()` generator is active. A listen-first consumer is safe
from loss inside the client only if three layers all hold the message:

- the server keeps it for a busy session;
- libpq keeps a message that arrives while it is reading a query result;
- psycopg 3 later passes that message to `notifies()` or a registered
  handler, rather than discarding it.

Behaviour may differ between psycopg releases. The conservative design does
not rely on in-library buffering for correctness. It re-reads the log after
every wait, whatever ended it, and keeps a safety-net poll, so client
buffering affects only latency. A short test can settle the behaviour for a
pinned version:

1. The listening connection runs `pg_sleep`, or sits with no generator open.
2. A second connection commits a NOTIFY on the same channel.
3. The listening connection then opens `notifies()` with a short timeout, and
   the test checks whether the notification arrives.

Repeat the test with and without a registered notify handler.

**Consequences of listen-first ordering:**

- **Spurious wake-ups are the price, and they are harmless.** Chandy and
  Lamport's "in both" case shows up as an extra read that comes back empty. A
  consumer reading idempotently from its stored position absorbs it.
- **Every reconnect is a fresh subscription.** Skipping the catch-up read
  after reconnecting leaves events committed during the outage covered only
  by the timeout.
- **Payloads can make the wait value-based.** If notifications carry the
  committed position, the consumer can skip a re-read when every pending
  payload is at or below its cursor. With positions assigned before commit,
  though, a payload below the cursor can mean a gap has just filled. Using
  payloads to suppress reads therefore needs the gap handling described below.

NOTIFY descends from the "alerters" that Stonebraker and Rowe listed among the
original POSTGRES design goals for active-database support. That paper does
not set out how registering for an alert interacts with concurrent commits.
The deliver-or-visible property came later, from the implementation.

### Coalescing and debouncing bursts within a consumer

The drain loop coalesces bursts by construction. Any number of notifications
that arrive while a drain runs reduce to "check once more". The wake signal
should therefore be **level-triggered**: one pending flag or one-slot event,
not a queue of per-event callbacks.

The one correctness rule is ordering: **clear the flag before starting the
drain, never after**. If the flag is cleared after the drain, a notification
that arrived mid-drain is erased while its event may still be unread. Only the
safety net would then recover it.

**Debouncing** goes further. The consumer waits a short "linger" window after
the first wake-up before draining, trading a little latency for larger
batches. This pays off when writes come in bursts and the overhead of each
read dominates. In interrupt-coalescing terms, it moves the switch from
interrupt-like to polling-like behaviour to a lower load.

PostgreSQL itself merges identical notifications on the same channel within a
single transaction, but not across transactions. Coalescing across commits is
therefore the consumer's job.

### Reducing fan-out when many consumers listen

With N consumers, each holding its own connection and listening on the same
channel, one commit costs N notification deliveries and N near-identical log
reads. Many of those reads are redundant. Vattani, Chierichetti and Lowenstein
call this structure a cache stampede: one trigger sends many processes to the
backing store at the same moment, which "severely limits the performance of
databases and web servers". An operational study of the DNS thundering-herd
problem found the same pattern of synchronised query spikes on caching
resolvers, and recommended fixing it in client software.

Carzaniga, Rosenblum and Wolf split an event-notification service into two
jobs: *selection* (which notifications match which subscriptions) and
*delivery*. Eugster et al.'s survey of publish/subscribe describes subscribers
that register interest and are notified asynchronously through an
intermediary.

A per-process dispatcher becomes that intermediary, in four cumulative steps.
This application to LISTEN/NOTIFY is inference, and the costs below come from
counting operations, not from measurement.

1. **One LISTEN connection per process.** The dispatcher holds the process's
   only listening connection and an in-memory registry of its consumers'
   subscriptions. Server deliveries per commit fall from N to P, the number of
   processes. Reads stay at N if every consumer still runs its own query.
2. **Selective wake-up.** Appenders put a compact selector, such as a category
   or stream identifier, in the NOTIFY payload, never the event body. The
   dispatcher wakes only matching consumers, as Google's Thialfi notifies
   applications only about objects they registered for. Because the wake-up
   is only a hint, falling back to waking everyone is always safe. The
   dispatcher should do so whenever the payload is missing, unparsable or
   unrecognised.
3. **Shared read.** For caught-up consumers on the same selector, the
   dispatcher runs one query from the lowest of their positions and gives each
   consumer the slice beyond its own. Consumers far behind are excluded and
   catch up alone, so one laggard does not make every shared read fetch its
   backlog.
4. **Single-flight coalescing.** If a read for a selector is already running
   when another notification for it arrives, the dispatcher marks the selector
   dirty and runs exactly one follow-up read. This is the lock-style defence
   against a stampede: a burst of b notifications costs at most two reads, not
   b.

| Design (one event matching k of N consumers in P processes) | Server deliveries | Log reads |
|---|---|---|
| Independent listeners | N | N |
| One shared connection per process | P | N |
| + selective wake-up | P (fewer with channel partitioning) | k |
| + shared read per selector | P (or fewer) | about one per process hosting a matching consumer |
| + single-flight coalescing (bursts) | P (or fewer) | at most 2 per selector per process per burst |
| Cross-process relay | 1, to the relay | 1 |

**Channel partitioning.** Appenders can NOTIFY a channel per category, so the
server does the selection and processes with no matching consumer are never
woken. It costs one NOTIFY per affected channel per transaction, and suits a
small, stable set of categories. It combines with payload selectors: the
channel selects coarsely on the server, the payload finely in the dispatcher.

**Desynchronise what remains.** All P dispatchers still wake on the same
commit, and safety-net polls that started together, for example after a
deployment, keep firing together. Vattani et al.'s defence is probabilistic
early expiration: each requester refreshes at a random point before the
deadline. They prove algorithms for this randomisation optimal. Applied here,
a small random jitter goes on each dispatcher's post-notification read and on
each safety-net interval. The jitter window should be small compared with the
latency budget.

**A relay across processes.** Production change-distribution systems remove
the remaining factor of P with a dedicated tier:

- LinkedIn's Databus refreshes or invalidates derived stores, indexes and
  caches when the primary changes.
- Facebook's Wormhole replicates changes among services including TAO, Graph
  Search and Memcache, at over 35 GB/s and about 50 million messages per
  second.

Neither paper measures where a relay starts to beat per-process sharing. In a
library that depends on PostgreSQL alone, the closest equivalent is to elect
one reader with an advisory lock. Its results would then have to travel back
through another NOTIFY or table write per event, so (inference) it saves
little unless P is large.

**Hazards that sharing introduces:**

- **Shared fate.** If the dispatcher's connection dies silently, every
  consumer in the process stops waking. Liveness detection and the safety net
  must run in the dispatcher, covering every consumer's position.
- **Head-of-line blocking.** In-memory handover needs a bounded queue per
  consumer. A consumer whose queue overflows drops out of the shared read and
  catches up alone.
- **Correctness rules move to the dispatcher.** Subscribe-before-read
  ordering and position safety now apply to the shared read. A mistake there
  affects every consumer in the process.
- **Payload limits.** Payload size is limited and identical notifications in
  one transaction merge, so selectors must be compact. A missing selector must
  mean "wake everyone".

### Switching to continuous batched polling under load, and back

**Livelock is not saturation.** Mogul and Ramakrishnan's receive livelock is
a collapse, not a plateau. A saturated system levels off at its maximum
loss-free rate. A livelocked one falls below it, because the high-priority
notification path keeps pre-empting the work that would finish each item.
Their remedy combines several measures:

- polling with quotas, round-robin across sources;
- disabling input when a downstream queue fills or a CPU limit is reached;
- dropping doomed work early, and finishing admitted work.

Other systems apply the same load-adaptive batching:

- **IX** processes bounded batches to completion, and its batches grow only
  when work is backed up.
- **SEDA** joins event-driven stages with explicit queues, and uses resource
  controllers, including one that tunes events taken per handler call, to keep
  each stage within its operating regime.
- **Pan Qing** adds feedback control to kernel protocol processing, noting
  that most earlier overload fixes "combine polling and interrupt processing
  together".
- **Salah, Elbadawi and Haidari** compare interrupt-handling schemes
  analytically at gigabit rates. Their record has no abstract, so their
  crossover results are not available here.

**A cost model for a PostgreSQL consumer** (inference). Let:

- `c_w` be the cost of waking up for one notification;
- `c_q` be the fixed cost of one read (round trip, planning, execution);
- `c_e` be the cost per event.

The two capacities are then:

- **One read per notification**, each returning about one event, gives a
  capacity of about **λ₁ = 1 / (c_w + c_q + c_e)**.
- **Batches of B** give about **λ_B = 1 / (c_q/B + c_e)**.

There is no universal threshold. As an unmeasured illustration, a read
dominated by a 1 ms round trip puts λ₁ below about 1,000 events per second
per consumer.

Because the log is the buffer, the livelock analogue shows up as wasted work
and growing lag rather than lost deliveries. Three designs produce it:

1. **A read per notification** even when an earlier read already covered the
   event. The read rate stays at λ while useful rows per read fall.
2. **Payload-driven processing.** Handling events from payloads one at a time
   prevents batching, caps capacity near 1 / (c_w + c_e), and lets backlog
   build in the connection's notification buffer.
3. **Per-notification reads across N consumers.** These put about N·λ queries
   per second on the database, against N·λ/B for batched polling. The extra
   queries compete with the commits that produce the events.

**Signals for switching.** The most robust signal is the result of the last
read. It calibrates itself; a rate threshold would have to be tuned per
deployment.

**Switch to continuous batched polling** when either of these holds:

- a read returns a full batch;
- notifications arrived while the read was running (a cheap proxy for λ ×
  read time ≥ 1).

While polling, the consumer:

- reads again immediately;
- drains and discards pending notifications;
- caps every batch;
- yields round-robin to other subscriptions and to housekeeping, such as
  lease heartbeats and checkpoint flushes;
- stops reading when the downstream handler queue is full, letting the log
  absorb the backpressure.

**Switch back** when reads come back short or empty and no notification
arrived during them.

**Hysteresis.** Chang et al. report that the interrupt-polling hybrids in
Linux and FreeBSD suffer when they control polling time "without regard to
the system state". They propose queue-based adaptive polling instead. They
also note that adaptive interrupt-polling and coalescing schemes "are designed
using heuristics", so the field offers no standard constants.

Queueing-control theory says why two thresholds are needed:

- **Heyman** proves that for an M/G/1 server with start-up, shut-down,
  running and holding costs, the optimal policy is "turn the server on when n
  customers are present, and turn it off when the system is empty". That is a
  hysteresis band.
- **Lu and Serfozo** show that when switching between rates costs something,
  optimal policies are *monotone hysteretic*: the threshold for switching up
  sits above the one for switching down.
- **Diab** learns such thresholds online when the costs are not known
  analytically.

A single threshold is exactly what makes the mode oscillate. A plain
notification-driven consumer that reads until a short batch and then waits is
Heyman's policy with n = 1. Near the crossover it runs about one on-off cycle
per burst.

The following settings are derived, not quoted, and should be checked against
measured costs and arrival traces:

- **Crossover.** Waiting costs about `c_wake` per wake-up, and polling costs
  `c_poll` per interval Δ. They cross near **λ\* ≈ c_poll / (c_wake·Δ)**,
  which is about 1/Δ when the two costs are similar. This is a cost
  crossover. It is different from the capacity limit λ₁ above, but both point
  to backlog signals rather than calibrated rates.
- **Band width.** Under the standard N-policy cost analysis, with switching
  cost K and staleness cost h, the band is **n\* ≈ √(2·K·λ·(1−ρ)/h)**, where ρ
  is the consumer's utilisation. The band grows only with the square root of
  the switching cost, so cheap switches allow a narrow band.
- **Entry: enter fast.** Switch on a single full batch, or when a smoothed
  count of events per wake-up exceeds an upper bound U set somewhat above
  λ\*. Count events per read rather than notifications: one transaction can
  append many events, and PostgreSQL collapses identical notifications within
  a transaction. Liu and Tseng treat bulk arrivals as their own hysteretic
  control case; their record gives only the paper's scope.
- **Exit: leave slowly.** Leave only after an idle dwell time T_off, never
  after a single empty poll. Take the larger of two values:
  - **A cap on the switch rate.** For roughly Poisson arrivals, T_off =
    ln(λ\*/r_max)/λ\*. For example, λ\* = 20/s and r_max = 1 per minute give
    T_off ≈ 0.36 s, about eight empty polls. Real streams are burstier, so a
    measured gap quantile (1 − r_max/λ\*) is better.
  - **A ski-rental floor** of M ≈ K/c_poll empty polls. A standard competitive
    argument keeps the combined cost of empty polls and switches within about
    twice the best achievable.

  Gandhi et al.'s AutoScale uses the same "delayed off" idea for idle
  servers under unpredictable load.
- **Noise margin.** If the switch uses an explicit rate estimate over a
  window w, the band must clear its noise of about √(λ\*/w). For example, z =
  3, λ\* = 20/s and w = 5 s give ±6/s. Add a minimum time in each mode as a
  backstop.
- **Make switches cheap.** Never UNLISTEN on a mode change. Keep the
  subscription live in both modes and discard queued hints while polling. The
  last empty poll then already ran under a live subscription, so the switch
  back needs no extra query.

### Sizing and adapting the safety-net poll

**The safety net is a lease on silence.** Between polls, the consumer's
belief that nothing new has arrived is valid only for a term T. When T expires
without a wake-up, it polls anyway. This changes how the interval is chosen.
A pure-polling interval is set by the latency you want. A safety-net interval
is set by the staleness you can tolerate *under failure*. Notifications handle
the common case, so T can be far longer than the fixed interval it replaces.

Bhide et al.'s pull side has the right shape. Its time-to-refresh adapts but
is always clamped between a TTR_min, which prevents over-polling, and a
TTR_max, which keeps the client checking even when nothing seems to change.
Three inputs then decide T:

- the staleness acceptable under failure sets the **upper** bound;
- fleet-wide query cost sets the **lower** bound;
- measured loss decides where the consumer sits between them.

**Upper bound.** Chen, Toueg and Aguilera's failure-detector analysis gives
the model. A detector that sends heartbeats every η and allows slack δ
detects a failure within about η + δ. By analogy, the largest interval the
randomiser can produce, plus high-percentile poll latency, should stay within
the acceptable failure staleness S_fail. TTR_max is this value. Choose it from
the failure budget, not from typical traffic.

**Lower bound.** N consumers each spending c per poll at interval T impose an
average load of N·c/T, so T_min ≥ N·c / (budgeted share of capacity). Three
results argue for budgeting well below saturation and for watching peaks:

- **Polling faster can make data staler.** Kaul, Yates and Gruteser show
  that update frequency has an interior optimum. Updating too often fills the
  queue, and waiting time *increases* average age. In their M/M/1
  first-come-first-served model, age is lowest at about 0.53 utilisation. The
  number is specific to that model, but the lesson carries over: past some
  aggregate poll load, shorter intervals make every consumer staler.
- **Periodic pollers synchronise.** Floyd and Jacobson show that independent
  periodic processes can lock into step through weak coupling, typically a
  timer reset after an event all of them see. The change is an abrupt phase
  transition. Their remedy is a large random component, about half the
  period, and timers that do not reset on shared events.

  For PostgreSQL consumers this is a concrete hazard (inference). One NOTIFY
  wakes every listener. If each restarts its safety-net timer then, the
  fleet's polls line up into bursts of N·c, and the same happens after a
  database restart. Draw each interval freshly from a wide range, such as
  [0.5T, 1.5T], and size the upper bound against the top of that range.
- **Fixed budgets favour even allocation.** Cho and Garcia-Molina find that
  spreading a fixed polling budget evenly across sources beats allocating in
  proportion to change rate. The optimum may poll the fastest-changing sources
  *less*. A consumer's safety-net rate should not simply grow with how busy
  its stream is.

If T_min exceeds T_max, polling alone cannot meet the staleness target at that
fleet size. The options are cheaper polls, fewer independent pollers, or
direct detection of channel failure, which lets the steady-state interval
relax.

**The structure of loss matters more than its rate** (an illustrative model,
not from the sources):

- **Isolated loss** is repaired by the next delivered notification, because
  every read picks up everything after the stored position. With Poisson
  arrivals at rate λ and independent loss probability p, a lost event's
  expected staleness is about (1 − e^(−λ(1−p)T)) / (λ(1−p)). That is roughly
  the time between events for busy streams, and approaches T for quiet ones.
- **Correlated loss** loses *every* notification until recovery. Examples are
  a dropped listener, the window before a re-issued LISTEN takes effect, or a
  consumer not listening at all. Only the safety net helps.

So the upper bound is sized for the correlated case, and the isolated loss
rate mostly determines how often polls are wasted.

**What to adapt on.** Classic adaptive pull follows the data's rate of
change. Bhide et al.'s time-to-refresh tracks observed change. Bright, Gal and
Raschid's history-based policies predict update times, with extra handling for
bursts and sparse histories. A *safety-net* poll should instead react to
evidence that push failed, because changes that arrive with notifications are
already handled (inference). A policy within [T_min, T_max]:

1. **Steady state.** Run near T_max, with wide jitter, on a timer that is not
   reset by notification-driven reads, or is reset only with fresh jitter.
2. **Suspected outage.** On reconnect, on LISTEN re-establishment, or when a
   poll finds events that were never announced, poll at once, then drop to
   T_min. This mirrors Bhide's fallback to pull when push fails.
3. **Recovery.** After k consecutive polls find nothing unannounced, grow the
   interval multiplicatively back towards T_max.
4. **Overload.** If poll latency rises, the queueing effect Kaul et al.
   describe has set in. Raise T_min rather than polling harder.

Heartbeats sharpen this. If channel liveness is checked directly (see below),
the safety net only has to cover isolated loss, and its upper bound can be
relaxed.

**One silence that no notification can end.** A consumer that reads only
below a snapshot frontier (described below) can find committed events it may
not yet deliver. When the unrelated transaction holding the frontier finally
finishes, it sends no notification, because it appended nothing. While events
are withheld, the consumer should re-check on a backoff timer capped at the
safety-net interval, instead of waiting for a NOTIFY.

### What the safety-net poll should ask the database

A safety-net poll needs only a yes or no answer: is there a committed, visible
event beyond this consumer's position that matches its filter? The candidate
queries differ in cost and correctness:

1. **The sequence high-water mark.** Its cost does not depend on log size,
   but sequences are non-transactional. The value counts in-flight and
   rolled-back draws, and per-session caching pushes it further ahead.
   - "Last value ≤ my position" proves there is nothing unread.
   - The reverse is only a hint, and stays true for good after any rollback.
   - Storing the value as a watermark can miss an append that drew its
     position before the check but commits after it.

   Use it only as a pre-filter.
2. **A head probe.** `max(position)`, or `ORDER BY position DESC LIMIT 1`,
   becomes a backward index scan (planner behaviour, from documentation). One
   probe gives the newest visible position, which the process compares in
   memory with every unfiltered consumer's position. This scales best with
   consumer count.
3. **An existence probe.** `EXISTS (… WHERE position > $1)`, or with a
   filter, `WHERE category = $2 AND position > $1`. The filtered form is cheap
   only with a composite B-tree index on (filter columns, position). Without
   one, cost grows with the non-matching events after the consumer's position,
   up to a full scan.
4. **A head row maintained by appenders.** Reading it costs about O(1), but
   every append updates one hot row. That serialises appenders on its row
   lock and creates a dead tuple per append. It is the wrong trade for a rare
   safety-net read.

**Cost against log size.** Bayer and McCreight's B-tree retrieves in time
proportional to log_k of the index size, where k is set by the page size.
Comer's survey explains why the B+-tree became the standard database index,
and Graefe surveys the modern techniques built on it. By the finding's own
arithmetic, an 8 KiB page holds a few hundred 8-byte keys, so the tree is
three or four levels deep even for billions of events. The rightmost
root-to-leaf path is touched by every append and every probe and stays
cached. A probe therefore costs a few buffer hits whether the log holds a
million events or a billion.

Two effects depend on the log's *tail*, not its length (PostgreSQL behaviour,
not from the records):

- **Heap visits.** Recent heap pages are usually not marked all-visible, so
  the probe visits the heap to check tuple visibility.
- **Invisible entries.** The probe walks past entries its snapshot cannot
  see: in-flight appends, and dead tuples from rollbacks. These remain until
  they are marked dead or vacuumed.

The one way a "cheap" probe grows with log size is a missing or mismatched
index.

**Cost against consumer count.** C consumers polling independently every T
make C/T probes per second. Each probe is dominated by fixed per-statement
cost (round trip, parse, plan, snapshot), so the load grows linearly with C.
Three ways to flatten it:

- **One head probe per process per tick.** Compare it in memory with every
  consumer's position. Database load then grows with the number of processes,
  not consumers.
- **One batched probe for filtered consumers.** A single statement joins
  their positions to a lateral existence probe, at about F·log N page visits
  for F distinct filters.
- **Prepared statements.** These remove repeated planning.

Kafka's replicated commit log is the same design. Whether a consumer has
unread data is a comparison between its own offset and the log end, which is
read once and shared. (The Kafka record's abstract covers only the commit-log
architecture; the offset comparison comes from Kafka's general design.)

Lehman and Yao's B-link tree lets readers proceed without read locks during
concurrent splits. PostgreSQL's B-tree derives from that design (background
knowledge, not from the record), so probes do not block appenders at the lock
level. Only brief buffer-content locking on the hot leaf remains. This is one
more reason to share a single probe per process.

**Visibility must match the read path.** If the consumer reads only below a
snapshot frontier, but its probe compares against the raw newest position, the
probe keeps reporting work the read cannot return, and the safety net turns
into a busy loop. Compare against the gated head.

### Detecting and recovering from missed notifications

Floyd et al.'s scalable reliable multicast framework gives the most developed
model of loss detection:

- receivers, not the sender, own reliability;
- data is named by source and sequence number, so a gap reveals a loss;
- loss of the *last* packet, which no later packet exposes, is detected by
  periodic session messages advertising the highest sequence number seen;
- repair requests use randomised timers to suppress duplicates.

Carried over to a consumer of a durable log:

1. **Recovery is always a re-read from the consumer's own checkpoint.** The
   log is the retransmission buffer, and no server state about consumers is
   needed. Because every wake-up drains from the checkpoint, gaps in
   notifications heal themselves. Explicit detection then serves mainly
   monitoring and tuning, not correctness.
2. **Gap detection needs notifications that carry a position.** Comparing a
   notified position with the checkpoint and with the previous notification
   reveals missed or merged notifications. With positions assigned before
   commit, a gap may be an in-flight transaction rather than a loss (see the
   next section). Treat such gaps as a monitoring signal only.
3. **Loss at the tail needs an equivalent of the session message.** A
   periodic probe of the (gated) head, compared with the checkpoint, catches
   the case where the last notification was lost and nothing has followed.
   The safety-net poll is this probe.
4. **Every reconnect is a known window of loss.** Notifications sent while the
   listener was down were never queued for it. Re-subscribe, then drain
   unconditionally.
5. **Count timeout-driven and reconnect-driven drains that find events.** That
   count measures missed notifications in production, and is the right input
   for the adaptive interval. More precisely, measure the fraction of events or
   batches first seen through a safety-net poll rather than a
   notification-triggered read.

**Detecting a listening connection that died silently.** A half-open socket,
a middlebox dropping an idle flow, or a failover can stop all notifications
without any error. Silence then looks the same as "no new events".

Chandra and Toueg characterise failure detectors by completeness and accuracy.
They show that useful detectors can still make mistakes, so a timeout over an
asynchronous channel yields suspicion, not proof. Heartbeat counters alone do
not settle the question. Aguilera, Chen and Toueg's timeout-free heartbeat
detector only produces growing counters, and later work on that model notes
that an algorithm using it "can do nothing without time-out technology" when
it must separate crashed peers from correct ones.

The consumer's response to suspicion makes errors cheap (inference): close
the connection, reconnect, re-issue LISTEN and catch up from the checkpoint.
That response is idempotent. A false suspicion costs one reconnect and one
query, never correctness, so the consumer can be tuned for faster detection
than consensus systems use.

The mechanism:

- Something emits a NOTIFY every η seconds, carrying a sequence number and a
  send timestamp, on the consumer's channel or on a shared heartbeat channel.
- The consumer suspects the connection when no fresh heartbeat has arrived by
  a deadline.
- Satzger et al.'s lazy monitoring shows that ordinary application messages
  can stand in for heartbeats, so real event notifications also prove the
  connection is alive. Explicit heartbeats are then strictly needed only while
  idle. Mixing in irregular, unnumbered event notifications pushes the
  detector towards a simple reset-on-receipt rule.

Chen, Toueg and Aguilera define a detector's quality of service from the
measured delay distribution D and loss probability p_L:

- **detection time T_D** (inference: keep it clearly below the safety-net
  interval, or the heartbeat adds little);
- **mistake recurrence time T_MR** (inference: N/T_MR is the fleet's
  spurious-reconnect rate);
- **mistake duration**, which matters less here because a suspicion ends in a
  reconnect.

Their detector, NFD-S, sends heartbeat i at time iη and trusts the sender at
freshness point iη + δ only if heartbeat i or a later one has arrived:

- **Worst-case detection.** T_D ≤ η + δ.
- **Chance of a false suspicion.**
  p₀ = ∏_{j=0}^{⌈δ/η⌉−1} [p_L + (1 − p_L)·Pr(D > δ − jη)].
- **Mean time between false suspicions.** About η / p₀.
- **Configuration.** Set δ = T_D^U − η, then choose the largest η that meets
  the accuracy target. If none does, the target cannot be met on that
  network.
- **Partial measurements.** The procedure still works from estimates of p_L,
  E(D) and V(D) alone, using a one-sided Chebyshev bound.
- **Comparison.** It beats the common rule of restarting a fixed timer on
  every heartbeat.

For a fixed detection bound, a shorter period puts more heartbeats in each
window. The false-suspicion rate then falls roughly geometrically while the
message cost grows only linearly.

A worked example (inference):

- With a 10 s detection bound, negligible loss and a 10⁻⁶ delay tail beyond
  5 s, η = δ = 5 s gives about two months between spurious reconnects per
  consumer.
- With p_L = 10⁻², the same settings give about eight minutes.
- With η = 2 s and δ = 8 s, four heartbeats fall in each window, and p₀ drops
  to about 10⁻⁸.

When clocks differ or delay drifts, several adaptive refinements apply:

- **Chen et al.** estimate each heartbeat's expected arrival from recent
  arrivals and add a safety margin α, using only the receiver's clock.
- **Bertier, Marin and Sens** compute that margin in the style of Jacobson's
  TCP retransmission timer: a smoothed delay plus a multiple of its smoothed
  deviation. Jacobson introduced that estimator because timers set from the
  mean alone fire spuriously as variance rises under load.
- **Yang et al.** predict arrivals with exponential smoothing, which follows
  linear trends, and probe the peer actively before declaring failure.
- **Zhao et al.** forecast delay with a small neural network, though
  (inference) a consumer gains little beyond what a smoothed mean and
  deviation give.

**Graded suspicion.** Hayashibara et al.'s φ accrual detector outputs a
continuous suspicion level. It fits a distribution to recent inter-arrival
times and reports φ = −log₁₀ of the probability that a heartbeat would arrive
later than the time already elapsed. A threshold Φ gives a false-suspicion
likelihood of about 10^−Φ. A consumer can respond in proportion (inference):

1. At low φ, run a cheap catch-up read.
2. At higher φ, actively probe the connection.
3. At the highest φ, reconnect.

**Pitfalls:**

- **Application stalls.** Wang and Li note that application-layer heartbeats
  are destabilised by scheduling and load. Timestamp heartbeats where the
  consumer actually sees them, or give LISTEN a dedicated connection, so that
  event-loop stalls are either measured or removed.
- **Correlated delay.** The quality-of-service analysis assumes independent
  delays and losses. A database-wide stall delays everyone's heartbeat at
  once. Probing before reconnecting, and jittering reconnects, prevent a
  reconnect storm.
- **Bandwidth.** Wang and Kuo treat bandwidth spent on failure detection as a
  budget. One NOTIFY per period on a shared channel reaches every listener at
  a cost independent of consumer count. Self-heartbeats from N consumers on
  one channel cost N² receipts per period.
- **Who sends the heartbeat.** A heartbeat sent from the consumer's own
  second connection shares its clock and tests the whole server-to-socket
  path. A heartbeat from an external emitter can stop for reasons unrelated to
  the consumer, and only an active probe distinguishes the two cases.

**Recipe.**

1. Number and timestamp heartbeats, and record their delay and sequence gaps.
2. Pick the detection bound from the staleness acceptable during a listener
   failure, below the safety-net interval, and the false-suspicion bound from
   the tolerable fleet reconnect rate.
3. Set η and δ by the Chen–Toueg–Aguilera rule, with a smaller η when loss is
   significant.
4. Adapt the margin online, using a mean plus deviation or a φ threshold.
5. On suspicion, probe, then reconnect, re-LISTEN and catch up.

**A stalled listener can break writers.** PostgreSQL's documentation says
that once the shared notification queue is full, transactions that call
NOTIFY fail at commit. If an append and its NOTIFY share a transaction, a
listener that stops reading, for example one left idle in a transaction, can
make appends fail. Listener liveness therefore protects producers as well as
consumers.

### Why position gaps are not a dependable loss signal

**A gap is ambiguous.** When positions are drawn from a sequence before
commit, a missing position looks the same whether its transaction is still
running or will never commit. A rule that sees only positions must either
wait on the hole, which stalls forever if it is permanent, or step past it,
which loses the event if it later fills. Rollbacks are not the only source of
permanent holes. Others include:

- failed statements that drew a value;
- per-session sequence caching;
- values pre-allocated and then lost in crash recovery.

A missing row carries no transaction ID, so the consumer can only ask whether
any transaction could still produce rows there.

**SERIALIZABLE does not help.** Ports and Grittner's account of serializable
snapshot isolation (SSI) in PostgreSQL includes Fekete et al.'s
batch-processing anomaly. A read-only report sees a batch as closed while a
receipt for it is still in flight. That has the same structure as a consumer
seeing position N while a lower position is uncommitted. SSI prevents the
anomaly only because the transactions conflict through a shared counter read
and written transactionally. A sequence draw sits outside transactional
isolation. Running the consumer at SERIALIZABLE therefore does not close the
gap (inference from SSI's dependency model).

The paper's "safe snapshots" and deferrable read-only transactions are
PostgreSQL's own precedent for waiting until concurrent writers resolve
before trusting what is seen.

The systems literature achieves "neither stall nor skip" by changing what
evidence the consumer uses, in four ways.

1. **A stable frontier from knowledge of in-flight work.**
   - **Spanner** serves a read at t only if t ≤ t_safe. t_safe sits just below
     the earliest prepare timestamp among prepared-but-uncommitted
     transactions, and no commit can fall below its prepare timestamp.
   - **Kafka**, as Wang et al. describe its transactional log, serves
     read-committed consumers only up to the "last stable offset", just before
     the first record of a still-open transaction. Commit and abort markers
     announce which ranges to discard, so a permanent gap is announced rather
     than inferred. A coordinator timeout aborts hung transactions.
   - **In PostgreSQL** (design inference from snapshot semantics), each append
     records `pg_current_xact_id()`. The consumer reads only events whose
     transaction ID is below the xmin reported by `pg_current_snapshot()`.
     Every transaction below xmin has finished, so any remaining gap among
     those events is permanent and safe to pass. The cursor must be the pair
     (transaction ID, position), ordered by transaction ID first. A transaction
     ID is assigned at a transaction's first write, while a position is
     assigned at insert, so a lower ID can hold a higher position. Delivery
     order becomes (transaction ID, position), which is acceptable where stream
     versions already order events within each stream.
2. **Resolve each hole explicitly.** CORFU's sequencer hands out positions
   before clients write. A reader that suspects a hole writes a junk value
   into it. Positions are write-once, so exactly one of the fill and the
   original write succeeds, and the hole is settled for good.

   A PostgreSQL analogue (an inference, not an established pattern) is to
   insert a placeholder at the suspect position with `ON CONFLICT DO NOTHING`.
   The insert blocks on an uncommitted conflicting row, then:
   - lands if the holder aborted, marking the gap permanent;
   - does nothing if the holder committed;
   - wins the race if the holder had drawn but not yet inserted, forcing the
     writer to retry.

   The costs are consumer writes to the log, extra contention, writer retries
   and the need for a lock timeout.
3. **A time-bounded heuristic.** "Wait T, then skip" is what Akidau et al.'s
   Dataflow model calls a heuristic watermark. It can be too slow (a stall) or
   too fast (a skip). Their remedy separates the completeness signal from
   triggering and lets late data revise earlier results. Wang et al. likewise
   treat completeness as a trade-off between correctness, latency and cost.
   A skip timeout can never guarantee both no stall and no skip. It is a
   monitored fallback, paired with re-checks of skipped positions that report
   late arrivals.
4. **Make transient gaps impossible.** CockroachDB's closed timestamps refuse
   writes at or below a closed frontier, so readers below it never wait. On one
   PostgreSQL node the equivalent is to serialise appends, for example with a
   lock held until commit around position assignment, so that position order
   equals commit order. Then any gap below the highest visible position is
   permanent, and the naive gap rule becomes correct.

**For a store built on PostgreSQL alone:**

- Do not run gap-based loss detection on raw positions.
- Gate reads on the snapshot xmin with a (transaction ID, position) cursor,
  and bound stalls with timeouts.
- Keep any time-based skip as a monitored, opt-in fallback.
- Keep notifications as pure hints. Their payload says nothing about
  uncommitted transactions, so it cannot settle a gap.

### The price of a safe frontier: gate stalls versus serialised appends

**Xmin gating trades append throughput for read latency, and it can stall.**
The gate is one global low watermark. The literature on garbage collection in
multiversion concurrency control (MVCC) engines shows the same structure from
the other side: reclamation is limited by the oldest snapshot.

- Wu et al. treat garbage collection as a main design dimension of MVCC.
- Lee et al.'s hybrid collector for SAP HANA exists so that long-lived
  snapshots do not block all reclamation.
- Böttcher et al. find the usual collectors "too coarse-grained" under
  long-running queries, and track the full set of active snapshots instead.
- Kim et al. report that, as of 2020, the latest MySQL and PostgreSQL releases
  were still vulnerable to long-lived transactions.

Streaming systems call this a low watermark. MillWheel takes the minimum over
inputs' oldest pending work, so one stuck input holds back everything
downstream. Akidau et al.'s treatment of watermarks frames them as a claim
about completeness, with the same trade-off between waiting and advancing
heuristically.

**Which sessions hold the gate.** The following mechanics come from
PostgreSQL's documentation, not from the records.

The vacuum horizon (`backend_xmin`) is held back by any open snapshot,
replication slot or `hot_standby_feedback`. It causes bloat, but it does not
move the consumer's gate. The gate is the oldest *assigned* transaction ID
still running. An ID is assigned only on a transaction's first write,
including row locks such as `SELECT … FOR UPDATE`. So the gate is held by
sessions that have written but not finished, for example:

- a request idle in transaction while it waits on an external call;
- a migration or batch job running in one long transaction;
- a client that is not in autocommit mode and wrote without committing;
- a prepared two-phase transaction, which survives restarts.

Transaction IDs are cluster-wide, so a session in another database on the
same server holds the gate too. Held back how far:

- **In time, there is no limit by default.** `statement_timeout`,
  `idle_in_transaction_session_timeout` and `transaction_timeout` (PostgreSQL
  17 and later) are all off by default.
- **In events, everything appended since the blocker's first write** is held
  back.
- **The only hard limit is transaction-ID wraparound.** After about two
  billion IDs the cluster stops accepting writes. That is roughly 25 days at
  1,000 writing transactions per second, and under three days at 10,000.

A consumer that writes its checkpoint and then reads the gate in the same
transaction holds back its own gate. It should read the gate in a
transaction that has not yet written.

**Detecting the stall.** An empty read is ambiguous: it can mean "caught up"
or "gated". Three signals tell these apart:

1. **Withheld events.** A cheap existence check for visible committed events
   beyond the frontier whose transaction ID is at or above the gate. Report
   this gap separately from processing lag.
2. **Gate age.** The wall-clock time since the gate last moved while events
   were withheld. Optionally add the transaction-ID age of the gate, to compare
   against wraparound.
3. **The blocker's identity.** Match the gate value against `backend_xid` in
   `pg_stat_activity` (pid, state, `xact_start`, `application_name`) and
   against `pg_prepared_xacts`. Seeing other roles' sessions needs a
   monitoring role such as `pg_read_all_stats`.

**Bounding the stall**, in order of preference:

1. Run the event store on its own cluster. A separate database on a shared
   cluster does not help.
2. Set server-side timeouts for application roles, and set
   `max_prepared_transactions = 0` where two-phase commit is unused.
3. Use autocommit or scoped transactions on every library-owned connection,
   including the listener.
4. Above a threshold, report the stall and keep waiting. Never step past the
   gate silently. Terminating the blocker should be an explicit, opt-in
   operator policy.
5. Where liveness matters more than strict order, offer heuristic advance with
   late-data reconciliation, in the style of Akidau et al., as an explicit
   semantic change.

The garbage-collection literature points to a structural fix: gate only on
transactions that could append events. That needs a way to tell appenders
apart from other sessions before they commit.

**Reporting.** Expose three states: caught up, behind and gated. When gated,
report:

- the number of withheld events (or a capped count);
- the stall duration;
- the transaction-ID age of the gate;
- the blocker's description.

Turn the health check degraded past the threshold, and name the blocker in
logs, because the fix almost always lies outside the consumer.

**Serialising appends removes the stall but caps throughput.** Serialised
appends are a single-server queue. The maximum commit rate is one transaction
per lock-hold time, however many appenders there are. The literature explains
why holding a lock until commit is costly:

- Graefe et al. observe that a transaction "may run in less time than it takes
  to log the transaction's commit record". Their controlled lock violation,
  like early lock release, "cuts overall lock duration to a fraction".
- Johnson et al.'s Aether identifies holding locks through the log flush as
  one of four logging barriers to scalability, and a follow-up journal paper
  extends this to multicore and multisocket hardware.
- Athanassoulis et al. evaluate early lock release directly.

In PostgreSQL (from documentation, to be checked per version), a
transaction-scoped lock is released only after the commit record is flushed,
and after a synchronous standby acknowledges it if one is configured.
Serialised appenders also never share a group-commit flush. As an
illustration, a 1 ms hold caps appends at about 1,000 transactions per
second, and a 5 ms hold at about 200. Putting several events in one
transaction raises events per second, but not transactions per second.

Xmin gating keeps concurrency. Its shared critical sections are short
(sequence draw, the rightmost index leaf, WAL insertion), and commits share
flushes. The multicore database literature favours this direction:

- Silo "avoids all centralized contention points, including that of
  centralized transaction ID assignment", and recovers a durable order
  afterwards.
- Eleda shows that the central log buffer is already a multicore bottleneck.
  Serialising appends adds a second, coarser one.

Gating's costs fall elsewhere: delivery waits on the oldest in-flight
transaction anywhere in the cluster, and the consumer becomes more complex.
Ways to shrink the serialised design's cost:

- take the lock late and send the lock, insert and commit in one round trip;
- batch events under one lock;
- use asynchronous commit, which releases locks before the flush but can lose
  acknowledged appends that consumers have already read on a crash.

Serialising per stream does not order the global log.

### Limits of the evidence

- **Nothing studies PostgreSQL LISTEN/NOTIFY directly.** No scholarly record
  examines LISTEN/NOTIFY semantics, psycopg 3 or libpq notification buffering,
  or measures PostgreSQL probe or append costs. Searches on these topics, on
  polling-cost scalability and on index-only-scan cost returned nothing
  relevant.
- **The two most load-bearing PostgreSQL claims are unverified.** These are
  the deliver-or-visible guarantee at LISTEN commit and the
  notification-queue-full failure mode. Both rest on implementation
  commentary and documentation, as do transaction-ID assignment, timeout
  defaults, lock release at commit and the planner's `max` rewrite. They
  should be confirmed against the manual and by tests on the pinned server and
  driver versions.
- **Many abstracts were truncated.** For several networking, hysteresis and
  interrupt-handling papers, specific parameter values could not be
  reproduced.
- **The formulas are illustrations, not results.** The cost crossovers, band
  widths, T_off expressions, safety-net bounds and isolated-loss staleness
  were all derived in this finding. They should be calibrated against measured
  per-query costs, notification delay and loss, and real arrival traces.
- **No standard constants exist.** The literature provides structures — a
  clamped adaptive interval, a hysteresis band, a heartbeat period and
  timeout, a jittered timer — not universal settings.

## Sources

- [Adaptive push-pull: disseminating dynamic Web data](https://doi.org/10.1109/tc.2002.1009150) — tier-1 — IEEE Transactions on Computers
- [An Adaptive Push/Pull Algorithm for AJAX Applications](https://openalex.org/W2106803624) — tier-2 — Research Repository (Delft University of Technology)
- [Leases: an efficient fault-tolerant mechanism for distributed file cache consistency](https://doi.org/10.1145/74850.74870) — tier-3 — doi.org (conference paper; venue not recorded)
- [Eliminating receive livelock in an interrupt-driven kernel](https://doi.org/10.1145/263326.263335) — tier-1 — ACM Transactions on Computer Systems
- [Experience with processes and monitors in Mesa](https://doi.org/10.1145/358818.358824) — tier-1 — Communications of the ACM
- [Research on Server Push Methods in Web Browser based Instant Messaging Applications](https://doi.org/10.4304/jsw.8.10.2644-2651) — tier-1 — Journal of Software
- [Integrating long polling with an MVC framework](https://openalex.org/W2513664727) — tier-3 — USENIX conference on Web application development
- [Implementation of SQL SERVER push service by using MSMQ](https://openalex.org/W2359017234) — tier-3 — Electronic Design Engineering
- [Apple's DNS Long-Lived Queries Protocol](https://doi.org/10.17487/rfc8764) — tier-3 — doi.org (report; venue not recorded)
- [Avoid Lost Wake-Up in Linux](https://openalex.org/W2978173257) — tier-3 — Computer Knowledge and Technology
- [Monitors](https://doi.org/10.1145/355620.361161) — tier-1 — Communications of the ACM
- [Monitor classification](https://doi.org/10.1145/214037.214100) — tier-1 — ACM Computing Surveys
- [The structure of the “THE”-multiprogramming system](https://doi.org/10.1145/363095.363143) — tier-1 — Communications of the ACM
- [Synchronization with eventcounts and sequencers](https://doi.org/10.1145/359060.359076) — tier-1 — Communications of the ACM
- [Distributed snapshots](https://doi.org/10.1145/214451.214456) — tier-1 — ACM Transactions on Computer Systems
- [A critique of ANSI SQL isolation levels](https://doi.org/10.1145/223784.223785) — tier-1 — ACM SIGMOD International Conference on Management of Data (SIGMOD)
- [Serializable snapshot isolation in PostgreSQL](https://doi.org/10.14778/2367502.2367523) — tier-1 — Proceedings of the VLDB Endowment
- [Concurrency Challenges in Database Systems: A Focus on Postgresql](https://doi.org/10.54047/bibted.1574178) — tier-1 — Bilgisayar Bilimleri ve Teknolojileri Dergisi
- [The design of POSTGRES](https://doi.org/10.1145/16894.16888) — tier-1 — ACM SIGMOD International Conference on Management of Data (SIGMOD)
- [Optimal probabilistic cache stampede prevention](https://doi.org/10.14778/2757807.2757813) — tier-1 — Proceedings of the VLDB Endowment
- [The DNS thundering herd problem](https://openalex.org/W3037836813) — tier-3 — openalex.org (venue not recorded in catalogue)
- [Design and evaluation of a wide-area event notification service](https://doi.org/10.1145/380749.380767) — tier-1 — ACM Transactions on Computer Systems
- [The many faces of publish/subscribe](https://doi.org/10.1145/857076.857078) — tier-1 — ACM Computing Surveys
- [Thialfi](https://doi.org/10.1145/2043556.2043570) — tier-3 — doi.org (venue not recorded in catalogue)
- [All aboard the Databus!](https://doi.org/10.1145/2391229.2391247) — tier-1 — ACM Symposium on Cloud Computing (SoCC)
- [Wormhole: reliable pub-sub to support geo-replicated internet services](https://openalex.org/W1472801964) — tier-3 — Networked Systems Design and Implementation
- [The IX Operating System](https://doi.org/10.1145/2997641) — tier-1 — ACM Transactions on Computer Systems
- [SEDA](https://doi.org/10.1145/502034.502057) — tier-3 — doi.org (conference paper; OpenAlex records no venue)
- [A New Self Adaptive Control Method for Network Protocol Processing](https://openalex.org/W2370499590) — tier-1 — Chinese Journal of Computers
- [Performance analysis and comparison of interrupt-handling schemes in gigabit networks](https://doi.org/10.1016/j.comcom.2007.06.013) — tier-1 — Computer Communications
- [A Queue-based Adaptive Polling Scheme to Improve System Performance in Gigabit Ethernet Networks](https://doi.org/10.1109/pccc.2007.358886) — tier-1 — IEEE International Performance, Computing, and Communications Conference
- [A Robust Device Hybrid Scheme to Improve System Performance in Gigabit Ethernet Networks](https://doi.org/10.1109/lcn.2007.20) — tier-3 — doi.org (venue not recorded in OpenAlex)
- [Optimal Operating Policies for M / G /1 Queuing Systems](https://doi.org/10.1287/opre.16.2.362) — tier-1 — Operations Research
- [M/M/1 Queueing Decision Processes with Monotone Hysteretic Optimal Policies](https://doi.org/10.1287/opre.32.5.1116) — tier-1 — Operations Research
- [Learning hysteretic routing policies for heterogeneous queues with switching costs](https://openalex.org/W7204015596) — tier-2 — University Library Linz repository (Johannes Kepler Universitat Linz)
- [Bilevel hysteretic service rate control for bulk arrival queue](https://doi.org/10.1016/s0360-8352%2899%2900071-6) — tier-1 — Computers & Industrial Engineering
- [AutoScale](https://doi.org/10.1145/2382553.2382556) — tier-1 — ACM Transactions on Computer Systems
- [On the quality of service of failure detectors](https://doi.org/10.1109/12.980014) — tier-1 — IEEE Transactions on Computers
- [Real-time status: How often should one update?](https://doi.org/10.1109/infcom.2012.6195689) — tier-3 — doi.org (no venue recorded)
- [The synchronization of periodic routing messages](https://doi.org/10.1109/90.298431) — tier-1 — IEEE/ACM Transactions on Networking
- [Effective page refresh policies for Web crawlers](https://doi.org/10.1145/958942.958945) — tier-1 — ACM Transactions on Database Systems
- [Adaptive pull-based policies for wide area data delivery](https://doi.org/10.1145/1138394.1138399) — tier-1 — ACM Transactions on Database Systems
- [Organization and maintenance of large ordered indexes](https://doi.org/10.1007/bf00288683) — tier-1 — Acta Informatica
- [Ubiquitous B-Tree](https://doi.org/10.1145/356770.356776) — tier-1 — ACM Computing Surveys
- [Modern B-Tree Techniques](https://doi.org/10.1561/1900000028) — tier-1 — Foundations and Trends in Databases
- [Efficient locking for concurrent operations on B-trees](https://doi.org/10.1145/319628.319663) — tier-1 — ACM Transactions on Database Systems
- [Building a replicated logging system with Apache Kafka](https://doi.org/10.14778/2824032.2824063) — tier-1 — Proceedings of the VLDB Endowment
- [A reliable multicast framework for light-weight sessions and application level framing](https://doi.org/10.1109/90.650139) — tier-1 — IEEE/ACM Transactions on Networking
- [Unreliable failure detectors for reliable distributed systems](https://doi.org/10.1145/226643.226647) — tier-1 — Journal of the ACM
- [Heartbeat: A timeout-free failure detector for quiescent reliable communication](https://doi.org/10.1007/bfb0030680) — tier-1 — Lecture Notes in Computer Science
- [Extended heartbeat failure detector](https://openalex.org/W2383517485) — tier-3 — Jisuanji gongcheng yu sheji
- [A Lazy Monitoring Approach for Heartbeat-Style Failure Detectors](https://doi.org/10.1109/ares.2008.51) — tier-1 — International Conference on Availability, Reliability and Security (ARES)
- [Implementation and performance evaluation of an adaptable failure detector](https://doi.org/10.1109/dsn.2002.1028920) — tier-1 — IEEE/IFIP International Conference on Dependable Systems and Networks (DSN)
- [Congestion avoidance and control](https://doi.org/10.1145/52325.52356) — tier-1 — ACM SIGCOMM Computer Communication Review
- [An Exponential Smoothing Adaptive Failure Detector in the Dual Model of Heartbeat and Interaction](https://doi.org/10.5626/jcse.2014.8.1.17) — tier-1 — Journal of Computing Science and Engineering
- [Forecasting heartbeat delay for failure detector over Internet using neural network](https://doi.org/10.1049/cp%3A20080778) — tier-1 — China-Ireland International Conference on Information and Communications Technologies (CIICT)
- [The φ accrual failure detector](https://doi.org/10.1109/reldis.2004.1353004) — tier-3 — venue not recorded (conference paper)
- [A New Real-Time Heartbeat Failure Detector](https://doi.org/10.1109/wicom.2008.1233) — tier-1 — International Conference on Wireless Communications, Networking and Mobile Computing
- [Communication strategies for heartbeat-style failure detectors in wireless ad hoc networks](https://doi.org/10.1109/dsn.2003.1209947) — tier-1 — IEEE/IFIP International Conference on Dependable Systems and Networks (DSN)
- [Spanner](https://doi.org/10.1145/2491245) — tier-1 — ACM Transactions on Computer Systems
- [Consistency and Completeness](https://doi.org/10.1145/3448016.3457556) — tier-3 — doi.org (conference paper; venue not recorded by OpenAlex)
- [CORFU](https://doi.org/10.1145/2535930) — tier-1 — ACM Transactions on Computer Systems
- [The dataflow model](https://doi.org/10.14778/2824032.2824076) — tier-1 — Proceedings of the VLDB Endowment
- [CockroachDB: The Resilient Geo-Distributed SQL Database](https://doi.org/10.1145/3318464.3386134) — tier-3 — doi.org (conference paper; venue not recorded by OpenAlex)
- [An empirical evaluation of in-memory multi-version concurrency control](https://doi.org/10.14778/3067421.3067427) — tier-1 — Proceedings of the VLDB Endowment
- [Hybrid Garbage Collection for Multi-Version Concurrency Control in SAP HANA](https://doi.org/10.1145/2882903.2903734) — tier-1 — ACM SIGMOD International Conference on Management of Data (SIGMOD)
- [Scalable garbage collection for in-memory MVCC systems](https://doi.org/10.14778/3364324.3364328) — tier-1 — Proceedings of the VLDB Endowment
- [Long-lived Transactions Made Less Harmful](https://doi.org/10.1145/3318464.3389714) — tier-3 — doi.org (venue not recorded; ACM conference paper)
- [MillWheel](https://doi.org/10.14778/2536222.2536229) — tier-1 — Proceedings of the VLDB Endowment
- [Watermarks in stream processing systems](https://doi.org/10.14778/3476311.3476389) — tier-1 — Proceedings of the VLDB Endowment
- [Controlled lock violation](https://doi.org/10.1145/2463676.2465325) — tier-3 — doi.org (no venue recorded)
- [Aether](https://doi.org/10.14778/1920841.1920928) — tier-1 — Proceedings of the VLDB Endowment
- [Scalability of write-ahead logging on multicore and multisocket hardware](https://doi.org/10.1007/s00778-011-0260-8) — tier-1 — The VLDB Journal
- [Improving OLTP Concurrency through Early Lock Release](https://openalex.org/W1893581217) — tier-3 — openalex.org (no venue recorded)
- [Speedy transactions in multicore in-memory databases](https://doi.org/10.1145/2517349.2522713) — tier-3 — doi.org (no venue recorded)
- [Scalable database logging for multicores](https://doi.org/10.14778/3149193.3149195) — tier-1 — Proceedings of the VLDB Endowment
