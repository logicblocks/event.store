---
type: "topic-research"
id: "08-how-do-notification-driven-and-polling-based-delivery-scale-openalex"
title: "How do notification-driven and polling-based delivery scale as listeners, channels, subscriptions and event throughput grow — measured notification latency and throughput, commit-time contention, thundering-herd wake-ups and fan-out cost — and what does published research on push versus pull delivery and adaptive polling say about the latency-versus-load trade-off?"
date: "2026-10-05T13:56:47+00:00"
author: "Toby Clemson"
producer: "research-topic"
status: "complete"
kind: "finding"
round: 1
depth: 3
question: "How do notification-driven and polling-based delivery scale as listeners, channels, subscriptions and event throughput grow — measured notification latency and throughput, commit-time contention, thundering-herd wake-ups and fan-out cost — and what does published research on push versus pull delivery and adaptive polling say about the latency-versus-load trade-off?"
source_profile: "openalex"
tags: ["research", "topic-research", "finding"]
last_updated: "2026-10-05T13:56:47+00:00"
last_updated_by: "Toby Clemson"
schema_version: 1
---

# How do notification-driven and polling-based delivery scale as listeners, channels, subscriptions and event throughput grow — measured notification latency and throughput, commit-time contention, thundering-herd wake-ups and fan-out cost — and what does published research on push versus pull delivery and adaptive polling say about the latency-versus-load trade-off?

## Question

How do notification-driven (push) and polling-based (pull) delivery of new
events behave as the number of listening sessions, notification channels,
subscriptions and the event rate grow? The question asks what has been
measured about four things:

- notification latency and throughput;
- contention at commit time;
- thundering-herd wake-ups;
- the cost of fanning notifications out.

It also asks what published research on push versus pull delivery, and on
adaptive polling, says about trading delivery latency against load. The
setting is a PostgreSQL-only event store accessed through psycopg 3. Its
consumers currently poll the event table on a fixed interval and could instead
be woken by `LISTEN`/`NOTIFY`.

## Findings

### The answer in brief

The peer-reviewed record indexed by OpenAlex contains no measurements of
PostgreSQL `LISTEN`/`NOTIFY`. No record reports notification latency,
notification throughput or commit-time contention as notifying writers,
listening sessions or channels are added. None compares notification-driven
with polling consumers on a PostgreSQL event table. The PostgreSQL-specific,
quantitative half of the question is therefore open. It has to be answered
from four places:

- the PostgreSQL project's documentation and source code;
- its mailing lists;
- practitioner benchmarks;
- benchmarks of the event store's own write and listen workload.

The general half of the question is well covered, and the literature agrees
with itself:

- **Push gives the lowest latency and costs nothing when idle, but its cost
  grows with event rate times the number of listeners woken.** Waking every
  waiter wastes work that grows linearly with the number of competing
  consumers. At high event rates, per-event wake-up overhead crowds out useful
  work.
- **Pull puts a bounded, predictable load on the server and batches naturally
  as the event rate rises, but its latency is tied to the poll interval.**
  Delay averages about half the interval and reaches the full interval at
  worst. Pull costs the most when nothing is happening.
- **Every source that proposes a remedy prefers an adaptive hybrid** to either
  pure form. Push serves as a cheap wake-up at low load, pull runs in batches at
  high load, and a pull safety net covers lost pushes.

Applied to PostgreSQL, the evidence supports four design choices:

- keep the position-tracked, batched read of the event table as the data path;
- use `NOTIFY` only as a coalesced wake-up signal (a doorbell);
- add a long fallback poll with fresh random jitter on every cycle;
- use a dispatch design that limits how many sessions each notification wakes.

Mapping the research onto PostgreSQL relies on the database's documented
behaviour, not on the cited records. Such statements are marked as inference
throughout.

### What the record does and does not contain

Catalogue searches aimed at `LISTEN`/`NOTIFY` performance return nothing. So do
searches aimed at PostgreSQL used as a message queue or broker with latency or
throughput measurements.

**The nearest PostgreSQL-specific work.** This is a Cambridge research
programme by Vargas, Bacon and Moody that joined PostgreSQL to the Hermes event
middleware. In "Integrating databases with publish/subscribe" (IEEE ICDCS
Workshops, 2005):

- each database manager defines and advertises change events;
- an event can span several physical relations and corresponds to a virtual
  relation of a security view;
- clients subscribe to those events and can narrow them, rather than
  registering continuous queries.

Change detection runs inside the database through its active trigger and rule
machinery, so the cost lands on the writing transaction. Events are then routed
by Hermes, a type- and attribute-based publish/subscribe middleware running
over a scalable overlay network (Pietzuch and Bacon). The follow-on paper,
"Event-Driven Database Information Sharing" (Lecture Notes in Computer Science,
BNCOD 2008), and Vargas's dissertation have no abstracts in the catalogue. None
of these records gives a figure for any of the following:

- trigger overhead on writes;
- publication throughput;
- delay from commit to subscriber.

**Neighbouring designs.** Two other systems show that tying event publication
to database transactions is established practice. Their records carry no cost
data either.

- NonStop SQL/MX built transactional queues into relational tables (Hanlon et
  al., ICDE 2004).
- TIBCO offered transactional publish/subscribe (Chan, ACM SIGMOD Record,
  1998).

**Other gaps.** No record measures any of the following:

- **Commit throughput on a shared resource.** No study measures it in
  PostgreSQL when every transaction must update one shared queue, counter or
  sequence. The nearest PostgreSQL-specific records are a reproducibility
  harness for a study of isolation anomalies and Knoche's method for
  attributing database lock contention to application code paths. The harness
  covers PostgreSQL, CockroachDB, MongoDB and a two-phase-locking middleware
  under contended workloads, and measures the tail-latency cost of removing
  anomalies. Neither record quantifies a single global serialisation point.
- **Jitter.** No study compares randomised jitter with lock-step polling of a
  shared database.
- **Gap avoidance.** No study measures the write-throughput or read-latency
  cost of techniques that stop position-based readers from skipping rows.
- **Checkpoint cost.** No study measures the cost of committing a consumer's
  position in the same transaction as its effects as batch size varies.

**What the literature does provide** is theory and adjacent measurements on:

- push versus pull dissemination;
- interrupts versus polling;
- waking one waiter versus waking all;
- load balancing and dispatch;
- polling on a budget;
- synchronisation of periodic actors;
- commit-path contention in research database engines.

To map that work onto PostgreSQL, this finding relies on the database's
documented semantics:

- A notification is delivered at commit to every session listening on its
  channel.
- Notifications are not durable. They reach only the sessions that are
  listening at that moment.
- `LISTEN` registrations and session-level advisory locks belong to a backend
  session.
- Undelivered notifications are held in a single server-wide queue.

### Push and pull have complementary costs

Bhide, Deolasee, Katkar, Panchbudhe, Ramamritham and Shenoy (IEEE Transactions
on Computers, 2002) set out the two basic ways of keeping clients current with
changing data. An earlier version appeared as Deolasee et al. at WWW.

- **Pull.** Clients "frequently pull the data based on the dynamics of the data
  and a user's coherency requirements".
- **Push.** Servers "maintain state information pertaining to clients and push
  only those changes that are of interest to a user".

The two have complementary properties in four respects: temporal coherency,
communication overhead, server-side state, and loss of freshness when the
server fails. The authors combine them in two adaptive schemes:

- **Push-and-Pull.** The client pulls on an adaptive schedule, and the server
  pushes the changes the client would otherwise miss.
- **Push-or-Pull.** The server picks a mode for each client according to its
  own load and the client's needs.

Their evaluation concludes that adaptive dissemination is needed to meet
varied freshness requirements, stay resilient to failures and use server and
network resources efficiently.

Broadcast research reaches the same conclusion:

- Acharya, Franklin and Zdonik (SIGMOD 1997) start from push-only broadcast for
  "very large client populations" and examine how to balance it with
  client-initiated pull.
- Hu and Chen (IEEE Transactions on Vehicular Technology) note that traffic
  "changes dynamically" and adjust the push–pull time balance as load changes.

No fixed mode is best at every load. The best balance moves with client
population and update rate.

Mapped onto PostgreSQL (inference), `NOTIFY` is the push side and the polling
consumer is the pull side:

| | Notification-driven | Fixed-interval polling (N consumers, interval T) |
|---|---|---|
| Cost when idle | Nothing beyond holding a session open | N ÷ T queries per second, all empty |
| Cost as event rate rises | Grows with events × listeners woken | Flat; each poll returns a larger batch |
| Delivery delay | Commit-to-wake path, independent of any interval | About T/2 on average, about T at worst |
| Server-side state | Per-session channel registrations and a shared notification queue | None beyond each consumer's stored position |
| Behaviour on failure | Notifications sent while a listener is disconnected are lost | Resumes from the stored position |

### Per-event wake-ups win at low load and degrade at high load

Mogul and Ramakrishnan (ACM Transactions on Computer Systems, 1997) give the
canonical account of this trade-off.

- **The problem.** Interrupt-driven systems "can provide low overhead and good
  latency at low offered load, but degrade significantly at higher arrival
  rates". The extreme is receive livelock: the system "spends all of its time
  processing interrupts" and "no packets are delivered to the user
  application".
- **The remedy.** An interrupt is used only as a wake-up. The handler turns
  off further interrupts, polls the queue in bounded quotas until it is empty,
  then turns interrupts back on.
- **The evaluation.** Kernel measurements showed this removes livelock without
  degrading other aspects of performance.

Later networking work refines when to switch between interrupts and polling:

- **Dovrolis, Thayer and Ramanathan** (HIP, ACM SIGOPS Operating Systems
  Review) note that interrupts beat polling on per-packet latency but carry
  high overhead on modern processors. They reduce that overhead with hybrid
  interrupt-polling.
- **Chang, Muppala, Zou and Li** (IEEE LCN) identify hybrid interrupt-polling
  and interrupt coalescing as the two common defences against livelock. They
  observe that the adaptive schemes proposed so far rely on local knowledge,
  are heuristic, and had not been systematically compared.
- **AlQahtani** (ISCIT) proposes a hybrid that switches between enabling
  interrupts, disabling them, and polling.
- **Salah, Elbadawi and Haidari, and Salah and Qahtan** (Computer
  Communications) compare interrupt-handling schemes and evaluate a hybrid
  experimentally. Their records carry no abstracts, so no results can be
  reported from them.

The usual evaluation method in this line of work combines analytical or
queueing models with measurements of throughput, latency and remaining CPU
capacity as offered load rises. It transfers directly to measuring consumer
latency and database load as the event rate rises.

Storage research adds a caution at microsecond scale. Three studies compare
classic polling, hybrid polling (sleep for part of the expected wait, then
poll) and interrupts for ultra-low-latency SSDs:

- Koh et al. (IEEE IISWC);
- Lee, Shin and Jeong (Journal of Systems Architecture);
- Harris and Altiparmak (HotStorage).

Harris and Altiparmak measured all three with a power meter. Contrary to common
belief, they found polling can be *more* energy-efficient than interrupts.
Whether waking on a signal or polling is cheaper depends on the workload and
has to be measured.

For a database consumer, the same remedy becomes a drain loop (inference):

1. Treat a notification as a doorbell, not as data.
2. On waking, query in bounded batches until a batch comes back shorter than
   the limit. Ignore notifications that arrive meanwhile, so a burst collapses
   into one wake-up.
3. Return to waiting, with a timeout equal to the fallback poll interval.

The number of queries then grows with the number of batches, not the number of
events. At low load each event costs about one wake-up and one query. At high
load the consumer is effectively polling continuously, and each query returns a
full batch. A short batch already proves the backlog is drained, so no
confirming empty query is needed. The bounded batch plays the role of Mogul and
Ramakrishnan's quota: it stops one busy stream from starving other work.

### How push cost grows with listeners: wake-all and thundering herds

PostgreSQL notifications wake everyone by design (documented behaviour). Every
session listening on a channel receives every notification sent to it, and
there is no option to notify just one listener. The systems literature shows
what waking every waiter costs:

- **The Linux Scalability Project** (Honeyman et al.) found a thundering herd
  in `accept`: every waiting thread was woken for each new connection. Changing
  it to "awaken only one, instead of all, waiting threads" improved
  macro-benchmark performance "by over 50% on high-performance hardware".
- **Pan et al.** (Hermes, 2025, a layer-7 load balancer unrelated to the
  Hermes middleware above) show that waking only one waiter is not a complete
  fix:
  - early `epoll` "suffered from the thundering herd problem";
  - exclusive wake-ups (Linux 4.5) avoid the herd but wake waiters in LIFO
    order, concentrating work "on a few workers";
  - port-reuse hashing spreads work but "suffers from hash collisions and lacks
    awareness of worker load".

  Removing wasted wake-ups therefore needs load-aware dispatch, not just a
  single wake-up.
- **Mergendahl et al.** (2022) show that thundering-herd wake-ups in microkernel
  inter-process communication can be exploited to amplify kernel interference
  and degrade response times in real-time systems. The herd amplifies
  interference as well as wasting work.
- **Sivaraman and Liu** report "a regular pattern of spikes in queries" at DNS
  caching resolvers. They recommend software mitigations that break the
  synchronisation.

Two PostgreSQL cases differ (inference):

- **Competing consumers on one channel.** Suppose N consumers share one work
  stream. Each event wakes all N, and they race to claim the work, for example
  with `SELECT … FOR UPDATE SKIP LOCKED`. N−1 of them run a query and find
  nothing. Wasted work per event grows linearly with N, and the empty claim
  queries hit the server as a correlated burst.
- **Independent subscribers.** When every subscriber genuinely needs every
  event, the fan-out is real work rather than waste. Its cost is still events ×
  listeners, plus one read per woken subscriber unless the reads are shared.

### How cost grows with channels and subscriptions

Carzaniga, Rosenblum and Wolf (ACM Transactions on Computer Systems, 2001)
identify two core services of an event-notification infrastructure. One is
"notification selection", meaning "determining which notifications match
which" subscriptions. The other is delivery. Eugster et al. (ACM Computing
Surveys, 2003) survey systems in which subscribers "register their interest in
an event, or a pattern of events" and are "asynchronously notified". The
variants differ in how expressive that interest can be.

PostgreSQL offers only selection by exact channel name, the simplest
topic-based form (inference). Any finer filtering has to happen in one of two
places:

- **More channels.** This means more registrations per session and more
  matching state on the server.
- **The consumer.** Consumers receive notifications and then discard them,
  which are wasted wake-ups.

Channel granularity is therefore the main control over fan-out cost:

- **One global channel** minimises server state but maximises wasted wake-ups.
- **One channel per stream, category or partition** minimises wasted wake-ups
  but needs more registrations.

Polling scales with subscriptions differently (inference):

- **Separate queries per subscription.** The poll budget must be divided among
  them, as discussed under "Polling on a fixed query budget" below.
- **A global, increasing position.** If the event store has a global,
  monotonically increasing position, one shared query ("rows after global
  position p") can serve many streams or categories. Adding subscriptions then
  adds no queries.

### Commit-time contention

No catalogued study measures PostgreSQL's own commit path for notifying
transactions, sequences or hot rows. A group of multicore concurrency-control
studies on research engines (DBx1000, Peloton, Shore-MT and Silo) does measure
the three ways a shared global resource serialises commits.

**Global counters**

- **Yu, Bezerra, Pavlo, Devadas and Stonebraker** ("Staring into the Abyss",
  PVLDB 2014) evaluated seven concurrency-control schemes, MVCC included, on a
  simulated machine scaling towards 1,000 cores. None scaled all the way; each
  failed on a different bottleneck, one of which was timestamp allocation from
  a shared counter:
  - a mutex-protected counter performed worst;
  - an atomic fetch-and-add counter scaled further, but flattened as all cores
    contended on one cache line;
  - batched allocation and hardware-clock allocation raised the ceiling.

  The study is also the standard reference for throughput collapse when a few
  hot tuples serialise every transaction, such as TPC-C's per-district
  next-order-ID counter.
- **Silo** (Tu, Zheng, Kohler, Liskov and Madden, SOSP 2013) "avoids all
  centralized contention points, including that of centralized transaction ID
  assignment". It derives serial order from per-record IDs plus coarse epochs,
  and the epochs also drive group commit.
- **Wu, Arulraj, Lin, Xian and Pavlo** (PVLDB 2017) find that, with many
  threads, MVCC's synchronisation overhead can outweigh the benefit of keeping
  multiple versions. That overhead includes shared timestamp and version-chain
  maintenance.
- **Larson et al.** (PVLDB 2011) report that standard concurrency control does
  not scale to the transaction rates main-memory engines can reach. They
  present multiversion schemes in which transactions never block during normal
  processing.

**Hot tuples held until commit.** Guo, Wu, Yan and Yu (Bamboo, SIGMOD 2021)
study the "every transaction writes one hot row" pattern. Under strict
two-phase locking, a transaction holds its hotspot lock until it completes.
Throughput through the hotspot is therefore capped by the duration of the whole
transaction, not just the hotspot access. Their prototype releases hotspot
locks early and tracks dependencies so it can cascade aborts. On synthetic
hotspot workloads and TPC-C, this recovers much of the lost parallelism.

**Commit-time flushes.** Johnson, Pandis, Stoica, Athanassoulis and Ailamaki
(Aether, PVLDB 2010) identify four sources of serialisation in write-ahead
logging:

- I/O delays;
- log-induced lock contention, where locks stay held while a transaction waits
  for its commit record to reach stable storage;
- excessive context switching;
- contention inside the log buffer.

Their remedies, implemented in Shore-MT, are:

- early lock release once the commit record is in the log buffer;
- flush pipelining, a form of group commit;
- a consolidated path for inserting into the log buffer.

This is the study most directly about commit-time contention. A hot lock held
across the commit flush makes the flush part of the serial section.

**The transferable model** (inference): throughput through a single global
resource is bounded by one over the time each transaction holds it.

- **Non-transactional atomic allocation** holds the resource only for the
  increment. That gives a real but high ceiling. PostgreSQL sequences work this
  way: `nextval` is not rolled back and can be cached per session.
- **A transactional update of one counter or queue row** holds the row lock
  until the transaction ends. With synchronous commit, that row's throughput
  approaches the rate at which commit records can be flushed, unless group
  commit spreads the cost of each flush across several transactions.
- **A global lock held through commit** has the shape Aether describes. The
  question assumes PostgreSQL serialises the commits of transactions that issue
  `NOTIFY`. That is a property of the server's implementation, which no
  catalogued record describes or measures. Taken as given, notifying writers
  serialise on a section that includes the commit flush. The absolute ceiling,
  and how it moves with writers, listeners and channels, must come from
  engineering benchmarks.

Absolute numbers from in-memory prototypes, some run on simulated hardware,
should not be read as PostgreSQL figures. Their ranking of mechanisms is the
part that transfers.

**A second commit-path risk** grows with the number and health of listeners
rather than writers (inference from documented behaviour). Undelivered
notifications sit in one server-wide queue. A listener that falls behind, or
sits inside a long transaction, stops that queue from being cleaned up. Once
the queue is full, every transaction that issues `NOTIFY` fails at commit. If
notification payloads carry the data, a slow consumer therefore pushes failure
onto the writers. Pull-based log brokers were designed to avoid exactly this
push-broker failure.

**The same arithmetic decides one of the gap-avoidance techniques below.**
Position order can be made to match commit order by holding one exclusive lock
from position allocation through commit. That caps appends at roughly one
transaction per critical-section time, however many connections exist. As
illustrative arithmetic, not a measurement, a 1 ms commit flush caps appends at
about 1,000 transactions per second. CORFU shows the opposite design. Its
shared log reaches about one million 4 KB operations per second on 16 to 32
machines, because the component handing out positions does no I/O, so "there
is no single I/O bottleneck".

### Notifications do not make position-based reads safe

A sequence hands out a position at insert, but the row becomes visible only at
commit. Suppose transaction T1 takes position 10, T2 takes 11, and T2 commits
first. A consumer can read row 11, advance past it, and never come back for row
10 once T1 commits. A notification fires at commit, so a wake-up caused by T2
arrives just when lower-numbered concurrent writes are most likely still in
flight. Push delivery makes this window more likely to be hit, not less
(inference).

**Stronger isolation does not fix this.**

- **The anomaly.** Fekete, O'Neil and O'Neil (ACM SIGMOD Record) show that,
  under snapshot isolation, a read-only transaction can see a state that no
  serial execution could produce. Their example is a batch-processing report.
- **PostgreSQL's SSI.** Ports and Grittner's account of PostgreSQL's
  Serializable Snapshot Isolation (PVLDB) uses that example as motivation.
  SSI detects the dangerous pattern and aborts one transaction. It also adds
  `READ ONLY DEFERRABLE` transactions, which wait for a safe snapshot and then
  run without tracking. The authors report SSI throughput close to plain
  snapshot isolation, and well above lock-based serialisability, on read-heavy
  workloads.
- **Why SSI does not help here** (inference). Sequences are deliberately
  non-transactional. The serial order "T2, consumer, T1" fully explains what
  the consumer saw, so there is no dependency cycle for SSI to detect.

The fix has to come from how positions are allocated, or from how the reader
decides which positions are safe to pass:

| Technique | Write-throughput cost | Read-latency cost |
|---|---|---|
| Commit-serialising lock around allocation and commit | High: appends serialise through the commit flush | Minimal |
| Transaction-ID watermark: an `xid8` column, read up to `pg_snapshot_xmin(pg_current_snapshot())` | Negligible: one indexed 8-byte column, and writers never wait on each other | Delivery stalls behind the oldest running transaction that holds an ID, anywhere in the database |
| Gap detection with a timeout | None | One full timeout per permanent gap (rollbacks, sequence values lost from a cache); rows are skipped if the timeout is shorter than the slowest append |
| Hole filling, in CORFU's style | None, unless a writer hits a filled hole and retries | The reader waits on an in-flight holder; not evaluated for PostgreSQL |
| Logical decoding of the write-ahead log (change data capture, CDC) | Log retention for the replication slot, not lock contention | Delay from commit to decode; one stream per slot |
| SSI with a transactional epoch counter | Tracking overhead close to snapshot isolation, plus aborts and retries | At least one epoch, plus any wait for a safe snapshot |

Notes on these techniques:

- **The watermark's theory.** Elnikety, Zwaenepoel and Pedone's generalised
  snapshot isolation shows that reading an older snapshot keeps snapshot
  isolation's key properties. Read-only transactions never block or abort, and
  never cause updates to block or abort. The watermark consumer gives up some
  freshness and imposes no writer contention.
- **The watermark's ordering caveat.** Transaction IDs are assigned at a
  transaction's first write, not at commit. Per-stream order across batches is
  only exact when the event insert is that first write, or when ordering is
  otherwise constrained.
- **Hole filling.** CORFU lets each position be written only once and lets a
  reader fill a hole with a junk entry. A stuck position then becomes a delay
  rather than a lost row. A PostgreSQL analogue would have the reader insert a
  placeholder row at the gap's primary key, blocking on any uncommitted holder
  through the unique index. That analogue is inference and unevaluated.
- **Log decoding.** DBLog, Netflix's CDC framework for MySQL and PostgreSQL,
  captures committed changes from the transaction log. It writes marker rows
  into a watermark table so it can merge table reads into the change stream
  without locks. Its record reports no throughput or latency figures.

None of these costs has been measured on a PostgreSQL event table; the
directions follow from the mechanisms. For a table reader woken by
notifications, the transaction-ID watermark is the only technique listed that
adds no writer contention while keeping table-based reads. Its main risk is
delivery stalls behind long-running or idle-in-transaction sessions.

### Polling on a fixed query budget

Pure-pull research frames polling as spending a limited budget.

- **Cho and García-Molina** (ACM Transactions on Database Systems, 2003) keep
  local copies of sources that "do not notify the copies … of new changes, so
  we need to periodically poll the sources", and "polling the sources takes
  significant time and resources". They model changes as a Poisson process and
  measure freshness and age, analytically and on real web change data. They
  report large gains over existing policies. The counter-intuitive result is
  that, for a fixed number of polls, polling each source in proportion to its
  change rate gives *worse* freshness than polling all sources uniformly.
- **Their companion paper** (ACM Transactions on Internet Technology) builds
  accurate change-rate estimators from poll histories that reveal only
  *whether* something changed since the last visit.
- **Sia, Cho and Cho** (IEEE TKDE) is the closest analogue to an event
  consumer. An RSS aggregator must retrieve new postings quickly using minimal
  resources. The metric is *delay* from posting to retrieval, not staleness.
  On real feeds, a policy built on learned, time-varying posting patterns
  delivered alerts faster than the best prior approach for the same resources.
- **Srinivasan, Liang and Ramamritham** (IEEE RTSS, 1998) choose each next
  poll from the observed dynamics of the data and the user's tolerance for
  staleness. The interval lengthens while nothing changes and shortens when
  changes appear, within bounds. This is the scholarly ancestor of empty-poll
  backoff.
- **Boxma, Levy and Weststrate** (Queueing Systems) study a single server
  dividing its attention among N stations, where each visit costs a switchover
  overhead. Efficient visit frequencies follow a square-root rule.
- **Azar et al.** (PNAS) weight pages by popularity as well as change rate.
  They show that near-optimal randomised refresh policies can be computed
  efficiently.

A delay model drawn from this work (reasoning, not a quoted result) shows when
adaptive polling helps:

- **One steady stream.** An event arriving during an interval of length T
  waits T/2 on average. Delay is convex in interval length, so for a fixed
  number of polls, equal intervals are optimal. A fixed-interval poller is
  already optimal for a steady event rate. Adaptive policies can only win by
  moving queries from quiet periods to busy ones.
- **Time-varying rate.** Minimising ∫ λ(t)/(2r(t)) dt subject to ∫ r(t) dt = N
  gives a polling rate r(t) ∝ √λ(t). The polling rate should follow the
  *square root* of the event rate, not the rate itself. This matches Cho and
  García-Molina's finding that proportional allocation over-spends on busy
  sources.
- **Many separately queried streams.** Minimising Σ w_i·λ_i/(2f_i) subject to
  Σ c_i·f_i = B gives f_i ∝ √(w_i·λ_i/c_i). Here c_i is the cost of one query
  and w_i an importance weight. This is the feed-monitoring rule generalised,
  and it matches the polling-table form.
- **How much it gains.** With N equal-cost streams, total rate Λ and B polls
  per second:
  - uniform allocation gives mean delay N/(2B);
  - rate-proportional allocation also gives N/(2B);
  - square-root allocation gives (Σ√λ_i)²/(2BΛ), which by Cauchy–Schwarz is
    never worse, and equal only when all rates are equal.

  Example: two streams at 99 and 1 events per second, with 10 polls per second.
  Uniform or proportional allocation gives a mean delay of 100 ms. Square-root
  allocation polls about 9.1 and 0.9 times per second and gives about 60 ms.
  The more skewed the rates, the bigger the gain.

**Why the model fits event-table consumers** (inference):

- **The metric.** An event consumer must deliver every event, so delay, not
  freshness, is the right metric. The freshness optimum starves fast-changing
  sources, so it would let the busiest streams fall furthest behind.
- **The cost structure.** A positional poll has a fixed cost per query: a
  round trip, planning and an index descent. Its per-row cost is paid once per
  event, however polls are scheduled. Only the fixed part is what the budget
  divides, the role that switchover time plays in polling tables.
- **Rate estimation.** An event-table poll returns every new row, so an
  exponentially weighted count of rows per second gives the arrival rate
  directly. That is easier than a crawler's task of inferring change rates
  from whether something changed.

**Conditions that change the rule** (inference):

- **Per-subscription latency targets or worst-case bounds** favour uniform
  polling or a cap on the longest interval. Under the square-root rule, a
  stream with 1/100 of another's rate waits 10 times longer. A compromise is
  to give every stream at least 1/T_max, where T_max is its longest acceptable
  interval, and split the remainder by the square-root rule.
- **Batch limits** impose a floor of f_i ≥ λ_i/L for a batch limit of L rows. A
  stream at that floor is limited by throughput, not latency, and should drain
  in a loop.
- **Heavy per-event processing** means pure square-root allocation slightly
  over-polls the busiest streams. This is the load correction in the
  polling-table model.
- **A shared global-position query** removes per-stream allocation entirely.
  Per-stream polls only pay off for subscriptions that truly need separate
  queries. A hybrid often works best: one shared scan for the quiet long tail,
  and dedicated polls for the few hot streams.
- **With notifications,** active streams trigger their own reads. The
  remaining poll budget is a safety net whose goal is a bounded worst-case
  delay. That calls for uniform, long fallback intervals, not a split based on
  rates.

**Empty-poll backoff in this light.** It is a crude, reactive estimator of the
arrival rate. Its cap is the worst-case delay for the first event after a quiet
spell, so the cap should be set from the latency requirement, not from cost
alone. The interval should drop back to its minimum as soon as a poll returns
rows.

### Synchronisation and jitter among many pollers

Research on periodic actors shows that synchronisation is easy to fall into and
that randomisation is the established remedy:

- **Floyd and Jacobson** (ACM SIGCOMM Computer Communication Review; extended
  in IEEE/ACM Transactions on Networking) show that "many
  apparently-independent periodic processes" can "inadvertently become
  synchronized". The shift to synchronised traffic is "not one of gradual
  degradation" but "a very abrupt 'phase transition'", and adding one router
  can tip the system. They recommend randomising the sources. The catalogue
  abstract is cut off before saying how much randomisation was needed.
- **Vattani, Chierichetti and Lowenstein** (PVLDB) treat the closest database
  case, the cache stampede, which "severely limits the performance of databases
  and web servers". Their remedy, probabilistic early expiration, is provably
  optimal. It shows that the size and placement of the random component can be
  optimised rather than guessed.
- **Kaburaki et al.** (IEEE Transactions on Wireless Communications) find that
  periodic sources on a random-access channel suffer "continuous packet
  collisions". Sources that collide once keep colliding every period, while
  clock drift slowly shifts the alignment.
- **Laya, Alonso and Alonso-Zárate** (IEEE Communications Surveys & Tutorials)
  document that LTE random access suffers "congestion and overloading in the
  presence of a huge number of devices".

No record measures jitter on database polling. Standard renewal-process
reasoning (not a measurement) gives the following:

- **Load peaks.** N consumers polling in lock-step every T seconds put N
  queries on the server at once, every period, and contention makes each one
  run longer. With independent random phases, the average load stays at N/T
  queries per second. Expected concurrency, though, falls to about N·d/T for a
  query duration d. Jitter barely changes the budget but greatly changes how it
  is spread over time.
- **Latency for a consumer reading its own stream.** Zero-mean jitter with
  variance σ² raises the mean wait from T/2 to T/2 + σ²/(2T); this is the
  inspection paradox.
  - For uniform jitter of ±50% of T, that is about 8% more mean latency, and
    the worst case grows to 1.5T.
  - Fully random (Poisson) poll times double the mean wait to the full
    interval.
  - Jitter that only ever lengthens the sleep cuts load but adds latency.
- **Latency for competing consumers sharing one stream.** With N consumers at
  independent random phases, the mean wait until any of them polls is about
  T/(N+1). In lock-step it stays at T/2, and N−1 of the simultaneous queries
  are redundant. Desynchronising therefore flattens load and also cuts latency
  for the same budget.

Practical consequences (inference):

- **Draw fresh jitter on every cycle.** A single random offset at start-up is
  weaker, because a fixed period preserves any alignment once it forms.
- **Expect coupling through shared database response times.** A poll loop
  usually sleeps for a fixed interval after its query completes. Consumers
  slowed down together finish together, and so start their next intervals
  together.
- **Test across a range of consumer counts.** Because the transition is
  abrupt, a small test can show smooth load while one more consumer produces
  synchronised peaks.

### What pull-based log brokers teach

The indexed records describe Kafka and its alternatives:

- Kafka is a publish/subscribe system "with its core architecture as a
  distributed commit log", built at LinkedIn (Wang et al., PVLDB).
- It "acts as a kind of write-ahead log that records messages to a persistent
  store and allows subscribers to read and apply these changes", and serves
  both offline and online consumers (Thein and Nyunt).
- Queue brokers built on "centralized brokers, complex routing logic, and
  tightly coupled acknowledgment semantics" can become bottlenecks (Ghanta).
- Dobbelaere and Esmaili compare Kafka with RabbitMQ, the reference push
  broker, both qualitatively and empirically.
- Outside messaging, gossip research reports pull's basic property: messages
  move only "when recipients request" them, which "enormously" reduces
  redundant traffic compared with push (Ahn and Kim).

The original Kafka design paper, which states the pull rationale, is not among
the indexed records. The records corroborate the commonly stated arguments only
in outline:

1. **The consumer sets the rate, so backpressure is implicit.** A slow consumer
   slows only itself. A push broker must estimate each consumer's capacity and
   run explicit flow control.
2. **Batching adapts to lag.** A consumer that falls behind receives larger
   contiguous batches. That spreads per-request overhead over more messages
   exactly when it matters.
3. **The broker keeps almost no per-consumer state.** Each consumer's progress
   is one offset per partition, with no per-message acknowledgement.
4. **Replay and mixed-speed consumers come for free.**
5. **Pull's weakness, idle spinning, is solved by long-polling, not by push.**
   The broker holds a fetch request open until data arrives or a timeout
   expires.

For a PostgreSQL event table, these arguments carry over to different degrees
(inference):

- **Carry over fully:** consumer-controlled rate and backpressure,
  consumer-owned positions, and replay. These should be kept. PostgreSQL can
  even do better than Kafka here, because a consumer can commit its position
  in the same transaction as its in-database side effects.
- **Carry over in form but not mechanism:** batching. Each PostgreSQL batch is
  an index range scan with MVCC visibility checks and row decoding, not a
  sequential segment read with zero-copy transfer. Batching should help a great
  deal, but will not reach Kafka's absolute rates.
- **Do not carry over:**
  - **Offsets that become visible in order.** Sequence values commit out of
    order, as described above.
  - **A cheap long-poll.** No PostgreSQL query blocks until rows exist, and an
    empty poll costs a full round trip on a dedicated backend process.
  - **Partition-based parallelism.** Kafka assigns whole partitions to members
    of a consumer group, and ordering holds only within a partition, the
    limitation the Kumar et al. global-ordering work targets. A single event
    table has a global order but no built-in partition ownership or
    rebalancing.

Push in PostgreSQL is therefore best used to rebuild Kafka's long-poll, not to
replace pull. `LISTEN` provides the wake-up hint and a timeout provides the
fallback. In psycopg 3, that means waiting on the connection's notification
generator with a timeout, then running the same range query. Hybrid push-pull
publish/subscribe designs also appear in IoT frameworks (Lohitha and
Pounambal). The case for them here rests on engineering reasoning rather than
comparative studies.

### Dispatch designs that bound wake-ups and keep load balanced

Queueing and scheduling research offers several designs for competing
consumers. Their application to PostgreSQL is inference.

**One listener and a central queue.** One session listens and hands work to a
pool. This turns N separate waiters into one shared queue served by k workers.

- ZygOS (Prekas, Kogias and Bugnion) builds on the result that one shared queue
  gives much lower tail latency than k per-worker queues. Separate queues let
  work pile up behind a busy worker while others sit idle.
- Sparrow (Ousterhout, Wendell, Zaharia and Stoica) cites the throughput and
  availability limits of central schedulers, and shows that decentralised,
  randomised sampling gets close to optimal.
- For an event-store consumer, the dispatcher only relays wake-ups, so its
  throughput ceiling is high and availability is the main risk.
- A natural layout is one listening session per process, which limits any herd
  to the number of processes, with in-process fan-out to workers through a
  local queue.

**Late binding and pull-based hand-off.**

- Join-Idle-Queue (Lu et al., Performance Evaluation, 2011) has idle servers
  register in an idle queue, so new jobs go to a known-idle server without load
  probes.
- Sparrow commits a worker to a task only when the worker is ready to run it.
- Zhou, Tan and Shroff (Proceedings of the ACM on Measurement and Analysis of
  Computing Systems) show that asking for work only when completely idle is
  *not* delay-optimal under heavy traffic. Sending work to any server whose
  queue is below a threshold *is* optimal, provided the threshold grows as load
  approaches capacity.
- In PostgreSQL terms, hand the notification hint to an idle worker, which then
  claims a specific row with `SKIP LOCKED` only when it is free. At high event
  rates, workers should advertise capacity for more than one item, through
  prefetch credits or batch claims.

**Randomised targeting.**

- Mitzenmacher's supermarket model (IEEE Transactions on Parallel and
  Distributed Systems) shows that sampling two servers and joining the shorter
  queue improves expected time in the system exponentially over picking one at
  random. Choosing among more than two adds only a constant-factor gain.
- Zhou et al. include this power-of-d approach among policies that are
  delay-optimal under heavy traffic.
- Writers rarely know consumer load, so this fits a dispatcher choosing among
  workers inside a process better than it fits the `NOTIFY` call site.

**A channel per consumer or partition.**

- Hashing streams to channels, each owned by one consumer, means each `NOTIFY`
  wakes exactly one consumer and keeps related work together. It is the
  database version of per-core accept queues.
- Affinity-Accept (Pesterev et al.) shows that partitioning improves locality
  but needs balancing: idle cores steal work from busy ones.
- ZygOS likewise uses work stealing to recover most of a shared queue's
  balance.

**Stealing collides with event ordering.** The work-stealing and
stream-processing literature resolves this the same way every time: the unit
stolen or rebalanced is the ordering domain itself, meaning a whole stream, key
or partition, never an individual event.

- **Why.** Blumofe and Leiserson's work-stealing guarantees (Journal of the
  ACM) hold for computations whose dependencies allow a stolen thread to run.
  Event n+1 of a stream depends on event n.
- **Key grouping** sends every tuple of a key to one instance. That preserves
  per-key order but suffers imbalance when keys are skewed.
  - Rivetti et al. (DEBS) attack the imbalance with better key-to-instance
    mappings.
  - Gedik et al. (IEEE Transactions on Parallel and Distributed Systems) change
    the number of parallel channels at run time by moving key ranges and their
    state.
  - Schneider et al. (PACT) show that automatic parallelisation can preserve
    first-in-first-out semantics even for stateful operators.
- **Key splitting** sends a hot key's events to several workers.
  - Partial Key Grouping (Nasir et al., ICDE and arXiv) and adaptive key
    partitioning (Liu et al., CCF Transactions on High Performance Computing)
    balance load much better this way.
  - Splitting is sound only for aggregations whose partial results can be
    merged. It is unsound for event-sourced projections, which must apply a
    stream's events in sequence.
- **Fine granularity.** Consistent Grouping (Nasir et al., arXiv) balances
  through many small units, each with a single owner, assigned according to
  capacity.
- **Order-safe handover** needs a barrier, so the old and new owners never
  process a stream at the same time:
  - SEEP (Castro Fernandez et al.) checkpoints externalised state and replays
    unprocessed input in order;
  - Rhino (Del Monte et al.) coordinates live ownership transfer and state
    migration;
  - Kafka (Wang et al., SIGMOD 2021) reassigns whole partitions, commits
    positions, state and outputs together in one transaction, and fences
    previous owners so they cannot commit.

For PostgreSQL consumers, this means (inference):

- Hash streams into many more partitions than consumers.
- Claim whole partitions, with `SKIP LOCKED` row locks or with advisory locks
  keyed by partition. Because the locks are released when the owning session
  ends, they behave much like leases.
- Commit each partition's position together with its effects.
- Fence stale owners with an epoch if ownership can lapse while a consumer is
  still running.
- Accept one limit: a single hot stream can never be processed faster than one
  consumer can process it.

**Electing a single listening session.** Electing one listener among identical
processes is easy; making failover safe is not. The established fixes for this
"zombie writer" problem are:

- Chubby (Burrows) offers advisory locks, used chiefly to elect primaries. It
  warns that a deposed holder's messages can still act after the lock passes
  to someone else. It mitigates this with a bounded lock-delay and with
  sequencer tokens that carry a lock generation number, which servers check.
- MillWheel (Akidau et al., PVLDB) checks a sequencer token at the storage
  layer.
- Kafka fences zombie producers with an epoch that rises with each new
  instance, and commits offsets inside the producing transaction (Wang et al.).

A PostgreSQL session-level advisory lock is not a lease in Gray and Cheriton's
sense. A lease is a time-limited grant, whose term trades the wait after a
failure against renewal traffic. An advisory lock has no term; it is released
only on unlock or when the session ends. Two consequences follow:

- **The database never grants the lock twice**, so split-brain cannot happen at
  the lock itself. Failover is only as fast as the server's detection of a dead
  client, which depends on TCP keepalive and user-timeout settings.
- **A deposed process can still act.** It may keep handing already-fetched
  work to workers that commit on other connections.

A safe handover follows four steps (inference):

1. **Fence first.** On winning the lock, increment a durable epoch in a
   committed transaction, before doing anything else.
2. **Check the epoch in each worker's transaction**, alongside its effects and
   position update, so stale workers' commits fail.
3. **Listen before catching up.** Issue and commit `LISTEN`, then read the
   stored positions and catch up from the table. Notifications committed while
   no one was listening are never delivered.
4. **Step down on any doubt.** On a connection error or a failed heartbeat
   query, stop dispatching at once.

**The sturdier design gives the elected listener no ownership at all.** It only
relays "something changed" signals to workers. Each worker claims a stream,
partition or batch inside its own processing transaction. Losing the listener
then affects only latency, because the workers' fallback polls still find new
rows.

**Remaining caveats:**

- Idle-session timeouts can kill a leader that sits idle while listening, so
  it needs periodic heartbeat queries.
- Advisory locks and `LISTEN` registrations are not replicated, so a failover
  of the primary database forces a fresh election.
- With asynchronous replication, epochs and positions can move backwards after
  such a failover, so idempotent handling is still needed.

### Why notifications can remain hints

The end-to-end argument (Saltzer, Reed and Clark, ACM Transactions on Computer
Systems) holds that duplicate suppression, delivery acknowledgement and crash
recovery can only be provided completely at the endpoints. Every system
compared here follows it. Delivery is at least once, and "exactly once" is a
property of effects. It is achieved by committing each effect together with a
record that the event was consumed, or by making the effect idempotent. Several
sources reach the same conclusion:

- Desai (Journal of Information Systems Engineering & Management):
  transactional semantics must be combined with idempotency and stage-aware
  retries.
- Helland (Communications of the ACM): receivers should be idempotent, keyed on
  stable message identifiers.
- Kleppmann, Beresford and Svingen (Communications of the ACM): log consumers
  restart from a checkpointed position and may reprocess events, so they must
  be idempotent or deduplicate.

For a pull-based PostgreSQL consumer, where the position is recorded relative
to the effect decides the guarantee:

| Design | Effects inside PostgreSQL | External effects |
|---|---|---|
| Position committed in the same transaction as the effects | Effectively once; a row lock or conditional update on the position also fences rival instances | At least once (idempotency or an outbox is needed) |
| Position committed separately, after the effects | At least once | At least once |
| Position committed separately, before the effects | At most once | At most once |
| Outbox row written in the checkpoint transaction and relayed later | Effectively once | At least once, with a stable idempotency key |
| Table of processed event IDs (an inbox), for consumption out of position order such as `SKIP LOCKED` claims | Effectively once | At least once |

How the published systems compare:

- **MillWheel.** Its per-key atomic write bundles state changes, pending
  outputs and the record's ID. It is the closest published design to the
  same-transaction checkpoint. With ordered positions, one position value
  replaces its set of per-record IDs.
- **Kafka consumers.** Plain offset commits correspond to separate
  checkpoints.
- **Kafka transactions** (Wang et al.) commit outputs, changelog records and
  offsets atomically, but only within Kafka. The paper frames this as a tunable
  trade-off between correctness, latency and cost, in which output becomes
  visible only when the transaction commits.
- **External sinks.** Andersson's thesis compares strategies for exactly-once
  effects when Kafka feeds external sinks. It notes that extending Kafka's
  guarantees beyond its own ecosystem remains a basic challenge.
- **Flink** (Carbone et al., PVLDB) snapshots source positions together with
  operator state. Externally visible latency is then bounded by the checkpoint
  interval.
- **The outbox.** Pratinav and Mishra describe the outbox pattern as the
  mechanism that guarantees atomicity between database state changes and event
  emission.

A single PostgreSQL consumer gets the same structural guarantee from one local
transaction. None of these sources measures what a same-transaction checkpoint
costs in PostgreSQL as batch size varies.

Two consequences for push delivery follow (inference):

- **Notifications only decide when to pull.** With the stored position as the
  only record of progress, a notification decides only *when* the consumer next
  reads. A lost or coalesced notification costs latency, never correctness, as
  long as a fallback poll runs.
- **The checkpoint must be a safe watermark.** An atomic checkpoint makes any
  skipped row permanent. The recorded position must sit below every in-flight
  writer, not merely at the highest position seen.

### How each approach scales, side by side

| Growth dimension | Notification-driven | Fixed-interval polling | Hybrid supported by the evidence |
|---|---|---|---|
| More listeners | Wake-ups and reads per event grow linearly; competing consumers waste N−1 wake-ups per event | Idle queries grow linearly (N ÷ T per second) | Wake-ups bounded by the number of listening processes; workers claim work with late binding |
| More channels or subscriptions | More registrations and matching state with fewer wasted wake-ups, or fewer channels with more wasted wake-ups | Separate per-subscription queries split a budget (square-root rule) unless one global-position query serves all | Coarse channels as doorbells plus a shared global-position read; dedicated reads only for hot subscriptions |
| Higher event throughput | Per-event wake-up overhead; notifying writers serialise on the commit path; a lagging listener can fill the queue writers depend on | Query cost stays flat; batches grow | Coalesced wake-ups and bounded batch drains, so queries grow with batches, not events |
| Idle cost | Near zero | Highest | One long fallback poll per consumer per interval |
| Delivery latency | Lowest at low load | T/2 on average, T at worst | Low at low load; worst case set by the fallback interval when a notification is lost |
| Failure | Notifications lost during disconnects | Resumes from the stored position | Resumes from the stored position; `LISTEN` committed before catch-up |

### Recommended design

These recommendations are inference from the evidence above:

- **Data path.** Keep the position-tracked, batched range read of the event
  table as the only data path. Read up to a safe watermark, such as a
  transaction-ID watermark, rather than the highest visible position.
- **Notifications.** Use `NOTIFY` as a coalesced doorbell whose payload carries
  at most a position. Never treat notifications as the source of truth. Keep
  listeners out of long transactions and draining notifications promptly, so a
  lagging listener cannot fill the server-wide queue that writers depend on.
- **Connecting.** On every connect or reconnect, commit `LISTEN` before running
  the catch-up query.
- **Waking.** On any wake-up, from a notification or the timeout, drain in
  bounded batches until a batch comes back short, then wait again.
- **Fallback poll.** Size the interval from the tolerable worst-case delay for a
  lost notification and the acceptable idle query cost, not from typical
  latency. Redraw its jitter on every cycle.
- **Competing consumers.** Limit wake-ups with one listening session per
  process. Hand work to workers with pull-based late binding, claiming rows
  with `SKIP LOCKED`, and use capacity credits that grow with load. If channels
  are partitioned for locality, partition finely, claim whole partitions, and
  add stealing or a fallback sweep.
- **Without notifications**, for example behind a transaction-pooling proxy:
  - use bounded empty-poll backoff that resets on any non-empty poll, with the
    cap set to the acceptable worst-case delay;
  - split polls across separately queried subscriptions by the square-root
    rule;
  - use a fixed interval for steady streams.
- **Checkpoints.** Commit consumer positions in the same transaction as
  in-database effects. Use an outbox or idempotency keys for external effects.

### Open questions for engineering sources and benchmarks

The scholarly record cannot answer the following. They need PostgreSQL's own
sources and measurements of the library's workload:

- How commit throughput and commit latency of notifying transactions change as
  notifying writers increase, with and without group commit and synchronous
  commit, and how much a `NOTIFY` adds over an otherwise identical append.
- How notification latency from commit to delivery changes as listening
  sessions and channels increase, and how the server-wide notification queue
  behaves as listeners grow or lag.
- How load peaks and delivery latency differ between jittered and lock-step
  polling as the consumer count varies, for loops that poll at a fixed rate and
  loops that sleep after each query completes.
- What gap-avoidance techniques cost in write throughput and read latency on a
  PostgreSQL event table, and how long transaction-ID watermark readers stall
  behind long transactions in practice.
- What same-transaction checkpoints cost as batch size varies, and how quickly
  PostgreSQL detects dead clients and releases advisory locks under different
  keepalive settings.

The evaluation method from the hybrid interrupt-polling literature transfers
directly. Measure delivery delay (from commit to consumer read) and queries per
unit time on traces of real, bursty event arrivals, as offered load rises.

## Sources

- [Integrating databases with publish/subscribe](https://doi.org/10.1109/icdcsw.2005.79) — tier-1 — IEEE International Conference on Distributed Computing Systems Workshops (ICDCSW)
- [Hermes: a distributed event-based middleware architecture](https://doi.org/10.1109/icdcsw.2002.1030837) — tier-1 — International Conference on Distributed Computing Systems Workshops
- [Event-Driven Database Information Sharing](https://doi.org/10.1007/978-3-540-70504-8_11) — tier-1 — Lecture Notes in Computer Science
- [Integrating databases and publish/subscribe](https://openalex.org/W3142320050) — tier-3 — openalex.org (dissertation, no venue recorded)
- [Publish/subscribe in NonStop SQL: transactional streams in a relational context](https://doi.org/10.1109/icde.2004.1320056) — tier-3 — doi.org (no venue recorded)
- [Transactional publish/subscribe](https://doi.org/10.1145/276305.276362) — tier-1 — ACM SIGMOD Record
- [Harness and results for "Witnessed Isolation Anomalies and Minimal Read Promotion for Serializable Execution in NoSQL/NewSQL"](https://doi.org/10.5281/zenodo.21752860) — tier-2 — Zenodo (CERN European Organization for Nuclear Research)
- [Combining Application-Level and Database-Level Monitoring to Analyze the Performance Impact of Database Lock Contention](https://openalex.org/W2604627408) — tier-1 — Softwaretechnik-Trends
- [Adaptive push-pull: disseminating dynamic Web data](https://doi.org/10.1109/tc.2002.1009150) — tier-1 — IEEE Transactions on Computers
- [Adaptive push-pull](https://doi.org/10.1145/371920.372066) — tier-1 — International World Wide Web Conference (WWW)
- [Balancing push and pull for data broadcast](https://doi.org/10.1145/253260.253293) — tier-3 — doi.org (ACM conference paper, venue not recorded)
- [Adaptive Multichannel Data Dissemination: Support of Dynamic Traffic Awareness and Push–Pull Time Balance](https://doi.org/10.1109/tvt.2004.841556) — tier-1 — IEEE Transactions on Vehicular Technology
- [Eliminating receive livelock in an interrupt-driven kernel](https://doi.org/10.1145/263326.263335) — tier-1 — ACM Transactions on Computer Systems
- [HIP](https://doi.org/10.1145/506084.506089) — tier-1 — ACM SIGOPS Operating Systems Review
- [A Robust Device Hybrid Scheme to Improve System Performance in Gigabit Ethernet Networks](https://doi.org/10.1109/lcn.2007.20) — tier-3 — doi.org (no venue recorded)
- [A novel hybrid scheme of interrupt enabling - disabling and polling (EDP) for high-speed computer networks](https://doi.org/10.1109/iscit.2007.4392042) — tier-1 — International Symposium on Communications and Information Technologies
- [Performance analysis and comparison of interrupt-handling schemes in gigabit networks](https://doi.org/10.1016/j.comcom.2007.06.013) — tier-1 — Computer Communications
- [Implementation and experimental performance evaluation of a hybrid interrupt-handling scheme](https://doi.org/10.1016/j.comcom.2008.10.001) — tier-1 — Computer Communications
- [Faster than Flash: An In-Depth Study of System Challenges for Emerging Ultra-Low Latency SSDs](https://doi.org/10.1109/iiswc47752.2019.9042009) — tier-1 — IEEE International Symposium on Workload Characterization
- [Efficient hybrid polling for ultra-low latency storage devices](https://doi.org/10.1016/j.sysarc.2021.102338) — tier-1 — Journal of Systems Architecture
- [When poll is more energy efficient than interrupt](https://doi.org/10.1145/3538643.3539747) — tier-1 — ACM Workshop on Hot Topics in Storage and File Systems (HotStorage)
- [The Linux Scalability Project](https://openalex.org/W1521570651) — tier-2 — Deep Blue (University of Michigan)
- [Hermes: Enhancing Layer-7 Cloud Load Balancers with Userspace-Directed I/O Event Notification](https://doi.org/10.1145/3718958.3750469) — tier-3 — doi.org (ACM conference paper, venue not recorded)
- [The Thundering Herd: Amplifying Kernel Interference to Attack Response Times](https://doi.org/10.1109/rtas54340.2022.00016) — tier-3 — doi.org (IEEE conference paper; no venue recorded)
- [The DNS thundering herd problem](https://openalex.org/W3037836813) — tier-3 — openalex.org (venue not recorded)
- [Design and evaluation of a wide-area event notification service](https://doi.org/10.1145/380749.380767) — tier-1 — ACM Transactions on Computer Systems
- [The many faces of publish/subscribe](https://doi.org/10.1145/857076.857078) — tier-1 — ACM Computing Surveys
- [Staring into the abyss](https://doi.org/10.14778/2735508.2735511) — tier-1 — Proceedings of the VLDB Endowment
- [Speedy transactions in multicore in-memory databases](https://doi.org/10.1145/2517349.2522713) — tier-3 — doi.org (ACM conference paper; no venue recorded by OpenAlex)
- [An empirical evaluation of in-memory multi-version concurrency control](https://doi.org/10.14778/3067421.3067427) — tier-1 — Proceedings of the VLDB Endowment
- [High-performance concurrency control mechanisms for main-memory databases](https://doi.org/10.14778/2095686.2095689) — tier-1 — Proceedings of the VLDB Endowment
- [Releasing Locks As Early As You Can: Reducing Contention of Hotspots by Violating Two-Phase Locking](https://doi.org/10.1145/3448016.3457294) — tier-1 — ACM SIGMOD International Conference on Management of Data (SIGMOD)
- [Aether](https://doi.org/10.14778/1920841.1920928) — tier-1 — Proceedings of the VLDB Endowment
- [CORFU](https://doi.org/10.1145/2535930) — tier-1 — ACM Transactions on Computer Systems
- [A read-only transaction anomaly under snapshot isolation](https://doi.org/10.1145/1031570.1031573) — tier-1 — ACM SIGMOD Record
- [Serializable snapshot isolation in PostgreSQL](https://doi.org/10.14778/2367502.2367523) — tier-1 — Proceedings of the VLDB Endowment
- [Database Replication Using Generalized Snapshot Isolation](https://doi.org/10.1109/reldis.2005.14) — tier-3 — doi.org conference paper (venue not recorded by OpenAlex)
- [DBLog: A Watermark Based Change-Data-Capture Framework](https://doi.org/10.48550/arxiv.2010.12597) — tier-2 — arXiv (Cornell University)
- [Effective page refresh policies for Web crawlers](https://doi.org/10.1145/958942.958945) — tier-1 — ACM Transactions on Database Systems
- [Estimating frequency of change](https://doi.org/10.1145/857166.857170) — tier-1 — ACM Transactions on Internet Technology
- [Efficient Monitoring Algorithm for Fast News Alerts](https://doi.org/10.1109/tkde.2007.1041) — tier-1 — IEEE Transactions on Knowledge and Data Engineering
- [Maintaining temporal coherency of virtual data warehouses](https://doi.org/10.1109/real.1998.739731) — tier-1 — IEEE Real-Time Systems Symposium (RTSS)
- [Efficient visit frequencies for polling tables: minimization of waiting cost](https://doi.org/10.1007/bf01158795) — tier-1 — Queueing Systems
- [Efficient visit frequencies for polling tables : minimization of waiting cost](https://openalex.org/W2564238886) — tier-2 — TU/e Research Portal
- [Tractable near-optimal policies for crawling](https://doi.org/10.1073/pnas.1801519115) — tier-1 — Proceedings of the National Academy of Sciences
- [The synchronization of periodic routing messages](https://doi.org/10.1145/167954.166241) — tier-1 — ACM SIGCOMM Computer Communication Review
- [The synchronization of periodic routing messages](https://doi.org/10.1109/90.298431) — tier-1 — IEEE/ACM Transactions on Networking
- [Optimal probabilistic cache stampede prevention](https://doi.org/10.14778/2757807.2757813) — tier-1 — Proceedings of the VLDB Endowment
- [Adaptive Resource Allocation Utilizing Periodic Traffic and Clock Drift in LPWAN](https://doi.org/10.1109/twc.2023.3311679) — tier-1 — IEEE Transactions on Wireless Communications
- [Is the Random Access Channel of LTE and LTE-A Suitable for M2M Communications? A Survey of Alternatives](https://doi.org/10.1109/surv.2013.111313.00244) — tier-1 — IEEE Communications Surveys & Tutorials
- [Building a replicated logging system with Apache Kafka](https://doi.org/10.14778/2824032.2824063) — tier-1 — Proceedings of the VLDB Endowment
- [Apache Kafka: A High-Throughput Distributed Messaging System](https://openalex.org/W3129849971) — tier-3 — openalex.org (venue not recorded)
- [From Broker-Centric Queues to Distributed Logs: Reliable Messaging Models for Enterprise Applications Using Apache Kafka](https://doi.org/10.5281/zenodo.18084827) — tier-2 — Zenodo (CERN European Organization for Nuclear Research)
- [Kafka versus RabbitMQ](https://doi.org/10.48550/arxiv.1709.00333) — tier-2 — arXiv (Cornell University)
- [Kafka versus RabbitMQ](https://doi.org/10.1145/3093742.3093908) — tier-3 — doi.org, ACM conference paper (venue not recorded)
- [Gossip-Pull System-based Dissemination Protocol Satisfying Message Causality Condition](https://doi.org/10.2991/cse.2013.72) — tier-3 — doi.org, conference paper (venue not recorded)
- [Global Message Ordering using Distributed Kafka Clusters](https://doi.org/10.1109/iit59782.2023.10366422) — tier-1 — International Conference on Innovations in Information Technology
- [Integrated publish/subscribe and push-pull method for cloud based IoT framework for real time data processing](https://doi.org/10.1016/j.measen.2023.100699) — tier-1 — Measurement Sensors
- [ZygOS](https://doi.org/10.1145/3132747.3132780) — tier-3 — doi.org (ACM conference paper; no venue recorded)
- [Sparrow](https://doi.org/10.1145/2517349.2522716) — tier-3 — doi.org (ACM conference paper; no venue recorded)
- [Join-Idle-Queue: A novel load balancing algorithm for dynamically scalable web services](https://doi.org/10.1016/j.peva.2011.07.015) — tier-1 — Performance Evaluation
- [Heavy-traffic Delay Optimality in Pull-based Load Balancing Systems](https://doi.org/10.1145/3287323) — tier-1 — Proceedings of the ACM on Measurement and Analysis of Computing Systems
- [Designing Low-Complexity Heavy-Traffic Delay-Optimal Load Balancing Schemes](https://doi.org/10.1145/3154498) — tier-1 — Proceedings of the ACM on Measurement and Analysis of Computing Systems
- [The power of two choices in randomized load balancing](https://doi.org/10.1109/71.963420) — tier-1 — IEEE Transactions on Parallel and Distributed Systems
- [Improving network connection locality on multicore systems](https://doi.org/10.1145/2168836.2168870) — tier-3 — doi.org (ACM conference paper; no venue recorded)
- [Scheduling multithreaded computations by work stealing](https://doi.org/10.1145/324133.324234) — tier-1 — Journal of the ACM
- [Efficient key grouping for near-optimal load balancing in stream processing systems](https://doi.org/10.1145/2675743.2771827) — tier-1 — ACM International Conference on Distributed Event-Based Systems (DEBS)
- [Elastic Scaling for Data Stream Processing](https://doi.org/10.1109/tpds.2013.295) — tier-1 — IEEE Transactions on Parallel and Distributed Systems
- [Auto-parallelizing stateful distributed streaming applications](https://doi.org/10.1145/2370816.2370826) — tier-1 — International Conference on Parallel Architectures and Compilation Techniques (PACT)
- [The power of both choices: Practical load balancing for distributed stream processing engines](https://doi.org/10.1109/icde.2015.7113279) — tier-3 — doi.org (IEEE ICDE 2015 conference paper; OpenAlex venue unrecorded)
- [Partial Key Grouping: Load-Balanced Partitioning of Distributed Streams](https://doi.org/10.48550/arxiv.1510.07623) — tier-2 — arXiv (Cornell University)
- [Adaptive key partitioning in distributed stream processing](https://doi.org/10.1007/s42514-023-00179-3) — tier-1 — CCF Transactions on High Performance Computing
- [Load Balancing for Skewed Streams on Heterogeneous Cluster](https://doi.org/10.48550/arxiv.1705.09073) — tier-2 — arXiv (Cornell University)
- [Integrating scale out and fault tolerance in stream processing using operator state management](https://doi.org/10.1145/2463676.2465282) — tier-3 — doi.org (ACM conference paper; OpenAlex venue unrecorded)
- [Rhino: Efficient Management of Very Large Distributed State for Stream Processing Engines](https://doi.org/10.1145/3318464.3389723) — tier-3 — doi.org (ACM conference paper; OpenAlex venue unrecorded)
- [The Chubby lock service for loosely-coupled distributed systems](https://doi.org/10.5555/1298455.1298487) — tier-3 — Operating Systems Design and Implementation (OSDI)
- [Leases: an efficient fault-tolerant mechanism for distributed file cache consistency](https://doi.org/10.1145/74850.74870) — tier-3 — doi.org (venue not recorded in OpenAlex)
- [MillWheel: fault-tolerant stream processing at Internet scale](https://doi.org/10.14778/2536222.2536229) — tier-1 — Proceedings of the VLDB Endowment
- [Consistency and Completeness: rethinking distributed stream processing in Apache Kafka](https://doi.org/10.1145/3448016.3457556) — tier-1 — ACM SIGMOD International Conference on Management of Data (SIGMOD)
- [End-to-end arguments in system design](https://doi.org/10.1145/357401.357402) — tier-1 — ACM Transactions on Computer Systems
- [Reliable Event-Driven Processing in Distributed Systems: Stage-Aware Retries, Idempotency, and Exactly-Once Semantics](https://doi.org/10.52783/jisem.v11i2s.14608) — tier-1 — Journal of Information Systems Engineering & Management
- [Idempotence is not a medical condition](https://doi.org/10.1145/2160718.2160734) — tier-1 — Communications of the ACM
- [Online event processing](https://doi.org/10.1145/3312527) — tier-1 — Communications of the ACM
- [Comparing Exactly-Once Delivery Strategies for External Sinks in Stream Processing Systems](https://openalex.org/W7165828937) — tier-2 — Diva portal (Dalarna University Library)
- [State management in Apache Flink®](https://doi.org/10.14778/3137765.3137777) — tier-1 — Proceedings of the VLDB Endowment
- [Outbox Pattern for Reliable Distributed Systems](https://doi.org/10.63397/iscsitr-ijcc_2025_06_06_002) — tier-1 — ISCSITR-International Journal of Cloud Computing
