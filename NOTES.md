Notes
=====

[x] position: within stream, similar to version (of aggregate / entity),
  zero based, contiguous, strictly monotonically increasing.
[x] sequence_number: at level of whole log, in and of itself not strictly 
  increasing, could have gaps
[~] external control to ensure totally ordered events as seen by an external
  observer
    - serializable transaction isolation level - throw exception if DB detects
      non-linearizable transaction
    - locks - lock on a key, other transactions wait on lock, with
      optional timeout
       - key is application defined
       - key could be: 
         [] stream name, 
         [] category name, 
         [x] log table
    - trail head of log by time, t
    - label events with their sequence asynchronously

[x] expected position
  [] explicitly test undefined position case
  [x] refactor WriteCondition implementation to be function based
[x] postgres adapter
[x] concurrency testing
[x] projection capability
  - add position (from last event)
  - refactor into Projection and Projector (think about how to model and pass 
    "state" of projection)
[] add snapshotting (decorator on Projector class?)

[] expose sequence number on events
[] add metadata on events

[] allow table name to be configured
[] add pre, persist and post hooks on event write
[] add support for cryptographic signing to make stream/category/log 
   tamper-proof

[] add example DDL including indexes

[] allow JSON serialisation / deserialisation to be overridden

[] make event scanning paged
[] add whole log scanning to store

[] split into multiple published packages

[] add in-process consumer support

[] add event schema support

[] move adapter tests into published package
