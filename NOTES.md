Notes
=====

[x] position: within stream, similar to version (of aggregate / entity),
  zero based, contiguous, strictly monotonically increasing.
[x] sequence_number: at level of whole log, in and of itself not strictly 
  increasing, could have gaps
[] external control to ensure totally ordered events as seen by an external
  observer
    - serializable transaction isolation level - throw exception if DB detects
      non-linearizable transaction
    - advisory locks - lock on a key, other transactions wait on lock, with
      optional timeout
       - key is application defined
       - key could be stream name, category name, whole log
    - trail head of log by time, t
    - label events with their sequence asynchronously
    - decision for now is to use advisory lock on whole log

[x] expected position
  - Explicitly test undefined position case
  - Refactor WriteCondition implementation to be function based
[x] postgres adapter
[] concurrency testing
[] projection capability
  - add position (from last event)
  - refactor into Projection and Projector (think about how to model and pass 
    "state" of projection)
  - think about how to add Snapshotting (decorator on Projector class?)

[] allow JSON serialisation / deserialisation to be overridden