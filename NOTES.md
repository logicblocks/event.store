Notes
=====

* position: within stream, similar to version (of aggregate / entity),
  zero based, contiguous, in and of itself not monotonically increasing,
* global_position: at level of whole log, in and of itself not strictly increasing, could have gaps

* external control to ensure totally ordered events as seen by an external
  observer
    - serializable transaction isolation level - throw exception if DB detects
      non-linearizable transaction
    - advisory locks - lock on a key, other transactions wait on lock, with
      optional timeout
       - key is application defined
       - key could be stream name, category name, whole log
    - trail head of log by time, t
    - label events with their sequence asynchronously