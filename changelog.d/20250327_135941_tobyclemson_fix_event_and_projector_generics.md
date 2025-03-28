### Changed

- The type parameters on `Projector` have been reordered to better reflect the
  transformation taking place. So rather than 
  `Projector[State, Identifier, Metadata]` where identifier identifies the 
  source of the events, the type signature is now 
  `Projector[Identifier, State, Metadata]`
- `NewEvent` and `StoredEvent` are now generic over their name type, in addition 
  to the payload, i.e., `NewEvent[Name, Payload]` and 
  `StoredEvent[Name, Payload]`. This change is to allow methods/functions to
  target specific events, for example, as 
  `StoredEvent[Literal["profile-created"], ProfilePayload]`. Previously, if two
  events had the same payload type but different names, it wasn't possible to
  restrict a method/function signature to only one of them.
