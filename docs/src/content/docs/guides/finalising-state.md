---
title: Finalising state
description: Do work once per projection rather than in every event handler.
---

Override `finalise_state` to do work once per projection rather than in every
event handler, for example validation or normalisation. Handlers can then make
cheap, unvalidated changes and leave the expensive work to the end:

```python
class ValidatedProfileProjector(ProfileProjector):
    def finalise_state(self, state: dict[str, str]) -> dict[str, str]:
        if "email" not in state:
            raise ValueError("profile has no email")
        return {**state, "email": state["email"].strip()}
```

`finalise_state` is called once at the end of each `project()` call, after
all events are applied and before `id_factory` derives the projection id,
including when the source has no events. Keep in mind that:

- with `ProjectionEventProcessor`, which projects one event at a time, it
  runs once per processed event, so the saving comes from multi-event folds
  such as rebuilds;
- its output is passed back in as the starting state when a projection is
  resumed (via `state=`, and always by `ProjectionEventProcessor`), so it must
  not change what later handlers, `update_metadata` or `id_factory` compute.
  Validation and recomputing derived fields are fine; lossy normalisation,
  such as clamping or truncation, isn't;
- its output must survive a round trip through the projection store, and it
  must not change fields `id_factory` uses for projections that are already
  stored, otherwise those projections need rebuilding;
- `update_metadata` and `apply()` see unfinalised state;
- handlers mustn't rely on coercion or defaults that validation would apply;
- `finalise_state` is a reserved name, so no event may be named
  `finalise-state`;
- subclasses that override `project()` must call `finalise_state`
  themselves.
