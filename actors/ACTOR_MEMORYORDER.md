# Actor memory ordering (moved)

This page moved. The proofs are split along the public/private boundary:

- Public activation, inbox, wave and engine-fence proofs, plus the shared atomic
  notation and handoff tables:
  [../docs/design/public-actor-ordering.md](../docs/design/public-actor-ordering.md)
- Private join, result, failure, cancellation, root-tracking and continuation
  proofs:
  [../docs/design/private-actor-ordering.md](../docs/design/private-actor-ordering.md)
- Private Entry/Service/cohort layout and slab reuse:
  [../docs/design/private-actor-layout.md](../docs/design/private-actor-layout.md)

The Farm sentinel and segment-lifetime rules remain in [../SPEC.md](../SPEC.md).
