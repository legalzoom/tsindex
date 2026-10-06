# Observe ignore inputs separately from source watches

## Context

Pruning ignored trees before Linux watch allocation bounds resource use, but
it removes the notifications that previously let those trees become eligible
after ignore rules changed. Git excludes can live in pruned metadata, shared
worktree directories, or outside the source tree entirely. Parent custom rules
can also affect separately configured nested workspaces.

## Decision

Track rule files and their indirections explicitly. Probe their nearest
existing parents nonrecursively, advancing those probes as parents arrive.
Discover embedded Git inputs during eligible-directory walks and metadata
events; reload indirections when processing changes.

Use the pinned `ignore` crate's global-file resolver and match its repository
metadata resolution. Preserve explicit `add_ignore` file-path semantics,
including process-relative paths. Calling Git would introduce another runtime
dependency and could select inputs the actual walker does not honor.

Share affected-target expansion between registration and incremental indexing.
Refresh nested indexes before releasing newly excluded watches. Keep watches
when indexing fails so subsequent notifications can recover the index.

## Consequences

Source walks remain pruned, and metadata/home directories receive only parent
probes. Native platforms perform an eligible-directory discovery walk while
retaining recursive source registration. Ignore-input matching uses changed
path hash membership to avoid a rule-input-by-event cross-product.

The resolver follows pinned dependency semantics. Dependency upgrades that
change Git input resolution require corresponding probe and regression-test
updates.
