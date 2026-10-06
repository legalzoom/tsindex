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
Track directory-scoped rules above each workspace root, including absent rule
paths so their creation is observed. Discover directory-scoped rule files and
embedded Git inputs during eligible-directory walks, and Git inputs through
metadata events; reload indirections when processing changes. Probe symlinked
rule files through their file-link chain, including a missing target, without
traversing directory symlink trees.
When a walk discovers new probe parents, repeat its affected targets after
installing the probes. Keep eligibility from the final pass so rule changes
before discovery neither leave included sources unwatched nor retain newly
excluded source watches.

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
