# dleveldb design

dleveldb is a standalone distributed key-value store. The local engine is standard LevelDB. Object bodies and packed-block files are out of scope; applications that need a packed-block object store should keep that layer themselves and use dleveldb only as the KV.

## Placement

- Key hash: `SHA-1(utf8(key))`, 160 bits.
- Vnode hash: `SHA-1(utf8(`${nodeId}:${seq}`))`. The vnode id string is `{nodeId}-{seq}` (no zero-padding in the hash input).
- Primary = successor of the key hash on the Chord ring.
- Preference list: walk the ring from the primary, taking at most one vnode per physical `nodeId`, until N vnodes are collected or the ring is exhausted.
- Effective N = `min(configured N, distinct physical nodes)`.
- W defaults to majority of effective N, and never exceeds effective N.

## On-disk layout

```
data/shards/{nodeId}-{seq}/    # Classic LevelDB directory
data/replicas/{owner-vnode}/   # replica copy of another vnode’s keys
```

Each key’s value is stored as a small envelope inside LevelDB (`DLKV` magic, CRC-32, mtime, size, then the raw bytes) so `head()` can return size / CRC / mtime. Raw LevelDB values without the envelope are still readable.

## Failure

Failed vnodes stay in the known set, marked down, so key ownership does not move. Chord routing (`findSuccessor`, stabilize, fingers) uses only live vnodes. Writes still target the original primary and fail if it is unreachable. Reads search all local shards and replica stores first, then other nodes’ local copies via `get-any` / `head-any`.

Join catch-up and range migration are not implemented (`repairOnJoin` is a stub).

## RPC

Canonical prefix: `/_dleveldb/`. Legacy `/_s3dfs/` and `/_s3dfs/dleveldb/` are accepted. Headers are `x-dleveldb-*` (and `x-s3dfs-*` as aliases).
