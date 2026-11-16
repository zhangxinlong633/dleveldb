# dleveldb

分布式 LevelDB：每个分片是一份标准 [LevelDB](https://github.com/google/leveldb)，值直接写进 LevelDB。对外是 Get / Put / Delete / 前缀扫描。节点用 Chord DHT 定位，默认 3 副本、写法定人数为多数。

Distributed LevelDB. Each shard is a plain LevelDB database (values live in LevelDB, not in packed block files). Nodes are connected with a Chord ring. Default replication factor is 3, with a majority write quorum.

## Install

```bash
npm install dleveldb
# or
pnpm add dleveldb
```

Node.js 20+ is required. Local storage uses [`classic-level`](https://github.com/Level/classic-level), the standard Node.js binding for LevelDB.

## Quick start

```ts
import { DLevelDB, loadConfig } from 'dleveldb';

const db = await DLevelDB.open(loadConfig({}, {
  dataDir: './data',
  nodeId: 'node1',
  advertiseAddr: '127.0.0.1:9000',
  vnodeCount: 4,
  replicas: 3,
}));

await db.put('logs/2026/a.log', Buffer.from('hello'));
const got = await db.get('logs/2026/a.log');
const listed = await db.scan({ prefix: 'logs/2026/', maxKeys: 100 });
await db.close();
```

Run a standalone node:

```bash
DLEVELDB_DATA_DIR=./data DLEVELDB_NODE_ID=node1 DLEVELDB_PORT=9000 npx dleveldb
```

Join a cluster by pointing at an existing peer:

```bash
DLEVELDB_DATA_DIR=./data-b \
DLEVELDB_NODE_ID=node2 \
DLEVELDB_PORT=9001 \
DLEVELDB_PEERS=127.0.0.1:9000 \
npx dleveldb
```

HTTP RPC is served under `/_dleveldb/` (legacy `/_s3dfs/` paths are also accepted).

## Layout

On each machine:

```
data/
  shards/{nodeId}-{seq}/     # primary LevelDB for each vnode
  replicas/{owner-vnode}/    # replica LevelDB copied from another vnode
```

- Virtual node id is `{nodeId}-{seq}`. Its ring position is `SHA-1(utf8(`${nodeId}:${seq}`))`.
- A key’s primary is the successor of `SHA-1(utf8(key))`.
- The preference list walks the ring and picks `N` vnodes on **distinct physical machines**.
- Effective `N = min(configured N, number of physical nodes)`. A single machine therefore stores one copy.
- Write quorum `W` defaults to `floor(N/2)+1`. If the primary is unreachable, the write fails; this version does not promote a new primary (avoids split-brain). Reads may be served from any local primary shard or replica, then from other nodes.

Prefix scan is scatter-gather: keys are placed by hash, so a prefix is not a single LevelDB seek except on a one-node, one-vnode deployment.

## Configuration

Environment variables use the `DLEVELDB_` prefix. `S3DFS_*` names are accepted as aliases.

| Variable | Default | Meaning |
| --- | --- | --- |
| `DLEVELDB_DATA_DIR` | `./data` | Data directory |
| `DLEVELDB_NODE_ID` | `node1` | Physical node id |
| `DLEVELDB_PORT` | `9000` | Listen port (used when advertise addr is omitted) |
| `DLEVELDB_ADVERTISE_ADDR` | `127.0.0.1:${PORT}` | Address other nodes use to reach this one |
| `DLEVELDB_PEERS` | empty | Comma-separated `host:port` seed peers |
| `DLEVELDB_VNODES` | `1` | Virtual nodes on this machine |
| `DLEVELDB_REPLICAS` | `3` | Configured replica factor N |
| `DLEVELDB_WRITE_QUORUM` | majority of effective N | Write quorum W |
| `DLEVELDB_SYNC` | `true` | Sync LevelDB writes to disk |
| `DLEVELDB_STABILIZE_INTERVAL_MS` | `500` | Chord stabilize interval |
| `DLEVELDB_RPC_TIMEOUT_MS` | `10000` | RPC timeout |

## Status

Implemented: vnode shards, Chord location, default 3 replicas, majority write quorum, read from remaining replicas after a primary is down, prefix scan.

Not implemented: join catch-up / range migration. A newly joined node does not automatically pull historical keys.

## License

MIT
