# dleveldb 设计说明

本文记录放置规则、磁盘格式、副本协议和 HTTP RPC。总体分层和读写路径见 [architecture.md](architecture.md)。

## 放置

- key 的哈希：`SHA-1(utf8(key))`，160 位，写成 40 个十六进制字符。
- vnode 的哈希：`SHA-1(utf8(`${nodeId}:${seq}`))`。注意哈希输入里的序号**不补零**，例如 `node1:0`、`node1:10`。对外展示的 id 则是 `{nodeId}-{seq}`，例如 `node1-0`。
- 主副本 = 环上 key 哈希的后继（successor）。
- 偏好列表：从主副本沿环前进，每个物理 `nodeId` 最多选一个 vnode，直到凑满 N 个，或环上没有更多物理机。
- 有效副本数：`N_eff = min(配置的 N, 当前不同物理机数量)`。
- 写法定人数：未配置时取 `N_eff` 的多数 `floor(N_eff / 2) + 1`；显式配置时也不会超过 `N_eff`。

## 磁盘布局

```
data/shards/{nodeId}-{seq}/    # 本机主副本，标准 LevelDB 目录（有 CURRENT、MANIFEST、*.ldb）
data/replicas/{owner-vnode}/   # 其他 vnode 的副本，同样是标准 LevelDB
```

不会再创建 packed-block 用的 `data/index` 或 `data/blocks`。

LevelDB 中的 value 默认编码为：

```
magic "DLKV"   4 bytes
crc32          uint32 LE，对原始 value 计算
mtimeMs        uint64 LE
size           uint32 LE
value          size bytes
```

`Get` 返回原始字节和元数据；`Head` 只返回 size / crc / mtime。若读到的是没有信封的旧数据，则按原始字节处理，mtime 记为 0。

## 副本与故障

- 写入先落主副本的 shard，再复制到偏好列表中的跟随者；跟随者把数据写到 `replicas/{主 vnode id}/`，而不是写进自己的主 shard。这样即使所有权日后仍指向原来的 vnode，副本也找得到。
- 失败的 vnode 留在 known 集合里并标为 down，key 的所有者不变。
- Chord 的 `findSuccessor`、stabilize、finger 只使用仍然活着的 vnode。
- 主副本不可达时，Put / Delete 失败（`PRIMARY_UNAVAILABLE` 或写法定人数不足），不会在本版本里切换主。
- 读取顺序：本机所有 shard → 本机所有已发现的 replica 目录 → 其他节点的 `get-any` / `head-any`。
- 加入环之后的范围迁移 / catch-up **尚未实现**。`repairOnJoin` 是占位，新节点不会自动拉取历史 key。

## HTTP RPC

规范前缀是 `/_dleveldb/`。同时也接受 `/_s3dfs/` 以及旧的 `/_s3dfs/dleveldb/`。请求头以 `x-dleveldb-*` 为准，并接受 `x-s3dfs-*` 作为别名。

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| GET | `/_dleveldb/cluster` | 集群状态：vnode、后继、有效 N / W |
| GET | `/_dleveldb/cluster/lookup?key=` | 某个 key 的哈希和偏好列表 |
| GET | `/_dleveldb/stats` | 本机统计 |
| GET | `/health` | API 进程探活（Nest 提供） |
| POST | `/_dleveldb/flush` | 刷新本地分片 |
| POST | `/_dleveldb/compact` | 压缩本地 LevelDB |
| POST | `/_dleveldb/kv/put` | 客户端写入（可被转发到主副本） |
| GET | `/_dleveldb/kv/get?key=` | 客户端读取 |
| GET | `/_dleveldb/kv/head?key=` | 只取元数据 |
| POST | `/_dleveldb/kv/delete` | 客户端删除 |
| POST | `/_dleveldb/kv/scan` | 前缀扫描（集群归并） |
| GET/POST | `/_dleveldb/chord/*` | Chord：ping、find_successor、notify、predecessor、successor_list、announce（部分是 GET） |
| POST | `/_dleveldb/put` | 仅当本机是主副本时写入 |
| POST | `/_dleveldb/replicate` | 跟随者接收复制 |
| POST | `/_dleveldb/replicate-delete` | 跟随者接收删除 |
| GET | `/_dleveldb/get-any?key=` | 在本机任意 shard / replica 中查找 |
| GET | `/_dleveldb/head-any?key=` | 同上，只取元数据 |

写入的 key 放在请求头 `x-dleveldb-key`（URL 编码）里，body 是原始字节。`Get` 成功时，响应头带 `x-dleveldb-size`、`x-dleveldb-crc`、`x-dleveldb-mtime`。
