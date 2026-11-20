import path from 'node:path';
import { ChordRing } from './chord.js';
import { effectiveReplicas, effectiveWriteQuorum } from './config.js';
import { hashKey } from './hash.js';
import { mergeScan } from './rpc.js';
import { HttpTransport } from './rpc.js';
import { openLocalShards, ReplicaStoreSet, sumStats, type LocalShard } from './shards.js';
import type {
  DLevelDBConfig,
  DLevelDBStats,
  DLevelDBStatus,
  KVMeta,
  KVValue,
  ScanOptions,
  ScanResult,
  VnodeInfo,
} from './types.js';
import { DLevelDBError } from './types.js';

const REPAIR_NOTE =
  '节点加入和周期稳定之后，会把本机负责的 key 从其他存活节点的主分片和副本里按较新的 mtime 补齐。主节点不可达时，偏好列表里第一个还活着的节点按写法定人数把数据写入副本。';

export class DLevelDB {
  readonly config: DLevelDBConfig;
  readonly ring: ChordRing;
  readonly transport: HttpTransport;
  private readonly shards: LocalShard[];
  private readonly shardById: Map<string, LocalShard>;
  private readonly replicas: ReplicaStoreSet;
  readonly usesLegacyLayout: boolean;
  private stabilizeTimer: ReturnType<typeof setInterval> | undefined;
  private joining = false;
  private closed = false;

  private constructor(
    config: DLevelDBConfig,
    shards: LocalShard[],
    usesLegacyLayout: boolean,
    replicas: ReplicaStoreSet,
    ring: ChordRing,
    transport: HttpTransport,
  ) {
    this.config = config;
    this.shards = shards;
    this.usesLegacyLayout = usesLegacyLayout;
    this.replicas = replicas;
    this.ring = ring;
    this.transport = transport;
    this.shardById = new Map(shards.map((s) => [s.info.id, s]));
  }

  static async open(config: DLevelDBConfig): Promise<DLevelDB> {
    const sync = config.sync ?? config.fsyncOnFlush ?? true;
    const { shards, usesLegacyLayout } = await openLocalShards({
      dataDir: path.resolve(config.dataDir),
      nodeId: config.nodeId,
      address: config.advertiseAddr,
      vnodeCount: config.vnodeCount,
      sync,
    });
    const ring = new ChordRing({
      nodeId: config.nodeId,
      address: config.advertiseAddr,
      localVnodes: shards.map((s) => s.info),
      successorListLen: Math.max(4, config.replicas),
    });
    const transport = new HttpTransport(config.advertiseAddr, config.rpcTimeoutMs);
    ring.setTransport(transport);
    const replicas = new ReplicaStoreSet(path.resolve(config.dataDir), sync);
    const db = new DLevelDB(config, shards, usesLegacyLayout, replicas, ring, transport);
    if (config.peers.length > 0) {
      void db.joinWithRetry(config.peers);
    }
    db.stabilizeTimer = setInterval(() => {
      void db.ring.stabilize();
      void db.repairOnJoin();
    }, config.stabilizeIntervalMs);
    db.stabilizeTimer.unref();
    return db;
  }

  async close(): Promise<void> {
    this.closed = true;
    if (this.stabilizeTimer) {
      clearInterval(this.stabilizeTimer);
      this.stabilizeTimer = undefined;
    }
    for (const shard of this.shards) {
      await shard.store.flush();
      await shard.store.close();
    }
    await this.replicas.close();
  }

  async put(key: string, value: Buffer): Promise<void> {
    const prefs = this.prefs(key);
    const primary = prefs[0];
    if (!primary) {
      throw new DLevelDBError('PRIMARY_UNAVAILABLE', 'empty preference list');
    }
    if (this.isLocal(primary.address)) {
      await this.putAsPrimary(key, value, prefs);
      return;
    }
    try {
      await this.transport.proxyPut(primary.address, key, value);
    } catch (err) {
      if (!this.primaryUnreachable(err)) {
        throw err;
      }
      await this.putWhilePrimaryDown(key, value, prefs);
    }
  }

  async get(key: string): Promise<KVValue | null> {
    return this.readValue(key, false);
  }

  async head(key: string): Promise<KVMeta | null> {
    const found = await this.readValue(key, true);
    if (!found) {
      return null;
    }
    return { size: found.size, crc: found.crc, mtimeMs: found.mtimeMs };
  }

  async delete(key: string): Promise<boolean> {
    const prefs = this.prefs(key);
    const primary = prefs[0];
    if (!primary) {
      throw new DLevelDBError('PRIMARY_UNAVAILABLE', 'empty preference list');
    }
    if (!this.isLocal(primary.address)) {
      return this.transport.proxyDelete(primary.address, key);
    }
    return this.deleteAsPrimary(key, prefs);
  }

  async scan(opts: ScanOptions = {}): Promise<ScanResult> {
    const maxKeys = Math.max(1, opts.maxKeys ?? 1000);
    const normalized: ScanOptions = { ...opts, maxKeys };
    if (this.isSingleStoreFastPath()) {
      return await this.shards[0]!.store.list(normalized);
    }

    const addresses = this.scanTargets();
    const reachable: string[] = [];
    const downNodeIds = new Set<string>();
    const downVnodes: string[] = [];

    for (const addr of addresses) {
      if (this.isLocal(addr)) {
        reachable.push(addr);
        continue;
      }
      const ok = await this.transport.ping(addr);
      if (ok) {
        reachable.push(addr);
      } else {
        for (const vnode of this.ring.sortedKnown()) {
          if (vnode.address === addr) {
            downNodeIds.add(vnode.nodeId);
            downVnodes.push(vnode.id);
          }
        }
      }
    }

    const parts: ScanResult[] = [];
    for (const addr of reachable) {
      if (this.isLocal(addr)) {
        parts.push(await this.scanLocalPrimaries(normalized));
      } else {
        try {
          parts.push(await this.transport.proxyScan(addr, normalized));
        } catch {
          for (const vnode of this.ring.sortedKnown()) {
            if (vnode.address === addr) {
              downVnodes.push(vnode.id);
            }
          }
        }
      }
    }

    const uniqueDown = [...new Set(downVnodes)];
    if (uniqueDown.length > 0) {
      for (const addr of reachable) {
        if (this.isLocal(addr)) {
          parts.push(await this.scanLocalReplicas(uniqueDown, normalized));
        } else {
          try {
            parts.push(await this.transport.proxyScanReplica(addr, uniqueDown, normalized));
          } catch {
            // ignore
          }
        }
      }
    }
    void downNodeIds;
    return mergeScan(parts, maxKeys);
  }

  async flush(): Promise<void> {
    for (const shard of this.shards) {
      await shard.store.flush();
    }
    await this.replicas.flush();
  }

  async compact(): Promise<void> {
    for (const shard of this.shards) {
      await shard.store.compact();
    }
    await this.replicas.compact();
  }

  lookup(key: string): { hash: string; preference: VnodeInfo[]; primary: VnodeInfo | null } {
    const hash = hashKey(key);
    const preference = this.prefs(key);
    return { hash, preference, primary: preference[0] ?? null };
  }

  stats(): DLevelDBStats {
    const base = sumStats(this.shards.map((s) => s.store.stats()));
    return {
      ...base,
      dataDir: path.resolve(this.config.dataDir),
      nodeId: this.config.nodeId,
      vnodeCount: this.config.vnodeCount,
      peers: [...this.config.peers],
      configuredReplicas: this.config.replicas,
      effectiveReplicas: this.effectiveN(),
      writeQuorum: this.writeQuorum(),
    };
  }

  status(): DLevelDBStatus {
    return {
      layer: '分布式 LevelDB',
      nodeId: this.config.nodeId,
      address: this.config.advertiseAddr,
      vnodes: this.ring.snapshot().map((s) => ({
        id: s.info.id,
        seq: s.info.seq,
        hash: s.info.hash,
        successor: s.successor,
        predecessor: s.predecessor,
        successorList: s.successorList,
      })),
      peers: [...this.config.peers],
      knownVnodes: this.ring.sortedKnown(),
      physicalNodes: this.ring.physicalNodeCount(),
      configuredReplicas: this.config.replicas,
      effectiveReplicas: this.effectiveN(),
      writeQuorum: this.writeQuorum(),
      usesLegacyLayout: this.usesLegacyLayout,
      repair: { implemented: true, note: REPAIR_NOTE },
    };
  }

  effectiveN(): number {
    return effectiveReplicas(this.config.replicas, this.ring.physicalNodeCount());
  }

  writeQuorum(): number {
    return effectiveWriteQuorum(this.config.writeQuorum, this.effectiveN());
  }

  isSingleStoreFastPath(): boolean {
    return this.config.vnodeCount === 1 && this.config.peers.length === 0 && this.shards.length === 1;
  }

  async scanLocalPrimaries(opts: ScanOptions): Promise<ScanResult> {
    const maxKeys = Math.max(1, opts.maxKeys ?? 1000);
    const parts = await Promise.all(this.shards.map((s) => s.store.list(opts)));
    return mergeScan(parts, maxKeys);
  }

  async scanLocalReplicas(vnodeIds: string[], opts: ScanOptions): Promise<ScanResult> {
    const maxKeys = Math.max(1, opts.maxKeys ?? 1000);
    const parts: ScanResult[] = [];
    for (const id of vnodeIds) {
      const store = await this.replicas.get(id);
      if (!store) {
        continue;
      }
      parts.push(await store.list(opts));
    }
    return mergeScan(parts, maxKeys);
  }

  async putAsPrimary(key: string, value: Buffer, prefs = this.prefs(key)): Promise<void> {
    const primary = prefs[0];
    if (!primary || !this.shardById.has(primary.id)) {
      throw new DLevelDBError('NOT_PRIMARY', `this node is not primary for ${key}`);
    }
    const shard = this.shardById.get(primary.id)!;
    await shard.store.put(key, value);
    const n = this.effectiveN();
    const w = this.writeQuorum();
    if (n <= 1) {
      return;
    }
    await shard.store.flush();
    const followers = prefs.slice(1, n);
    const results = await Promise.allSettled(
      followers.map((vnode) => this.replicatePut(vnode, primary.id, key, value)),
    );
    const acks = 1 + results.filter((r) => r.status === 'fulfilled').length;
    if (acks < w) {
      throw new DLevelDBError('QUORUM_FAILED', `write quorum ${w} not met (acks=${acks}, n=${n})`);
    }
  }

  /** Primary is down. The first live preference node writes the value onto W live copies. */
  async putWhilePrimaryDown(
    key: string,
    value: Buffer,
    prefs = this.prefs(key),
    forwarded = false,
  ): Promise<void> {
    const live: VnodeInfo[] = [];
    for (const vnode of prefs) {
      if (this.isLocal(vnode.address) || (await this.transport.ping(vnode.address))) {
        live.push(vnode);
      }
    }
    const w = this.writeQuorum();
    if (live.length < w) {
      throw new DLevelDBError(
        'QUORUM_FAILED',
        `write quorum ${w} not met while primary is down (live=${live.length})`,
      );
    }
    const coordinator = live[0];
    if (!coordinator) {
      throw new DLevelDBError('PRIMARY_UNAVAILABLE', 'no live replica');
    }
    if (!this.isLocal(coordinator.address)) {
      if (forwarded) {
        throw new DLevelDBError('NOT_PRIMARY', 'not the failover coordinator');
      }
      await this.transport.proxyFailoverPut(coordinator.address, key, value);
      return;
    }
    const owner = prefs[0]?.id;
    if (!owner) {
      throw new DLevelDBError('PRIMARY_UNAVAILABLE', 'empty preference list');
    }
    const results = await Promise.allSettled(live.map((vnode) => this.replicatePut(vnode, owner, key, value)));
    const acks = results.filter((result) => result.status === 'fulfilled').length;
    if (acks < w) {
      throw new DLevelDBError('QUORUM_FAILED', `write quorum ${w} not met while primary is down (acks=${acks})`);
    }
  }

  async deleteAsPrimary(key: string, prefs = this.prefs(key)): Promise<boolean> {
    const primary = prefs[0];
    if (!primary || !this.shardById.has(primary.id)) {
      throw new DLevelDBError('NOT_PRIMARY', `this node is not primary for ${key}`);
    }
    const shard = this.shardById.get(primary.id)!;
    const deleted = await shard.store.delete(key);
    const n = this.effectiveN();
    const w = this.writeQuorum();
    if (n <= 1) {
      return deleted;
    }
    await shard.store.flush();
    const followers = prefs.slice(1, n);
    const results = await Promise.allSettled(
      followers.map((vnode) => this.replicateDelete(vnode, primary.id, key)),
    );
    const acks = 1 + results.filter((r) => r.status === 'fulfilled').length;
    if (acks < w) {
      throw new DLevelDBError('QUORUM_FAILED', `delete quorum ${w} not met (acks=${acks}, n=${n})`);
    }
    return deleted;
  }

  async replicateLocalPut(ownerVnodeId: string, key: string, value: Buffer): Promise<void> {
    const store = await this.replicas.open(ownerVnodeId);
    const existing = await store.get(key);
    if (existing && existing.value.equals(value)) {
      return;
    }
    await store.put(key, value);
    if (this.effectiveN() > 1) {
      await store.flush();
    }
  }

  async replicateLocalDelete(ownerVnodeId: string, key: string): Promise<boolean> {
    const store = await this.replicas.open(ownerVnodeId);
    const deleted = await store.delete(key);
    if (this.effectiveN() > 1) {
      await store.flush();
    }
    return deleted;
  }

  async localPrimaryGet(key: string, vnodeId: string): Promise<KVValue | null> {
    const shard = this.shardById.get(vnodeId);
    if (shard) {
      return shard.store.get(key);
    }
    return null;
  }

  async localPrimaryHead(key: string, vnodeId: string): Promise<KVMeta | null> {
    const shard = this.shardById.get(vnodeId);
    if (shard) {
      return shard.store.head(key);
    }
    return null;
  }

  async localReplicaGet(key: string, ownerVnodeId: string): Promise<KVValue | null> {
    const store = await this.replicas.get(ownerVnodeId);
    return store ? store.get(key) : null;
  }

  async localReplicaHead(key: string, ownerVnodeId: string): Promise<KVMeta | null> {
    const store = await this.replicas.get(ownerVnodeId);
    return store ? store.head(key) : null;
  }

  async localAnyGet(key: string): Promise<KVValue | null> {
    for (const shard of this.shards) {
      const found = await shard.store.get(key);
      if (found) {
        return found;
      }
    }
    await this.replicas.discover();
    for (const id of this.replicas.ids()) {
      const found = await this.localReplicaGet(key, id);
      if (found) {
        return found;
      }
    }
    return null;
  }

  async localAnyHead(key: string): Promise<KVMeta | null> {
    for (const shard of this.shards) {
      const meta = await shard.store.head(key);
      if (meta) {
        return meta;
      }
    }
    await this.replicas.discover();
    for (const id of this.replicas.ids()) {
      const meta = await this.localReplicaHead(key, id);
      if (meta) {
        return meta;
      }
    }
    return null;
  }

  ownsPrimary(vnodeId: string): boolean {
    return this.shardById.has(vnodeId);
  }

  private async readValue(key: string, headOnly: boolean): Promise<KVValue | null> {
    // Look at every local primary shard and replica store first. After the
    // original primary is evicted, this node may have been promoted, but the
    // bytes still live under data/replicas/{old-owner}/, not the new shard.
    if (headOnly) {
      const meta = await this.localAnyHead(key);
      if (meta) {
        return { ...meta, value: Buffer.alloc(0) };
      }
    } else {
      const found = await this.localAnyGet(key);
      if (found) {
        return found;
      }
    }

    const tried = new Set<string>([this.config.advertiseAddr]);
    const seen = new Set<string>();
    const candidates: VnodeInfo[] = [];
    for (const vnode of [...this.prefs(key), ...this.ring.sortedKnown()]) {
      if (seen.has(vnode.address)) {
        continue;
      }
      seen.add(vnode.address);
      candidates.push(vnode);
    }

    for (const vnode of candidates) {
      if (tried.has(vnode.address)) {
        continue;
      }
      tried.add(vnode.address);
      try {
        if (headOnly) {
          const meta = await this.transport.proxyHeadAny(vnode.address, key);
          if (meta) {
            return { ...meta, value: Buffer.alloc(0) };
          }
          continue;
        }
        const found = await this.transport.proxyGetAny(vnode.address, key);
        if (found) {
          return found;
        }
      } catch {
        continue;
      }
    }
    return null;
  }

  private async replicatePut(vnode: VnodeInfo, ownerVnodeId: string, key: string, value: Buffer): Promise<void> {
    if (this.isLocal(vnode.address)) {
      await this.replicateLocalPut(ownerVnodeId, key, value);
      return;
    }
    await this.transport.replicatePut(vnode.address, ownerVnodeId, key, value);
  }

  private async replicateDelete(vnode: VnodeInfo, ownerVnodeId: string, key: string): Promise<void> {
    if (this.isLocal(vnode.address)) {
      await this.replicateLocalDelete(ownerVnodeId, key);
      return;
    }
    await this.transport.replicateDelete(vnode.address, ownerVnodeId, key);
  }

  private prefs(key: string): VnodeInfo[] {
    return this.ring.preferenceList(hashKey(key), this.effectiveN());
  }

  private isLocal(address: string): boolean {
    return address === this.config.advertiseAddr;
  }

  private scanTargets(): string[] {
    const addrs = new Set<string>([this.config.advertiseAddr, ...this.config.peers, ...this.ring.physicalAddresses()]);
    return [...addrs];
  }

  private async joinWithRetry(peers: string[]): Promise<void> {
    if (this.joining) {
      return;
    }
    this.joining = true;
    const delays = [200, 500, 1000, 2000, 2000, 5000];
    let attempt = 0;
    while (!this.closed) {
      for (const peer of peers) {
        try {
          await this.ring.join(peer);
          await this.ring.stabilize();
          void this.repairOnJoin();
          this.joining = false;
          return;
        } catch {
          // try next peer
        }
      }
      if (this.closed) {
        break;
      }
      const delay = delays[Math.min(attempt, delays.length - 1)] ?? 5000;
      attempt += 1;
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }

  /** Copy keys this node is responsible for from any live peer, keeping the newer mtime. */
  async repairOnJoin(): Promise<void> {
    if (this.repairing || this.closed) {
      return;
    }
    this.repairing = true;
    try {
      const vnodeIds = this.ring.sortedKnown().map((vnode) => vnode.id);
      for (const addr of this.scanTargets()) {
        if (this.isLocal(addr) || !(await this.transport.ping(addr))) {
          continue;
        }
        try {
          await this.repairFrom(addr, vnodeIds);
        } catch {
          // This peer can be down. Another live peer still has the copies.
        }
      }
    } finally {
      this.repairing = false;
    }
  }

  private repairing = false;

  private primaryUnreachable(err: unknown): boolean {
    return err instanceof DLevelDBError && (err.code === 'PRIMARY_UNAVAILABLE' || err.code === 'TIMEOUT');
  }

  private async repairFrom(addr: string, vnodeIds: string[]): Promise<void> {
    const pages = [
      await this.transport.proxyScan(addr, { maxKeys: 10_000 }),
      vnodeIds.length > 0 ? await this.transport.proxyScanReplica(addr, vnodeIds, { maxKeys: 10_000 }) : { objects: [], truncated: false },
    ];
    for (const page of pages) {
      for (const entry of page.objects) {
        const prefs = this.prefs(entry.key);
        const owner = prefs[0];
        if (!owner || !prefs.some((vnode) => this.isLocal(vnode.address))) {
          continue;
        }
        const remote = await this.transport.proxyGetAny(addr, entry.key);
        if (!remote) {
          continue;
        }
        const local = await this.localAnyGet(entry.key);
        if (local && local.mtimeMs >= remote.mtimeMs) {
          continue;
        }
        if (this.ownsPrimary(owner.id)) {
          const shard = this.shardById.get(owner.id);
          if (shard) {
            await shard.store.put(entry.key, remote.value, remote.mtimeMs);
          }
        } else {
          await this.replicateLocalPut(owner.id, entry.key, remote.value);
        }
      }
    }
  }
}
