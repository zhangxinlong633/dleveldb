import fs from 'node:fs';
import path from 'node:path';
import { crc32 } from 'node:zlib';
import { ClassicLevel } from 'classic-level';
import type { KVEntry, KVMeta, KVValue, ScanOptions, ScanResult } from './types.js';

const MAGIC = Buffer.from('DLKV');
const HEADER_SIZE = 4 + 4 + 8 + 4;

export interface LevelStoreStats {
  objects: number;
  deleted: number;
  bytesLogical: number;
  bytesOnDisk: number;
  blockFiles: number;
  puts: number;
  gets: number;
  deletes: number;
  flushes: number;
  fsyncs: number;
  indexSlots: number;
}

export function isLevelDbDir(dir: string): boolean {
  return fs.existsSync(path.join(dir, 'CURRENT')) || fs.existsSync(path.join(dir, 'LOCK'));
}

function isNotFound(err: unknown): boolean {
  if (!err || typeof err !== 'object') {
    return false;
  }
  const code = (err as { code?: string }).code;
  return code === 'LEVEL_NOT_FOUND' || code === 'NOT_FOUND';
}

function encodeRecord(value: Buffer, mtimeMs = Date.now()): Buffer {
  const rec = Buffer.alloc(HEADER_SIZE + value.length);
  MAGIC.copy(rec, 0);
  rec.writeUInt32LE(crc32(value) >>> 0, 4);
  rec.writeBigUInt64LE(BigInt(mtimeMs), 8);
  rec.writeUInt32LE(value.length, 16);
  value.copy(rec, HEADER_SIZE);
  return rec;
}

function decodeRecord(raw: Buffer): KVValue {
  if (raw.length >= HEADER_SIZE && raw.subarray(0, 4).equals(MAGIC)) {
    const crc = raw.readUInt32LE(4);
    const mtimeMs = Number(raw.readBigUInt64LE(8));
    const size = raw.readUInt32LE(16);
    const end = Math.min(raw.length, HEADER_SIZE + size);
    const value = raw.subarray(HEADER_SIZE, end);
    return { value, size: value.length, crc, mtimeMs };
  }
  return { value: raw, size: raw.length, crc: crc32(raw) >>> 0, mtimeMs: 0 };
}

function dirSize(dir: string): number {
  let total = 0;
  if (!fs.existsSync(dir)) {
    return 0;
  }
  const stack = [dir];
  while (stack.length > 0) {
    const current = stack.pop()!;
    let entries: fs.Dirent[];
    try {
      entries = fs.readdirSync(current, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const entry of entries) {
      const full = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(full);
      } else if (entry.isFile()) {
        try {
          total += fs.statSync(full).size;
        } catch {
          // ignore
        }
      }
    }
  }
  return total;
}

export class LevelStore {
  private puts = 0;
  private gets = 0;
  private deletes = 0;
  private flushes = 0;
  private fsyncs = 0;
  private objects = 0;
  private deleted = 0;
  private bytesLogical = 0;

  private constructor(
    readonly dataDir: string,
    private readonly db: ClassicLevel<string, Buffer>,
    private readonly sync: boolean,
  ) {}

  static async open(dataDir: string, opts: { sync?: boolean } = {}): Promise<LevelStore> {
    fs.mkdirSync(dataDir, { recursive: true });
    const db = new ClassicLevel<string, Buffer>(dataDir, {
      keyEncoding: 'utf8',
      valueEncoding: 'buffer',
    });
    await db.open();
    return new LevelStore(dataDir, db, opts.sync ?? false);
  }

  async put(key: string, value: Buffer): Promise<void> {
    const existing = await this.peek(key);
    await this.db.put(key, encodeRecord(value), { sync: this.sync });
    this.puts += 1;
    if (this.sync) {
      this.fsyncs += 1;
    }
    if (existing) {
      this.bytesLogical += value.length - existing.size;
    } else {
      this.objects += 1;
      this.bytesLogical += value.length;
    }
  }

  async get(key: string): Promise<KVValue | null> {
    const found = await this.peek(key);
    if (found) {
      this.gets += 1;
    }
    return found;
  }

  async head(key: string): Promise<KVMeta | null> {
    const found = await this.get(key);
    if (!found) {
      return null;
    }
    return { size: found.size, crc: found.crc, mtimeMs: found.mtimeMs };
  }

  async delete(key: string): Promise<boolean> {
    const existing = await this.peek(key);
    if (!existing) {
      return false;
    }
    await this.db.del(key, { sync: this.sync });
    this.deletes += 1;
    this.deleted += 1;
    this.objects = Math.max(0, this.objects - 1);
    this.bytesLogical = Math.max(0, this.bytesLogical - existing.size);
    if (this.sync) {
      this.fsyncs += 1;
    }
    return true;
  }

  async list(opts: ScanOptions = {}): Promise<ScanResult> {
    const prefix = opts.prefix ?? '';
    const startAfter = opts.startAfter;
    const maxKeys = Math.max(1, opts.maxKeys ?? 1000);
    const objects: KVEntry[] = [];
    let truncated = false;
    const iterator = this.db.iterator({
      gte: startAfter && startAfter >= prefix ? startAfter : prefix,
      keys: true,
      values: true,
    });
    try {
      for await (const [key, raw] of iterator) {
        if (prefix && !key.startsWith(prefix)) {
          break;
        }
        if (startAfter && key <= startAfter) {
          continue;
        }
        if (objects.length >= maxKeys) {
          truncated = true;
          break;
        }
        if (raw === undefined) {
          continue;
        }
        const rec = decodeRecord(raw);
        objects.push({ key, size: rec.size, crc: rec.crc, mtimeMs: rec.mtimeMs });
      }
    } finally {
      await iterator.close();
    }
    return { objects, truncated };
  }

  async flush(): Promise<void> {
    this.flushes += 1;
    if (this.sync) {
      return;
    }
    // LevelDB has no explicit flush; a sync no-op write persists the memtable WAL.
    const tick = `__dleveldb_flush__:${Date.now()}:${Math.random()}`;
    await this.db.put(tick, Buffer.alloc(0), { sync: true });
    await this.db.del(tick, { sync: true });
    this.fsyncs += 1;
  }

  async compact(): Promise<void> {
    let first: string | undefined;
    let last: string | undefined;
    for await (const key of this.db.keys()) {
      if (first === undefined) {
        first = key;
      }
      last = key;
    }
    if (first !== undefined && last !== undefined) {
      await this.db.compactRange(first, last);
    }
  }

  async close(): Promise<void> {
    if (this.db.status !== 'closed') {
      await this.db.close();
    }
  }

  stats(): LevelStoreStats {
    return {
      objects: this.objects,
      deleted: this.deleted,
      bytesLogical: this.bytesLogical,
      bytesOnDisk: dirSize(this.dataDir),
      blockFiles: 0,
      puts: this.puts,
      gets: this.gets,
      deletes: this.deletes,
      flushes: this.flushes,
      fsyncs: this.fsyncs,
      indexSlots: this.objects,
    };
  }

  private async peek(key: string): Promise<KVValue | null> {
    try {
      const raw = await this.db.get(key);
      if (raw === undefined) {
        return null;
      }
      return decodeRecord(raw);
    } catch (err) {
      if (isNotFound(err)) {
        return null;
      }
      throw err;
    }
  }
}
