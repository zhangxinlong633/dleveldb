import { Injectable, type NestMiddleware } from '@nestjs/common';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { dispatchRpc, type BufferedRequest } from 'dleveldb';
import { KvService } from './kv.service';

@Injectable()
export class KvMiddleware implements NestMiddleware {
  constructor(private readonly kv: KvService) {}

  async use(req: IncomingMessage, res: ServerResponse, next: () => void): Promise<void> {
    const handled = await dispatchRpc(this.kv.kv(), req as BufferedRequest, res);
    if (!handled) {
      next();
    }
  }
}
