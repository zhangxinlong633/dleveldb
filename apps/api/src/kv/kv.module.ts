import { Global, MiddlewareConsumer, Module, NestModule } from '@nestjs/common';
import { KvMiddleware } from './kv.middleware';
import { KvService } from './kv.service';

@Global()
@Module({
  providers: [KvService],
  exports: [KvService],
})
export class KvModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    consumer.apply(KvMiddleware).forRoutes('*');
  }
}
