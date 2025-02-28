import { Module } from '@nestjs/common';
import { MigrationController } from './migration.controller';
import { MigrationService } from './migration.service';

@Module({
  imports: [],
  controllers: [MigrationController],
  providers: [MigrationService],
})
export class AppModule {}