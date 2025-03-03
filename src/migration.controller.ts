import { Controller, Get, Post } from '@nestjs/common';
import { MigrationService } from './migration.service';

@Controller('migration')
export class MigrationController {
  constructor(private readonly migrationService: MigrationService) {}

  @Post('mysql-to-postgres')
  async migrateMySQLToPostgres() {
    return await this.migrationService.migrateData();
  }
}