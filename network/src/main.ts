import { mkdirSync } from "node:fs";
import path from "node:path";
import { buildApp } from "./app.js";
import { loadConfig } from "./config.js";
import { openDatabase } from "./db/connection.js";
import { applyMigrations } from "./db/migrate.js";

async function main(): Promise<void> {
  const config = loadConfig();
  const app = await buildApp(config);
  mkdirSync(path.dirname(config.dbPath), { recursive: true });
  const db = openDatabase(config.dbPath);
  const applied = applyMigrations(db);
  db.close();
  if (applied.length > 0) {
    app.log.info({ migrations: applied }, "Applied database migrations");
  }
  await app.listen({ port: config.port, host: config.host });
  for (const signal of ["SIGINT", "SIGTERM"] as const) {
    process.once(signal, () => {
      void app.close().then(
        () => process.exit(0),
        () => process.exit(1),
      );
    });
  }
}

try {
  await main();
} catch (error) {
  console.error(error);
  process.exit(1);
}
