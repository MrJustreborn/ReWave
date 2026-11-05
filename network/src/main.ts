import { mkdirSync } from "node:fs";
import path from "node:path";
import { loadConfig } from "./config.js";
import { openDatabase } from "./db/connection.js";
import { applyMigrations } from "./db/migrate.js";
import { SystemClock } from "./shared/index.js";
import { createMediaModule } from "./modules/media/index.js";
import { createChannelModule } from "./modules/channel/index.js";
import { createSchedulingModule } from "./modules/scheduling/index.js";
import { createWatchingModule } from "./modules/watching/index.js";
import { buildServer, type ServerModule } from "./server/app.js";

async function main(): Promise<void> {
  const config = loadConfig();
  mkdirSync(path.dirname(config.dbPath), { recursive: true });
  const db = openDatabase(config.dbPath);
  const applied = applyMigrations(db);

  const clock = new SystemClock();
  const modules: ServerModule[] = [
    createMediaModule({ db }),
    createChannelModule({ db, dockerSocket: config.dockerSocket, clock }),
    createSchedulingModule(),
    createWatchingModule({ clock, keepAliveSeconds: config.watchKeepAliveSeconds }),
  ];

  const app = await buildServer(config, modules);
  if (applied.length > 0) {
    app.log.info({ migrations: applied }, "Applied database migrations");
  }
  await app.listen({ port: config.port, host: config.host });

  for (const signal of ["SIGINT", "SIGTERM"] as const) {
    process.once(signal, () => {
      void app.close().then(
        () => {
          db.close();
          process.exit(0);
        },
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
