import { mkdirSync, readFileSync, readdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { loadConfig } from "../config.js";
import { openDatabase, type SqliteDatabase } from "./connection.js";

export function applyMigrations(db: SqliteDatabase): string[] {
  db.exec(
    "CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY, applied_at TEXT NOT NULL DEFAULT (datetime('now')))",
  );
  const migrationsDir = fileURLToPath(new URL("./migrations", import.meta.url));
  const files = readdirSync(migrationsDir)
    .filter((file) => file.endsWith(".sql"))
    .sort();
  const appliedRows = db.prepare("SELECT name FROM schema_migrations").all() as { name: string }[];
  const applied = new Set(appliedRows.map((row) => row.name));
  const newlyApplied: string[] = [];
  for (const file of files) {
    if (applied.has(file)) {
      continue;
    }
    const sql = readFileSync(path.join(migrationsDir, file), "utf8");
    db.exec("BEGIN");
    try {
      db.exec(sql);
      db.prepare("INSERT INTO schema_migrations (name) VALUES (?)").run(file);
      db.exec("COMMIT");
    } catch (error) {
      db.exec("ROLLBACK");
      throw error;
    }
    newlyApplied.push(file);
  }
  return newlyApplied;
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const config = loadConfig();
  mkdirSync(path.dirname(config.dbPath), { recursive: true });
  const db = openDatabase(config.dbPath);
  const newlyApplied = applyMigrations(db);
  db.close();
  console.log(newlyApplied.length > 0 ? `Applied: ${newlyApplied.join(", ")}` : "Nothing to apply");
}
