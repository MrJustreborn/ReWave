import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, readdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { loadConfig } from "../config.js";
import { openDatabase, type SqliteDatabase } from "./connection.js";

const MIGRATION_FILE_PATTERN = /^\d{4}_[a-z0-9_]+\.sql$/;

export interface MigrationStatusEntry {
  name: string;
  state: "applied" | "pending" | "missing";
  appliedAt: string | null;
}

function migrationsDir(): string {
  return fileURLToPath(new URL("./migrations", import.meta.url));
}

function checksumOf(sql: string): string {
  return createHash("sha256").update(sql).digest("hex");
}

function loadMigrationFiles(): string[] {
  const files = readdirSync(migrationsDir())
    .filter((file) => file.endsWith(".sql"))
    .sort();
  for (const file of files) {
    if (!MIGRATION_FILE_PATTERN.test(file)) {
      throw new Error(`Migration file "${file}" violates the naming convention NNNN_description.sql`);
    }
  }
  return files;
}

function readMigration(name: string): string {
  return readFileSync(path.join(migrationsDir(), name), "utf8");
}

function ensureHistoryTable(db: SqliteDatabase): void {
  db.exec(
    "CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY, checksum TEXT NOT NULL, applied_at TEXT NOT NULL DEFAULT (datetime('now')))",
  );
  try {
    db.prepare("SELECT checksum FROM schema_migrations LIMIT 0").all();
    return;
  } catch {
    db.exec("ALTER TABLE schema_migrations ADD COLUMN checksum TEXT");
  }
  const update = db.prepare("UPDATE schema_migrations SET checksum = ? WHERE name = ?");
  const rows = db.prepare("SELECT name FROM schema_migrations").all() as { name: string }[];
  for (const row of rows) {
    const filePath = path.join(migrationsDir(), row.name);
    if (existsSync(filePath)) {
      update.run(checksumOf(readFileSync(filePath, "utf8")), row.name);
    }
  }
}

function validateMigrations(db: SqliteDatabase, files: string[]): void {
  const rows = db
    .prepare("SELECT name, checksum FROM schema_migrations")
    .all() as { name: string; checksum: string | null }[];
  const fileSet = new Set(files);
  for (const row of rows) {
    if (!fileSet.has(row.name)) {
      throw new Error(`Applied migration "${row.name}" no longer exists — restore it instead of deleting or renaming`);
    }
  }
  const rowsByName = new Map(rows.map((row) => [row.name, row]));
  for (const file of files) {
    const row = rowsByName.get(file);
    if (row === undefined || row.checksum === null) {
      continue;
    }
    if (row.checksum !== checksumOf(readMigration(file))) {
      throw new Error(
        `Migration "${file}" changed since it was applied (checksum mismatch) — restore the original or add a new migration`,
      );
    }
  }
  const appliedNames = rows.map((row) => row.name).sort();
  const lastApplied = appliedNames[appliedNames.length - 1];
  if (lastApplied !== undefined) {
    for (const file of files) {
      if (rowsByName.get(file) === undefined && file < lastApplied) {
        throw new Error(
          `Out-of-order migration "${file}" — it sorts before already-applied "${lastApplied}"; apply migrations in order`,
        );
      }
    }
  }
}

export function applyMigrations(db: SqliteDatabase): string[] {
  ensureHistoryTable(db);
  const files = loadMigrationFiles();
  validateMigrations(db, files);
  const applied = new Set(
    (db.prepare("SELECT name FROM schema_migrations").all() as { name: string }[]).map((row) => row.name),
  );
  const insert = db.prepare("INSERT INTO schema_migrations (name, checksum) VALUES (?, ?)");
  const newlyApplied: string[] = [];
  for (const file of files) {
    if (applied.has(file)) {
      continue;
    }
    const sql = readMigration(file);
    db.exec("BEGIN");
    try {
      db.exec(sql);
      insert.run(file, checksumOf(sql));
      db.exec("COMMIT");
    } catch (error) {
      db.exec("ROLLBACK");
      throw error;
    }
    newlyApplied.push(file);
  }
  return newlyApplied;
}

export function migrationStatus(db: SqliteDatabase): MigrationStatusEntry[] {
  ensureHistoryTable(db);
  const files = loadMigrationFiles();
  const fileSet = new Set(files);
  const rows = db
    .prepare("SELECT name, applied_at FROM schema_migrations")
    .all() as { name: string; applied_at: string }[];
  const rowsByName = new Map(rows.map((row) => [row.name, row]));
  const entries: MigrationStatusEntry[] = files.map((file) => {
    const row = rowsByName.get(file);
    return row === undefined
      ? { name: file, state: "pending", appliedAt: null }
      : { name: file, state: "applied", appliedAt: row.applied_at };
  });
  for (const row of rows) {
    if (!fileSet.has(row.name)) {
      entries.push({ name: row.name, state: "missing", appliedAt: row.applied_at });
    }
  }
  return entries.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const command = process.argv[2] ?? "apply";
  if (command !== "apply" && command !== "status") {
    console.error("Usage: npm run migrate [-- status]");
    process.exit(1);
  }
  const config = loadConfig();
  mkdirSync(path.dirname(config.dbPath), { recursive: true });
  const db = openDatabase(config.dbPath);
  if (command === "status") {
    for (const entry of migrationStatus(db)) {
      const appliedAt = entry.appliedAt === null ? "" : ` (applied ${entry.appliedAt})`;
      console.log(`${entry.state.padEnd(7)} ${entry.name}${appliedAt}`);
    }
  } else {
    const newlyApplied = applyMigrations(db);
    console.log(newlyApplied.length > 0 ? `Applied: ${newlyApplied.join(", ")}` : "Nothing to apply");
  }
  db.close();
}
