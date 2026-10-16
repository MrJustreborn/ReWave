import { DatabaseSync } from "node:sqlite";

export type SqliteDatabase = DatabaseSync;

export function openDatabase(dbPath: string): SqliteDatabase {
  const db = new DatabaseSync(dbPath);
  db.exec("PRAGMA journal_mode = WAL");
  db.exec("PRAGMA foreign_keys = ON");
  return db;
}
