import { ValidationError } from "./errors.js";

const SQLITE_TIMESTAMP_PATTERN = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/;

export function parseSqliteTimestamp(value: string, field: string): Date {
  if (!SQLITE_TIMESTAMP_PATTERN.test(value)) {
    throw new ValidationError(`${field} is not a SQLite UTC timestamp, got "${value}"`);
  }
  return new Date(`${value.replace(" ", "T")}Z`);
}

export function formatSqliteTimestamp(value: Date): string {
  return value.toISOString().slice(0, 19).replace("T", " ");
}
