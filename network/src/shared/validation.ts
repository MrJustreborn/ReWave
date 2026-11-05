import { ValidationError } from "./errors.js";

export function nonEmptyString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new ValidationError(`${field} must be a non-empty string`);
  }
  return value;
}

export function nonNegativeNumber(value: unknown, field: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new ValidationError(`${field} must be a finite number >= 0`);
  }
  return value;
}

export function nonEmptyStringArray(value: unknown, field: string): string[] {
  if (!Array.isArray(value)) {
    throw new ValidationError(`${field} must be an array of strings`);
  }
  return value.map((entry) => nonEmptyString(entry, field));
}
