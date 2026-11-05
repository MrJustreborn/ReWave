import { ValidationError, nonEmptyString, nonEmptyStringArray } from "../../../shared/index.js";

export type Weekday = "monday" | "tuesday" | "wednesday" | "thursday" | "friday" | "saturday" | "sunday";

const WEEKDAYS: readonly Weekday[] = [
  "monday",
  "tuesday",
  "wednesday",
  "thursday",
  "friday",
  "saturday",
  "sunday",
];

export type WallClockTime = string & { readonly __brand: "WallClockTime" };

const WALL_CLOCK_PATTERN = /^([01][0-9]|2[0-3]):[0-5][0-9]$/;
const TEMPLATE_NAME_PATTERN = /^[A-Za-z0-9_-]+$/;

export interface SlotJson {
  start: string;
  end: string;
  tags: string[];
}

export interface ScheduleJson {
  templates: { [name: string]: SlotJson[] };
  days: { [day: string]: string };
}

function wallClockTime(value: unknown, field: string): WallClockTime {
  if (typeof value !== "string" || !WALL_CLOCK_PATTERN.test(value)) {
    throw new ValidationError(`${field} must be an HH:MM wall-clock time, got "${String(value)}"`);
  }
  return value as WallClockTime;
}

export class Slot {
  private constructor(
    readonly start: WallClockTime,
    readonly end: WallClockTime,
    readonly tags: readonly string[],
  ) {}

  static fromJson(raw: unknown): Slot {
    if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
      throw new ValidationError("schedule slot must be an object");
    }
    const record = raw as Record<string, unknown>;
    return new Slot(
      wallClockTime(record["start"], "slot.start"),
      wallClockTime(record["end"], "slot.end"),
      Object.freeze(nonEmptyStringArray(record["tags"], "slot.tags")),
    );
  }

  toJson(): SlotJson {
    return { start: this.start, end: this.end, tags: [...this.tags] };
  }
}

export class Schedule {
  private constructor(
    readonly templates: ReadonlyMap<string, readonly Slot[]>,
    readonly days: ReadonlyMap<Weekday, string>,
  ) {}

  static empty(): Schedule {
    return new Schedule(new Map(), new Map());
  }

  static fromJson(raw: unknown): Schedule {
    if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
      throw new ValidationError("schedule must be an object");
    }
    const record = raw as Record<string, unknown>;
    const templates = parseTemplates(record["templates"]);
    const days = parseDays(record["days"], templates);
    return new Schedule(templates, days);
  }

  toJson(): ScheduleJson {
    const templates: { [name: string]: SlotJson[] } = {};
    for (const [name, slots] of this.templates) {
      templates[name] = slots.map((slot) => slot.toJson());
    }
    const days: { [day: string]: string } = {};
    for (const [day, name] of this.days) {
      days[day] = name;
    }
    return { templates, days };
  }
}

function parseTemplates(raw: unknown): Map<string, readonly Slot[]> {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    throw new ValidationError("schedule.templates must be an object");
  }
  const templates = new Map<string, readonly Slot[]>();
  for (const [name, slots] of Object.entries(raw)) {
    if (!TEMPLATE_NAME_PATTERN.test(name)) {
      throw new ValidationError(`schedule template name "${name}" must match ${TEMPLATE_NAME_PATTERN.source}`);
    }
    if (!Array.isArray(slots) || slots.length === 0) {
      throw new ValidationError(`schedule template "${name}" must contain at least one slot`);
    }
    templates.set(name, Object.freeze(slots.map((slot) => Slot.fromJson(slot))));
  }
  return templates;
}

function parseDays(raw: unknown, templates: ReadonlyMap<string, readonly Slot[]>): Map<Weekday, string> {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    throw new ValidationError("schedule.days must be an object");
  }
  const days = new Map<Weekday, string>();
  for (const [day, template] of Object.entries(raw)) {
    if (!(WEEKDAYS as readonly string[]).includes(day)) {
      throw new ValidationError(`schedule day "${day}" is not a weekday`);
    }
    const name = nonEmptyString(template, `schedule.days.${day}`);
    if (!templates.has(name)) {
      throw new ValidationError(`schedule.days.${day} references unknown template "${name}"`);
    }
    days.set(day as Weekday, name);
  }
  return days;
}
