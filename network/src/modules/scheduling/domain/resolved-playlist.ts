import { ValidationError, nonEmptyString, nonNegativeNumber } from "../../../shared/index.js";

export type StreamEntryType = "preroll" | "content" | "ad";

const STREAM_ENTRY_TYPES: readonly StreamEntryType[] = ["preroll", "content", "ad"];

export type TimecodeOffset = string & { readonly __brand: "TimecodeOffset" };

const TIMECODE_PATTERN = /^[0-9]{2}:[0-5][0-9]:[0-5][0-9]$/;

function timecodeOffset(value: unknown, field: string): TimecodeOffset {
  if (typeof value !== "string" || !TIMECODE_PATTERN.test(value)) {
    throw new ValidationError(`${field} must be an HH:MM:SS timecode, got "${String(value)}"`);
  }
  return value as TimecodeOffset;
}

export interface StreamEntryInput {
  readonly file: string;
  readonly type: StreamEntryType;
  readonly duration: number;
  readonly startOffset?: string;
  readonly endOffset?: string;
}

export class StreamEntry {
  private constructor(
    readonly file: string,
    readonly type: StreamEntryType,
    readonly duration: number,
    readonly startOffset: TimecodeOffset | undefined,
    readonly endOffset: TimecodeOffset | undefined,
  ) {}

  static create(input: StreamEntryInput): StreamEntry {
    if (!(STREAM_ENTRY_TYPES as readonly string[]).includes(input.type)) {
      throw new ValidationError(`stream entry type must be one of ${STREAM_ENTRY_TYPES.join(", ")}`);
    }
    return new StreamEntry(
      nonEmptyString(input.file, "stream entry file"),
      input.type,
      nonNegativeNumber(input.duration, "stream entry duration"),
      input.startOffset === undefined ? undefined : timecodeOffset(input.startOffset, "stream entry start"),
      input.endOffset === undefined ? undefined : timecodeOffset(input.endOffset, "stream entry end"),
    );
  }
}

export class ResolvedSlot {
  private constructor(
    readonly start: Date,
    readonly end: Date,
    readonly stream: readonly StreamEntry[],
  ) {}

  static create(start: Date, end: Date, stream: readonly StreamEntry[]): ResolvedSlot {
    if (stream.length === 0) {
      throw new ValidationError("resolved slot must contain at least one stream entry");
    }
    if (end.getTime() <= start.getTime()) {
      throw new ValidationError("resolved slot end must be after its start");
    }
    return new ResolvedSlot(start, end, Object.freeze([...stream]));
  }
}

export class ResolvedPlaylist {
  private constructor(readonly slots: readonly ResolvedSlot[]) {}

  static create(slots: readonly ResolvedSlot[]): ResolvedPlaylist {
    return new ResolvedPlaylist(Object.freeze([...slots]));
  }
}
