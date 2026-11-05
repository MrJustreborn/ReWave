import { ValidationError, nonEmptyString } from "../../../shared/index.js";
import type { ChannelId } from "../../../shared/index.js";
import type { ContentPool } from "./content-pool.js";
import type { Schedule } from "./schedule.js";

export type ChannelStatus = "created" | "running" | "stopped" | "error";

const CHANNEL_STATUSES: readonly ChannelStatus[] = ["created", "running", "stopped", "error"];

export function isChannelStatus(value: unknown): value is ChannelStatus {
  return typeof value === "string" && (CHANNEL_STATUSES as readonly string[]).includes(value);
}

export interface ChannelDefinition {
  readonly name: string;
  readonly type: string;
  readonly contentPool: ContentPool;
  readonly schedule: Schedule;
}

export function createChannelDefinition(
  name: string,
  type: string,
  contentPool: ContentPool,
  schedule: Schedule,
): ChannelDefinition {
  return {
    name: nonEmptyString(name, "channel name"),
    type: nonEmptyString(type, "channel type"),
    contentPool,
    schedule,
  };
}

export interface ChannelCreation {
  readonly id: ChannelId;
  readonly definition: ChannelDefinition;
  readonly createdAt: Date;
}

export interface ChannelSnapshot {
  readonly id: ChannelId;
  readonly definition: ChannelDefinition;
  readonly status: ChannelStatus;
  readonly createdAt: Date;
}

export class Channel {
  private constructor(
    readonly id: ChannelId,
    readonly definition: ChannelDefinition,
    readonly status: ChannelStatus,
    readonly createdAt: Date,
  ) {}

  get name(): string {
    return this.definition.name;
  }

  get type(): string {
    return this.definition.type;
  }

  get contentPool(): ContentPool {
    return this.definition.contentPool;
  }

  get schedule(): Schedule {
    return this.definition.schedule;
  }

  static create(creation: ChannelCreation): Channel {
    return new Channel(creation.id, creation.definition, "created", creation.createdAt);
  }

  static reconstitute(snapshot: ChannelSnapshot): Channel {
    if (!isChannelStatus(snapshot.status)) {
      throw new ValidationError(`invalid channel status "${String(snapshot.status)}"`);
    }
    return new Channel(snapshot.id, snapshot.definition, snapshot.status, snapshot.createdAt);
  }

  withDefinition(definition: ChannelDefinition): Channel {
    return new Channel(this.id, definition, this.status, this.createdAt);
  }

  start(): Channel {
    return this.status === "running" ? this : new Channel(this.id, this.definition, "running", this.createdAt);
  }

  stop(): Channel {
    return this.status === "running"
      ? new Channel(this.id, this.definition, "stopped", this.createdAt)
      : this;
  }
}
