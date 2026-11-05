import { ValidationError } from "./errors.js";

declare const brand: unique symbol;

export type Brand<T, B extends string> = T & { readonly [brand]: B };

export type ChannelId = Brand<string, "ChannelId">;
export type VideoId = Brand<string, "VideoId">;

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function brandedUuid<T extends Brand<string, string>>(value: string, kind: string): T {
  if (!UUID_PATTERN.test(value)) {
    throw new ValidationError(`${kind} must be a UUID, got "${value}"`);
  }
  return value as T;
}

export function channelId(value: string): ChannelId {
  return brandedUuid(value, "channelId");
}

export function videoId(value: string): VideoId {
  return brandedUuid(value, "videoId");
}
