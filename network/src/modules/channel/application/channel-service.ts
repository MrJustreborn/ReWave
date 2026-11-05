import { NotImplementedError } from "../../../shared/index.js";
import type { ChannelId, Clock } from "../../../shared/index.js";
import type { Channel, ChannelDefinition } from "../domain/channel.js";
import type { ChannelRepository } from "../domain/channel-repository.js";
import type { ChannelRuntime } from "../domain/channel-runtime.js";

export class ChannelService {
  constructor(
    private readonly channels: ChannelRepository,
    private readonly runtime: ChannelRuntime,
    private readonly clock: Clock,
  ) {}

  async listChannels(): Promise<Channel[]> {
    throw new NotImplementedError("listChannels is not implemented yet");
  }

  async createChannel(definition: ChannelDefinition): Promise<Channel> {
    throw new NotImplementedError("createChannel is not implemented yet");
  }

  async getChannel(id: ChannelId): Promise<Channel> {
    throw new NotImplementedError("getChannel is not implemented yet");
  }

  async replaceChannel(id: ChannelId, definition: ChannelDefinition): Promise<Channel> {
    throw new NotImplementedError("replaceChannel is not implemented yet");
  }

  async deleteChannel(id: ChannelId): Promise<void> {
    throw new NotImplementedError("deleteChannel is not implemented yet");
  }

  async startChannel(id: ChannelId): Promise<Channel> {
    throw new NotImplementedError("startChannel is not implemented yet");
  }

  async stopChannel(id: ChannelId): Promise<Channel> {
    throw new NotImplementedError("stopChannel is not implemented yet");
  }
}
