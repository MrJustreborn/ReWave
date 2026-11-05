import Docker from "dockerode";
import { NotImplementedError } from "../../../../shared/index.js";
import type { Channel } from "../../domain/channel.js";
import type { ChannelRuntime } from "../../domain/channel-runtime.js";

export class DockerChannelRuntime implements ChannelRuntime {
  private readonly docker: Docker;

  constructor(socketPath: string) {
    this.docker = new Docker({ socketPath });
  }

  async start(channel: Channel): Promise<void> {
    throw new NotImplementedError(`starting the container of channel "${channel.name}" is not implemented yet`);
  }

  async stop(channel: Channel): Promise<void> {
    throw new NotImplementedError(`stopping the container of channel "${channel.name}" is not implemented yet`);
  }
}
