import Docker from "dockerode";
import type { Config } from "../config.js";

export function createDockerClient(config: Config): Docker {
  return new Docker({ socketPath: config.dockerSocket });
}
