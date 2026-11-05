import Fastify, { type FastifyInstance } from "fastify";
import cors from "@fastify/cors";
import type { Config } from "../config.js";
import { createAuthHook } from "./auth.js";
import { setErrorMapping } from "./error-mapper.js";

export interface ServerModule {
  registerRoutes(app: FastifyInstance): void;
}

export async function buildServer(config: Config, modules: readonly ServerModule[]): Promise<FastifyInstance> {
  const app = Fastify({ logger: { level: config.logLevel }, forceCloseConnections: true });
  await app.register(cors);
  setErrorMapping(app);
  app.addHook("preHandler", createAuthHook(config));
  app.get("/healthz", async () => ({ status: "ok" }));
  for (const module of modules) {
    module.registerRoutes(app);
  }
  return app;
}
