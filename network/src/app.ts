import Fastify, { type FastifyInstance } from "fastify";
import cors from "@fastify/cors";
import { createAuthHook } from "./auth.js";
import type { Config } from "./config.js";
import { registerChannelRoutes } from "./routes/channels.js";
import { registerVideoRoutes } from "./routes/videos.js";

export async function buildApp(config: Config): Promise<FastifyInstance> {
  const app = Fastify({ logger: true, forceCloseConnections: true });
  await app.register(cors);
  app.addHook("preHandler", createAuthHook(config));
  app.get("/healthz", async () => ({ status: "ok" }));
  registerChannelRoutes(app);
  registerVideoRoutes(app);
  return app;
}
