import type { FastifyReply, FastifyRequest } from "fastify";
import { createRemoteJWKSet, jwtVerify } from "jose";
import type { Config } from "./config.js";

const SCOPES_BY_OPERATION: Readonly<Record<string, string>> = {
  listChannels: "channels:read",
  createChannel: "channels:write",
  getChannel: "channels:read",
  replaceChannel: "channels:write",
  deleteChannel: "channels:write",
  startChannel: "channels:write",
  stopChannel: "channels:write",
  watchChannel: "watch",
  getChannelPlaylist: "channels:read",
  listVideos: "videos:read",
  addVideo: "videos:write",
  getVideo: "videos:read",
  deleteVideo: "videos:write",
};

function tokenScopes(scope: unknown): string[] {
  if (typeof scope === "string") {
    return scope.split(" ");
  }
  if (Array.isArray(scope)) {
    return scope.filter((entry): entry is string => typeof entry === "string");
  }
  return [];
}

export function createAuthHook(config: Config) {
  let jwks: ReturnType<typeof createRemoteJWKSet> | undefined;

  return async (request: FastifyRequest, reply: FastifyReply): Promise<void> => {
    if (config.authMode === "off") {
      return;
    }
    const routeConfig = request.routeOptions.config as { operationId?: string } | undefined;
    if (routeConfig?.operationId === undefined) {
      return;
    }
    const requiredScope = SCOPES_BY_OPERATION[routeConfig.operationId];
    if (requiredScope === undefined) {
      reply.code(403).send({ code: 403, message: `No scope configured for ${routeConfig.operationId}` });
      return;
    }
    const header = request.headers.authorization;
    if (header === undefined || !header.startsWith("Bearer ")) {
      reply.header("WWW-Authenticate", 'Bearer realm="rewave-network"');
      reply.code(401).send({ code: 401, message: "Missing bearer token" });
      return;
    }
    jwks ??= createRemoteJWKSet(new URL(config.jwksUrl as string));
    try {
      const { payload } = await jwtVerify(header.slice("Bearer ".length), jwks, {
        issuer: config.issuer,
        audience: config.audience,
      });
      if (!tokenScopes(payload.scope).includes(requiredScope)) {
        reply.code(403).send({ code: 403, message: `Token is missing the required scope ${requiredScope}` });
        return;
      }
    } catch (error) {
      reply.header("WWW-Authenticate", 'Bearer realm="rewave-network", error="invalid_token"');
      const message = error instanceof Error ? error.message : "Invalid token";
      reply.code(401).send({ code: 401, message });
      return;
    }
  };
}
