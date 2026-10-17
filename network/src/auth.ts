import type { FastifyReply, FastifyRequest } from "fastify";
import { createRemoteJWKSet, jwtVerify } from "jose";
import type { Config } from "./config.js";
import { SCOPES_BY_OPERATION } from "./generated/security.js";
import type { RouteConfig } from "./routes/route-config.js";

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
    const routeConfig = request.routeOptions.config as unknown as RouteConfig | undefined;
    if (routeConfig?.operationId === undefined) {
      return;
    }
    const requiredScopes = SCOPES_BY_OPERATION[routeConfig.operationId];
    if (!Array.isArray(requiredScopes)) {
      reply
        .code(403)
        .send({ code: 403, message: `No scopes configured for ${String(routeConfig.operationId)}` });
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
      const granted = tokenScopes(payload.scope);
      const missing = requiredScopes.filter((required) => !granted.includes(required));
      if (missing.length > 0) {
        reply.code(403).send({ code: 403, message: `Token is missing required scope(s): ${missing.join(" ")}` });
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
