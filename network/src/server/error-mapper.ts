import type { FastifyInstance } from "fastify";
import { ConflictError, NotFoundError, ValidationError, NotImplementedError } from "../shared/index.js";

export function setErrorMapping(app: FastifyInstance): void {
  app.setErrorHandler((error, request, reply) => {
    if (error instanceof ValidationError) {
      return reply.code(400).send({ code: 400, message: error.message });
    }
    if (error instanceof NotFoundError) {
      return reply.code(404).send({ code: 404, message: error.message });
    }
    if (error instanceof ConflictError) {
      return reply.code(409).send({ code: 409, message: error.message });
    }
    if (error instanceof NotImplementedError) {
      return reply.code(501).send({ code: 501, message: error.message });
    }
    const statusCode = (error as { statusCode?: unknown }).statusCode;
    if (typeof statusCode === "number") {
      return reply.code(statusCode).send({ code: statusCode, message: error instanceof Error ? error.message : String(error) });
    }
    request.log.error({ err: error }, "Unhandled error");
    return reply.code(500).send({ code: 500, message: "Internal server error" });
  });
}
