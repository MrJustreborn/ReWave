import type { FastifyReply } from "fastify";

export function notImplemented(reply: FastifyReply, operationId: string) {
  return reply.code(501).send({ code: 501, message: `${operationId} is not implemented yet` });
}
