import type { operations } from "../generated/schema.js";

export type OperationId = keyof operations;

export interface RouteConfig {
  operationId: OperationId;
}
