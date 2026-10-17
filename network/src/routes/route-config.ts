import type { operations } from "../generated/schema.js";

export interface RouteConfig {
  operationId: keyof operations;
}
