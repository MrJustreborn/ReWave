export type LogLevel = "fatal" | "error" | "warn" | "info" | "debug" | "trace";

const LOG_LEVELS: readonly LogLevel[] = ["fatal", "error", "warn", "info", "debug", "trace"];

export interface Config {
  port: number;
  host: string;
  dbPath: string;
  mediaRoot: string;
  dockerSocket: string;
  watchKeepAliveSeconds: number;
  authMode: "off" | "jwks";
  jwksUrl: string | undefined;
  issuer: string | undefined;
  audience: string | undefined;
  logLevel: LogLevel;
}

function loadDotEnv(): void {
  try {
    process.loadEnvFile();
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code !== "ENOENT") {
      throw error;
    }
  }
}

function optionalEnv(name: string): string | undefined {
  const value = process.env[name];
  return value === undefined || value === "" ? undefined : value;
}

function numberEnv(name: string, fallback: number): number {
  const value = optionalEnv(name);
  if (value === undefined) {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  if (Number.isNaN(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer, got "${value}"`);
  }
  return parsed;
}

function logLevelEnv(name: string, fallback: LogLevel): LogLevel {
  const value = optionalEnv(name);
  if (value === undefined) {
    return fallback;
  }
  if (!(LOG_LEVELS as readonly string[]).includes(value)) {
    throw new Error(`${name} must be one of ${LOG_LEVELS.join(", ")}, got "${value}"`);
  }
  return value as LogLevel;
}

export function loadConfig(): Config {
  loadDotEnv();
  const authMode = optionalEnv("AUTH_MODE") === "jwks" ? "jwks" : "off";
  const jwksUrl = optionalEnv("AUTH_JWKS_URL");
  if (authMode === "jwks" && jwksUrl === undefined) {
    throw new Error("AUTH_MODE=jwks requires AUTH_JWKS_URL to be set");
  }
  return {
    port: numberEnv("PORT", 8080),
    host: optionalEnv("HOST") ?? "0.0.0.0",
    dbPath: optionalEnv("DB_PATH") ?? "data/network.db",
    mediaRoot: optionalEnv("MEDIA_ROOT") ?? "/media-pool",
    dockerSocket: optionalEnv("DOCKER_SOCKET") ?? "/var/run/docker.sock",
    watchKeepAliveSeconds: numberEnv("WATCH_KEEPALIVE_SECONDS", 300),
    authMode,
    jwksUrl,
    issuer: optionalEnv("AUTH_ISSUER"),
    audience: optionalEnv("AUTH_AUDIENCE"),
    logLevel: logLevelEnv("LOG_LEVEL", "info"),
  };
}
