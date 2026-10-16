export interface Config {
  port: number;
  host: string;
  dbPath: string;
  mediaRoot: string;
  dockerSocket: string;
  authMode: "off" | "jwks";
  jwksUrl: string | undefined;
  issuer: string | undefined;
  audience: string | undefined;
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

export function loadConfig(): Config {
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
    authMode,
    jwksUrl,
    issuer: optionalEnv("AUTH_ISSUER"),
    audience: optionalEnv("AUTH_AUDIENCE"),
  };
}
