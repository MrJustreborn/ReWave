import { existsSync, readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const SRC_ROOT = fileURLToPath(new URL("../src", import.meta.url));

const MODULE_NAME_PATTERN = /^[a-z][a-z0-9-]*$/;
const LAYER_NAMES = ["domain", "application", "infrastructure", "api"] as const;
type Layer = (typeof LAYER_NAMES)[number];

const MODULE_DEPENDENCIES: Record<string, readonly string[]> = {
  media: [],
  channel: [],
  scheduling: ["channel", "media"],
  watching: ["channel"],
};

const NODE_BUILTINS = new Set([
  "assert",
  "buffer",
  "child_process",
  "crypto",
  "events",
  "fs",
  "http",
  "https",
  "net",
  "os",
  "path",
  "stream",
  "string_decoder",
  "timers",
  "url",
  "util",
  "zlib",
]);

type FileClass =
  | { kind: "module-layer"; module: string; layer: Layer }
  | { kind: "module-index"; module: string }
  | { kind: "shared" }
  | { kind: "server" }
  | { kind: "db" }
  | { kind: "config" }
  | { kind: "main" }
  | { kind: "generated" }
  | { kind: "unknown" };

const violations: string[] = [];

function fail(file: string, message: string): void {
  violations.push(`src/${file}: ${message}`);
}

function toRelative(absolutePath: string): string {
  return path.relative(SRC_ROOT, absolutePath).split(path.sep).join("/");
}

function classify(rel: string): FileClass {
  if (rel === "main.ts") {
    return { kind: "main" };
  }
  if (rel === "config.ts") {
    return { kind: "config" };
  }
  if (rel.startsWith("generated/")) {
    return { kind: "generated" };
  }
  if (rel.startsWith("shared/")) {
    return { kind: "shared" };
  }
  if (rel.startsWith("server/")) {
    return { kind: "server" };
  }
  if (rel.startsWith("db/")) {
    return { kind: "db" };
  }
  const moduleMatch = /^modules\/([^/]+)(?:\/(.*))?$/.exec(rel);
  if (moduleMatch !== null) {
    const moduleName = moduleMatch[1] as string;
    const rest = moduleMatch[2] ?? "";
    if (rest === "index.ts") {
      return { kind: "module-index", module: moduleName };
    }
    const layerMatch = /^([a-z]+)\//.exec(rest);
    if (layerMatch !== null && (LAYER_NAMES as readonly string[]).includes(layerMatch[1] as string)) {
      return { kind: "module-layer", module: moduleName, layer: layerMatch[1] as Layer };
    }
  }
  return { kind: "unknown" };
}

function listSourceFiles(): string[] {
  const files: string[] = [];

  function walk(dir: string): void {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const absolute = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        if (toRelative(absolute) === "generated") {
          continue;
        }
        walk(absolute);
      } else if (entry.isFile() && entry.name.endsWith(".ts")) {
        files.push(toRelative(absolute));
      }
    }
  }

  walk(SRC_ROOT);
  return files.sort();
}

function checkModuleAnatomy(modulesDir: string): string[] {
  const modules: string[] = [];
  for (const entry of readdirSync(modulesDir, { withFileTypes: true })) {
    if (entry.name.endsWith(".ts")) {
      fail(`modules/${entry.name}`, "loose file in modules/ — modules live in their own subdirectory");
      continue;
    }
    if (!entry.isDirectory()) {
      continue;
    }
    const moduleName = entry.name;
    const moduleDir = path.join(modulesDir, moduleName);
    if (!MODULE_NAME_PATTERN.test(moduleName)) {
      fail(`modules/${moduleName}`, `module name must match ${MODULE_NAME_PATTERN.source}`);
    }
    if (!(moduleName in MODULE_DEPENDENCIES)) {
      fail(
        `modules/${moduleName}`,
        "module is missing from MODULE_DEPENDENCIES in scripts/check-architecture.ts — register its allowed dependencies",
      );
    }
    for (const layer of LAYER_NAMES) {
      if (!existsSync(path.join(moduleDir, layer))) {
        fail(`modules/${moduleName}`, `missing layer directory "${layer}/"`);
      }
    }
    if (!existsSync(path.join(moduleDir, "index.ts"))) {
      fail(`modules/${moduleName}`, 'missing public API file "index.ts"');
    }
    for (const item of readdirSync(moduleDir, { withFileTypes: true })) {
      const isKnownDir = (LAYER_NAMES as readonly string[]).includes(item.name);
      const isIndex = item.name === "index.ts";
      if (item.isFile() && !isIndex) {
        fail(`modules/${moduleName}/${item.name}`, "loose file in module — code lives in domain/, application/, infrastructure/ or api/");
      } else if (item.isDirectory() && !isKnownDir) {
        fail(`modules/${moduleName}/${item.name}`, "unknown directory in module — use domain/, application/, infrastructure/ or api/");
      }
    }
    modules.push(moduleName);
  }
  for (const declared of Object.keys(MODULE_DEPENDENCIES)) {
    if (!modules.includes(declared)) {
      fail("modules", `MODULE_DEPENDENCIES declares "${declared}" but modules/${declared} does not exist`);
    }
  }
  return modules;
}

interface ParsedImport {
  specifier: string;
  typeOnly: boolean;
}

function parseImports(content: string): ParsedImport[] {
  const stripped = content.replace(/\/\*[\s\S]*?\*\//g, " ").replace(/\/\/[^\n]*/g, " ");
  const imports: ParsedImport[] = [];

  const fromRe = /\bfrom\s*["']([^"']+)["']/g;
  for (const match of stripped.matchAll(fromRe)) {
    const specifier = match[1] as string;
    const matchIndex = match.index ?? 0;
    const statementStart = Math.max(
      stripped.lastIndexOf("import", matchIndex - 1),
      stripped.lastIndexOf("export", matchIndex - 1),
    );
    if (statementStart === -1) {
      continue;
    }
    const head = stripped.slice(statementStart, matchIndex);
    const typeOnly = /^(?:import|export)\s+type[\s{]/.test(head);
    const clause = head.replace(/^(?:import|export)(\s+type)?/, "");
    if (!/^[\s{}*,\w]*$/.test(clause)) {
      continue;
    }
    imports.push({ specifier, typeOnly });
  }

  const sideEffectRe = /(?:^|[;\n])\s*import\s*["']([^"']+)["']/g;
  for (const match of stripped.matchAll(sideEffectRe)) {
    imports.push({ specifier: match[1] as string, typeOnly: false });
  }

  const dynamicRe = /\bimport\s*\(\s*["']([^"']+)["']\s*\)/g;
  for (const match of stripped.matchAll(dynamicRe)) {
    imports.push({ specifier: match[1] as string, typeOnly: false });
  }

  return imports;
}

type SpecKind = "relative" | "node" | "npm";

function classifySpecifier(specifier: string): SpecKind {
  if (specifier.startsWith(".") || specifier.startsWith("/")) {
    return "relative";
  }
  if (specifier.startsWith("node:") || NODE_BUILTINS.has(specifier)) {
    return "node";
  }
  return "npm";
}

function resolveRelative(importer: string, specifier: string): string | undefined {
  if (!specifier.endsWith(".js")) {
    fail(importer, `relative import "${specifier}" must use an explicit .js extension (NodeNext style)`);
    return undefined;
  }
  const dir = path.posix.join("src", path.posix.dirname(importer));
  const resolved = path.posix.normalize(path.posix.join(dir, specifier)).replace(/\.js$/, ".ts");
  if (!resolved.startsWith("src/") || resolved.startsWith("src/../")) {
    fail(importer, `relative import "${specifier}" leaves src/ — everything the server needs lives under src/`);
    return undefined;
  }
  return resolved.slice("src/".length);
}

function isSharedIndex(targetClass: FileClass | undefined, targetRel: string | undefined): boolean {
  return targetClass?.kind === "shared" && targetRel === "shared/index.ts";
}

function isSameModule(importer: FileClass, targetClass: FileClass | undefined): boolean {
  const importerModule =
    importer.kind === "module-layer" || importer.kind === "module-index" ? importer.module : undefined;
  const targetModule =
    targetClass?.kind === "module-layer" || targetClass?.kind === "module-index" ? targetClass.module : undefined;
  return importerModule !== undefined && importerModule === targetModule;
}

function allowedInDependencies(importer: FileClass, targetClass: FileClass | undefined): boolean {
  if (importer.kind !== "module-layer" && importer.kind !== "module-index") {
    return false;
  }
  const allowed = MODULE_DEPENDENCIES[importer.module] ?? [];
  return targetClass?.kind === "module-index" && allowed.includes(targetClass.module);
}

function checkImport(
  importer: string,
  importerClass: FileClass,
  spec: ParsedImport,
  targetClass: FileClass | undefined,
  targetRel: string | undefined,
): void {
  const specKind = classifySpecifier(spec.specifier);

  const sharedIndex = isSharedIndex(targetClass, targetRel);
  if (targetClass?.kind === "shared" && !sharedIndex && importerClass.kind !== "shared") {
    fail(
      importer,
      `import of "${spec.specifier}" must go through the kernel entry src/shared/index.ts (never deep-import shared kernel files)`,
    );
    return;
  }

  switch (importerClass.kind) {
    case "module-layer": {
      const { layer, module } = importerClass;
      if (layer === "domain") {
        const ok =
          sharedIndex ||
          (targetClass?.kind === "module-layer" &&
            targetClass.layer === "domain" &&
            isSameModule(importerClass, targetClass));
        if (!ok) {
          fail(
            importer,
            `domain layer must only import its own domain and the shared kernel (src/shared/index.ts) — "${spec.specifier}" is not allowed there`,
          );
        }
        return;
      }
      if (layer === "application") {
        const ownLayers =
          targetClass?.kind === "module-layer" &&
          isSameModule(importerClass, targetClass) &&
          (targetClass.layer === "domain" || targetClass.layer === "application");
        const otherModule = allowedInDependencies(importerClass, targetClass);
        if (!ownLayers && !sharedIndex && !otherModule) {
          const allowed = MODULE_DEPENDENCIES[module] ?? [];
          fail(
            importer,
            `application layer may import its own domain/application, src/shared/index.ts and the public API (index.ts) of: ${allowed.length > 0 ? allowed.join(", ") : "no other module"} — "${spec.specifier}" is not allowed`,
          );
        }
        return;
      }
      if (layer === "infrastructure") {
        const sameModule = isSameModule(importerClass, targetClass);
        const ownDomain =
          targetClass?.kind === "module-layer" && sameModule && targetClass.layer === "domain";
        const ownInfra =
          targetClass?.kind === "module-layer" && sameModule && targetClass.layer === "infrastructure";
        const dbConnection = targetRel === "db/connection.ts";
        if (!ownDomain && !ownInfra && !dbConnection && !sharedIndex && specKind === "relative") {
          fail(
            importer,
            `infrastructure adapters may import their own domain (ports/entities), their own layer, src/db/connection.ts and the shared kernel — "${spec.specifier}" is not allowed`,
          );
        }
        return;
      }
      const own =
        targetClass?.kind === "module-layer" &&
        isSameModule(importerClass, targetClass) &&
        (targetClass.layer === "domain" || targetClass.layer === "application" || targetClass.layer === "api");
      const generated = targetClass?.kind === "generated";
      if (!own && !sharedIndex && !generated && specKind === "relative") {
        fail(
          importer,
          `api adapters may import their own domain/application/api, the shared kernel and src/generated/ — "${spec.specifier}" is not allowed`,
        );
      }
      return;
    }
    case "module-index": {
      if (specKind !== "relative" && !spec.typeOnly) {
        fail(
          importer,
          `the module factory may reference external packages only as types — "${spec.specifier}" is a runtime dependency and belongs inside a layer that needs it`,
        );
        return;
      }
      const same = isSameModule(importerClass, targetClass);
      const dbConnection = targetRel === "db/connection.ts";
      const otherModule = targetClass?.kind === "module-index" && !same;
      if (!same && !dbConnection && !sharedIndex && specKind === "relative") {
        if (otherModule) {
          fail(
            importer,
            "cross-module wiring belongs to the composition root (src/main.ts) or an application service — the module factory must not import another module",
          );
        } else {
          fail(
            importer,
            `the module factory may import its own module, src/db/connection.ts and the shared kernel — "${spec.specifier}" is not allowed`,
          );
        }
      }
      return;
    }
    case "shared": {
      const sibling = targetClass?.kind === "shared";
      const schemaOnly = targetRel === "generated/schema.ts";
      if (!sibling && !schemaOnly) {
        fail(
          importer,
          `the shared kernel must stay self-contained (only sibling files and the type-only src/generated/schema.ts) — "${spec.specifier}" is not allowed`,
        );
      }
      return;
    }
    case "server": {
      const sibling = targetClass?.kind === "server";
      const config = targetClass?.kind === "config";
      const generated = targetClass?.kind === "generated";
      const moduleIndex = targetClass?.kind === "module-index";
      if (!sibling && !config && !generated && !moduleIndex && !sharedIndex && specKind === "relative") {
        if (targetClass?.kind === "module-layer" || targetClass?.kind === "db") {
          fail(
            importer,
            "the server assembly may only use modules through their public API (index.ts); storage wiring belongs to the composition root",
          );
        } else {
          fail(importer, `server layer cannot import "${spec.specifier}"`);
        }
      }
      return;
    }
    case "db": {
      if (specKind === "npm") {
        fail(importer, "db infrastructure stays on node builtins and its own files — no npm dependencies");
        return;
      }
      const sibling = targetClass?.kind === "db";
      const config = targetClass?.kind === "config";
      if (!sibling && !config && !sharedIndex && specKind === "relative") {
        fail(importer, `db infrastructure may import its own files, src/config.ts and the shared kernel — "${spec.specifier}" is not allowed`);
      }
      return;
    }
    case "config": {
      if (specKind !== "node") {
        fail(importer, "configuration reads only from the environment (node builtins) — no other imports allowed");
      }
      return;
    }
    case "main": {
      if (specKind === "npm") {
        fail(importer, "the composition root wires existing code — it does not depend on npm packages directly");
        return;
      }
      const moduleIndex = targetClass?.kind === "module-index";
      const db = targetClass?.kind === "db";
      const server = targetClass?.kind === "server";
      const config = targetClass?.kind === "config";
      if (!moduleIndex && !db && !server && !config && !sharedIndex && specKind === "relative") {
        fail(
          importer,
          "the composition root may only import src/config.ts, src/db/, src/server/, module factories (modules/*/index.ts) and the shared kernel",
        );
      }
      return;
    }
    default: {
      return;
    }
  }
}

const files = listSourceFiles();
const modules = checkModuleAnatomy(path.join(SRC_ROOT, "modules"));

for (const file of files) {
  const fileClass = classify(file);
  if (fileClass.kind === "unknown") {
    fail(
      file,
      "unclassified source file — place code in src/shared/, src/server/, src/db/, modules/<name>/{domain,application,infrastructure,api}, or keep it in src/main.ts / src/config.ts",
    );
    continue;
  }
  if (fileClass.kind === "generated") {
    continue;
  }

  const content = readFileSync(path.join(SRC_ROOT, file), "utf8");
  const seen = new Set<string>();
  for (const spec of parseImports(content)) {
    const dedupeKey = `${spec.specifier}:${spec.typeOnly ? "t" : "v"}`;
    if (seen.has(dedupeKey)) {
      continue;
    }
    seen.add(dedupeKey);

    if (classifySpecifier(spec.specifier) === "relative") {
      const targetRel = resolveRelative(file, spec.specifier);
      if (targetRel !== undefined) {
        checkImport(file, fileClass, spec, classify(targetRel), targetRel);
      }
    } else {
      checkImport(file, fileClass, spec, undefined, undefined);
    }
  }
}

if (violations.length > 0) {
  console.error(`check-architecture: FAILED (${violations.length} violation(s))\n`);
  for (const violation of violations) {
    console.error(`  ${violation}`);
  }
  process.exit(1);
}

console.log(
  `check-architecture: OK — ${files.length} source files, ${modules.length} modules (${modules.join(", ")})`,
);
