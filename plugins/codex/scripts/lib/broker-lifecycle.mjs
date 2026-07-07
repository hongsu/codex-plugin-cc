import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { createBrokerEndpoint, parseBrokerEndpoint } from "./broker-endpoint.mjs";
import { resolveStateDir, resolveStateRoot } from "./state.mjs";

export const PID_FILE_ENV = "CODEX_COMPANION_APP_SERVER_PID_FILE";
export const LOG_FILE_ENV = "CODEX_COMPANION_APP_SERVER_LOG_FILE";
const BROKER_STATE_FILE = "broker.json";
const SESSION_ID_ENV = "CODEX_COMPANION_SESSION_ID";
const BROKER_STATE_LOCK_STALE_MS = 30000;
const BROKER_STATE_LOCK_TIMEOUT_MS = 5000;
const BROKER_SHUTDOWN_TIMEOUT_MS = 1000;

export function resolveSessionId(options = {}) {
  if (options.sessionId) {
    return options.sessionId;
  }
  const env = options.env ?? process.env;
  return env[SESSION_ID_ENV] ?? null;
}

function brokerSessionOwners(session) {
  const owners = [];
  if (Array.isArray(session?.sessionIds)) {
    owners.push(...session.sessionIds);
  }
  if (session?.sessionId) {
    owners.push(session.sessionId);
  }
  return [...new Set(owners.filter(Boolean))];
}

export function hasBrokerSessionOwners(session) {
  return brokerSessionOwners(session).length > 0;
}

function withBrokerSessionOwner(session, sessionId) {
  if (!sessionId) {
    return session;
  }
  const owners = brokerSessionOwners(session);
  if (!owners.includes(sessionId)) {
    owners.push(sessionId);
  }
  return {
    ...session,
    sessionId: owners[0] ?? sessionId,
    sessionIds: owners
  };
}

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function withBrokerStateFileLock(stateFile, fn, options = {}) {
  const lockDir = `${stateFile}.lock`;
  const timeoutMs = options.timeoutMs ?? BROKER_STATE_LOCK_TIMEOUT_MS;
  const staleMs = options.staleMs ?? BROKER_STATE_LOCK_STALE_MS;
  const deadline = Date.now() + timeoutMs;

  while (true) {
    try {
      fs.mkdirSync(lockDir);
      break;
    } catch (error) {
      if (error?.code !== "EEXIST") {
        throw error;
      }
      try {
        const stat = fs.statSync(lockDir);
        if (Date.now() - stat.mtimeMs > staleMs) {
          fs.rmSync(lockDir, { recursive: true, force: true });
          continue;
        }
      } catch {
        continue;
      }
      if (Date.now() >= deadline) {
        throw new Error(`Timed out waiting for broker state lock: ${stateFile}`);
      }
      await sleep(25);
    }
  }

  try {
    return await fn();
  } finally {
    fs.rmSync(lockDir, { recursive: true, force: true });
  }
}

export function createBrokerSessionDir(prefix = "cxc-") {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function connectToEndpoint(endpoint) {
  const target = parseBrokerEndpoint(endpoint);
  return net.createConnection({ path: target.path });
}

export async function waitForBrokerEndpoint(endpoint, timeoutMs = 2000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const ready = await new Promise((resolve) => {
      const socket = connectToEndpoint(endpoint);
      socket.on("connect", () => {
        socket.end();
        resolve(true);
      });
      socket.on("error", () => resolve(false));
    });
    if (ready) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  return false;
}

export async function sendBrokerShutdown(endpoint, { timeoutMs = BROKER_SHUTDOWN_TIMEOUT_MS } = {}) {
  await new Promise((resolve) => {
    let settled = false;
    let timer = null;
    const socket = connectToEndpoint(endpoint);
    const finish = () => {
      if (settled) {
        return;
      }
      settled = true;
      if (timer) {
        clearTimeout(timer);
      }
      resolve();
    };
    if (timeoutMs > 0) {
      timer = setTimeout(() => {
        socket.destroy();
        finish();
      }, timeoutMs);
    }
    socket.setEncoding("utf8");
    socket.on("connect", () => {
      socket.write(`${JSON.stringify({ id: 1, method: "broker/shutdown", params: {} })}\n`);
    });
    socket.on("data", () => {
      socket.end();
      finish();
    });
    socket.on("error", finish);
    socket.on("close", finish);
  });
}

export function spawnBrokerProcess({ scriptPath, cwd, endpoint, pidFile, logFile, env = process.env }) {
  const logFd = fs.openSync(logFile, "a");
  const child = spawn(process.execPath, [scriptPath, "serve", "--endpoint", endpoint, "--cwd", cwd, "--pid-file", pidFile], {
    cwd,
    env,
    detached: true,
    stdio: ["ignore", logFd, logFd]
  });
  child.unref();
  fs.closeSync(logFd);
  return child;
}

function resolveBrokerStateFile(cwd) {
  return path.join(resolveStateDir(cwd), BROKER_STATE_FILE);
}

export function loadBrokerSession(cwd) {
  const stateFile = resolveBrokerStateFile(cwd);
  if (!fs.existsSync(stateFile)) {
    return null;
  }

  try {
    return JSON.parse(fs.readFileSync(stateFile, "utf8"));
  } catch {
    return null;
  }
}

export function saveBrokerSession(cwd, session) {
  const stateDir = resolveStateDir(cwd);
  fs.mkdirSync(stateDir, { recursive: true });
  fs.writeFileSync(resolveBrokerStateFile(cwd), `${JSON.stringify(session, null, 2)}\n`, "utf8");
}

export function clearBrokerSession(cwd) {
  const stateFile = resolveBrokerStateFile(cwd);
  if (fs.existsSync(stateFile)) {
    fs.unlinkSync(stateFile);
  }
}

async function isBrokerEndpointReady(endpoint) {
  if (!endpoint) {
    return false;
  }
  try {
    return await waitForBrokerEndpoint(endpoint, 150);
  } catch {
    return false;
  }
}

export async function ensureBrokerSession(cwd, options = {}) {
  const existing = loadBrokerSession(cwd);
  if (existing && (await isBrokerEndpointReady(existing.endpoint))) {
    const stateFile = resolveBrokerStateFile(cwd);
    return await withBrokerStateFileLock(stateFile, () => {
      const current = loadBrokerSession(cwd) ?? existing;
      const withOwner = withBrokerSessionOwner(current, resolveSessionId(options));
      if (withOwner !== current) {
        saveBrokerSession(cwd, withOwner);
      }
      return withOwner;
    });
  }

  if (existing) {
    teardownBrokerSession({
      endpoint: existing.endpoint ?? null,
      pidFile: existing.pidFile ?? null,
      logFile: existing.logFile ?? null,
      sessionDir: existing.sessionDir ?? null,
      pid: existing.pid ?? null,
      killProcess: options.killProcess ?? null
    });
    clearBrokerSession(cwd);
  }

  const sessionDir = createBrokerSessionDir();
  const endpointFactory = options.createBrokerEndpoint ?? createBrokerEndpoint;
  const endpoint = endpointFactory(sessionDir, options.platform);
  const pidFile = path.join(sessionDir, "broker.pid");
  const logFile = path.join(sessionDir, "broker.log");
  const scriptPath =
    options.scriptPath ??
    fileURLToPath(new URL("../app-server-broker.mjs", import.meta.url));

  const child = spawnBrokerProcess({
    scriptPath,
    cwd,
    endpoint,
    pidFile,
    logFile,
    env: options.env ?? process.env
  });

  const ready = await waitForBrokerEndpoint(endpoint, options.timeoutMs ?? 2000);
  if (!ready) {
    teardownBrokerSession({
      endpoint,
      pidFile,
      logFile,
      sessionDir,
      pid: child.pid ?? null,
      killProcess: options.killProcess ?? null
    });
    return null;
  }

  const session = {
    endpoint,
    pidFile,
    logFile,
    sessionDir,
    pid: child.pid ?? null,
    sessionId: resolveSessionId(options)
  };
  const withOwner = withBrokerSessionOwner(session, session.sessionId);
  saveBrokerSession(cwd, withOwner);
  return withOwner;
}

export async function teardownBrokersForSession(sessionId, { killProcess = null, shutdownTimeoutMs = BROKER_SHUTDOWN_TIMEOUT_MS } = {}) {
  if (!sessionId) {
    return 0;
  }
  const stateRoot = resolveStateRoot();
  if (!fs.existsSync(stateRoot)) {
    return 0;
  }

  let count = 0;
  for (const entry of fs.readdirSync(stateRoot)) {
    const stateFile = path.join(stateRoot, entry, BROKER_STATE_FILE);
    if (!fs.existsSync(stateFile)) {
      continue;
    }

    await withBrokerStateFileLock(stateFile, async () => {
      if (!fs.existsSync(stateFile)) {
        return;
      }

      let session;
      try {
        session = JSON.parse(fs.readFileSync(stateFile, "utf8"));
      } catch {
        return;
      }
      const owners = brokerSessionOwners(session);
      if (!owners.includes(sessionId)) {
        return;
      }
      const remainingOwners = owners.filter((owner) => owner !== sessionId);
      if (remainingOwners.length > 0) {
        fs.writeFileSync(
          stateFile,
          `${JSON.stringify({ ...session, sessionId: remainingOwners[0], sessionIds: remainingOwners }, null, 2)}\n`,
          "utf8"
        );
        return;
      }

      if (session.endpoint) {
        await sendBrokerShutdown(session.endpoint, { timeoutMs: shutdownTimeoutMs });
      }
      teardownBrokerSession({
        endpoint: session.endpoint ?? null,
        pidFile: session.pidFile ?? null,
        logFile: session.logFile ?? null,
        sessionDir: session.sessionDir ?? null,
        pid: session.pid ?? null,
        killProcess
      });
      if (fs.existsSync(stateFile)) {
        fs.unlinkSync(stateFile);
      }
      count += 1;
    });
  }
  return count;
}

export function teardownBrokerSession({ endpoint = null, pidFile, logFile, sessionDir = null, pid = null, killProcess = null }) {
  if (Number.isFinite(pid) && killProcess) {
    try {
      killProcess(pid);
    } catch {
      // Ignore missing or already-exited broker processes.
    }
  }

  if (pidFile && fs.existsSync(pidFile)) {
    fs.unlinkSync(pidFile);
  }

  if (logFile && fs.existsSync(logFile)) {
    fs.unlinkSync(logFile);
  }

  if (endpoint) {
    try {
      const target = parseBrokerEndpoint(endpoint);
      if (target.kind === "unix" && fs.existsSync(target.path)) {
        fs.unlinkSync(target.path);
      }
    } catch {
      // Ignore malformed or already-removed broker endpoints during teardown.
    }
  }

  const resolvedSessionDir = sessionDir ?? (pidFile ? path.dirname(pidFile) : logFile ? path.dirname(logFile) : null);
  if (resolvedSessionDir && fs.existsSync(resolvedSessionDir)) {
    try {
      fs.rmdirSync(resolvedSessionDir);
    } catch {
      // Ignore non-empty or missing directories.
    }
  }
}
