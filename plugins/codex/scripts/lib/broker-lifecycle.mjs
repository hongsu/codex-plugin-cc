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
const BROKER_LOCK_TIMEOUT_CODE = "EBROKERSTATELOCKTIMEOUT";

let brokerLockTokenSeq = 0;

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

// True when the broker is still owned by a session other than `sessionId`.
// Used by SessionEnd to decide whether the cwd fallback must still run: if the
// only remaining owner is the ending session (e.g. its session-keyed teardown
// was skipped under lock contention), the broker must NOT be left behind.
export function hasOtherBrokerSessionOwners(session, sessionId) {
  return brokerSessionOwners(session).some((owner) => owner !== sessionId);
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

function isBrokerLockTimeout(error) {
  return error?.code === BROKER_LOCK_TIMEOUT_CODE;
}

async function withBrokerStateFileLock(stateFile, fn, options = {}) {
  const lockDir = `${stateFile}.lock`;
  const tokenFile = path.join(lockDir, "owner");
  const token = `${process.pid}-${brokerLockTokenSeq += 1}`;
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
      let stat = null;
      try {
        stat = fs.lstatSync(lockDir);
      } catch {
        // The lock may have disappeared between mkdirSync and lstatSync. Retry
        // through the normal deadline/backoff path instead of spinning.
      }
      if (stat && !stat.isDirectory()) {
        try {
          // unlinkSync cannot remove a directory. If another contender replaced
          // the invalid path with a real lock after lstatSync, this fails safely
          // instead of renaming or deleting that contender's lock.
          fs.unlinkSync(lockDir);
        } catch {
          // Lost the replacement race or cannot remove the invalid path. Retry
          // through the normal deadline/backoff path below.
        }
      }
      if (stat && Date.now() - stat.mtimeMs > staleMs) {
        // Reclaim atomically: rename the stale directory to a private name so
        // only one reclaimer can take it — a blind rmSync lets two reclaimers
        // each delete the other's freshly created lock. After the rename,
        // re-check the mtime: if the directory we grabbed was refreshed after
        // our stat (a fresh lock, not the stale one), put it back untouched.
        const claimed = `${lockDir}.reclaim-${process.pid}-${(brokerLockTokenSeq += 1)}`;
        let reclaimed = false;
        try {
          fs.renameSync(lockDir, claimed);
          if (Date.now() - fs.statSync(claimed).mtimeMs > staleMs) {
            fs.rmSync(claimed, { recursive: true, force: true });
            reclaimed = true;
          } else {
            try {
              fs.renameSync(claimed, lockDir);
            } catch {
              // A new lock already took the path; drop the moved copy.
              fs.rmSync(claimed, { recursive: true, force: true });
            }
          }
        } catch {
          // Lost the reclaim race; fall through and retry acquisition.
        }
        if (reclaimed) {
          continue;
        }
      }
      if (Date.now() >= deadline) {
        throw Object.assign(new Error(`Timed out waiting for broker state lock: ${stateFile}`), {
          code: BROKER_LOCK_TIMEOUT_CODE
        });
      }
      await sleep(25);
    }
  }

  // Stamp ownership so the finally only releases a lock we still hold — a
  // stale reclaim by another waiter must not have its lock deleted from under it.
  let stamped = false;
  try {
    fs.writeFileSync(tokenFile, token, "utf8");
    stamped = true;
  } catch {
    // Could not stamp (quota/permission race). We still created the lock dir,
    // so release it unconditionally below rather than leaking it.
  }

  try {
    return await fn();
  } finally {
    let owned = true;
    if (stamped) {
      try {
        owned = fs.readFileSync(tokenFile, "utf8") === token;
      } catch {
        owned = false;
      }
    }
    if (owned) {
      fs.rmSync(lockDir, { recursive: true, force: true });
    }
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
    // Graceful shutdown is best-effort: a corrupt/unsupported endpoint makes
    // connectToEndpoint (parseBrokerEndpoint) throw synchronously. Never reject,
    // so callers always fall through to the forced process/file teardown.
    let socket;
    try {
      socket = connectToEndpoint(endpoint);
    } catch {
      finish();
      return;
    }
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

// Write broker.json atomically (temp + rename) so a concurrent reader — e.g. an
// unlocked ownership pre-check in another session's teardown — never observes a
// half-written file and mis-parses it.
function writeBrokerStateFile(stateFile, session) {
  const tmp = `${stateFile}.tmp-${process.pid}-${(brokerLockTokenSeq += 1)}`;
  fs.writeFileSync(tmp, `${JSON.stringify(session, null, 2)}\n`, "utf8");
  fs.renameSync(tmp, stateFile);
}

export function saveBrokerSession(cwd, session) {
  const stateDir = resolveStateDir(cwd);
  fs.mkdirSync(stateDir, { recursive: true });
  writeBrokerStateFile(resolveBrokerStateFile(cwd), session);
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

// Spawn a broker process and wait for it to accept connections. Returns an
// unsaved session record, or null if it never became ready. No locking or
// persistence — the caller owns those.
async function spawnReadyBroker(cwd, options) {
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

  return {
    endpoint,
    pidFile,
    logFile,
    sessionDir,
    pid: child.pid ?? null,
    sessionId: resolveSessionId(options)
  };
}

// Tear down whatever broker is recorded for cwd (best effort) so a fresh one
// can replace it.
function discardBrokerSession(cwd, session, options) {
  if (!session) {
    return;
  }
  teardownBrokerSession({
    endpoint: session.endpoint ?? null,
    pidFile: session.pidFile ?? null,
    logFile: session.logFile ?? null,
    sessionDir: session.sessionDir ?? null,
    pid: session.pid ?? null,
    killProcess: options.killProcess ?? null
  });
  clearBrokerSession(cwd);
}

// Adopt cwd's recorded broker for this session if it is still live, else spawn
// and persist a fresh one. Callers run this inside the state-file lock so two
// racing sessions cannot each leave an orphaned broker with no broker.json.
async function adoptOrSpawnBroker(cwd, options) {
  const current = loadBrokerSession(cwd);
  if (current && (await isBrokerEndpointReady(current.endpoint))) {
    const withOwner = withBrokerSessionOwner(current, resolveSessionId(options));
    if (withOwner !== current) {
      saveBrokerSession(cwd, withOwner);
    }
    return withOwner;
  }
  discardBrokerSession(cwd, current, options);

  const session = await spawnReadyBroker(cwd, options);
  if (!session) {
    return null;
  }
  const withOwner = withBrokerSessionOwner(session, session.sessionId);
  saveBrokerSession(cwd, withOwner);
  return withOwner;
}

export async function ensureBrokerSession(cwd, options = {}) {
  const stateFile = resolveBrokerStateFile(cwd);
  const lockOptions = options.lockTimeoutMs == null ? {} : { timeoutMs: options.lockTimeoutMs };

  // Fast path: reuse a ready broker. The readiness probe runs outside the lock,
  // so the broker may have been torn down (or replaced) before we acquired it;
  // trust only the locked re-read, never the pre-lock snapshot.
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const existing = loadBrokerSession(cwd);
    if (!existing || !(await isBrokerEndpointReady(existing.endpoint))) {
      break;
    }
    const reused = await withBrokerStateFileLock(stateFile, () => {
      const current = loadBrokerSession(cwd);
      if (!current || current.endpoint !== existing.endpoint) {
        return null;
      }
      const withOwner = withBrokerSessionOwner(current, resolveSessionId(options));
      if (withOwner !== current) {
        saveBrokerSession(cwd, withOwner);
      }
      return withOwner;
    }, lockOptions);
    if (reused) {
      return reused;
    }
    // State changed while we waited for the lock — re-probe: a replacement
    // broker may already be live and reusable.
  }

  // Slow path: adopt-or-spawn under the lock so concurrent spawns don't orphan
  // brokers. resolveStateDir must exist before we can create the lock dir.
  fs.mkdirSync(resolveStateDir(cwd), { recursive: true });
  return withBrokerStateFileLock(stateFile, () => adoptOrSpawnBroker(cwd, options), lockOptions);
}

export async function teardownBrokersForSession(
  sessionId,
  {
    killProcess = null,
    shutdownTimeoutMs = BROKER_SHUTDOWN_TIMEOUT_MS,
    lockTimeoutMs = BROKER_STATE_LOCK_TIMEOUT_MS,
    budgetMs = null
  } = {}
) {
  if (!sessionId) {
    return 0;
  }
  const stateRoot = resolveStateRoot();
  if (!fs.existsSync(stateRoot)) {
    return 0;
  }

  const deadline = budgetMs != null ? Date.now() + budgetMs : null;
  let count = 0;
  let teardownError = null;
  for (const entry of fs.readdirSync(stateRoot)) {
    if (deadline != null && Date.now() >= deadline) {
      break;
    }
    const stateFile = path.join(stateRoot, entry, BROKER_STATE_FILE);
    if (!fs.existsSync(stateFile)) {
      continue;
    }

    // Ownership pre-check without the lock: never block on a lock held for a
    // workspace this session does not own. The owner set only ever grows to
    // include our sessionId (reuse) or shrinks when we ourselves remove it, so
    // an unlocked read cannot falsely exclude a broker we own. A parse failure
    // (e.g. a genuinely corrupt file) is NOT treated as "not ours" — fall
    // through to the locked re-read, which is authoritative, rather than
    // skipping a broker that might belong to this session.
    let preview = null;
    try {
      preview = JSON.parse(fs.readFileSync(stateFile, "utf8"));
    } catch {
      preview = null;
    }
    if (preview && !brokerSessionOwners(preview).includes(sessionId)) {
      continue;
    }

    const remaining = deadline != null ? deadline - Date.now() : lockTimeoutMs;
    if (remaining <= 0) {
      break;
    }
    const entryLockTimeout = Math.min(lockTimeoutMs, remaining);

    try {
      await withBrokerStateFileLock(
        stateFile,
        async () => {
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
            writeBrokerStateFile(stateFile, {
              ...session,
              sessionId: remainingOwners[0],
              sessionIds: remainingOwners
            });
            return;
          }

          // Bound the graceful-shutdown wait by the remaining scan budget, not
          // just the per-RPC default: several unresponsive endpoints could
          // otherwise each burn shutdownTimeoutMs and push the whole scan past
          // the SessionEnd hook's budget. Out of budget → skip the RPC and let
          // teardownBrokerSession terminate the process directly.
          if (session.endpoint) {
            const shutdownWait =
              deadline != null ? Math.min(shutdownTimeoutMs, deadline - Date.now()) : shutdownTimeoutMs;
            if (shutdownWait > 0) {
              await sendBrokerShutdown(session.endpoint, { timeoutMs: shutdownWait });
            }
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
        },
        { timeoutMs: entryLockTimeout }
      );
    } catch (error) {
      if (!isBrokerLockTimeout(error)) {
        teardownError ??= error;
        continue;
      }
      // This entry's lock remained unavailable within the timeout. Skip it so
      // the rest of this session's brokers still get torn down instead of
      // aborting the whole scan.
    }
  }
  if (teardownError) {
    throw teardownError;
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
