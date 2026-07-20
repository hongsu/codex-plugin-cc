import fs from "node:fs";
import { createHash } from "node:crypto";
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
const MAX_BROKER_STATE_BYTES = 64 * 1024;
const ENDED_OWNER_MARKER_PREFIX = `${BROKER_STATE_FILE}.ended-`;

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

function isLockOwnerAlive(token) {
  const pid = Number.parseInt(token?.split("-", 1)[0] ?? "", 10);
  if (!Number.isSafeInteger(pid) || pid <= 0) {
    return true;
  }
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error?.code !== "ESRCH";
  }
}

function reclaimBarrierPrefix(lockDir) {
  return `${path.basename(lockDir)}.reclaim-`;
}

function recoverBrokerReclaimBarriers(lockDir) {
  const parent = path.dirname(lockDir);
  const prefix = reclaimBarrierPrefix(lockDir);
  let blocked = false;
  for (const entry of fs.readdirSync(parent, { withFileTypes: true })) {
    if (!entry.isDirectory() || !entry.name.startsWith(prefix)) {
      continue;
    }
    const barrier = path.join(parent, entry.name);
    const reclaimerPid = Number.parseInt(entry.name.slice(prefix.length).split("-", 1)[0], 10);
    if (isLockOwnerAlive(`${reclaimerPid}-reclaimer`)) {
      blocked = true;
      continue;
    }
    const movedLock = path.join(barrier, "lock");
    if (fs.existsSync(movedLock)) {
      let ownerToken = null;
      try {
        ownerToken = fs.readFileSync(path.join(movedLock, "owner"), "utf8");
      } catch {
        // An unreadable moved lock is preserved conservatively.
      }
      if (!ownerToken || isLockOwnerAlive(ownerToken)) {
        if (!fs.existsSync(lockDir)) {
          try {
            fs.renameSync(movedLock, lockDir);
            fs.rmSync(barrier, { recursive: true, force: true });
            continue;
          } catch {
            // Another process restored or published the canonical lock.
          }
        }
        blocked = true;
        continue;
      }
    }
    fs.rmSync(barrier, { recursive: true, force: true });
  }
  return blocked;
}

function releaseOwnedBrokerLock(lockDir, token) {
  const tokenFile = path.join(lockDir, "owner");
  try {
    if (fs.readFileSync(tokenFile, "utf8") === token) {
      fs.rmSync(lockDir, { recursive: true, force: true });
      return;
    }
  } catch {
    // A reclaimer may have moved the lock behind a visible barrier.
  }
  const parent = path.dirname(lockDir);
  const prefix = reclaimBarrierPrefix(lockDir);
  for (const entry of fs.readdirSync(parent, { withFileTypes: true })) {
    if (!entry.isDirectory() || !entry.name.startsWith(prefix)) {
      continue;
    }
    const barrier = path.join(parent, entry.name);
    const movedLock = path.join(barrier, "lock");
    try {
      if (fs.readFileSync(path.join(movedLock, "owner"), "utf8") === token) {
        fs.rmSync(movedLock, { recursive: true, force: true });
        fs.rmSync(barrier, { recursive: true, force: true });
        break;
      }
    } catch {
      // This barrier belongs to another lock generation.
    }
  }
  // Orphan recovery can restore this generation while release is inspecting
  // the moved path. Recheck the canonical token before returning.
  try {
    if (fs.readFileSync(tokenFile, "utf8") === token) {
      fs.rmSync(lockDir, { recursive: true, force: true });
    }
  } catch {
    // This owner no longer has a published lock generation.
  }
}

async function withBrokerStateFileLock(stateFile, fn, options = {}) {
  const lockDir = `${stateFile}.lock`;
  const tokenFile = path.join(lockDir, "owner");
  const token = `${process.pid}-${brokerLockTokenSeq += 1}`;
  const timeoutMs = options.timeoutMs ?? BROKER_STATE_LOCK_TIMEOUT_MS;
  const staleMs = options.staleMs ?? BROKER_STATE_LOCK_STALE_MS;
  const deadline = Date.now() + timeoutMs;
  fs.mkdirSync(path.dirname(stateFile), { recursive: true });

  while (true) {
    if (recoverBrokerReclaimBarriers(lockDir)) {
      if (Date.now() >= deadline) {
        throw Object.assign(new Error(`Timed out waiting for broker state lock: ${stateFile}`), {
          code: BROKER_LOCK_TIMEOUT_CODE
        });
      }
      await sleep(25);
      continue;
    }
    let candidate = null;
    try {
      if (fs.existsSync(lockDir)) {
        throw Object.assign(new Error(`Broker state lock exists: ${stateFile}`), { code: "EEXIST" });
      }
      candidate = fs.mkdtempSync(`${lockDir}.candidate-${process.pid}-`);
      fs.writeFileSync(path.join(candidate, "owner"), token, { encoding: "utf8", flag: "wx" });
      fs.renameSync(candidate, lockDir);
      candidate = null;
      if (recoverBrokerReclaimBarriers(lockDir)) {
        if (fs.readFileSync(tokenFile, "utf8") === token) {
          fs.rmSync(lockDir, { recursive: true, force: true });
        }
        await sleep(25);
        continue;
      }
      break;
    } catch (error) {
      if (candidate) {
        fs.rmSync(candidate, { recursive: true, force: true });
      }
      if (!fs.existsSync(lockDir)) {
        if (["EEXIST", "ENOTEMPTY", "EPERM"].includes(error?.code)) {
          if (Date.now() >= deadline) {
            throw Object.assign(new Error(`Timed out waiting for broker state lock: ${stateFile}`), {
              code: BROKER_LOCK_TIMEOUT_CODE
            });
          }
          await sleep(25);
          continue;
        }
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
        let removed = false;
        try {
          // unlinkSync cannot remove a directory. If another contender replaced
          // the invalid path with a real lock after lstatSync, this fails safely
          // instead of renaming or deleting that contender's lock.
          fs.unlinkSync(lockDir);
          removed = true;
        } catch {
          // Lost the replacement race or cannot remove the invalid path. Retry
          // through the normal deadline/backoff path below.
        }
        if (removed) {
          continue;
        }
        stat = null;
      }
      if (stat && Date.now() - stat.mtimeMs > staleMs) {
        let ownerToken = null;
        try {
          ownerToken = fs.readFileSync(tokenFile, "utf8");
        } catch {
          // Canonical locks are atomically published with an owner token.
          // An unreadable token is preserved rather than reclaimed unsafely.
        }
        if (ownerToken && !isLockOwnerAlive(ownerToken)) {
          const barrier = fs.mkdtempSync(`${lockDir}.reclaim-${process.pid}-`);
          const claimed = path.join(barrier, "lock");
          let reclaimed = false;
          try {
            const currentToken = fs.readFileSync(tokenFile, "utf8");
            if (currentToken !== ownerToken) {
              continue;
            }
            fs.renameSync(lockDir, claimed);
            if (fs.readFileSync(path.join(claimed, "owner"), "utf8") === ownerToken &&
                !isLockOwnerAlive(ownerToken)) {
              fs.rmSync(claimed, { recursive: true, force: true });
              reclaimed = true;
            } else {
              while (fs.existsSync(lockDir) && Date.now() < deadline) {
                await sleep(25);
              }
              if (!fs.existsSync(lockDir)) {
                fs.renameSync(claimed, lockDir);
              }
            }
          } catch {
            // Keep the barrier visible until the moved lock is restored or a
            // later process recovers it after this reclaimer exits.
          } finally {
            if (!fs.existsSync(claimed)) {
              fs.rmSync(barrier, { recursive: true, force: true });
            }
          }
          if (reclaimed) {
            continue;
          }
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

  try {
    return await fn();
  } finally {
    releaseOwnedBrokerLock(lockDir, token);
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
  if (timeoutMs <= 0) {
    return;
  }
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
    timer = setTimeout(() => {
      socket.destroy();
      finish();
    }, timeoutMs);
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

function readBoundedUtf8(descriptor, maxBytes) {
  const buffer = Buffer.allocUnsafe(maxBytes + 1);
  const bytesRead = fs.readSync(descriptor, buffer, 0, buffer.length, 0);
  return bytesRead > maxBytes ? null : buffer.toString("utf8", 0, bytesRead);
}

function readBrokerStateFile(stateFile) {
  let descriptor = null;
  try {
    const before = fs.lstatSync(stateFile);
    if (!before.isFile() || before.size > MAX_BROKER_STATE_BYTES) {
      return null;
    }
    const noFollow = fs.constants.O_NOFOLLOW ?? 0;
    const nonBlock = fs.constants.O_NONBLOCK ?? 0;
    descriptor = fs.openSync(stateFile, fs.constants.O_RDONLY | noFollow | nonBlock);
    const opened = fs.fstatSync(descriptor);
    if (!opened.isFile() || opened.size > MAX_BROKER_STATE_BYTES ||
        opened.dev !== before.dev || opened.ino !== before.ino) {
      return null;
    }
    const contents = readBoundedUtf8(descriptor, MAX_BROKER_STATE_BYTES);
    return contents == null ? null : JSON.parse(contents);
  } catch {
    return null;
  } finally {
    if (descriptor != null) {
      try {
        fs.closeSync(descriptor);
      } catch {
        // Ignore a descriptor already closed by a concurrent test shim.
      }
    }
  }
}

export function loadBrokerSession(cwd) {
  return readBrokerStateFile(resolveBrokerStateFile(cwd));
}

function endedOwnerMarkerFile(stateFile, sessionId) {
  const digest = createHash("sha256").update(sessionId).digest("hex");
  return path.join(path.dirname(stateFile), `${ENDED_OWNER_MARKER_PREFIX}${digest}`);
}

function recordEndedBrokerOwner(stateFile, sessionId) {
  if (!sessionId || Buffer.byteLength(sessionId, "utf8") > 1024) {
    return;
  }
  fs.mkdirSync(path.dirname(stateFile), { recursive: true });
  try {
    fs.writeFileSync(endedOwnerMarkerFile(stateFile, sessionId), sessionId, { encoding: "utf8", flag: "wx" });
  } catch (error) {
    if (error?.code !== "EEXIST") {
      throw error;
    }
  }
}

function applyEndedBrokerOwners(stateFile, session) {
  const markerFiles = [];
  const endedOwners = new Set();
  for (const entry of fs.readdirSync(path.dirname(stateFile), { withFileTypes: true })) {
    if (!entry.isFile() || !entry.name.startsWith(ENDED_OWNER_MARKER_PREFIX)) {
      continue;
    }
    const markerFile = path.join(path.dirname(stateFile), entry.name);
    let descriptor = null;
    try {
      const before = fs.lstatSync(markerFile);
      if (!before.isFile() || before.size > 1024) {
        continue;
      }
      const noFollow = fs.constants.O_NOFOLLOW ?? 0;
      const nonBlock = fs.constants.O_NONBLOCK ?? 0;
      descriptor = fs.openSync(markerFile, fs.constants.O_RDONLY | noFollow | nonBlock);
      const opened = fs.fstatSync(descriptor);
      if (!opened.isFile() || opened.size > 1024 ||
          opened.dev !== before.dev || opened.ino !== before.ino) {
        continue;
      }
      const owner = readBoundedUtf8(descriptor, 1024);
      const expectedName = owner
        ? `${ENDED_OWNER_MARKER_PREFIX}${createHash("sha256").update(owner).digest("hex")}`
        : null;
      if (owner && entry.name === expectedName) {
        endedOwners.add(owner);
        markerFiles.push(markerFile);
      }
    } catch {
      // Leave unreadable markers for a later cleanup attempt.
    } finally {
      if (descriptor != null) {
        try {
          fs.closeSync(descriptor);
        } catch {
          // Ignore a descriptor already closed by a concurrent test shim.
        }
      }
    }
  }
  if (endedOwners.size === 0) {
    return { session, markerFiles };
  }
  if (!session) {
    return { session, markerFiles };
  }
  const owners = brokerSessionOwners(session).filter((owner) => !endedOwners.has(owner));
  return {
    session: {
      ...session,
      sessionId: owners[0] ?? null,
      sessionIds: owners
    },
    markerFiles
  };
}

function clearEndedOwnerMarkers(markerFiles) {
  for (const markerFile of markerFiles) {
    try {
      fs.unlinkSync(markerFile);
    } catch {
      // A later locked pass can safely retry stale marker cleanup.
    }
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
  const stateFile = resolveBrokerStateFile(cwd);
  const pending = applyEndedBrokerOwners(stateFile, loadBrokerSession(cwd));
  const current = pending.session;
  if (current && (await isBrokerEndpointReady(current.endpoint))) {
    const withOwner = withBrokerSessionOwner(current, resolveSessionId(options));
    if (withOwner !== current || pending.markerFiles.length > 0) {
      saveBrokerSession(cwd, withOwner);
    }
    clearEndedOwnerMarkers(pending.markerFiles);
    return withOwner;
  }
  discardBrokerSession(cwd, current, options);
  clearEndedOwnerMarkers(pending.markerFiles);

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
      const pending = applyEndedBrokerOwners(stateFile, loadBrokerSession(cwd));
      const current = pending.session;
      if (!current || current.endpoint !== existing.endpoint) {
        return null;
      }
      const withOwner = withBrokerSessionOwner(current, resolveSessionId(options));
      if (withOwner !== current || pending.markerFiles.length > 0) {
        saveBrokerSession(cwd, withOwner);
      }
      clearEndedOwnerMarkers(pending.markerFiles);
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

export async function teardownBrokerForCwd(
  cwd,
  sessionId,
  {
    fallbackSession = null,
    killProcess = null,
    shutdownTimeoutMs = BROKER_SHUTDOWN_TIMEOUT_MS,
    lockTimeoutMs = BROKER_STATE_LOCK_TIMEOUT_MS
  } = {}
) {
  const stateFile = resolveBrokerStateFile(cwd);
  if (!fs.existsSync(stateFile) && !fallbackSession) {
    return false;
  }
  fs.mkdirSync(resolveStateDir(cwd), { recursive: true });
  // Publish the end intent before waiting for the state lock. A lock holder
  // that completes while this hook is waiting can then reconcile the owner,
  // and a timeout cannot leave a tombstone that arrived after the final pass.
  recordEndedBrokerOwner(stateFile, sessionId);

  try {
    return await withBrokerStateFileLock(
      stateFile,
      async () => {
        const pending = applyEndedBrokerOwners(stateFile, loadBrokerSession(cwd));
        const current = pending.session;
        const owners = brokerSessionOwners(current);
        if (sessionId && owners.length > 0) {
          if (!owners.includes(sessionId)) {
            if (pending.markerFiles.length > 0) {
              writeBrokerStateFile(stateFile, current);
              clearEndedOwnerMarkers(pending.markerFiles);
            }
            return false;
          }
          const remainingOwners = owners.filter((owner) => owner !== sessionId);
          if (remainingOwners.length > 0) {
            writeBrokerStateFile(stateFile, {
              ...current,
              sessionId: remainingOwners[0],
              sessionIds: remainingOwners
            });
            clearEndedOwnerMarkers(pending.markerFiles);
            return false;
          }
        }

        const session = current ?? fallbackSession;
        if (session?.endpoint) {
          await sendBrokerShutdown(session.endpoint, { timeoutMs: shutdownTimeoutMs });
        }
        teardownBrokerSession({
          endpoint: session?.endpoint ?? null,
          pidFile: session?.pidFile ?? null,
          logFile: session?.logFile ?? null,
          sessionDir: session?.sessionDir ?? null,
          pid: session?.pid ?? null,
          killProcess
        });
        if (fs.existsSync(stateFile)) {
          fs.unlinkSync(stateFile);
        }
        clearEndedOwnerMarkers(pending.markerFiles);
        return true;
      },
      { timeoutMs: lockTimeoutMs }
    );
  } catch (error) {
    if (isBrokerLockTimeout(error)) {
      return false;
    }
    throw error;
  }
}

export async function teardownBrokersForSession(
  sessionId,
  {
    killProcess = null,
    shutdownTimeoutMs = BROKER_SHUTDOWN_TIMEOUT_MS,
    lockTimeoutMs = BROKER_STATE_LOCK_TIMEOUT_MS,
    budgetMs = null,
    excludeCwd = null
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
  const excludedStateFile = excludeCwd ? resolveBrokerStateFile(excludeCwd) : null;
  for (const entry of fs.readdirSync(stateRoot, { withFileTypes: true })) {
    if (deadline != null && Date.now() >= deadline) {
      break;
    }
    if (!entry.isDirectory() || entry.isSymbolicLink()) {
      continue;
    }
    const stateFile = path.join(stateRoot, entry.name, BROKER_STATE_FILE);
    if (stateFile === excludedStateFile) {
      continue;
    }
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
      preview = readBrokerStateFile(stateFile);
    } catch {
      preview = null;
    }
    if (preview && !brokerSessionOwners(preview).includes(sessionId)) {
      continue;
    }
    recordEndedBrokerOwner(stateFile, sessionId);

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

          const pending = applyEndedBrokerOwners(stateFile, readBrokerStateFile(stateFile));
          const session = pending.session;
          if (!session) {
            clearEndedOwnerMarkers(pending.markerFiles);
            return;
          }
          const owners = brokerSessionOwners(session);
          if (!owners.includes(sessionId)) {
            if (pending.markerFiles.length > 0) {
              if (owners.length === 0) {
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
              } else {
                writeBrokerStateFile(stateFile, session);
              }
              clearEndedOwnerMarkers(pending.markerFiles);
            }
            return;
          }
          const remainingOwners = owners.filter((owner) => owner !== sessionId);
          if (remainingOwners.length > 0) {
            writeBrokerStateFile(stateFile, {
              ...session,
              sessionId: remainingOwners[0],
              sessionIds: remainingOwners
            });
            clearEndedOwnerMarkers(pending.markerFiles);
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
          clearEndedOwnerMarkers(pending.markerFiles);
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
