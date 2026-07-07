import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import test from "node:test";
import assert from "node:assert/strict";

import { makeTempDir } from "./helpers.mjs";
import { createBrokerEndpoint, parseBrokerEndpoint } from "../plugins/codex/scripts/lib/broker-endpoint.mjs";
import { resolveStateDir, resolveStateRoot } from "../plugins/codex/scripts/lib/state.mjs";
import {
  ensureBrokerSession,
  loadBrokerSession,
  resolveSessionId,
  resolveSessionPid,
  saveBrokerSession,
  teardownBrokersForSession
} from "../plugins/codex/scripts/lib/broker-lifecycle.mjs";
import { handleSessionEnd } from "../plugins/codex/scripts/session-lifecycle-hook.mjs";

function deadPid() {
  const result = spawnSync(process.execPath, ["-e", ""]);
  assert.equal(result.status, 0);
  return result.pid;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Minimal stand-in for app-server-broker.mjs: honors the spawn contract
// (serve --endpoint --cwd --pid-file) enough for waitForBrokerEndpoint.
const FAKE_BROKER_SCRIPT = `import fs from "node:fs";
import net from "node:net";

const args = process.argv.slice(2);
const get = (name) => args[args.indexOf(name) + 1];
const sockPath = get("--endpoint").replace(/^unix:/, "");
const server = net.createServer((socket) => socket.end());
server.listen(sockPath, () => {
  fs.writeFileSync(get("--pid-file"), String(process.pid), "utf8");
});
`;

function writeFakeBrokerScript() {
  const scriptPath = path.join(makeTempDir(), "fake-broker.mjs");
  fs.writeFileSync(scriptPath, FAKE_BROKER_SCRIPT, "utf8");
  return scriptPath;
}

const stateRootForTest = resolveStateRoot;

test("resolveStateRoot uses CLAUDE_PLUGIN_DATA/state when set", () => {
  const pluginData = makeTempDir();
  const prev = process.env.CLAUDE_PLUGIN_DATA;
  process.env.CLAUDE_PLUGIN_DATA = pluginData;
  try {
    assert.equal(resolveStateRoot(), path.join(pluginData, "state"));
  } finally {
    if (prev == null) delete process.env.CLAUDE_PLUGIN_DATA;
    else process.env.CLAUDE_PLUGIN_DATA = prev;
  }
});

test("resolveStateRoot falls back to a tmp dir when CLAUDE_PLUGIN_DATA is unset", () => {
  const prev = process.env.CLAUDE_PLUGIN_DATA;
  delete process.env.CLAUDE_PLUGIN_DATA;
  try {
    assert.equal(resolveStateRoot(), path.join(os.tmpdir(), "codex-companion"));
  } finally {
    if (prev != null) process.env.CLAUDE_PLUGIN_DATA = prev;
  }
});

test("resolveSessionId prefers explicit option, then env, then null", () => {
  assert.equal(resolveSessionId({ sessionId: "explicit" }), "explicit");
  assert.equal(resolveSessionId({ env: { CODEX_COMPANION_SESSION_ID: "from-env" } }), "from-env");
});

test("resolveSessionId reads process.env when no option/env given", () => {
  const prev = process.env.CODEX_COMPANION_SESSION_ID;
  process.env.CODEX_COMPANION_SESSION_ID = "proc-env";
  try {
    assert.equal(resolveSessionId({}), "proc-env");
  } finally {
    if (prev == null) delete process.env.CODEX_COMPANION_SESSION_ID;
    else process.env.CODEX_COMPANION_SESSION_ID = prev;
  }
});

test("resolveSessionId returns null when nothing is set", () => {
  const prev = process.env.CODEX_COMPANION_SESSION_ID;
  delete process.env.CODEX_COMPANION_SESSION_ID;
  try {
    assert.equal(resolveSessionId({ env: {} }), null);
  } finally {
    if (prev != null) process.env.CODEX_COMPANION_SESSION_ID = prev;
  }
});

function writeBrokerJson(stateRoot, dirName, session) {
  const dir = path.join(stateRoot, dirName);
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, "broker.json");
  fs.writeFileSync(file, JSON.stringify(session), "utf8");
  return file;
}

function withPluginData(fn) {
  const pluginData = makeTempDir();
  const prev = process.env.CLAUDE_PLUGIN_DATA;
  process.env.CLAUDE_PLUGIN_DATA = pluginData;
  return Promise.resolve(fn(pluginData)).finally(() => {
    if (prev == null) delete process.env.CLAUDE_PLUGIN_DATA;
    else process.env.CLAUDE_PLUGIN_DATA = prev;
  });
}

async function withReadyBroker(fn) {
  const sessionDir = makeTempDir();
  const endpoint = createBrokerEndpoint(sessionDir);
  const target = parseBrokerEndpoint(endpoint);
  const requests = [];
  const server = net.createServer((socket) => {
    socket.setEncoding("utf8");
    socket.on("data", (chunk) => {
      requests.push(chunk);
      socket.write(`${JSON.stringify({ id: 1, result: {} })}\n`);
      socket.end();
    });
  });

  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(target.path, () => {
      server.off("error", reject);
      resolve();
    });
  });

  try {
    return await fn({ endpoint, requests, sessionDir });
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

async function withHangingBroker(fn) {
  const sessionDir = makeTempDir();
  const endpoint = createBrokerEndpoint(sessionDir);
  const target = parseBrokerEndpoint(endpoint);
  const requests = [];
  const sockets = [];
  const server = net.createServer((socket) => {
    sockets.push(socket);
    socket.setEncoding("utf8");
    socket.on("data", (chunk) => {
      requests.push(chunk);
    });
  });

  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(target.path, () => {
      server.off("error", reject);
      resolve();
    });
  });

  const destroySockets = () => {
    for (const socket of sockets) {
      socket.destroy();
    }
  };

  try {
    return await fn({ endpoint, requests, sessionDir, destroySockets });
  } finally {
    destroySockets();
    await new Promise((resolve) => server.close(resolve));
  }
}

function waitForChild(child) {
  return new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      resolve({ code, signal });
    });
  });
}

test("teardownBrokersForSession tears down a broker registered for a different cwd", async () => {
  await withPluginData(async () => {
    const stateRoot = stateRootForTest();
    const sessionDir = makeTempDir();
    const pidFile = path.join(sessionDir, "broker.pid");
    const logFile = path.join(sessionDir, "broker.log");
    fs.writeFileSync(pidFile, "12345\n");
    fs.writeFileSync(logFile, "");
    const brokerJson = writeBrokerJson(stateRoot, "worktree-deadbeefdeadbeef", {
      endpoint: "unix:/tmp/codex-test-nonexistent.sock",
      pidFile, logFile, sessionDir, pid: 12345, sessionId: "S"
    });

    const killed = [];
    const count = await teardownBrokersForSession("S", { killProcess: (pid) => killed.push(pid) });

    assert.equal(count, 1);
    assert.deepEqual(killed, [12345]);
    assert.equal(fs.existsSync(brokerJson), false);
  });
});

test("teardownBrokersForSession leaves non-matching sessionId brokers intact", async () => {
  await withPluginData(async () => {
    const stateRoot = stateRootForTest();
    const brokerJson = writeBrokerJson(stateRoot, "other-1111111111111111", {
      endpoint: "unix:/tmp/codex-test-nonexistent2.sock",
      pidFile: null, logFile: null, sessionDir: null, pid: null, sessionId: "S"
    });
    const count = await teardownBrokersForSession("OTHER", { killProcess: () => {} });
    assert.equal(count, 0);
    assert.equal(fs.existsSync(brokerJson), true);
  });
});

test("teardownBrokersForSession ignores broker.json without sessionId (legacy)", async () => {
  await withPluginData(async () => {
    const stateRoot = stateRootForTest();
    const brokerJson = writeBrokerJson(stateRoot, "legacy-2222222222222222", {
      endpoint: "unix:/tmp/codex-test-nonexistent3.sock", pid: null
    });
    const count = await teardownBrokersForSession("S", { killProcess: () => {} });
    assert.equal(count, 0);
    assert.equal(fs.existsSync(brokerJson), true);
  });
});

test("teardownBrokersForSession is a no-op for empty sessionId", async () => {
  await withPluginData(async () => {
    const count = await teardownBrokersForSession("", { killProcess: () => {} });
    assert.equal(count, 0);
  });
});

test("reusing a ready broker transfers cleanup ownership to the later session", async () => {
  await withPluginData(async () => {
    await withReadyBroker(async ({ endpoint, requests, sessionDir }) => {
      const cwd = makeTempDir();
      const pidFile = path.join(sessionDir, "broker.pid");
      const logFile = path.join(sessionDir, "broker.log");
      fs.writeFileSync(pidFile, "12345\n");
      fs.writeFileSync(logFile, "");
      saveBrokerSession(cwd, {
        endpoint,
        pidFile,
        logFile,
        sessionDir,
        pid: 12345,
        sessionId: "A"
      });

      const reused = await ensureBrokerSession(cwd, { env: { CODEX_COMPANION_SESSION_ID: "B" } });
      assert.equal(reused.endpoint, endpoint);
      assert.deepEqual(loadBrokerSession(cwd).sessionIds, ["A", "B"]);

      const killed = [];
      assert.equal(await teardownBrokersForSession("A", { killProcess: (pid) => killed.push(pid) }), 0);
      assert.deepEqual(killed, []);
      assert.equal(loadBrokerSession(cwd).sessionId, "B");
      assert.deepEqual(loadBrokerSession(cwd).sessionIds, ["B"]);
      assert.equal(requests.length, 0);

      assert.equal(await teardownBrokersForSession("B", { killProcess: (pid) => killed.push(pid) }), 1);
      assert.deepEqual(killed, [12345]);
      assert.equal(loadBrokerSession(cwd), null);
      assert.equal(requests.length, 1);
    });
  });
});

test("handleSessionEnd removes only the ending owner from a shared cwd broker", async () => {
  await withPluginData(async () => {
    await withReadyBroker(async ({ endpoint, requests, sessionDir }) => {
      const cwd = makeTempDir();
      const pidFile = path.join(sessionDir, "broker.pid");
      const logFile = path.join(sessionDir, "broker.log");
      fs.writeFileSync(pidFile, "12345\n");
      fs.writeFileSync(logFile, "");
      saveBrokerSession(cwd, {
        endpoint,
        pidFile,
        logFile,
        sessionDir,
        pid: 12345,
        sessionId: "A",
        sessionIds: ["A", "B"]
      });

      await handleSessionEnd({ cwd, session_id: "A" });

      assert.equal(loadBrokerSession(cwd).sessionId, "B");
      assert.deepEqual(loadBrokerSession(cwd).sessionIds, ["B"]);
      assert.equal(requests.length, 0);
    });
  });
});

test("concurrent SessionEnd hooks tear down a shared broker after the last owner exits", async () => {
  await withPluginData(async (pluginData) => {
    await withReadyBroker(async ({ endpoint, requests, sessionDir }) => {
      const cwd = makeTempDir();
      const markerDir = makeTempDir();
      const pidFile = path.join(sessionDir, "broker.pid");
      const logFile = path.join(sessionDir, "broker.log");
      fs.writeFileSync(pidFile, "12345\n");
      fs.writeFileSync(logFile, "");
      const brokerJson = writeBrokerJson(stateRootForTest(), "worktree-race-deadbeef", {
        endpoint,
        pidFile,
        logFile,
        sessionDir,
        pid: 12345,
        sessionId: "A",
        sessionIds: ["A", "B"]
      });

      const moduleUrl = pathToFileURL(path.resolve("plugins/codex/scripts/lib/broker-lifecycle.mjs")).href;
      const script = `
        import fs from "node:fs";
        import path from "node:path";

        const stateFile = process.env.TEST_BROKER_STATE_FILE;
        const markerDir = process.env.TEST_MARKER_DIR;
        const sessionId = process.env.TEST_SESSION_ID;
        const otherSessionId = sessionId === "A" ? "B" : "A";
        const originalWriteFileSync = fs.writeFileSync.bind(fs);

        fs.writeFileSync = (file, data, ...args) => {
          if (file === stateFile && String(data).includes('"sessionIds"')) {
            originalWriteFileSync(path.join(markerDir, sessionId + ".ready"), "", "utf8");
            const otherReady = path.join(markerDir, otherSessionId + ".ready");
            const deadline = Date.now() + 500;
            while (!fs.existsSync(otherReady) && Date.now() < deadline) {}
          }
          return originalWriteFileSync(file, data, ...args);
        };

        const { teardownBrokersForSession } = await import(process.env.TEST_BROKER_MODULE_URL);
        await teardownBrokersForSession(sessionId, { killProcess: () => {} });
      `;

      const makeChild = (sessionId) =>
        spawn(process.execPath, ["--input-type=module", "-e", script], {
          cwd: path.resolve("."),
          env: {
            ...process.env,
            CLAUDE_PLUGIN_DATA: pluginData,
            TEST_BROKER_STATE_FILE: brokerJson,
            TEST_BROKER_MODULE_URL: moduleUrl,
            TEST_MARKER_DIR: markerDir,
            TEST_SESSION_ID: sessionId
          },
          stdio: ["ignore", "pipe", "pipe"]
        });

      const childA = makeChild("A");
      const childB = makeChild("B");
      const [resultA, resultB] = await Promise.all([waitForChild(childA), waitForChild(childB)]);

      assert.deepEqual([resultA.code, resultB.code], [0, 0]);
      assert.equal(fs.existsSync(brokerJson), false);
      assert.equal(requests.length, 1);
    });
  });
});

test("handleSessionEnd still tears down session brokers when job cleanup fails", async () => {
  await withPluginData(async () => {
    const cwd = makeTempDir();
    const workspaceStateDir = resolveStateDir(cwd);
    saveBrokerSession(cwd, {
      endpoint: "unix:/tmp/codex-test-nonexistent-cleanup.sock",
      pidFile: null,
      logFile: null,
      sessionDir: null,
      pid: null,
      sessionId: "S",
      sessionIds: ["S"]
    });
    const badLogFile = path.join(workspaceStateDir, "bad.log");
    fs.mkdirSync(badLogFile);
    const stateFile = path.join(workspaceStateDir, "state.json");
    fs.writeFileSync(
      stateFile,
      `${JSON.stringify({
        version: 1,
        config: { stopReviewGate: false },
        jobs: [{ id: "completed", status: "completed", sessionId: "S", logFile: badLogFile }]
      }, null, 2)}\n`,
      "utf8"
    );

    await assert.rejects(() => handleSessionEnd({ cwd, session_id: "S" }), { code: "EISDIR" });
    assert.equal(fs.existsSync(path.join(workspaceStateDir, "broker.json")), false);
  });
});

test("teardownBrokersForSession times out unresponsive broker shutdown requests", async () => {
  await withPluginData(async () => {
    await withHangingBroker(async ({ endpoint, requests, sessionDir, destroySockets }) => {
      const stateRoot = stateRootForTest();
      const brokerJson = writeBrokerJson(stateRoot, "hanging-shutdown-deadbeef", {
        endpoint,
        pidFile: null,
        logFile: null,
        sessionDir,
        pid: null,
        sessionId: "S",
        sessionIds: ["S"]
      });

      let completed = false;
      const teardown = teardownBrokersForSession("S", { killProcess: () => {}, shutdownTimeoutMs: 50 })
        .then(() => {
          completed = true;
        });
      try {
        await new Promise((resolve) => setTimeout(resolve, 150));
        assert.equal(completed, true);
      } finally {
        destroySockets();
        await teardown;
      }
      assert.equal(fs.existsSync(brokerJson), false);
      assert.equal(requests.length, 1);
    });
  });
});

test("resolveSessionPid prefers explicit option, then env, then null", () => {
  assert.equal(resolveSessionPid({ sessionPid: 4321 }), 4321);
  assert.equal(resolveSessionPid({ env: { CODEX_COMPANION_SESSION_PID: "1234" } }), 1234);
  assert.equal(resolveSessionPid({ env: { CODEX_COMPANION_SESSION_PID: "not-a-pid" } }), null);
  assert.equal(resolveSessionPid({ env: {} }), null);
});

test("teardownBrokersForSession prunes a dead co-owner and tears down the broker", async () => {
  await withPluginData(async () => {
    await withReadyBroker(async ({ endpoint, requests, sessionDir }) => {
      const stateRoot = stateRootForTest();
      const gonePid = deadPid();
      const brokerJson = writeBrokerJson(stateRoot, "worktree-deadowner01dead", {
        endpoint,
        pidFile: null,
        logFile: null,
        sessionDir,
        pid: null,
        sessionId: "DEAD",
        sessionIds: ["DEAD", "B"],
        sessionPids: { DEAD: gonePid }
      });

      // DEAD's session pid is gone, so B is effectively the last live owner:
      // its SessionEnd must shut the broker down instead of leaving it behind.
      const count = await teardownBrokersForSession("B", { killProcess: () => {} });

      assert.equal(count, 1);
      assert.equal(fs.existsSync(brokerJson), false);
      assert.equal(requests.length, 1);
    });
  });
});

test("teardownBrokersForSession keeps a live co-owner and its recorded pid", async () => {
  await withPluginData(async () => {
    const stateRoot = stateRootForTest();
    const brokerJson = writeBrokerJson(stateRoot, "worktree-liveowner1live", {
      endpoint: "unix:/tmp/codex-test-nonexistent5.sock",
      pidFile: null,
      logFile: null,
      sessionDir: null,
      pid: null,
      sessionId: "LIVE",
      sessionIds: ["LIVE", "B"],
      sessionPids: { LIVE: process.pid }
    });

    const count = await teardownBrokersForSession("B", { killProcess: () => {} });

    assert.equal(count, 0);
    const session = JSON.parse(fs.readFileSync(brokerJson, "utf8"));
    assert.equal(session.sessionId, "LIVE");
    assert.deepEqual(session.sessionIds, ["LIVE"]);
    assert.deepEqual(session.sessionPids, { LIVE: process.pid });
  });
});

test("handleSessionEnd falls through to cwd teardown when the only recorded owner is dead", async () => {
  await withPluginData(async () => {
    const cwd = makeTempDir();
    const gonePid = deadPid();
    saveBrokerSession(cwd, {
      endpoint: "unix:/tmp/codex-test-nonexistent6.sock",
      pidFile: null,
      logFile: null,
      sessionDir: null,
      pid: null,
      sessionId: "DEAD",
      sessionIds: ["DEAD"],
      sessionPids: { DEAD: gonePid }
    });

    // The ending session never owned this broker, but its sole owner's session
    // pid is gone — the legacy cwd path must reclaim it rather than early-return.
    await handleSessionEnd({ cwd, session_id: "OTHER" });

    assert.equal(loadBrokerSession(cwd), null);
  });
});

test("ensureBrokerSession records the reusing session's pid for liveness checks", async () => {
  await withPluginData(async () => {
    await withReadyBroker(async ({ endpoint, sessionDir }) => {
      const cwd = makeTempDir();
      saveBrokerSession(cwd, {
        endpoint,
        pidFile: null,
        logFile: null,
        sessionDir,
        pid: null,
        sessionId: "A"
      });

      const reused = await ensureBrokerSession(cwd, {
        env: { CODEX_COMPANION_SESSION_ID: "B", CODEX_COMPANION_SESSION_PID: String(process.pid) }
      });

      assert.deepEqual(reused.sessionIds, ["A", "B"]);
      assert.deepEqual(loadBrokerSession(cwd).sessionPids, { B: process.pid });
    });
  });
});

test("ensureBrokerSession does not resurrect a broker torn down while waiting for the state lock", async () => {
  await withPluginData(async () => {
    await withReadyBroker(async ({ endpoint, sessionDir }) => {
      const cwd = makeTempDir();
      saveBrokerSession(cwd, {
        endpoint,
        pidFile: null,
        logFile: null,
        sessionDir,
        pid: null,
        sessionId: "A",
        sessionIds: ["A"]
      });
      const stateFile = path.join(resolveStateDir(cwd), "broker.json");
      fs.mkdirSync(`${stateFile}.lock`);

      const pending = ensureBrokerSession(cwd, {
        env: { CODEX_COMPANION_SESSION_ID: "B" },
        scriptPath: writeFakeBrokerScript()
      });

      // While B is parked on the lock (readiness probe already passed), A's
      // SessionEnd shuts the broker down and removes broker.json.
      await sleep(200);
      fs.unlinkSync(stateFile);
      fs.rmSync(parseBrokerEndpoint(endpoint).path, { force: true });
      fs.rmdirSync(`${stateFile}.lock`);

      const session = await pending;
      try {
        assert.ok(session);
        assert.notEqual(session.endpoint, endpoint);
        assert.deepEqual(session.sessionIds, ["B"]);
        assert.equal(loadBrokerSession(cwd).endpoint, session.endpoint);
      } finally {
        if (session?.pid) {
          try {
            process.kill(session.pid);
          } catch {
            // Already gone.
          }
        }
      }
    });
  });
});

test("ensureBrokerSession reuses a live replacement broker after losing the lock race", async () => {
  await withPluginData(async () => {
    await withReadyBroker(async ({ endpoint: staleEndpoint, sessionDir: staleDir }) => {
      await withReadyBroker(async ({ endpoint: freshEndpoint, sessionDir: freshDir }) => {
        const cwd = makeTempDir();
        saveBrokerSession(cwd, {
          endpoint: staleEndpoint,
          pidFile: null,
          logFile: null,
          sessionDir: staleDir,
          pid: null,
          sessionId: "A",
          sessionIds: ["A"]
        });
        const stateFile = path.join(resolveStateDir(cwd), "broker.json");
        fs.mkdirSync(`${stateFile}.lock`);

        const pending = ensureBrokerSession(cwd, { env: { CODEX_COMPANION_SESSION_ID: "B" } });

        // While B waits, the stale broker is replaced by a different live one.
        await sleep(200);
        saveBrokerSession(cwd, {
          endpoint: freshEndpoint,
          pidFile: null,
          logFile: null,
          sessionDir: freshDir,
          pid: null,
          sessionId: "C",
          sessionIds: ["C"]
        });
        fs.rmdirSync(`${stateFile}.lock`);

        const session = await pending;
        assert.equal(session.endpoint, freshEndpoint);
        assert.deepEqual(session.sessionIds, ["C", "B"]);
        assert.deepEqual(loadBrokerSession(cwd).sessionIds, ["C", "B"]);
      });
    });
  });
});

test("handleSessionEnd tears down broker even when cwd mismatches (regression #380)", async () => {
  await withPluginData(async () => {
    const stateRoot = stateRootForTest();
    const sessionDir = makeTempDir();
    const pidFile = path.join(sessionDir, "broker.pid");
    fs.writeFileSync(pidFile, "999999999\n"); // non-existent pid, harmless to signal
    const brokerJson = writeBrokerJson(stateRoot, "worktree-33333333deadbeef", {
      endpoint: "unix:/tmp/codex-test-nonexistent4.sock",
      pidFile, logFile: null, sessionDir, pid: 999999999, sessionId: "S"
    });

    // cwd is a DIFFERENT path than the broker's workspace — the cwd-based path
    // would miss; the session-based path must still tear it down.
    await handleSessionEnd({ cwd: makeTempDir(), session_id: "S" });

    assert.equal(fs.existsSync(brokerJson), false);
  });
});
