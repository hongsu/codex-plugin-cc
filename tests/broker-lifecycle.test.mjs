import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
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
  saveBrokerSession,
  teardownBrokersForSession
} from "../plugins/codex/scripts/lib/broker-lifecycle.mjs";
import { handleSessionEnd } from "../plugins/codex/scripts/session-lifecycle-hook.mjs";

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
