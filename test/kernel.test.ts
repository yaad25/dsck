import { test } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import {
  DSCKernel,
  BoundedLRUMemoryManager,
  InMemoryColdStoreDriver,
  FileColdStoreDriver,
  PersistentWALRecorder,
  EventStreamEmitter,
  GraphBuilder,
  ScopedTopologicalPlanner,
  StateNode,
  ASTExpression,
} from "../src/kernel";

function makeKernel(maxHotSessions = 10) {
  return new DSCKernel(new BoundedLRUMemoryManager(maxHotSessions, new InMemoryColdStoreDriver()));
}

function refAST(nodeId: string): ASTExpression {
  return { type: "REF", nodeId };
}

// ----------------------------------------------------------------------------
// Diamond dependency: A -> B -> C, A -> C directly.
// C must recompute exactly once, using the *updated* B.
// ----------------------------------------------------------------------------
test("diamond dependency: downstream node recomputes exactly once with fresh inputs", async () => {
  const kernel = makeKernel();
  const sessionId = "s1";

  const mm = (kernel as any).memoryManager as BoundedLRUMemoryManager;
  const session = await mm.getSession(sessionId);

  const nodeA: StateNode = { id: "A", value: 10, updatedAtTick: 0, accessCount: 0, dependents: ["B", "C"] };
  const nodeB: StateNode = {
    id: "B",
    value: 0,
    updatedAtTick: 0,
    accessCount: 0,
    dependents: ["C"],
    formulaAST: { type: "BINARY_OP", op: "MUL", left: refAST("A"), right: { type: "LITERAL", value: 2 } },
  };
  const nodeC: StateNode = {
    id: "C",
    value: 0,
    updatedAtTick: 0,
    accessCount: 0,
    dependents: [],
    formulaAST: { type: "BINARY_OP", op: "ADD", left: refAST("A"), right: refAST("B") },
  };
  session.nodes.set("A", nodeA);
  session.nodes.set("B", nodeB);
  session.nodes.set("C", nodeC);

  const result = await kernel.executeAtomicTick({
    sessionId,
    opcode: "MUTATE",
    targetNode: "A",
    payload: 10,
  });

  assert.equal(result.status, "COMMITTED");
  assert.equal(result.stateDiff["B"], 20);
  assert.equal(result.stateDiff["C"], 30);

  assert.equal(session.nodes.get("C")!.accessCount, 1);
});

// ----------------------------------------------------------------------------
// Cycle detection
// ----------------------------------------------------------------------------
test("scoped planner throws on a real cycle", () => {
  const nodes = new Map<string, StateNode>();
  nodes.set("X", { id: "X", value: 0, updatedAtTick: 0, accessCount: 0, dependents: ["Y"] });
  nodes.set("Y", { id: "Y", value: 0, updatedAtTick: 0, accessCount: 0, dependents: ["X"] });

  assert.throws(
    () => ScopedTopologicalPlanner.planExecutionOrder(nodes, "X"),
    /cycle/i
  );
});

test("kernel rolls back cleanly when a tick hits a cycle", async () => {
  const kernel = makeKernel();
  const sessionId = "s2";
  const mm = (kernel as any).memoryManager as BoundedLRUMemoryManager;
  const session = await mm.getSession(sessionId);

  session.nodes.set("X", { id: "X", value: 1, updatedAtTick: 0, accessCount: 0, dependents: ["Y"] });
  session.nodes.set("Y", { id: "Y", value: 2, updatedAtTick: 0, accessCount: 0, dependents: ["X"] });

  const before = JSON.stringify([...session.nodes.entries()]);
  const result = await kernel.executeAtomicTick({
    sessionId,
    opcode: "MUTATE",
    targetNode: "X",
    payload: 99,
  });

  assert.equal(result.status, "ROLLED_BACK");
  assert.match(result.error ?? "", /cycle/i);
  const after = JSON.stringify([...session.nodes.entries()]);
  assert.equal(after, before);
});

// ----------------------------------------------------------------------------
// Confidence gate: only enforced when explicitly opted into
// ----------------------------------------------------------------------------
test("confidenceScore is ignored unless a threshold is configured", async () => {
  const kernel = makeKernel();
  const result = await kernel.executeAtomicTick({
    sessionId: "s3",
    opcode: "MUTATE",
    targetNode: "A",
    payload: 5,
    confidenceScore: 0.01,
  });
  assert.equal(result.status, "COMMITTED");
});

test("confidenceScore below configured threshold requires fallback", async () => {
  const kernel = new DSCKernel(
    new BoundedLRUMemoryManager(10, new InMemoryColdStoreDriver()),
    { confidenceThreshold: 0.85 }
  );
  const result = await kernel.executeAtomicTick({
    sessionId: "s4",
    opcode: "MUTATE",
    targetNode: "A",
    payload: 5,
    confidenceScore: 0.5,
  });
  assert.equal(result.status, "JIT_FALLBACK_REQUIRED");
});

// ----------------------------------------------------------------------------
// LRU eviction: must never exceed maxHotSessions, even under a burst
// ----------------------------------------------------------------------------
test("bounded LRU never exceeds maxHotSessions even with many sessions created at once", async () => {
  const mm = new BoundedLRUMemoryManager(3, new InMemoryColdStoreDriver());

  for (let i = 0; i < 20; i++) {
    await mm.getSession(`session-${i}`);
    assert.ok(
      mm.hotSessionCount <= 3,
      `hotSessionCount ${mm.hotSessionCount} exceeded cap of 3 after session-${i}`
    );
  }
  assert.equal(mm.hotSessionCount, 3);
});

test("evicted session round-trips through cold storage", async () => {
  const cold = new InMemoryColdStoreDriver();
  const mm = new BoundedLRUMemoryManager(2, cold);

  const s1 = await mm.getSession("keep-me");
  s1.nodes.set("V", { id: "V", value: 42, updatedAtTick: 1, accessCount: 1, dependents: [] });

  await mm.getSession("s-b");
  await mm.getSession("s-c");
  await mm.getSession("s-d");

  const reloaded = await mm.getSession("keep-me");
  assert.equal(reloaded.nodes.get("V")?.value, 42);
});

// ----------------------------------------------------------------------------
// Concurrency: many concurrent ticks on the same session must not interleave
// ----------------------------------------------------------------------------
test("concurrent ticks on one session are serialized (no lost updates)", async () => {
  const kernel = makeKernel();
  const sessionId = "concurrent";

  const ticks = Array.from({ length: 50 }, (_, i) =>
    kernel.executeAtomicTick({
      sessionId,
      opcode: "MUTATE",
      targetNode: "counter",
      payload: i,
    })
  );
  const results = await Promise.all(ticks);
  assert.ok(results.every((r) => r.status === "COMMITTED"));

  const mm = (kernel as any).memoryManager as BoundedLRUMemoryManager;
  const session = await mm.getSession(sessionId);
  assert.equal(session.tickId, 50);
});

// ----------------------------------------------------------------------------
// Out-of-the-Box Persistent Storage (FileColdStoreDriver)
// ----------------------------------------------------------------------------
test("FileColdStoreDriver persists evicted sessions to disk and reloads them", async () => {
  const testDir = path.join(__dirname, "../scratch/test-cold-store-" + Date.now());
  const cold = new FileColdStoreDriver(testDir);
  const mm = new BoundedLRUMemoryManager(2, cold);

  const s1 = await mm.getSession("persistent-session");
  s1.nodes.set("price", { id: "price", value: 99.99, updatedAtTick: 1, accessCount: 1, dependents: [] });

  await mm.getSession("sess-2");
  await mm.getSession("sess-3");

  const rawFile = await cold.get("persistent-session");
  assert.ok(rawFile !== null, "FileColdStoreDriver should return non-null for evicted session");
  assert.match(rawFile, /99.99/);

  const reloaded = await mm.getSession("persistent-session");
  assert.equal(reloaded.nodes.get("price")?.value, 99.99);

  await fs.rm(testDir, { recursive: true, force: true });
});

// ----------------------------------------------------------------------------
// Write-Ahead Log (WAL) & Replay Capabilities
// ----------------------------------------------------------------------------
test("PersistentWALRecorder logs ticks to disk and replays graph state", async () => {
  const walPath = path.join(__dirname, `../scratch/test-wal-${Date.now()}/session.wal.jsonl`);
  const walRecorder = new PersistentWALRecorder(walPath);

  const mm = new BoundedLRUMemoryManager(5, new InMemoryColdStoreDriver());
  const kernel = new DSCKernel(mm, { walRecorder });

  const sessionId = "wal-session";

  await kernel.executeAtomicTick({ sessionId, opcode: "MUTATE", targetNode: "item_price", payload: 10 });
  await kernel.executeAtomicTick({ sessionId, opcode: "MUTATE", targetNode: "quantity", payload: 3 });
  await kernel.executeAtomicTick({ sessionId, opcode: "MUTATE", targetNode: "item_price", payload: 15 });

  const entries = await walRecorder.readEntries(sessionId);
  assert.equal(entries.length, 3);
  assert.equal(entries[2].stateDiff["item_price"], 15);

  const replayedSession = await kernel.replaySessionFromWAL(sessionId);
  assert.equal(replayedSession.tickId, 3);
  assert.equal(replayedSession.nodes.get("item_price")?.value, 15);
  assert.equal(replayedSession.nodes.get("quantity")?.value, 3);

  await fs.rm(path.dirname(walPath), { recursive: true, force: true });
});

// ----------------------------------------------------------------------------
// Real-Time Event Stream Emitter
// ----------------------------------------------------------------------------
test("EventStreamEmitter notifies subscribers on commit, rollback, and eviction", async () => {
  const eventEmitter = new EventStreamEmitter();
  const commits: any[] = [];
  const rollbacks: any[] = [];
  const evictions: any[] = [];

  eventEmitter.on("commit", (evt) => commits.push(evt));
  eventEmitter.on("rollback", (evt) => rollbacks.push(evt));
  eventEmitter.on("evict", (evt) => evictions.push(evt));

  const cold = new InMemoryColdStoreDriver();
  const mm = new BoundedLRUMemoryManager(2, cold, 3600, eventEmitter);
  const kernel = new DSCKernel(mm, { eventEmitter });

  const commitRes = await kernel.executeAtomicTick({
    sessionId: "evt-sess-1",
    opcode: "MUTATE",
    targetNode: "stat",
    payload: 100,
  });
  assert.equal(commitRes.status, "COMMITTED");
  assert.equal(commits.length, 1);
  assert.equal(commits[0].stateDiff["stat"], 100);

  const session = await mm.getSession("evt-sess-1");
  session.nodes.set("A", { id: "A", value: 1, updatedAtTick: 0, accessCount: 0, dependents: ["B"] });
  session.nodes.set("B", { id: "B", value: 2, updatedAtTick: 0, accessCount: 0, dependents: ["A"] });

  await kernel.executeAtomicTick({
    sessionId: "evt-sess-1",
    opcode: "MUTATE",
    targetNode: "A",
    payload: 5,
  });
  assert.equal(rollbacks.length, 1);
  assert.match(rollbacks[0].error, /cycle/i);

  await mm.getSession("evt-sess-2");
  await mm.getSession("evt-sess-3");
  assert.equal(evictions.length, 1);
  assert.equal(evictions[0].sessionId, "evt-sess-1");
});

// ----------------------------------------------------------------------------
// Automatic Formula Wireup (registerFormula) & GraphBuilder
// ----------------------------------------------------------------------------
test("registerFormula automatically wires forward-edge dependencies for downstream nodes", async () => {
  const kernel = makeKernel();
  const sessionId = "auto-wire-sess";

  await kernel.executeAtomicTick({ sessionId, opcode: "MUTATE", targetNode: "price", payload: 50 });
  await kernel.executeAtomicTick({ sessionId, opcode: "MUTATE", targetNode: "qty", payload: 2 });

  await kernel.registerFormula(sessionId, "subtotal", {
    type: "BINARY_OP",
    op: "MUL",
    left: refAST("price"),
    right: refAST("qty"),
  });

  const tickRes = await kernel.executeAtomicTick({
    sessionId,
    opcode: "MUTATE",
    targetNode: "price",
    payload: 60,
  });

  assert.equal(tickRes.status, "COMMITTED");
  assert.equal(tickRes.stateDiff["price"], 60);
  assert.equal(tickRes.stateDiff["subtotal"], 120);
});

test("GraphBuilder enables fluent chaining of node and formula definitions", async () => {
  const kernel = makeKernel();
  const builder = new GraphBuilder(kernel, "builder-sess");

  await builder.addNode("base_stat", 10);
  await builder.addFormula("buffed_stat", {
    type: "BINARY_OP",
    op: "ADD",
    left: refAST("base_stat"),
    right: { type: "LITERAL", value: 5 },
  });

  const tickRes = await kernel.executeAtomicTick({
    sessionId: "builder-sess",
    opcode: "MUTATE",
    targetNode: "base_stat",
    payload: 20,
  });

  assert.equal(tickRes.status, "COMMITTED");
  assert.equal(tickRes.stateDiff["buffed_stat"], 25);
});

// ----------------------------------------------------------------------------
// Extended AST Expressions (FUNC_CALL: MIN/MAX/ROUND & IF_THEN_ELSE)
// ----------------------------------------------------------------------------
test("MultiVarAxiomSandbox evaluates MIN, MAX, ROUND, and IF_THEN_ELSE formulas", async () => {
  const kernel = makeKernel();
  const sessionId = "math-sess";

  await kernel.executeAtomicTick({ sessionId, opcode: "MUTATE", targetNode: "score", payload: 75 });

  await kernel.registerFormula(sessionId, "final_grade", {
    type: "IF_THEN_ELSE",
    condition: {
      type: "BINARY_OP",
      op: "GTE",
      left: refAST("score"),
      right: { type: "LITERAL", value: 50 },
    },
    thenExpr: {
      type: "FUNC_CALL",
      func: "MIN",
      args: [
        { type: "BINARY_OP", op: "MUL", left: refAST("score"), right: { type: "LITERAL", value: 1.2 } },
        { type: "LITERAL", value: 100 },
      ],
    },
    elseExpr: { type: "LITERAL", value: 0 },
  });

  const res1 = await kernel.executeAtomicTick({ sessionId, opcode: "MUTATE", targetNode: "score", payload: 75 });
  assert.equal(res1.stateDiff["final_grade"], 90);

  const res2 = await kernel.executeAtomicTick({ sessionId, opcode: "MUTATE", targetNode: "score", payload: 95 });
  assert.equal(res2.stateDiff["final_grade"], 100);

  const res3 = await kernel.executeAtomicTick({ sessionId, opcode: "MUTATE", targetNode: "score", payload: 30 });
  assert.equal(res3.stateDiff["final_grade"], 0);
});

// ----------------------------------------------------------------------------
// Logical & Equality AST Operators (EQ, NEQ, AND, OR)
// ----------------------------------------------------------------------------
test("MultiVarAxiomSandbox evaluates EQ, NEQ, AND, and OR operators", async () => {
  const kernel = makeKernel();
  const sessionId = "logic-sess";

  await kernel.executeAtomicTick({ sessionId, opcode: "MUTATE", targetNode: "role", payload: "admin" });
  await kernel.executeAtomicTick({ sessionId, opcode: "MUTATE", targetNode: "active", payload: true });

  // Formula: is_authorized = (role == "admin") AND (active == true)
  await kernel.registerFormula(sessionId, "is_authorized", {
    type: "BINARY_OP",
    op: "AND",
    left: {
      type: "BINARY_OP",
      op: "EQ",
      left: refAST("role"),
      right: { type: "LITERAL", value: "admin" },
    },
    right: {
      type: "BINARY_OP",
      op: "EQ",
      left: refAST("active"),
      right: { type: "LITERAL", value: true },
    },
  });

  const res1 = await kernel.executeAtomicTick({ sessionId, opcode: "MUTATE", targetNode: "role", payload: "admin" });
  assert.equal(res1.stateDiff["is_authorized"], true);

  const res2 = await kernel.executeAtomicTick({ sessionId, opcode: "MUTATE", targetNode: "active", payload: false });
  assert.equal(res2.stateDiff["is_authorized"], false);
});

// ----------------------------------------------------------------------------
// WAL Compaction (compactWAL)
// ----------------------------------------------------------------------------
test("PersistentWALRecorder compacts log history into a single snapshot", async () => {
  const walPath = path.join(__dirname, `../scratch/test-compact-${Date.now()}/session.wal.jsonl`);
  const walRecorder = new PersistentWALRecorder(walPath);

  const mm = new BoundedLRUMemoryManager(5, new InMemoryColdStoreDriver());
  const kernel = new DSCKernel(mm, { walRecorder });

  const sessionId = "compact-sess";

  for (let i = 1; i <= 10; i++) {
    await kernel.executeAtomicTick({ sessionId, opcode: "MUTATE", targetNode: "val", payload: i });
  }

  const entriesBefore = await walRecorder.readEntries(sessionId);
  assert.equal(entriesBefore.length, 10);

  await walRecorder.compact(sessionId);

  const entriesAfter = await walRecorder.readEntries(sessionId);
  assert.equal(entriesAfter.length, 1);
  assert.equal(entriesAfter[0].stateDiff["val"], 10);

  const replayed = await kernel.replaySessionFromWAL(sessionId);
  assert.equal(replayed.nodes.get("val")?.value, 10);

  await fs.rm(path.dirname(walPath), { recursive: true, force: true });
});
