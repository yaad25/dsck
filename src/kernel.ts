/**
 * DSCK — Delta-State Computation Kernel
 * A small reactive dependency-graph engine: set a value on one node,
 * and every downstream node that depends on it (directly or transitively)
 * recomputes exactly once, in correct topological order — even for
 * "diamond" dependency shapes where two paths converge on one node.
 *
 * Example: item_price -> subtotal -> final_total
 *          item_price -------------> final_total
 * Naive re-evaluation recomputes final_total twice (once with a stale
 * subtotal). DSCK computes a scoped topological order first, so every
 * node runs once, after all of its own inputs have already settled.
 */

import * as fs from "fs/promises";
import * as fsSync from "fs";
import * as path from "path";

export type ScalarValue = number | string | boolean;
export type Opcode = "MUTATE" | "COMPILE_NODE" | "ROLLBACK";

export interface DeltaPacket {
  sessionId: string;
  opcode: Opcode;
  targetNode: string;
  payload: ScalarValue;
  /**
   * Optional caller-supplied confidence score in [0, 1] for this write
   * (e.g. from an upstream classifier or validation step). If provided
   * and below `confidenceThreshold` (default 0, i.e. disabled), the tick
   * is rejected with JIT_FALLBACK_REQUIRED instead of being applied.
   * Omit this field entirely if you have no such score — the kernel does
   * not invent one.
   */
  confidenceScore?: number;
}

// ============================================================================
// AST formulas: safe, deterministic, multi-variable + Math, Logical & Conditional
// ============================================================================
export type ASTExpression =
  | { type: "LITERAL"; value: number | string | boolean }
  | { type: "REF"; nodeId: string }
  | {
      type: "BINARY_OP";
      op: "ADD" | "SUB" | "MUL" | "DIV" | "GTE" | "LTE" | "EQ" | "NEQ" | "AND" | "OR";
      left: ASTExpression;
      right: ASTExpression;
    }
  | {
      type: "FUNC_CALL";
      func: "MIN" | "MAX" | "ABS" | "ROUND" | "FLOOR" | "CEIL";
      args: ASTExpression[];
    }
  | {
      type: "IF_THEN_ELSE";
      condition: ASTExpression;
      thenExpr: ASTExpression;
      elseExpr: ASTExpression;
    };

export class ASTUtils {
  /**
   * Recursively extracts all referenced node IDs from an AST expression.
   */
  public static extractRefNodeIds(expr: ASTExpression): string[] {
    const refs = new Set<string>();

    const traverse = (node: ASTExpression) => {
      if (node.type === "REF") {
        refs.add(node.nodeId);
      } else if (node.type === "BINARY_OP") {
        traverse(node.left);
        traverse(node.right);
      } else if (node.type === "FUNC_CALL") {
        for (const arg of node.args) traverse(arg);
      } else if (node.type === "IF_THEN_ELSE") {
        traverse(node.condition);
        traverse(node.thenExpr);
        traverse(node.elseExpr);
      }
    };

    traverse(expr);
    return Array.from(refs);
  }
}

export interface StateNode {
  id: string;
  value: ScalarValue;
  updatedAtTick: number;
  accessCount: number;
  dependents: string[]; // forward edges: thisNode -> dependentNode
  formulaAST?: ASTExpression;
}

export interface SessionEnvelope {
  sessionId: string;
  tickId: number;
  nodes: Map<string, StateNode>;
}

export interface UndoRecord {
  nodeId: string;
  existed: boolean;
  previousValue?: ScalarValue;
  previousUpdatedAtTick?: number;
  previousAccessCount?: number;
}

export interface FlightLogEntry {
  sessionId: string;
  tickId: number;
  timestamp: number;
  packet: DeltaPacket;
  undoStack: UndoRecord[];
  stateDiff: Record<string, ScalarValue>;
}

// ============================================================================
// FIFO promise mutex — one active tick per session, no polling
// ============================================================================
export class AsyncFIFOMutex {
  private queues = new Map<string, Array<() => void>>();

  public async acquire(sessionId: string): Promise<() => void> {
    if (!this.queues.has(sessionId)) {
      this.queues.set(sessionId, []);
      return () => this.release(sessionId);
    }
    return new Promise<() => void>((resolve) => {
      this.queues.get(sessionId)!.push(() => resolve(() => this.release(sessionId)));
    });
  }

  private release(sessionId: string): void {
    const queue = this.queues.get(sessionId);
    if (!queue || queue.length === 0) {
      this.queues.delete(sessionId);
      return;
    }
    const next = queue.shift()!;
    next();
  }
}

// ============================================================================
// Fixed-capacity ring buffer for the audit/flight log
// ============================================================================
export class FlightRecorderRingBuffer {
  private buffer: Array<FlightLogEntry | undefined>;
  private head = 0;
  private size = 0;

  constructor(private readonly capacity = 1024) {
    if (capacity <= 0) throw new Error("capacity must be > 0");
    this.buffer = new Array(capacity);
  }

  public push(entry: FlightLogEntry): void {
    this.buffer[this.head] = entry;
    this.head = (this.head + 1) % this.capacity;
    if (this.size < this.capacity) this.size++;
  }

  public findTick(sessionId: string, tickId: number): FlightLogEntry | undefined {
    for (let i = 0; i < this.size; i++) {
      const idx = (this.head - 1 - i + this.capacity) % this.capacity;
      const item = this.buffer[idx];
      if (item && item.sessionId === sessionId && item.tickId === tickId) return item;
    }
    return undefined;
  }

  public get length(): number {
    return this.size;
  }
}

// ============================================================================
// Out-of-the-box persistent Write-Ahead Log (WAL) recorder, Replay & Compaction
// ============================================================================
export class PersistentWALRecorder {
  private logFilePath: string;

  constructor(walFilePath: string) {
    this.logFilePath = walFilePath;
    const dir = path.dirname(walFilePath);
    if (!fsSync.existsSync(dir)) {
      fsSync.mkdirSync(dir, { recursive: true });
    }
  }

  public async append(entry: FlightLogEntry): Promise<void> {
    const line = JSON.stringify(entry) + "\n";
    await fs.appendFile(this.logFilePath, line, "utf-8");
  }

  public async readEntries(sessionId?: string): Promise<FlightLogEntry[]> {
    try {
      const content = await fs.readFile(this.logFilePath, "utf-8");
      const lines = content.split("\n").filter((l: string) => l.trim().length > 0);
      const entries: FlightLogEntry[] = lines.map((line: string) => JSON.parse(line));
      if (sessionId) {
        return entries.filter((e) => e.sessionId === sessionId);
      }
      return entries;
    } catch (err: any) {
      if (err.code === "ENOENT") return [];
      throw err;
    }
  }

  /**
   * Compacts the WAL file for a specified session by replacing all its historical
   * ticks with a single consolidated snapshot entry, saving disk space.
   */
  public async compact(sessionId: string): Promise<void> {
    try {
      const allEntries = await this.readEntries();
      const sessionEntries = allEntries.filter((e) => e.sessionId === sessionId);
      if (sessionEntries.length <= 1) return;

      const latestTick = sessionEntries[sessionEntries.length - 1];
      const consolidatedState: Record<string, ScalarValue> = {};

      for (const entry of sessionEntries) {
        if (entry.stateDiff) {
          Object.assign(consolidatedState, entry.stateDiff);
        }
      }

      const compactedEntry: FlightLogEntry = {
        sessionId,
        tickId: latestTick.tickId,
        timestamp: Date.now(),
        packet: {
          sessionId,
          opcode: "MUTATE",
          targetNode: "__WAL_COMPACTED_SNAPSHOT__",
          payload: 0,
        },
        undoStack: [],
        stateDiff: consolidatedState,
      };

      const remainingEntries = allEntries.filter((e) => e.sessionId !== sessionId);
      remainingEntries.push(compactedEntry);

      const content = remainingEntries.map((e) => JSON.stringify(e)).join("\n") + "\n";
      const tempPath = `${this.logFilePath}.${Date.now()}.tmp`;
      await fs.writeFile(tempPath, content, "utf-8");
      await fs.rename(tempPath, this.logFilePath);
    } catch (err: any) {
      if (err.code === "ENOENT") return;
      throw err;
    }
  }
}

// ============================================================================
// Zero-dependency Real-Time Event Stream Emitter
// ============================================================================
export type KernelEventType = "commit" | "rollback" | "jit_fallback" | "evict";

export interface KernelEventMap {
  commit: { sessionId: string; tickId: number; stateDiff: Record<string, ScalarValue>; packet: DeltaPacket };
  rollback: { sessionId: string; tickId: number; error?: string; packet: DeltaPacket };
  jit_fallback: { sessionId: string; packet: DeltaPacket };
  evict: { sessionId: string; tickId: number };
}

export type KernelEventListener<K extends KernelEventType> = (data: KernelEventMap[K]) => void;

export class EventStreamEmitter {
  private listeners = new Map<KernelEventType, Set<KernelEventListener<any>>>();

  public on<K extends KernelEventType>(event: K, listener: KernelEventListener<K>): () => void {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set());
    }
    this.listeners.get(event)!.add(listener);
    return () => this.off(event, listener);
  }

  public off<K extends KernelEventType>(event: K, listener: KernelEventListener<K>): void {
    const set = this.listeners.get(event);
    if (set) {
      set.delete(listener);
    }
  }

  public emit<K extends KernelEventType>(event: K, data: KernelEventMap[K]): void {
    const set = this.listeners.get(event);
    if (set) {
      for (const listener of set) {
        try {
          listener(data);
        } catch (err) {
          if (typeof console !== "undefined" && console.error) {
            console.error(`[EventStreamEmitter] Listener for '${event}' threw an error:`, err);
          }
        }
      }
    }
  }
}

// ============================================================================
// Pluggable cold-storage adapter + bounded LRU hot cache
// ============================================================================
export interface ExternalColdStoreDriver {
  get(sessionId: string): Promise<string | null>;
  setWithTTL(sessionId: string, payload: string, ttlSeconds: number): Promise<void>;
}

/** In-memory driver for tests/local dev. */
export class InMemoryColdStoreDriver implements ExternalColdStoreDriver {
  private store = new Map<string, string>();
  async get(sessionId: string): Promise<string | null> {
    return this.store.get(sessionId) ?? null;
  }
  async setWithTTL(sessionId: string, payload: string): Promise<void> {
    this.store.set(sessionId, payload);
  }
}

/**
 * Built-in production-ready File/Disk Cold Store Driver.
 * Stores evicted session envelopes safely on disk with atomic writes.
 */
export class FileColdStoreDriver implements ExternalColdStoreDriver {
  constructor(private readonly storageDir: string) {
    if (!fsSync.existsSync(storageDir)) {
      fsSync.mkdirSync(storageDir, { recursive: true });
    }
  }

  private getFilePath(sessionId: string): string {
    const safeName = sessionId.replace(/[^a-zA-Z0-9_-]/g, "_");
    return path.join(this.storageDir, `${safeName}.session.json`);
  }

  async get(sessionId: string): Promise<string | null> {
    try {
      const filePath = this.getFilePath(sessionId);
      return await fs.readFile(filePath, "utf-8");
    } catch (err: any) {
      if (err.code === "ENOENT") return null;
      throw err;
    }
  }

  async setWithTTL(sessionId: string, payload: string, _ttlSeconds = 3600): Promise<void> {
    const filePath = this.getFilePath(sessionId);
    const tempPath = `${filePath}.${Date.now()}.${Math.random().toString(36).slice(2)}.tmp`;
    await fs.writeFile(tempPath, payload, "utf-8");
    await fs.rename(tempPath, filePath);
  }

  async delete(sessionId: string): Promise<void> {
    try {
      await fs.unlink(this.getFilePath(sessionId));
    } catch (err: any) {
      if (err.code !== "ENOENT") throw err;
    }
  }
}

export class BoundedLRUMemoryManager {
  private hotRAM = new Map<string, SessionEnvelope>();

  constructor(
    private readonly maxHotSessions: number,
    private readonly coldDriver: ExternalColdStoreDriver,
    private readonly coldTTLSeconds = 3600,
    private readonly eventEmitter?: EventStreamEmitter
  ) {
    if (maxHotSessions <= 0) throw new Error("maxHotSessions must be > 0");
  }

  public get hotSessionCount(): number {
    return this.hotRAM.size;
  }

  public async getSession(sessionId: string): Promise<SessionEnvelope> {
    if (this.hotRAM.has(sessionId)) {
      const env = this.hotRAM.get(sessionId)!;
      this.hotRAM.delete(sessionId);
      this.hotRAM.set(sessionId, env); // refresh LRU position
      return env;
    }

    const raw = await this.coldDriver.get(sessionId);
    const env: SessionEnvelope = raw
      ? {
          sessionId,
          tickId: JSON.parse(raw).tickId,
          nodes: new Map<string, StateNode>(JSON.parse(raw).entries),
        }
      : { sessionId, tickId: 0, nodes: new Map<string, StateNode>() };

    await this.evictIfNeeded();
    this.hotRAM.set(sessionId, env);
    return env;
  }

  private async evictIfNeeded(): Promise<void> {
    while (this.hotRAM.size >= this.maxHotSessions) {
      const oldestKey = this.hotRAM.keys().next().value;
      if (oldestKey === undefined) break;
      const oldestEnv = this.hotRAM.get(oldestKey)!;
      const serialized = JSON.stringify({
        tickId: oldestEnv.tickId,
        entries: Array.from(oldestEnv.nodes.entries()),
      });
      await this.coldDriver.setWithTTL(oldestKey, serialized, this.coldTTLSeconds);
      this.hotRAM.delete(oldestKey);
      if (this.eventEmitter) {
        this.eventEmitter.emit("evict", { sessionId: oldestKey, tickId: oldestEnv.tickId });
      }
    }
  }
}

// ============================================================================
// Scoped cycle guard + topological ripple planner
// ============================================================================
export class ScopedTopologicalPlanner {
  /**
   * Traverses only the sub-graph reachable from startNodeId, checks for
   * cycles in the same pass, and returns dependents in an order where
   * every node appears after all of its own upstream dependencies.
   */
  public static planExecutionOrder(
    nodes: Map<string, StateNode>,
    startNodeId: string
  ): string[] {
    const visited = new Set<string>();
    const visiting = new Set<string>();
    const postOrder: string[] = [];

    const dfs = (currentId: string): void => {
      if (visiting.has(currentId)) {
        throw new Error(`Scoped DAG cycle detected involving node '${currentId}'`);
      }
      if (visited.has(currentId)) return;

      visiting.add(currentId);
      const node = nodes.get(currentId);
      if (node) {
        for (const depId of node.dependents) dfs(depId);
      }
      visiting.delete(currentId);
      visited.add(currentId);
      postOrder.push(currentId);
    };

    dfs(startNodeId);
    postOrder.reverse();
    return postOrder.slice(1); // exclude startNodeId itself
  }
}

// ============================================================================
// Safe multi-variable AST evaluator (no eval/new Function — no RCE surface)
// Extended with MIN, MAX, ABS, ROUND, FLOOR, CEIL, IF_THEN_ELSE, EQ, NEQ, AND, OR
// ============================================================================
export class MultiVarAxiomSandbox {
  public static evaluate(
    expr: ASTExpression,
    graph: Map<string, StateNode>,
    depth = 0
  ): ScalarValue {
    if (depth > 32) throw new Error("AST max depth exceeded");

    switch (expr.type) {
      case "LITERAL":
        return expr.value;
      case "REF": {
        const refNode = graph.get(expr.nodeId);
        if (!refNode) throw new Error(`Missing reference node '${expr.nodeId}'`);
        return refNode.value;
      }
      case "BINARY_OP": {
        const l = this.evaluate(expr.left, graph, depth + 1);
        const r = this.evaluate(expr.right, graph, depth + 1);
        switch (expr.op) {
          case "ADD":
            return Number((Number(l) + Number(r)).toFixed(6));
          case "SUB":
            return Number((Number(l) - Number(r)).toFixed(6));
          case "MUL":
            return Number((Number(l) * Number(r)).toFixed(6));
          case "DIV":
            if (Number(r) === 0) throw new Error("Division by zero in Axiom Sandbox");
            return Number((Number(l) / Number(r)).toFixed(6));
          case "GTE":
            return Number(l) >= Number(r);
          case "LTE":
            return Number(l) <= Number(r);
          case "EQ":
            return l === r;
          case "NEQ":
            return l !== r;
          case "AND":
            return Boolean(l) && Boolean(r);
          case "OR":
            return Boolean(l) || Boolean(r);
        }
      }
      case "FUNC_CALL": {
        const evaluatedArgs = expr.args.map((arg) => Number(this.evaluate(arg, graph, depth + 1)));
        switch (expr.func) {
          case "MIN":
            return Math.min(...evaluatedArgs);
          case "MAX":
            return Math.max(...evaluatedArgs);
          case "ABS":
            return Math.abs(evaluatedArgs[0] ?? 0);
          case "ROUND":
            return Math.round(evaluatedArgs[0] ?? 0);
          case "FLOOR":
            return Math.floor(evaluatedArgs[0] ?? 0);
          case "CEIL":
            return Math.ceil(evaluatedArgs[0] ?? 0);
        }
      }
      case "IF_THEN_ELSE": {
        const condVal = this.evaluate(expr.condition, graph, depth + 1);
        const isTrue = Boolean(condVal);
        return isTrue
          ? this.evaluate(expr.thenExpr, graph, depth + 1)
          : this.evaluate(expr.elseExpr, graph, depth + 1);
      }
    }
  }
}

// ============================================================================
// The kernel
// ============================================================================
export interface TickResult {
  status: "COMMITTED" | "JIT_FALLBACK_REQUIRED" | "ROLLED_BACK";
  sessionTickId: number;
  stateDiff: Record<string, ScalarValue>;
  error?: string;
}

export interface DSCKernelOptions {
  confidenceThreshold?: number;
  flightLogCapacity?: number;
  walRecorder?: PersistentWALRecorder;
  eventEmitter?: EventStreamEmitter;
}

export class DSCKernel {
  private mutex = new AsyncFIFOMutex();
  private flightRecorder: FlightRecorderRingBuffer;

  constructor(
    private readonly memoryManager: BoundedLRUMemoryManager,
    private readonly options: DSCKernelOptions = {}
  ) {
    this.flightRecorder = new FlightRecorderRingBuffer(options.flightLogCapacity ?? 1024);
  }

  public getFlightRecorder(): FlightRecorderRingBuffer {
    return this.flightRecorder;
  }

  public getEventEmitter(): EventStreamEmitter | undefined {
    return this.options.eventEmitter;
  }

  /**
   * Registers a formula AST on a target node and automatically inspects the AST
   * to wire forward-edge dependencies on all referenced upstream nodes.
   */
  public async registerFormula(
    sessionId: string,
    targetNodeId: string,
    formulaAST: ASTExpression
  ): Promise<SessionEnvelope> {
    const session = await this.memoryManager.getSession(sessionId);

    let targetNode = session.nodes.get(targetNodeId);
    if (!targetNode) {
      targetNode = {
        id: targetNodeId,
        value: 0,
        updatedAtTick: session.tickId,
        accessCount: 0,
        dependents: [],
      };
      session.nodes.set(targetNodeId, targetNode);
    }
    targetNode.formulaAST = formulaAST;

    const refNodeIds = ASTUtils.extractRefNodeIds(formulaAST);
    for (const refId of refNodeIds) {
      let refNode = session.nodes.get(refId);
      if (!refNode) {
        refNode = {
          id: refId,
          value: 0,
          updatedAtTick: session.tickId,
          accessCount: 0,
          dependents: [targetNodeId],
        };
        session.nodes.set(refId, refNode);
      } else {
        if (!refNode.dependents.includes(targetNodeId)) {
          refNode.dependents.push(targetNodeId);
        }
      }
    }

    return session;
  }

  public async executeAtomicTick(packet: DeltaPacket): Promise<TickResult> {
    const threshold = this.options.confidenceThreshold ?? 0;
    if (packet.confidenceScore !== undefined && packet.confidenceScore < threshold) {
      if (this.options.eventEmitter) {
        this.options.eventEmitter.emit("jit_fallback", { sessionId: packet.sessionId, packet });
      }
      return { status: "JIT_FALLBACK_REQUIRED", sessionTickId: -1, stateDiff: {} };
    }

    const releaseLock = await this.mutex.acquire(packet.sessionId);
    const session = await this.memoryManager.getSession(packet.sessionId);
    const undoStack: UndoRecord[] = [];
    const stateDiff: Record<string, ScalarValue> = {};

    try {
      session.tickId += 1;

      this.recordUndo(session.nodes, packet.targetNode, undoStack);

      const target = session.nodes.get(packet.targetNode) ?? {
        id: packet.targetNode,
        value: packet.payload,
        updatedAtTick: session.tickId,
        accessCount: 0,
        dependents: [],
      };
      target.value = packet.payload;
      target.updatedAtTick = session.tickId;
      target.accessCount += 1;
      session.nodes.set(packet.targetNode, target);
      stateDiff[packet.targetNode] = target.value;

      const executionOrder = ScopedTopologicalPlanner.planExecutionOrder(
        session.nodes,
        packet.targetNode
      );

      for (const depId of executionOrder) {
        const depNode = session.nodes.get(depId);
        if (depNode && depNode.formulaAST) {
          this.recordUndo(session.nodes, depId, undoStack);
          depNode.value = MultiVarAxiomSandbox.evaluate(depNode.formulaAST, session.nodes);
          depNode.updatedAtTick = session.tickId;
          depNode.accessCount += 1;
          stateDiff[depId] = depNode.value;
        }
      }

      const logEntry: FlightLogEntry = {
        sessionId: packet.sessionId,
        tickId: session.tickId,
        timestamp: Date.now(),
        packet,
        undoStack,
        stateDiff,
      };

      this.flightRecorder.push(logEntry);

      if (this.options.walRecorder) {
        await this.options.walRecorder.append(logEntry);
      }

      if (this.options.eventEmitter) {
        this.options.eventEmitter.emit("commit", {
          sessionId: packet.sessionId,
          tickId: session.tickId,
          stateDiff,
          packet,
        });
      }

      return { status: "COMMITTED", sessionTickId: session.tickId, stateDiff };
    } catch (err) {
      for (let i = undoStack.length - 1; i >= 0; i--) {
        const u = undoStack[i];
        if (!u.existed) {
          session.nodes.delete(u.nodeId);
        } else {
          const node = session.nodes.get(u.nodeId)!;
          node.value = u.previousValue!;
          node.updatedAtTick = u.previousUpdatedAtTick!;
          node.accessCount = u.previousAccessCount!;
        }
      }
      session.tickId -= 1;

      const errorMessage = (err as Error).message;

      if (this.options.eventEmitter) {
        this.options.eventEmitter.emit("rollback", {
          sessionId: packet.sessionId,
          tickId: session.tickId,
          error: errorMessage,
          packet,
        });
      }

      return {
        status: "ROLLED_BACK",
        sessionTickId: session.tickId,
        stateDiff: {},
        error: errorMessage,
      };
    } finally {
      releaseLock();
    }
  }

  public async replaySessionFromWAL(sessionId: string): Promise<SessionEnvelope> {
    if (!this.options.walRecorder) {
      throw new Error("Cannot replay from WAL: no walRecorder provided in kernel options");
    }
    const entries = await this.options.walRecorder.readEntries(sessionId);
    const session: SessionEnvelope = {
      sessionId,
      tickId: 0,
      nodes: new Map<string, StateNode>(),
    };

    for (const entry of entries) {
      if (entry.stateDiff) {
        session.tickId = entry.tickId;
        for (const [nodeId, value] of Object.entries(entry.stateDiff)) {
          const existing = session.nodes.get(nodeId);
          if (existing) {
            existing.value = value;
            existing.updatedAtTick = entry.tickId;
          } else {
            session.nodes.set(nodeId, {
              id: nodeId,
              value,
              updatedAtTick: entry.tickId,
              accessCount: 1,
              dependents: [],
            });
          }
        }
      }
    }
    return session;
  }

  private recordUndo(
    nodes: Map<string, StateNode>,
    nodeId: string,
    undoStack: UndoRecord[]
  ): void {
    const existing = nodes.get(nodeId);
    if (!existing) {
      undoStack.push({ nodeId, existed: false });
    } else {
      undoStack.push({
        nodeId,
        existed: true,
        previousValue: existing.value,
        previousUpdatedAtTick: existing.updatedAtTick,
        previousAccessCount: existing.accessCount,
      });
    }
  }
}

// ============================================================================
// Fluent GraphBuilder Chaining Helper
// ============================================================================
export class GraphBuilder {
  constructor(private readonly kernel: DSCKernel, private readonly sessionId: string) {}

  public async addNode(id: string, initialValue: ScalarValue): Promise<this> {
    await this.kernel.executeAtomicTick({
      sessionId: this.sessionId,
      opcode: "MUTATE",
      targetNode: id,
      payload: initialValue,
    });
    return this;
  }

  public async addFormula(id: string, formulaAST: ASTExpression): Promise<this> {
    await this.kernel.registerFormula(this.sessionId, id, formulaAST);
    return this;
  }
}
